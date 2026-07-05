# watcher.py — Surveillance dossier avec mise à jour ACL
# Mis à jour le 29/06/2026 — ACL intégrées

import os
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
TIKA_SERVERS = os.getenv("TIKA_SERVERS", "http://localhost:9998").split(",")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
DOCS_FOLDER = os.getenv("DOCS_FOLDER", "/documents")

import time
import logging
import hashlib
from pathlib import Path
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler
from elasticsearch import Elasticsearch
from acl_extractor import extract_acl, FileACL
from indexer import index_file, SUPPORTED, is_excluded
from archive_extractor import is_archive

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Watcher] %(message)s",
    handlers=[
        logging.FileHandler("watcher.log"),
        logging.StreamHandler()
    ]
)

ES_HOST = ES_HOST
es = Elasticsearch(ES_HOST, retry_on_timeout=True, max_retries=3)


def file_hash(filepath: str) -> str:
    """Hash du chemin normalisé — identique à indexer.py et worker.py."""
    normalized = str(Path(filepath).resolve())
    return hashlib.md5(normalized.encode()).hexdigest()


def delete_from_index(filepath: str):
    """
    Supprime un document de l'index par son ID (hash du chemin normalisé).
    Plus fiable que delete_by_query : pas de problème de chemin relatif/absolu,
    et fonctionne même si le fichier est déjà supprimé du disque.

    Cas particulier des archives (.zip, .tar.*, .7z) : elles ne sont
    jamais indexées en tant que document unique — seuls leurs membres
    le sont, avec une identité "archive::chemin/interne". Une recherche
    par wildcard sur ce préfixe supprime donc tous les documents issus
    de l'archive supprimée.
    """
    normalized = str(Path(filepath).resolve())
    doc_id     = hashlib.md5(normalized.encode()).hexdigest()
    try:
        es.delete(index="documents", id=doc_id, refresh=True)
        logging.info(f"🗑️  Supprimé de l'index : {normalized}")
    except Exception as e:
        if "NotFoundError" in type(e).__name__ or "404" in str(e):
            logging.debug(f"Document seul introuvable (normal pour une archive) : {normalized}")
        else:
            logging.error(f"Erreur suppression ({normalized}) : {e}")

    if is_archive(Path(filepath)):
        try:
            res = es.delete_by_query(
                index="documents",
                query={"wildcard": {"filepath": f"{normalized}::*"}},
                refresh=True,
            )
            n = res.get("deleted", 0)
            logging.info(f"🗑️  {n} membre(s) d'archive supprimé(s) de l'index : {normalized}")
        except Exception as e:
            logging.error(f"Erreur suppression des membres d'archive ({normalized}) : {e}")


def update_acl_only(filepath: str):
    """
    Met à jour uniquement le champ acl sans relancer Tika.
    Utilisé quand seules les permissions du fichier ont changé.
    """
    try:
        doc_id = file_hash(filepath)
        acl    = extract_acl(filepath)
        es.update(
            index="documents",
            id=doc_id,
            doc={
                "acl": {
                    "owner":       acl.owner,
                    "group":       acl.group,
                    "users":       acl.users,
                    "groups":      acl.groups,
                    "public":      acl.public,
                    "permissions": acl.permissions,
                }
            }
        )
        logging.info(f"🔑 ACL mises à jour : {filepath} "
                     f"(owner={acl.owner}, groups={acl.groups})")
    except Exception as e:
        logging.error(f"Erreur update ACL ({filepath}) : {e}")


def get_indexed_acl(filepath: str) -> dict | None:
    """Récupère les ACL actuellement indexées pour un fichier."""
    try:
        doc_id = file_hash(filepath)
        res    = es.get(index="documents", id=doc_id, source=["acl"])
        return res["_source"].get("acl")
    except Exception:
        return None


def _copy_document(old_identity: str, new_identity: str, updated_acl=None) -> bool:
    """
    Copie un document déjà indexé vers une nouvelle identité (nouveau
    chemin), SANS relancer Tika. Utilisé pour les renommages/déplacements
    purs où le contenu du fichier n'a pas changé — l'extraction de
    contenu (opération coûteuse) est déjà en base, seul le chemin change.

    Retourne True si la copie a réussi (l'ancien document existait),
    False sinon (l'appelant doit alors déclencher une réindexation
    complète en repli).
    """
    old_id = hashlib.md5(old_identity.encode()).hexdigest()
    new_id = hashlib.md5(new_identity.encode()).hexdigest()
    try:
        old_doc = es.get(index="documents", id=old_id)["_source"]
    except Exception:
        return False

    new_doc = dict(old_doc)
    new_doc["filepath"] = new_identity
    if updated_acl is not None:
        new_doc["acl"] = {
            "owner":       updated_acl.owner,
            "group":       updated_acl.group,
            "users":       updated_acl.users,
            "groups":      updated_acl.groups,
            "public":      updated_acl.public,
            "permissions": updated_acl.permissions,
        }

    es.index(index="documents", id=new_id, document=new_doc)
    es.delete(index="documents", id=old_id, refresh=True)
    return True


def _rename_prefix(old_root: str, new_root: str) -> int:
    """
    Renomme en base TOUS les documents dont le filepath commence par
    old_root — utilisé quand un DOSSIER ENTIER est déplacé/renommé.
    Couvre aussi bien les fichiers normaux (filepath = chemin disque)
    que les membres d'archive (filepath = "archive.zip::membre").
    Aucune réextraction Tika : seul le champ filepath est réécrit.

    NB : pagination simple (size=1000), suffisante pour un dossier de
    taille raisonnable. Pour des dossiers de plusieurs milliers de
    fichiers, remplacer par une pagination search_after.
    """
    query = {
        "bool": {
            "should": [
                {"prefix": {"filepath": f"{old_root}/"}},
                {"prefix": {"filepath": f"{old_root}::"}},
                {"term":   {"filepath": old_root}},
            ],
            "minimum_should_match": 1,
        }
    }
    renamed = 0
    try:
        resp = es.search(index="documents", query=query, size=1000)
    except Exception as e:
        logging.error(f"Erreur recherche pour renommage de préfixe : {e}")
        return 0

    for hit in resp["hits"]["hits"]:
        old_id   = hit["_id"]
        doc      = dict(hit["_source"])
        old_path = doc["filepath"]
        new_path = new_root + old_path[len(old_root):]
        doc["filepath"] = new_path
        new_id = hashlib.md5(new_path.encode()).hexdigest()
        try:
            es.index(index="documents", id=new_id, document=doc)
            es.delete(index="documents", id=old_id)
            renamed += 1
        except Exception as e:
            logging.error(f"Erreur renommage ({old_path} -> {new_path}) : {e}")

    if renamed:
        es.indices.refresh(index="documents")
    return renamed


def _rename_archive_members(old_root: str, new_root: str, updated_acl=None) -> int:
    """Renomme tous les membres indexés d'une archive déplacée/renommée."""
    query = {"prefix": {"filepath": f"{old_root}::"}}
    renamed = 0
    try:
        resp = es.search(index="documents", query=query, size=1000)
    except Exception as e:
        logging.error(f"Erreur recherche membres d'archive : {e}")
        return 0

    for hit in resp["hits"]["hits"]:
        old_id = hit["_id"]
        doc    = dict(hit["_source"])
        suffix = doc["filepath"].split("::", 1)[1]
        new_identity = f"{new_root}::{suffix}"
        doc["filepath"] = new_identity
        if updated_acl is not None:
            doc["acl"] = {
                "owner":       updated_acl.owner,
                "group":       updated_acl.group,
                "users":       updated_acl.users,
                "groups":      updated_acl.groups,
                "public":      updated_acl.public,
                "permissions": updated_acl.permissions,
            }
        new_id = hashlib.md5(new_identity.encode()).hexdigest()
        try:
            es.index(index="documents", id=new_id, document=doc)
            es.delete(index="documents", id=old_id)
            renamed += 1
        except Exception as e:
            logging.error(f"Erreur renommage membre ({old_id}) : {e}")

    if renamed:
        es.indices.refresh(index="documents")
    return renamed


class DocumentHandler(FileSystemEventHandler):

    def _is_supported(self, path):
        p = Path(path)
        return p.suffix.lower() in SUPPORTED or is_archive(p)

    def _is_temp(self, path):
        # is_excluded (indexer.py) exclut tout fichier commençant par
        # "~" ou ".~" (verrous Word/LibreOffice). On garde en plus les
        # patterns spécifiques à d'autres éditeurs (# Emacs, .tmp).
        name = Path(path).name
        return is_excluded(name) or name.startswith("#") or name.endswith(".tmp")

    def on_created(self, event):
        if event.is_directory or not self._is_supported(event.src_path) or self._is_temp(event.src_path):
            return
        logging.info(f"📄 Nouveau fichier : {event.src_path}")
        self._safe_index(event.src_path)

    def on_modified(self, event):
        if event.is_directory or not self._is_supported(event.src_path) or self._is_temp(event.src_path):
            return
        logging.info(f"✏️  Fichier modifié : {event.src_path}")

        # Les archives ne sont jamais indexées comme document unique
        # (seuls leurs membres le sont) — pas de diff ACL possible sur
        # un doc qui n'existe pas : on supprime tous ses membres puis
        # on réextrait/réindexe systématiquement.
        if is_archive(Path(event.src_path)):
            delete_from_index(event.src_path)
            self._safe_index(event.src_path)
            return

        # Vérifier si seules les ACL ont changé
        old_acl = get_indexed_acl(event.src_path)
        new_acl = extract_acl(event.src_path)

        if old_acl and (
            old_acl.get("owner")  == new_acl.owner and
            old_acl.get("group")  == new_acl.group and
            set(old_acl.get("users",  [])) == set(new_acl.users) and
            set(old_acl.get("groups", [])) == set(new_acl.groups) and
            old_acl.get("public") == new_acl.public
        ):
            # Contenu potentiellement modifié, réindexation complète
            delete_from_index(event.src_path)
            self._safe_index(event.src_path)
        else:
            # Seules les ACL ont changé : mise à jour légère
            logging.info(f"🔑 Changement ACL détecté : {event.src_path}")
            update_acl_only(event.src_path)

    def on_deleted(self, event):
        if event.is_directory or not self._is_supported(event.src_path):
            return
        # Reconstituer le chemin absolu tel qu'il a été stocké à l'indexation
        # (str(Path(p).absolute()) dans indexer.py)
        abs_path = str(Path(event.src_path).absolute())
        logging.info(f"🗑️  Fichier supprimé : {abs_path}")
        delete_from_index(abs_path)

    def on_moved(self, event):
        src = str(Path(event.src_path).absolute())
        dst = str(Path(event.dest_path).absolute())

        if event.is_directory:
            # Renommage/déplacement d'un DOSSIER ENTIER : tous les
            # documents dont le filepath commence par l'ancien chemin
            # sont renommés directement en base (réécriture du seul
            # champ filepath), SANS relancer Tika sur chaque fichier.
            logging.info(f"📁 Dossier déplacé : {src} → {dst}")
            n = _rename_prefix(src, dst)
            logging.info(f"   {n} document(s) renommé(s) en base, sans réextraction")
            return

        if not self._is_supported(event.src_path):
            return

        logging.info(f"🔀 Déplacé : {src} → {dst}")

        if is_archive(Path(dst)):
            # Une archive n'est jamais indexée comme document unique,
            # seuls ses membres le sont ("archive::membre") — il faut
            # renommer leur préfixe individuellement.
            acl = extract_acl(dst)
            n = _rename_archive_members(src, dst, updated_acl=acl)
            logging.info(f"   {n} membre(s) d'archive renommé(s), sans réextraction")
            return

        # Renommage d'un fichier normal : copie légère du document déjà
        # indexé vers la nouvelle identité, sans relancer Tika. Les ACL
        # sont rafraîchies (opération rapide : stat + getfacl, pas de
        # comparaison avec Tika) au cas où le déplacement change aussi
        # les droits (changement de dossier parent).
        acl = extract_acl(dst)
        if _copy_document(src, dst, updated_acl=acl):
            logging.info(f"   ✅ Renommé sans réextraction Tika : {dst}")
        else:
            # Document introuvable sous l'ancienne identité (jamais
            # indexé, ou déjà renommé via l'événement dossier ci-dessus)
            # : repli sur une indexation complète par sécurité.
            logging.info(f"   Renommage léger impossible — indexation complète : {dst}")
            self._safe_index(dst)

    def _safe_index(self, filepath: str, retries: int = 3, delay: float = 2):
        for attempt in range(retries):
            try:
                path = Path(filepath)
                prev = -1
                while prev != path.stat().st_size:
                    prev = path.stat().st_size
                    time.sleep(0.5)
                index_file(filepath)
                return
            except Exception as e:
                logging.warning(f"Tentative {attempt+1}/{retries} ({filepath}) : {e}")
                time.sleep(delay)
        logging.error(f"❌ Impossible d'indexer : {filepath}")


def start_watcher(folder_path: str, recursive: bool = True):
    # PollingObserver est requis pour les partages réseau (CIFS, NFS, SMB)
    # car inotify ne reçoit pas les événements filesystem sur ces montages.
    # L'intervalle de polling est configurable via WATCHER_POLL_INTERVAL
    # (défaut 10 secondes — réduire si détection plus rapide souhaitée).
    poll_interval = int(os.getenv("WATCHER_POLL_INTERVAL", "10"))

    handler  = DocumentHandler()
    observer = PollingObserver(timeout=poll_interval)
    observer.schedule(handler, folder_path, recursive=recursive)
    observer.start()
    logging.info(
        f"👁️  Surveillance démarrée : {folder_path} "
        f"(mode polling toutes les {poll_interval}s — compatible CIFS/NFS)"
    )
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    start_watcher(DOCS_FOLDER)
