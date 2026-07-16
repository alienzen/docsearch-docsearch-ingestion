# watcher.py — Surveillance multi-source avec mise à jour ACL
# Mis à jour le 08/07/2026 — multi-source (un observateur par source)

import os
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
TIKA_SERVERS = os.getenv("TIKA_SERVERS", "http://localhost:9998").split(",")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
# Même nom de variable et même défaut que docsearch-api/saved_collections.py
# — les deux services doivent s'accorder sur le nom de cet index pour que
# la réconciliation (voir _reconcile_collections) trouve la bonne cible.
SAVED_COLLECTIONS_INDEX = os.getenv("SAVED_COLLECTIONS_INDEX", "saved_collections")
# Idem pour les surcharges de mots-clés personnalisés — voir
# _migrate_custom_keyword_overrides() et docsearch-api/custom_keywords.py.
CUSTOM_KEYWORDS_INDEX = os.getenv("CUSTOM_KEYWORDS_INDEX", "custom_keywords")

import time
import logging
import hashlib
import json
from pathlib import Path
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler
from elasticsearch import Elasticsearch, NotFoundError
from acl_extractor import extract_acl, FileACL
from indexer import index_file, is_excluded, relative_to_docs_folder, create_index, wait_for_es
from archive_extractor import is_archive
from filetype_config import get_enabled_extensions
from runtime_config import get_param
from path_filter import is_path_allowed
from file_sources_config import Source, get_sources

# ── Battement de cœur (pour le panneau d'administration) ──────
# Écrit l'heure du dernier cycle de surveillance dans Redis, afin que
# docsearch-api puisse détecter un watcher figé/planté sans avoir
# besoin d'un accès Docker (voir /admin/status côté docsearch-api).
HEARTBEAT_KEY = "docsearch:heartbeat:watcher"

def _write_heartbeat():
    try:
        import redis
        client = redis.Redis(
            host=os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            socket_connect_timeout=2, socket_timeout=2,
        )
        client.set(HEARTBEAT_KEY, json.dumps({"ts": time.time()}), ex=120)
    except Exception as e:
        logging.debug(f"[heartbeat] Redis injoignable : {e}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Watcher] %(message)s",
    handlers=[
        logging.FileHandler("watcher.log"),
        logging.StreamHandler()
    ]
)

es = Elasticsearch(ES_HOST, retry_on_timeout=True, max_retries=3)


def file_hash(filepath: str) -> str:
    """Hash du chemin normalisé — identique à indexer.py et worker.py."""
    normalized = str(Path(filepath).resolve())
    return hashlib.md5(normalized.encode()).hexdigest()


def delete_from_index(filepath: str, source: Source):
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
        es.delete(index=source.es_index, id=doc_id, refresh=True)
        logging.info(f"🗑️  Supprimé de l'index '{source.es_index}' : {normalized}")
    except Exception as e:
        if "NotFoundError" in type(e).__name__ or "404" in str(e):
            logging.debug(f"Document seul introuvable (normal pour une archive) : {normalized}")
        else:
            logging.error(f"Erreur suppression ({normalized}) : {e}")

    if is_archive(Path(filepath)):
        try:
            res = es.delete_by_query(
                index=source.es_index,
                query={"wildcard": {"filepath": f"{normalized}::*"}},
                refresh=True,
            )
            n = res.get("deleted", 0)
            logging.info(f"🗑️  {n} membre(s) d'archive supprimé(s) de l'index : {normalized}")
        except Exception as e:
            logging.error(f"Erreur suppression des membres d'archive ({normalized}) : {e}")


def update_acl_only(filepath: str, source: Source):
    """
    Met à jour uniquement le champ acl sans relancer Tika.
    Utilisé quand seules les permissions du fichier ont changé.
    """
    try:
        doc_id = file_hash(filepath)
        acl    = extract_acl(filepath)
        es.update(
            index=source.es_index,
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


def get_indexed_acl(filepath: str, source: Source) -> dict | None:
    """Récupère les ACL actuellement indexées pour un fichier."""
    try:
        doc_id = file_hash(filepath)
        res    = es.get(index=source.es_index, id=doc_id, source=["acl"])
        return res["_source"].get("acl")
    except Exception:
        return None


def _copy_document(old_identity: str, new_identity: str, source: Source, updated_acl=None) -> bool:
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
        old_doc = es.get(index=source.es_index, id=old_id)["_source"]
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

    es.index(index=source.es_index, id=new_id, document=new_doc)
    es.delete(index=source.es_index, id=old_id, refresh=True)
    _reconcile_doc_id_references({old_id: new_id})
    return True


def _reconcile_collections(id_map: dict[str, str]):
    """
    Après un renommage/déplacement détecté comme tel (pas de réextraction
    Tika, mais un nouvel _id — voir file_hash), met à jour les collections
    utilisateur (docsearch-api/saved_collections.py) qui référencent
    l'ancien doc_id pour qu'elles pointent vers le nouveau, sans quoi
    l'entrée resterait affichée comme "Document indisponible" côté UI.

    Écrit directement dans l'index ES `saved_collections` plutôt que
    d'appeler docsearch-api en HTTP : les deux services partagent déjà de
    l'infra sans passer par l'API l'un de l'autre (voir filetype_config.py/
    runtime_config.py côté Redis) — ça évite d'ajouter une dépendance HTTP
    et un mécanisme d'authentification machine-à-machine à ce conteneur
    pour ce seul besoin.

    Best-effort et jamais bloquant : une collection est un confort
    utilisateur, pas la donnée de vérité du document (voir
    saved_collections.py) — un échec ici est loggé mais ne doit jamais
    faire échouer le renommage lui-même, déjà effectué à ce stade.

    Ne couvre QUE les renommages détectés comme tels par watchdog
    (on_moved) — le repli suppression+création (déplacement inter-source,
    outil qui copie puis supprime) ne passe jamais par cette fonction :
    à ce moment-là, aucun code ne connaît plus la correspondance entre
    l'ancien et le nouvel identifiant.
    """
    if not id_map:
        return
    try:
        resp = es.search(
            index=SAVED_COLLECTIONS_INDEX,
            query={"terms": {"doc_ids": list(id_map)}},
            # Même principe que _rename_prefix : pagination simple,
            # suffisante pour un volume de collections raisonnable.
            size=1000,
        )
    except NotFoundError:
        return  # aucune collection créée pour l'instant — rien à réconcilier
    except Exception as e:
        logging.warning(f"[reconcile] Recherche des collections impossible : {e}")
        return

    updated = 0
    for hit in resp["hits"]["hits"]:
        doc_ids = hit["_source"].get("doc_ids", [])
        new_doc_ids = [id_map.get(d, d) for d in doc_ids]
        if new_doc_ids == doc_ids:
            continue
        try:
            es.update(index=SAVED_COLLECTIONS_INDEX, id=hit["_id"], doc={"doc_ids": new_doc_ids})
            updated += 1
        except Exception as e:
            logging.warning(f"[reconcile] Mise à jour de la collection {hit['_id']} impossible : {e}")

    if updated:
        logging.info(f"🔗 {updated} collection(s) réconciliée(s) après renommage ({len(id_map)} document(s) déplacé(s))")


def _migrate_custom_keyword_overrides(id_map: dict[str, str]):
    """
    Même besoin que _reconcile_collections, pour l'index custom_keywords
    (docsearch-api/custom_keywords.py) — mais plus simple : un seul
    document par doc_id (pas une liste de références à parcourir), donc un
    simple déplacement par id plutôt qu'une recherche + réécriture.
    """
    if not id_map:
        return
    migrated = 0
    for old_id, new_id in id_map.items():
        try:
            entry = es.get(index=CUSTOM_KEYWORDS_INDEX, id=old_id)["_source"]
        except NotFoundError:
            continue
        except Exception as e:
            logging.warning(f"[reconcile] Lecture surcharge mots-clés impossible ({old_id}) : {e}")
            continue
        try:
            es.index(index=CUSTOM_KEYWORDS_INDEX, id=new_id, document=entry)
            es.delete(index=CUSTOM_KEYWORDS_INDEX, id=old_id)
            migrated += 1
        except Exception as e:
            logging.warning(f"[reconcile] Migration surcharge mots-clés impossible ({old_id} → {new_id}) : {e}")

    if migrated:
        logging.info(f"🏷️  {migrated} surcharge(s) de mots-clés migrée(s) après renommage")


def _reconcile_doc_id_references(id_map: dict[str, str]):
    """Point d'entrée unique appelé après un renommage détecté (voir
    _copy_document/_rename_prefix/_rename_archive_members) — regroupe
    toutes les données annexes qui référencent un doc_id devenu obsolète."""
    _reconcile_collections(id_map)
    _migrate_custom_keyword_overrides(id_map)


def _new_path_allowed(new_path: str, source: Source) -> bool:
    """
    Vérifie si le NOUVEAU chemin (après renommage de dossier) reste
    autorisé par les filtres d'inclusion/exclusion. Pour un membre
    d'archive ("archive.zip::membre"), c'est l'emplacement de
    l'archive elle-même qui compte (avant le "::").
    """
    archive_part = new_path.split("::", 1)[0]
    rel = relative_to_docs_folder(archive_part, source)
    allowed, _ = is_path_allowed(rel, source.name)
    return allowed


def _rename_prefix(old_root: str, new_root: str, source: Source) -> int:
    """
    Renomme en base TOUS les documents dont le filepath commence par
    old_root — utilisé quand un DOSSIER ENTIER est déplacé/renommé.
    Couvre aussi bien les fichiers normaux (filepath = chemin disque)
    que les membres d'archive (filepath = "archive.zip::membre").
    Aucune réextraction Tika : seul le champ filepath est réécrit.

    Si le NOUVEL emplacement est désormais exclu par un filtre de
    chemin (path_filter.py), le document est retiré de l'index plutôt
    que renommé — évite de laisser des documents indexés dans une
    zone qu'on vient d'exclure.

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
    renamed, removed = 0, 0
    id_map: dict[str, str] = {}
    try:
        resp = es.search(index=source.es_index, query=query, size=1000)
    except Exception as e:
        logging.error(f"Erreur recherche pour renommage de préfixe : {e}")
        return 0

    for hit in resp["hits"]["hits"]:
        old_id   = hit["_id"]
        doc      = dict(hit["_source"])
        old_path = doc["filepath"]
        new_path = new_root + old_path[len(old_root):]

        if not _new_path_allowed(new_path, source):
            try:
                es.delete(index=source.es_index, id=old_id)
                removed += 1
                logging.info(f"   Nouvel emplacement exclu — retiré de l'index : {new_path}")
            except Exception as e:
                logging.error(f"Erreur suppression ({old_path}) : {e}")
            continue

        doc["filepath"] = new_path
        new_id = hashlib.md5(new_path.encode()).hexdigest()
        try:
            es.index(index=source.es_index, id=new_id, document=doc)
            es.delete(index=source.es_index, id=old_id)
            renamed += 1
            id_map[old_id] = new_id
        except Exception as e:
            logging.error(f"Erreur renommage ({old_path} -> {new_path}) : {e}")

    if renamed or removed:
        es.indices.refresh(index=source.es_index)
    if removed:
        logging.info(f"   ({removed} document(s) retiré(s) car nouvel emplacement exclu)")
    _reconcile_doc_id_references(id_map)
    return renamed


def _rename_archive_members(old_root: str, new_root: str, source: Source, updated_acl=None) -> int:
    """Renomme tous les membres indexés d'une archive déplacée/renommée."""
    query = {"prefix": {"filepath": f"{old_root}::"}}
    renamed = 0
    id_map: dict[str, str] = {}
    try:
        resp = es.search(index=source.es_index, query=query, size=1000)
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
            es.index(index=source.es_index, id=new_id, document=doc)
            es.delete(index=source.es_index, id=old_id)
            renamed += 1
            id_map[old_id] = new_id
        except Exception as e:
            logging.error(f"Erreur renommage membre ({old_id}) : {e}")

    if renamed:
        es.indices.refresh(index=source.es_index)
    _reconcile_doc_id_references(id_map)
    return renamed


class DocumentHandler(FileSystemEventHandler):
    """
    Un DocumentHandler est lié à UNE SEULE source (voir file_sources_config.py)
    — chaque source surveillée a son propre observateur watchdog et sa
    propre instance de ce handler, pour que tous les appels ES/Kafka
    qu'il déclenche ciblent le bon index et le bon dossier de référence.
    """

    def __init__(self, source: Source):
        self.source = source

    def _is_supported(self, path):
        p = Path(path)
        # Pré-filtre rapide (extension seule) — le contrôle définitif
        # (extension + taille) est fait dans index_file() → is_allowed().
        # Séparé ainsi car ce pré-filtre sert aussi pour is_archive(),
        # qui n'a pas de notion de "taille max" au niveau du fichier
        # archive lui-même (ses membres sont vérifiés individuellement).
        return p.suffix.lower() in get_enabled_extensions(self.source.name) or is_archive(p)

    def _is_temp(self, path):
        # is_excluded (indexer.py) exclut tout fichier commençant par
        # "~" ou ".~" (verrous Word/LibreOffice). On garde en plus les
        # patterns spécifiques à d'autres éditeurs (# Emacs, .tmp).
        name = Path(path).name
        return is_excluded(name) or name.startswith("#") or name.endswith(".tmp")

    def _path_allowed(self, path) -> bool:
        """
        Pré-filtre inclusion/exclusion de sous-dossiers (path_filter.py).
        index_file() revérifie de toute façon en interne (défense en
        profondeur) — ce pré-filtre évite surtout de lancer inutilement
        la boucle d'attente de stabilisation de _safe_index() pour un
        fichier qu'on sait déjà destiné à être rejeté.
        """
        rel_path = relative_to_docs_folder(path, self.source)
        allowed, reason = is_path_allowed(rel_path, self.source.name)
        if not allowed:
            logging.debug(f"[IGNORÉ] {path} — {reason}")
        return allowed

    def on_created(self, event):
        if event.is_directory or not self._is_supported(event.src_path) or self._is_temp(event.src_path):
            return
        if not self._path_allowed(event.src_path):
            return
        logging.info(f"📄 Nouveau fichier [{self.source.name}] : {event.src_path}")
        self._safe_index(event.src_path)

    def on_modified(self, event):
        if event.is_directory or not self._is_supported(event.src_path) or self._is_temp(event.src_path):
            return
        if not self._path_allowed(event.src_path):
            # Le fichier est dans un dossier désormais exclu — s'il avait
            # été indexé avant l'ajout du filtre, le retirer proprement.
            delete_from_index(event.src_path, self.source)
            return
        logging.info(f"✏️  Fichier modifié [{self.source.name}] : {event.src_path}")

        # Les archives ne sont jamais indexées comme document unique
        # (seuls leurs membres le sont) — pas de diff ACL possible sur
        # un doc qui n'existe pas : on supprime tous ses membres puis
        # on réextrait/réindexe systématiquement.
        if is_archive(Path(event.src_path)):
            delete_from_index(event.src_path, self.source)
            self._safe_index(event.src_path)
            return

        # Vérifier si seules les ACL ont changé
        old_acl = get_indexed_acl(event.src_path, self.source)
        new_acl = extract_acl(event.src_path)

        if old_acl and (
            old_acl.get("owner")  == new_acl.owner and
            old_acl.get("group")  == new_acl.group and
            set(old_acl.get("users",  [])) == set(new_acl.users) and
            set(old_acl.get("groups", [])) == set(new_acl.groups) and
            old_acl.get("public") == new_acl.public
        ):
            # Contenu potentiellement modifié, réindexation complète
            delete_from_index(event.src_path, self.source)
            self._safe_index(event.src_path)
        else:
            # Seules les ACL ont changé : mise à jour légère
            logging.info(f"🔑 Changement ACL détecté : {event.src_path}")
            update_acl_only(event.src_path, self.source)

    def on_deleted(self, event):
        if event.is_directory or not self._is_supported(event.src_path):
            return
        # Reconstituer le chemin absolu tel qu'il a été stocké à l'indexation
        # (str(Path(p).absolute()) dans indexer.py)
        abs_path = str(Path(event.src_path).absolute())
        logging.info(f"🗑️  Fichier supprimé [{self.source.name}] : {abs_path}")
        delete_from_index(abs_path, self.source)

    def on_moved(self, event):
        src = str(Path(event.src_path).absolute())
        dst = str(Path(event.dest_path).absolute())

        if event.is_directory:
            # Renommage/déplacement d'un DOSSIER ENTIER : tous les
            # documents dont le filepath commence par l'ancien chemin
            # sont renommés directement en base (réécriture du seul
            # champ filepath), SANS relancer Tika sur chaque fichier.
            logging.info(f"📁 Dossier déplacé [{self.source.name}] : {src} → {dst}")
            n = _rename_prefix(src, dst, self.source)
            logging.info(f"   {n} document(s) renommé(s) en base, sans réextraction")
            return

        if not self._is_supported(event.src_path):
            return

        logging.info(f"🔀 Déplacé [{self.source.name}] : {src} → {dst}")

        if not self._path_allowed(dst):
            # Déplacé VERS un emplacement désormais exclu : retirer de
            # l'index plutôt que de renommer (le fichier existe toujours
            # sur le disque, mais ne doit plus être indexé à cet endroit).
            delete_from_index(src, self.source)
            logging.info(f"   Déplacé vers un emplacement exclu — retiré de l'index : {dst}")
            return

        if is_archive(Path(dst)):
            # Une archive n'est jamais indexée comme document unique,
            # seuls ses membres le sont ("archive::membre") — il faut
            # renommer leur préfixe individuellement.
            acl = extract_acl(dst)
            n = _rename_archive_members(src, dst, self.source, updated_acl=acl)
            logging.info(f"   {n} membre(s) d'archive renommé(s), sans réextraction")
            return

        # Renommage d'un fichier normal : copie légère du document déjà
        # indexé vers la nouvelle identité, sans relancer Tika. Les ACL
        # sont rafraîchies (opération rapide : stat + getfacl, pas de
        # comparaison avec Tika) au cas où le déplacement change aussi
        # les droits (changement de dossier parent).
        acl = extract_acl(dst)
        if _copy_document(src, dst, self.source, updated_acl=acl):
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
                index_file(filepath, self.source)
                return
            except Exception as e:
                logging.warning(f"Tentative {attempt+1}/{retries} ({filepath}) : {e}")
                time.sleep(delay)
        logging.error(f"❌ Impossible d'indexer : {filepath}")


def start_watcher():
    """
    Surveille TOUTES les sources enregistrées (file_sources_config.py)
    simultanément — un PollingObserver + un DocumentHandler par source,
    démarré/arrêté dynamiquement à chaque itération de la boucle
    principale en fonction du registre courant. C'est ce qui permet
    d'ajouter (ou de retirer) une source à chaud, sans redémarrer ce
    conteneur : ./manage.sh add-file-source suffit, ce process la détecte
    dans les ~5s qui suivent.

    PollingObserver est requis pour les partages réseau (CIFS, NFS, SMB)
    car inotify ne reçoit pas les événements filesystem sur ces montages.
    L'intervalle de polling (watcher_poll_interval) est modifiable à
    chaud via ./manage.sh set-config — le changer nécessite de recréer
    chaque observateur (le timeout est fixé à sa construction), ce que
    la boucle ci-dessous fait automatiquement dès qu'elle détecte un
    changement.
    """
    wait_for_es(es)

    # {source_name: {"observer": PollingObserver, "handler": DocumentHandler, "folder": str}}
    active: dict[str, dict] = {}
    current_interval = get_param("watcher_poll_interval")

    def _start_observer(source: Source, interval: int) -> tuple[PollingObserver, "DocumentHandler"]:
        handler = DocumentHandler(source)
        obs = PollingObserver(timeout=interval)
        obs.schedule(handler, source.folder, recursive=True)
        obs.start()
        return obs, handler

    def _stop_observer(name: str):
        entry = active.pop(name, None)
        if entry is None:
            return
        entry["observer"].stop()
        entry["observer"].join()

    def _sync_sources(interval: int):
        """Démarre/arrête les observateurs pour coller au registre
        courant. Redémarre aussi un observateur dont le dossier a
        changé (rare : équivaut à changer subfolder via add-file-source sur
        une source déjà active).

        Un observateur déjà actif garde le MÊME DocumentHandler tant que
        son dossier ne change pas — sans la mise à jour ci-dessous, son
        `.source` (searchable/ocr_enabled/label/...) resterait figé à
        l'instantané capturé lors du démarrage de l'observateur, et une
        bascule OCR (ou tout autre réglage) faite depuis l'admin après coup
        serait silencieusement ignorée pour cette source tant que ce
        conteneur watcher n'est pas redémarré. Source étant un
        @dataclass(frozen=True), `!=` compare bien tous les champs — on
        rafraîchit juste la référence tenue par le handler, sans toucher à
        l'observateur ni perdre d'événements en file."""
        sources = get_sources()

        for name in list(active):
            if name not in sources or active[name]["folder"] != sources[name].folder:
                logging.info(f"🛑 Source retirée ou modifiée — arrêt de la surveillance : {name}")
                _stop_observer(name)
            elif active[name]["handler"].source != sources[name]:
                logging.info(f"🔄 Configuration mise à jour pour la source '{name}' (OCR/label/…)")
                active[name]["handler"].source = sources[name]

        for name, source in sources.items():
            if name not in active:
                if not Path(source.folder).is_dir():
                    logging.warning(
                        f"⏭️  Source '{name}' enregistrée mais dossier introuvable "
                        f"({source.folder}) — surveillance différée jusqu'à sa création."
                    )
                    continue
                # Garantit le mapping ES explicite + l'alias fédéré AVANT
                # que l'observateur ne puisse déclencher le moindre
                # événement — jusqu'ici, seul producer.py (donc
                # ./manage.sh init) appelait create_index() ; un fichier
                # déposé dans le dossier d'une source jamais "init"
                # laissait Elasticsearch auto-créer l'index à la première
                # écriture (mapping dynamique, sans alias), invisible à la
                # recherche fédérée sans aucune erreur visible. Idempotent
                # (create ou put_mapping selon l'existant) : sans danger à
                # rappeler ici même si l'index existe déjà.
                create_index(source)
                obs, handler = _start_observer(source, interval)
                active[name] = {
                    "observer": obs,
                    "handler":  handler,
                    "folder":   source.folder,
                }
                logging.info(
                    f"👁️  Surveillance démarrée : {source.folder} (source '{name}' → "
                    f"index '{source.es_index}', polling toutes les {interval}s)"
                )

    _sync_sources(current_interval)
    try:
        while True:
            time.sleep(5)
            _write_heartbeat()

            new_interval = get_param("watcher_poll_interval")
            if new_interval != current_interval:
                logging.info(
                    f"🔄 Intervalle de polling modifié : {current_interval}s → "
                    f"{new_interval}s — redémarrage de tous les observateurs."
                )
                for name in list(active):
                    _stop_observer(name)
                current_interval = new_interval

            _sync_sources(current_interval)
    except KeyboardInterrupt:
        for name in list(active):
            _stop_observer(name)


if __name__ == "__main__":
    start_watcher()
