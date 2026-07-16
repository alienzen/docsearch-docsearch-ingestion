# indexer.py — Indexation initiale avec ACL
# Mis à jour le 08/07/2026 — Tika 3.3.1.0 · Elasticsearch 9.4.2 · ACL POSIX · multi-source

import os
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
TIKA_SERVERS = os.getenv("TIKA_SERVERS", "http://localhost:9998").split(",")
# Même nom de variable et même défaut que docsearch-api/custom_keywords.py
# — voir apply_keyword_overrides() plus bas.
CUSTOM_KEYWORDS_INDEX = os.getenv("CUSTOM_KEYWORDS_INDEX", "custom_keywords")

import hashlib
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone

import tempfile
from tika import parser as tika_parser
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan as es_scan, bulk as es_bulk
from acl_extractor import extract_acl
from archive_extractor import (
    is_archive, archive_kind, safe_extract_archive, ArchiveExtractionError, max_depth
)
from filetype_config import is_allowed, get_enabled_extensions
from path_filter import is_path_allowed, matches_pattern
from file_sources_config import Source, get_source, DEFAULT_SOURCE_NAME, ES_SEARCH_ALIAS
from runtime_config import get_param

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

ES_HOST      = ES_HOST
TIKA_SERVERS = TIKA_SERVERS

es = Elasticsearch(
    ES_HOST,
    retry_on_timeout=True,
    max_retries=3,
    request_timeout=60,
)


def wait_for_es(client: Elasticsearch = es, timeout: int = 300, interval: int = 5) -> None:
    """Bloque jusqu'à ce qu'Elasticsearch réponde. À froid (mono-nœud,
    VM contrainte), ES peut mettre 60-90s avant d'accepter la moindre
    connexion — les 3 retries du client (~15s) ne suffisent pas à couvrir
    ce délai, et le premier appel ES d'un process qui démarre trop tôt
    lève sans ça une exception non rattrapée (boucle de redémarrage)."""
    start = time.time()
    while True:
        try:
            if client.ping():
                return
        except Exception:
            pass
        if time.time() - start > timeout:
            raise RuntimeError(f"Elasticsearch injoignable après {timeout}s — abandon.")
        logging.info("⏳ En attente d'Elasticsearch...")
        time.sleep(interval)

# Les extensions/tailles autorisées sont maintenant configurables à chaud
# via filetype_config.py (Redis) — voir is_allowed() et get_enabled_extensions().
# SUPPORTED est conservé UNIQUEMENT pour compatibilité avec du code externe
# qui l'importerait encore ; préférer get_enabled_extensions() désormais.
SUPPORTED = {".doc", ".docx", ".ppt", ".pptx",
             ".xls", ".xlsx", ".txt", ".pdf", ".pst"}

# Archives dont le contenu est indexé (voir archive_extractor.py) :
# .zip, .tar, .tar.gz/.tgz, .tar.bz2/.tbz2, .tar.xz/.txz, .7z (si py7zr installé)


def _resolve_source(source: Source | None) -> Source:
    """La plupart des fonctions de ce module acceptent `source=None` pour
    les points d'entrée CLI/tests mono-source : repli sur la source par
    défaut ('documents'), qui existe toujours (voir file_sources_config.py)."""
    return source if source is not None else get_source(DEFAULT_SOURCE_NAME)


def create_index(source: Source | None = None):
    source = _resolve_source(source)
    mapping = {
        "mappings": {
            "properties": {
                "filename":    {"type": "keyword"},
                "filepath":    {
                    "type": "keyword",
                    # Même principe que author.text : filepath reste en
                    # keyword (nécessaire pour is_path_allowed, purge_path
                    # et l'affichage exact), filepath.text devient
                    # cherchable en texte libre (ex: "rapport" trouve
                    # /documents/Finance/rapport_2023.pdf).
                    "fields": {"text": {"type": "text"}},
                },
                "extension":   {"type": "keyword"},
                "type":        {"type": "keyword"},
                # Nom de la source d'origine (file_sources_config.py) — permet
                # de filtrer/facetter une recherche fédérée sur plusieurs
                # index sans avoir à connaître les noms d'index bruts.
                "source":      {"type": "keyword"},
                "content":     {"type": "text", "analyzer": "french"},
                "title":       {"type": "text"},
                "author":      {
                    "type": "keyword",
                    # Sous-champ analysé — permet une recherche en texte
                    # libre partielle ("Dupont" trouve "Martin Dupont"),
                    # tout en gardant "author" en keyword pour le filtre
                    # exact utilisé par les facettes/chips de l'interface.
                    "fields": {"text": {"type": "text"}},
                },
                # Mots-clés du document (propriété "Keywords"/"Mots-clés"
                # des métadonnées Office/PDF, voir get_keywords()) — même
                # motif que author : keyword pour la facette/le filtre
                # exact, sous-champ .text pour la recherche libre partielle.
                "keywords":    {
                    "type": "keyword",
                    "fields": {"text": {"type": "text"}},
                },
                "size":        {"type": "long"},
                "date_created":  {"type": "date"},
                "date_modified": {"type": "date"},
                "indexed_at":  {"type": "date"},
                "doc_hash":    {"type": "keyword"},
                # ── Champs ACL ───────────────────────────
                "acl": {
                    "properties": {
                        "owner":       {"type": "keyword"},
                        "group":       {"type": "keyword"},
                        "users":       {"type": "keyword"},
                        "groups":      {"type": "keyword"},
                        "public":      {"type": "boolean"},
                        "permissions": {"type": "keyword"},
                    }
                },
                # ── Vecteur RAG (option) ─────────────────
                "content_vector": {
                    "type":       "dense_vector",
                    "dims":       1024,
                    "index":      True,
                    "similarity": "cosine",
                },
                # ── Champs PST ───────────────────────────
                "folder":          {"type": "keyword"},
                "folder_top":      {"type": "keyword"},
                "sender_email":    {"type": "keyword"},
                "has_attachments": {"type": "boolean"},
                "recipients": {
                    "type": "nested",
                    "properties": {
                        "name":  {"type": "text"},
                        "email": {"type": "keyword"},
                    }
                },
            }
        },
        "settings": {
            "number_of_shards":   3,
            "number_of_replicas": 1,
            "analysis": {
                "analyzer": {
                    "french": {
                        "tokenizer": "standard",
                        "filter": ["lowercase", "french_stop", "french_stemmer"]
                    }
                },
                "filter": {
                    "french_stop":    {"type": "stop",    "stopwords": "_french_"},
                    "french_stemmer": {"type": "stemmer", "language": "light_french"},
                }
            }
        }
    }
    if not es.indices.exists(index=source.es_index):
        es.indices.create(index=source.es_index, body=mapping)
        logging.info(f"Index '{source.es_index}' créé avec support ACL (source '{source.name}').")
    else:
        # Index déjà existant (installation en place) : on applique quand
        # même les `properties` du mapping via put_mapping — ES traite
        # l'ajout d'un champ absent comme une opération additive sans
        # danger (pas de réindexation, aucune donnée existante touchée),
        # et refuse silencieusement rien de plus qu'une incompatibilité de
        # type sur un champ déjà défini. Ça permet à create_index() de
        # rester auto-réparatrice pour tout nouveau champ ajouté ici sans
        # exiger de script de migration séparé : un simple
        # `./manage.sh init <source>` (qui appelle create_index() via
        # producer.py) suffit à propager un nouveau champ de mapping.
        es.indices.put_mapping(index=source.es_index, properties=mapping["mappings"]["properties"])

    # Rejoint l'alias fédéré — c'est ce qui permet à docsearch-api de
    # chercher sur toutes les sources sans connaître leurs noms d'index
    # à l'avance. `is_write_index` non précisé : cet alias n'est utilisé
    # qu'en lecture (recherche), jamais comme cible d'écriture.
    if not es.indices.exists_alias(name=ES_SEARCH_ALIAS, index=source.es_index):
        es.indices.put_alias(index=source.es_index, name=ES_SEARCH_ALIAS)


def _ocr_headers(ocr_enabled: bool) -> dict:
    """
    En-têtes Tika pour piloter l'OCR Tesseract (déjà embarqué dans
    l'image apache/tika:*-full, y compris le pack linguistique français —
    vérifié : `tesseract --list-langs` liste "fra" par défaut, aucune
    image custom nécessaire).

    ocr_enabled=False renvoie explicitement "no_ocr" plutôt que de ne
    passer aucun en-tête : vérifié empiriquement que Tika OCRise déjà un
    PDF scanné SANS aucun en-tête (comportement par défaut proche de
    "auto"), mais avec une langue Tesseract non française qui écorche les
    accents ("décrit" → "deécrit") — l'absence d'en-tête n'est donc PAS
    équivalente à "pas d'OCR", il faut le désactiver explicitement pour
    les sources qui n'en veulent pas (coût CPU).

    ocr_enabled=True applique la stratégie et la langue configurées
    globalement (runtime_config.py — un pack de langue par source
    n'aurait pas de sens, il est figé dans l'image Tika pour tout le
    cluster). "auto" (valeur par défaut de ocr_strategy) ne déclenche
    Tesseract que sur les pages sans texte extractible — mesuré à ~30ms
    de surcoût sur un PDF déjà textuel contre plusieurs secondes sur un
    PDF réellement scanné.
    """
    if not ocr_enabled:
        return {"X-Tika-PDFOcrStrategy": "no_ocr"}
    return {
        "X-Tika-PDFOcrStrategy": get_param("ocr_strategy"),
        "X-Tika-OCRLanguage":    get_param("ocr_languages"),
    }


def extract_text(filepath: str, ocr_enabled: bool = False) -> tuple[str, dict]:
    """
    Deux appels séparés à Tika plutôt qu'un seul service="all" :

    "all" interroge /rmeta, qui retourne les métadonnées de façon
    RÉCURSIVE — document principal ET chaque ressource interne du
    conteneur (ex: la vignette docProps/thumbnail.jpeg incluse dans
    la plupart des .docx). Le client tika-python fusionne ensuite tout
    dans un seul dict, et la vignette (traitée après le document
    principal dans le flux récursif) écrase le vrai titre par son
    propre nom de ressource — d'où un "title" valant littéralement
    "/docProps/thumbnail.jpeg" pour tous les .docx, bug déjà constaté
    et documenté : https://github.com/chrismattmann/tika-python/issues/62

    service="meta" interroge /meta (métadonnées du document PRINCIPAL
    uniquement, sans récursion dans les ressources internes) — élimine
    le problème à la racine. service="text" pour le contenu, via /tika.

    Coût : deux requêtes HTTP vers Tika au lieu d'une — accepté en
    échange de métadonnées fiables (le même bug de fusion récursive
    pouvait aussi corrompre author/date de la même façon).

    `ocr_enabled` (voir _ocr_headers()) n'est appliqué qu'à l'appel
    service="text" — l'appel service="meta" n'a rien à extraire
    visuellement, inutile de lui faire porter ces en-têtes.
    """
    import random
    server = random.choice(TIKA_SERVERS)
    content_result  = tika_parser.from_file(filepath, serverEndpoint=server, service="text", headers=_ocr_headers(ocr_enabled))
    metadata_result = tika_parser.from_file(filepath, serverEndpoint=server, service="meta")
    content  = (content_result.get("content") or "").strip()
    metadata = metadata_result.get("metadata") or {}
    return content, metadata


def get_author(metadata: dict) -> str:
    return (
        metadata.get("office:author")
        or metadata.get("dc:creator")
        or metadata.get("meta:author")
        or ""
    )


def get_title(metadata: dict, fallback: str) -> str:
    return metadata.get("dc:title") or metadata.get("office:title") or fallback


def get_keywords(metadata: dict) -> list[str]:
    """
    Mots-clés du document (propriété "Mots-clés"/"Keywords" des
    métadonnées Office/PDF). dc:subject est délibérément exclu : vérifié
    empiriquement contre Tika 3.3.1 que ce champ fusionne à la fois le
    Sujet ET les Mots-clés dans un même tableau, sans distinction —
    pdf:docinfo:keywords (PDF) et meta:keyword (Office OOXML docx/xlsx/
    pptx via cp:keywords, et legacy doc/xls/ppt via SummaryInformation)
    sont les seules sources fiables.
    """
    raw = _first_metadata_value(metadata, ("pdf:docinfo:keywords", "meta:keyword"))
    if not raw:
        return []
    return [kw.strip() for kw in re.split(r"[;,]", raw) if kw.strip()]


def apply_keyword_overrides(doc_id: str, keywords: list[str]) -> list[str]:
    """
    Réapplique par-dessus les mots-clés fraîchement extraits par Tika les
    ajouts/retraits qu'un utilisateur a faits à la main (docsearch-api/
    custom_keywords.py, POST/DELETE /document/{id}/keywords) — sans ça,
    toute réindexation (fichier modifié détecté par le watcher, ou
    ./manage.sh init relancé) écraserait silencieusement ces modifications,
    puisque get_keywords() ne connaît que les métadonnées du fichier et
    worker.py réécrit le document entier à chaque passage.

    Best-effort : aucune surcharge enregistrée (cas normal) ou ES/index
    injoignable ne doit jamais empêcher l'indexation elle-même — on
    retombe simplement sur les mots-clés Tika bruts.
    """
    try:
        entry = es.get(index=CUSTOM_KEYWORDS_INDEX, id=doc_id)["_source"]
    except Exception:
        return keywords
    removed = set(entry.get("removed", []))
    merged = [kw for kw in keywords if kw not in removed]
    for kw in entry.get("added", []):
        if kw not in merged:
            merged.append(kw)
    return merged


def _first_metadata_value(metadata: dict, keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = metadata.get(key)
        if value:
            return value[0] if isinstance(value, list) else value
    return None


def get_date_created(metadata: dict, fallback_path: Path | None = None) -> str | None:
    """
    Date de CRÉATION du document, à partir des métadonnées Tika.

    ⚠️ Repli sur le système de fichiers : sous Linux, il n'existe pas de
    date de création fiable au niveau de la plupart des systèmes de
    fichiers (contrairement à Windows/macOS) — st_ctime ne représente
    PAS la date de création mais la date de dernier changement de
    métadonnées (permissions, renommage...), ce qui la rendrait
    trompeuse. Le repli utilise donc st_mtime (comme get_date_modified)
    plutôt que d'inventer une date de création fictive — mieux vaut une
    valeur honnête (identique à la date de modification) qu'une fausse
    précision.
    """
    value = _first_metadata_value(metadata, ("dcterms:created", "Creation-Date", "meta:creation-date"))
    if value:
        return value

    if fallback_path is not None:
        try:
            return datetime.fromtimestamp(
                fallback_path.stat().st_mtime, tz=timezone.utc
            ).isoformat()
        except OSError:
            pass
    return None


def get_date_modified(metadata: dict, fallback_path: Path | None = None) -> str | None:
    """Date de DERNIÈRE MODIFICATION du document, à partir des métadonnées
    Tika, avec repli sur la date de modification du fichier sur le
    disque (st_mtime, fiable sur Linux) si absente des métadonnées."""
    value = _first_metadata_value(metadata, ("dcterms:modified", "Last-Modified", "meta:save-date"))
    if value:
        return value

    if fallback_path is not None:
        try:
            return datetime.fromtimestamp(
                fallback_path.stat().st_mtime, tz=timezone.utc
            ).isoformat()
        except OSError:
            pass
    return None


def compute_folder_fields(identity: str, source: Source | None = None) -> tuple[str, str]:
    """
    Retourne (folder, folder_top) à partir de l'identité d'un document :
    - folder     : chemin du dossier complet, relatif au dossier de la
                   source — utilisé pour un filtrage précis (un
                   sous-dossier exact ou toute son arborescence)
    - folder_top : premier segment seul — utilisé pour la facette
                   (nombre de valeurs distinctes raisonnable, même sur
                   un corpus de plusieurs millions de documents)

    Pour un membre d'archive ("archive.zip::membre"), c'est le dossier
    de L'ARCHIVE elle-même qui compte — cohérent avec path_filter.py
    et purge_path(), qui ne portent jamais sur le chemin interne d'un
    membre.
    """
    source = _resolve_source(source)
    archive_part = identity.split("::", 1)[0]
    docs_root = Path(source.folder).resolve()

    try:
        rel_dir = Path(archive_part).resolve().parent.relative_to(docs_root)
        folder = "" if str(rel_dir) == "." else str(rel_dir)
    except ValueError:
        folder = str(Path(archive_part).parent)

    folder_top = folder.split("/")[0] if folder else ""
    return folder, folder_top


def file_hash(identity: str) -> str:
    """
    Hash de l'identité du document — reproductible même après suppression
    du fichier. `identity` doit déjà être normalisée par l'appelant :
    - fichier normal   : str(Path(filepath).resolve())
    - membre d'archive : "archive_resolue::chemin/dans/larchive"
    (ne PAS appeler Path().resolve() ici : une identité d'archive n'est
    pas un chemin disque réel et resolve() y perdrait son sens).
    """
    return hashlib.md5(identity.encode()).hexdigest()


def is_excluded(filename: str) -> bool:
    """
    Fichiers à ignorer systématiquement : fichiers temporaires ou verrous
    créés par les suites bureautiques (Word/LibreOffice) lors de l'édition.
    ~$rapport.docx  → verrou Word
    ~rapport.tmp    → fichier temporaire
    .~lock.rapport# → verrou LibreOffice
    """
    return filename.startswith("~") or filename.startswith(".~")


def _index_document(tika_path: Path, identity: str, filename: str,
                     extension: str, acl, size: int, source: Source,
                     doc_type: str = "document"):
    """
    Extrait le contenu (Tika) et indexe un document dans ES.

    `identity` est la chaîne utilisée pour calculer doc_id et pour le
    champ `filepath` — c'est un vrai chemin disque pour un fichier
    normal, ou "archive.zip::dossier/fichier.pdf" pour un membre
    d'archive (qui n'a pas de chemin disque stable, le fichier n'existe
    que dans un dossier temporaire pendant l'extraction).
    `tika_path` est le chemin RÉEL sur disque à donner à Tika pour
    extraire le contenu (peut différer de `identity`).
    """
    doc_id = file_hash(identity)
    if es.exists(index=source.es_index, id=doc_id):
        logging.info(f"  [SKIP] {identity}")
        return

    logging.info(f"  [INDEX] {identity}")
    content, metadata = extract_text(str(tika_path), ocr_enabled=source.ocr_enabled)
    folder, folder_top = compute_folder_fields(identity, source)

    doc = {
        "filename":   filename,
        "filepath":   identity,
        "extension":  extension,
        "type":       doc_type,
        "source":     source.name,
        "content":    content,
        "title":      get_title(metadata, Path(filename).stem),
        "author":     get_author(metadata),
        "keywords":   apply_keyword_overrides(doc_id, get_keywords(metadata)),
        "date_created":  get_date_created(metadata, fallback_path=tika_path if Path(tika_path).exists() else None),
        "date_modified": get_date_modified(metadata, fallback_path=tika_path if Path(tika_path).exists() else None),
        "folder":     folder,
        "folder_top": folder_top,
        "size":       size,
        "indexed_at": datetime.now(timezone.utc).isoformat(),
        "doc_hash":   doc_id,
        "acl": {
            "owner":       acl.owner,
            "group":       acl.group,
            "users":       acl.users,
            "groups":      acl.groups,
            "public":      acl.public,
            "permissions": acl.permissions,
        },
    }
    es.index(index=source.es_index, id=doc_id, document=doc)


def _process_archive(archive_real_path: Path, identity_root: str, acl, source: Source, depth: int = 0):
    """
    Extrait une archive dans un dossier temporaire et indexe chaque
    membre supporté. Les archives imbriquées sont traitées récursivement
    jusqu'à ARCHIVE_MAX_DEPTH. Tous les membres héritent des ACL de
    l'archive parente (comme pour les emails d'un fichier PST).
    """
    with tempfile.TemporaryDirectory(prefix="docsearch_archive_") as tmp:
        tmp_path = Path(tmp)
        try:
            extracted = safe_extract_archive(archive_real_path, tmp_path)
        except ArchiveExtractionError as e:
            logging.warning(f"  [ARCHIVE IGNORÉE] {archive_real_path.name} : {e}")
            return

        for real_path, rel_member_path in extracted:
            if is_excluded(real_path.name):
                continue

            identity = f"{identity_root}::{rel_member_path}"

            if is_archive(real_path):
                if depth < max_depth():
                    logging.info(f"  [ARCHIVE IMBRIQUÉE] {identity}")
                    _process_archive(real_path, identity, acl, source, depth + 1)
                else:
                    logging.warning(
                        f"  [PROFONDEUR MAX] Archive imbriquée ignorée : {identity}"
                    )
                continue

            extension = real_path.suffix.lower()
            size = real_path.stat().st_size
            allowed, reason = is_allowed(extension, size, source.name)
            if not allowed:
                logging.info(f"  [IGNORÉ] {identity} — {reason}")
                continue

            _index_document(
                tika_path=real_path,
                identity=identity,
                filename=real_path.name,
                extension=extension,
                acl=acl,
                size=size,
                source=source,
                doc_type="archive_member",
            )


def index_archive(filepath: str, source: Source | None = None):
    """Point d'entrée pour indexer le contenu d'une archive (.zip, .tar.*, .7z)."""
    source = _resolve_source(source)
    path = Path(filepath)
    identity_root = str(path.resolve())
    acl = extract_acl(filepath)   # héritée par tous les membres de l'archive
    logging.info(f"📦 Ouverture archive : {path.name} (source '{source.name}')")
    _process_archive(path, identity_root, acl, source, depth=0)
    logging.info(f"✅ Archive traitée : {path.name}")


def relative_to_docs_folder(filepath: str, source: Source | None = None) -> str:
    """
    Calcule le chemin d'un fichier relatif au dossier de `source` — c'est
    ce chemin qui est comparé aux motifs d'inclusion/exclusion de
    path_filter.py (jamais un chemin absolu, pour que les motifs
    restent valables quel que soit le point de montage).
    Si le fichier est hors du dossier de la source (cas rare, usage
    direct du module hors pipeline normal), retourne le nom seul —
    aucun filtrage de chemin n'est alors possible mais ça n'empêche pas
    l'indexation.
    """
    source = _resolve_source(source)
    try:
        return str(Path(filepath).resolve().relative_to(Path(source.folder).resolve()))
    except ValueError:
        return Path(filepath).name


def index_file(filepath: str, source: Source | None = None):
    source = _resolve_source(source)
    path = Path(filepath)
    if is_excluded(path.name):
        logging.debug(f"  [IGNORÉ] {path.name} (fichier temporaire)")
        return

    rel_path = relative_to_docs_folder(filepath, source)
    allowed, reason = is_path_allowed(rel_path, source.name)
    if not allowed:
        logging.info(f"  [IGNORÉ] {path.name} — {reason}")
        return

    if is_archive(path):
        kind = archive_kind(path)
        size = path.stat().st_size
        allowed, reason = is_allowed(kind, size, source.name)
        if not allowed:
            logging.info(f"  [IGNORÉ] {path.name} — {reason}")
            return
        index_archive(filepath, source)
        return

    extension = path.suffix.lower()
    size = path.stat().st_size
    allowed, reason = is_allowed(extension, size, source.name)
    if not allowed:
        logging.info(f"  [IGNORÉ] {path.name} — {reason}")
        return

    identity = str(path.resolve())
    acl = extract_acl(filepath)
    _index_document(
        tika_path=path,
        identity=identity,
        filename=path.name,
        extension=path.suffix.lower(),
        acl=acl,
        size=path.stat().st_size,
        source=source,
        doc_type="document",
    )


def optimize_for_bulk(source: Source | None = None):
    source = _resolve_source(source)
    es.indices.put_settings(index=source.es_index, settings={
        "index": {
            "refresh_interval":    "-1",
            "number_of_replicas":  "0",
            "translog.durability": "async",
        }
    })
    logging.info(f"⚡ Mode bulk activé ({source.es_index}).")


def restore_after_bulk(source: Source | None = None):
    source = _resolve_source(source)
    es.indices.put_settings(index=source.es_index, settings={
        "index": {
            "refresh_interval":   "30s",
            "number_of_replicas": "1",
        }
    })
    es.indices.forcemerge(index=source.es_index, max_num_segments=5)
    logging.info(f"✅ Index restauré ({source.es_index}).")


def index_folder(folder_path: str, source: Source | None = None):
    source = _resolve_source(source)
    create_index(source)
    optimize_for_bulk(source)
    count = 0
    for root, _, files in os.walk(folder_path):
        for filename in files:
            try:
                index_file(os.path.join(root, filename), source)
                count += 1
            except Exception as e:
                logging.error(f"  [ERREUR] {filename} : {e}")
    restore_after_bulk(source)
    logging.info(f"✅ {count} fichiers indexés dans '{source.es_index}' (source '{source.name}').")


def _relative_candidates(filepath: str, source: Source) -> list[str]:
    """
    Calcule le chemin relatif au dossier de `source` à comparer à un
    motif de purge, à partir du champ `filepath` stocké dans un document
    ES.

    Pour un membre d'archive ("archive.zip::membre"), seul l'emplacement
    de L'ARCHIVE ELLE-MÊME est retourné — jamais le chemin interne du
    membre. C'est cohérent avec le reste du système : index_file() ne
    vérifie le filtre de chemin qu'une seule fois, pour l'archive dans
    son ensemble, avant d'en extraire les membres (voir _process_archive) ;
    le chemin interne d'un membre n'a jamais sa propre existence en tant
    que "chemin sur le disque" filtrable indépendamment.
    """
    docs_root = str(Path(source.folder).resolve())
    archive_part = filepath.split("::", 1)[0]

    try:
        rel = str(Path(archive_part).resolve().relative_to(docs_root))
    except ValueError:
        rel = archive_part

    return [rel]


def purge_path(pattern: str, source: Source | None = None, dry_run: bool = True) -> int:
    """
    Supprime de l'index tous les documents DÉJÀ INDEXÉS dont le chemin
    (relatif au dossier de `source`) correspond à `pattern` — même
    syntaxe glob que path_filter.py (exclude-path/include-path).

    Utile car exclude-path n'agit que sur les futurs passages
    (scan/watcher) : cette fonction nettoie l'EXISTANT.

    dry_run=True (défaut) : ne supprime rien, retourne seulement le
    nombre de documents qui correspondraient au motif — toujours
    utiliser ce mode en premier pour vérifier avant de purger pour de
    bon (l'opération réelle est irréversible sans réindexation).

    Utilise le scan/scroll ES (pas une simple recherche size=1000) pour
    rester correct même sur un index de plusieurs millions de documents.
    """
    source = _resolve_source(source)
    to_delete = []
    matched = 0

    for hit in es_scan(
        es, index=source.es_index,
        query={"query": {"match_all": {}}},
        _source=["filepath"],
    ):
        filepath = hit["_source"].get("filepath", "")
        if not filepath:
            continue

        candidates = _relative_candidates(filepath, source)
        if any(matches_pattern(c, pattern) for c in candidates):
            matched += 1
            if not dry_run:
                to_delete.append({
                    "_op_type": "delete",
                    "_index":   source.es_index,
                    "_id":      hit["_id"],
                })

    if to_delete:
        ok, errors = es_bulk(es, to_delete, raise_on_error=False)
        if errors:
            logging.error(f"[purge_path] {len(errors)} erreur(s) de suppression")
        es.indices.refresh(index=source.es_index)

    return matched


if __name__ == "__main__":
    default_source = get_source(DEFAULT_SOURCE_NAME)
    index_folder(default_source.folder, default_source)
