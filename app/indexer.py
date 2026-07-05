# indexer.py — Indexation initiale avec ACL
# Mis à jour le 29/06/2026 — Tika 3.3.1.0 · Elasticsearch 9.4.2 · ACL POSIX

import os
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
TIKA_SERVERS = os.getenv("TIKA_SERVERS", "http://localhost:9998").split(",")
DOCS_FOLDER = os.getenv("DOCS_FOLDER", "/documents")

import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone

import tempfile
from tika import parser as tika_parser
from elasticsearch import Elasticsearch
from acl_extractor import extract_acl
from archive_extractor import (
    is_archive, safe_extract_archive, ArchiveExtractionError, max_depth
)
from filetype_config import is_allowed, get_enabled_extensions

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

# Les extensions/tailles autorisées sont maintenant configurables à chaud
# via filetype_config.py (Redis) — voir is_allowed() et get_enabled_extensions().
# SUPPORTED est conservé UNIQUEMENT pour compatibilité avec du code externe
# qui l'importerait encore ; préférer get_enabled_extensions() désormais.
SUPPORTED = {".doc", ".docx", ".ppt", ".pptx",
             ".xls", ".xlsx", ".txt", ".pdf", ".pst"}

# Archives dont le contenu est indexé (voir archive_extractor.py) :
# .zip, .tar, .tar.gz/.tgz, .tar.bz2/.tbz2, .tar.xz/.txz, .7z (si py7zr installé)


def create_index():
    mapping = {
        "mappings": {
            "properties": {
                "filename":    {"type": "keyword"},
                "filepath":    {"type": "keyword"},
                "extension":   {"type": "keyword"},
                "type":        {"type": "keyword"},
                "content":     {"type": "text", "analyzer": "french"},
                "title":       {"type": "text"},
                "author":      {"type": "keyword"},
                "size":        {"type": "long"},
                "date":        {"type": "date"},
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
    if not es.indices.exists(index="documents"):
        es.indices.create(index="documents", body=mapping)
        logging.info("Index 'documents' créé avec support ACL.")


def extract_text(filepath: str) -> tuple[str, dict]:
    import random
    server = random.choice(TIKA_SERVERS)
    parsed = tika_parser.from_file(filepath, serverEndpoint=server)
    return (parsed.get("content") or "").strip(), (parsed.get("metadata") or {})


def get_author(metadata: dict) -> str:
    return (
        metadata.get("office:author")
        or metadata.get("dc:creator")
        or metadata.get("meta:author")
        or ""
    )


def get_title(metadata: dict, fallback: str) -> str:
    return metadata.get("dc:title") or metadata.get("office:title") or fallback


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
                     extension: str, acl, size: int, doc_type: str = "document"):
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
    if es.exists(index="documents", id=doc_id):
        logging.info(f"  [SKIP] {identity}")
        return

    logging.info(f"  [INDEX] {identity}")
    content, metadata = extract_text(str(tika_path))

    doc = {
        "filename":   filename,
        "filepath":   identity,
        "extension":  extension,
        "type":       doc_type,
        "content":    content,
        "title":      get_title(metadata, Path(filename).stem),
        "author":     get_author(metadata),
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
    es.index(index="documents", id=doc_id, document=doc)


def _process_archive(archive_real_path: Path, identity_root: str, acl, depth: int = 0):
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
                    _process_archive(real_path, identity, acl, depth + 1)
                else:
                    logging.warning(
                        f"  [PROFONDEUR MAX] Archive imbriquée ignorée : {identity}"
                    )
                continue

            extension = real_path.suffix.lower()
            size = real_path.stat().st_size
            allowed, reason = is_allowed(extension, size)
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
                doc_type="archive_member",
            )


def index_archive(filepath: str):
    """Point d'entrée pour indexer le contenu d'une archive (.zip, .tar.*, .7z)."""
    path = Path(filepath)
    identity_root = str(path.resolve())
    acl = extract_acl(filepath)   # héritée par tous les membres de l'archive
    logging.info(f"📦 Ouverture archive : {path.name}")
    _process_archive(path, identity_root, acl, depth=0)
    logging.info(f"✅ Archive traitée : {path.name}")


def index_file(filepath: str):
    path = Path(filepath)
    if is_excluded(path.name):
        logging.debug(f"  [IGNORÉ] {path.name} (fichier temporaire)")
        return

    if is_archive(path):
        index_archive(filepath)
        return

    extension = path.suffix.lower()
    size = path.stat().st_size
    allowed, reason = is_allowed(extension, size)
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
        doc_type="document",
    )


def optimize_for_bulk():
    es.indices.put_settings(index="documents", settings={
        "index": {
            "refresh_interval":    "-1",
            "number_of_replicas":  "0",
            "translog.durability": "async",
        }
    })
    logging.info("⚡ Mode bulk activé.")


def restore_after_bulk():
    es.indices.put_settings(index="documents", settings={
        "index": {
            "refresh_interval":   "30s",
            "number_of_replicas": "1",
        }
    })
    es.indices.forcemerge(index="documents", max_num_segments=5)
    logging.info("✅ Index restauré.")


def index_folder(folder_path: str):
    create_index()
    optimize_for_bulk()
    count = 0
    for root, _, files in os.walk(folder_path):
        for filename in files:
            try:
                index_file(os.path.join(root, filename))
                count += 1
            except Exception as e:
                logging.error(f"  [ERREUR] {filename} : {e}")
    restore_after_bulk()
    logging.info(f"✅ {count} fichiers indexés.")


if __name__ == "__main__":
    index_folder(DOCS_FOLDER)
