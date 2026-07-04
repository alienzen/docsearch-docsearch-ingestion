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

from tika import parser as tika_parser
from elasticsearch import Elasticsearch
from acl_extractor import extract_acl

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

SUPPORTED = {".doc", ".docx", ".ppt", ".pptx",
             ".xls", ".xlsx", ".txt", ".pdf", ".pst"}


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


def file_hash(filepath: str) -> str:
    """Hash du chemin normalisé — reproductible même après suppression."""
    normalized = str(Path(filepath).resolve())
    return hashlib.md5(normalized.encode()).hexdigest()


def index_file(filepath: str):
    path = Path(filepath)
    if path.suffix.lower() not in SUPPORTED:
        return

    doc_id = file_hash(filepath)
    if es.exists(index="documents", id=doc_id):
        logging.info(f"  [SKIP] {path.name}")
        return

    logging.info(f"  [INDEX] {path.name}")
    content, metadata = extract_text(filepath)

    # ── Extraction ACL ────────────────────────────
    acl = extract_acl(filepath)

    doc = {
        "filename":   path.name,
        "filepath":   str(Path(filepath).resolve()),
        "extension":  path.suffix.lower(),
        "type":       "document",
        "content":    content,
        "title":      get_title(metadata, path.stem),
        "author":     get_author(metadata),
        "size":       path.stat().st_size,
        "indexed_at": datetime.now(timezone.utc).isoformat(),
        "doc_hash":   doc_id,
        # ── ACL indexées ─────────────────────────
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
