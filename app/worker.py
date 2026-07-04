# worker.py — Worker d'indexation avec ACL
# Mis à jour le 29/06/2026 — Tika 3.3.1.0 · ES 9.4.2 · ACL

import os
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
TIKA_SERVERS = os.getenv("TIKA_SERVERS", "http://localhost:9998").split(",")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
WORKER_BATCH_SIZE = int(os.getenv("WORKER_BATCH_SIZE", "200"))
DOCS_FOLDER = os.getenv("DOCS_FOLDER", "/documents")

import json
import random
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from tika import parser as tika_parser
from acl_extractor import extract_acl
from indexer import get_author, get_title, SUPPORTED, is_excluded, index_archive
from archive_extractor import is_archive

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Worker] %(message)s"
)

ES_HOST    = ES_HOST
TIKA_SVRS  = TIKA_SERVERS
KAFKA_BOOT = KAFKA_BOOTSTRAP
BATCH      = WORKER_BATCH_SIZE

es = Elasticsearch(ES_HOST, retry_on_timeout=True, max_retries=3, request_timeout=60)


def extract(filepath: str) -> tuple[str, dict]:
    server = random.choice(TIKA_SVRS)
    parsed = tika_parser.from_file(filepath, serverEndpoint=server)
    return (parsed.get("content") or "").strip(), (parsed.get("metadata") or {})


def build_action(filepath: str, content: str, metadata: dict, extension: str) -> dict:
    path   = Path(filepath)
    doc_id = hashlib.md5(str(Path(filepath).resolve()).encode()).hexdigest()

    # Extraction ACL
    acl = extract_acl(filepath)

    return {
        "_op_type": "index",
        "_index":   "documents",
        "_id":      doc_id,
        "_source": {
            "filename":   path.name,
            "filepath":   str(Path(filepath).resolve()),
            "extension":  extension,
            "type":       "document",
            "content":    content,
            "title":      get_title(metadata, path.stem),
            "author":     get_author(metadata),
            "size":       path.stat().st_size,
            "indexed_at": datetime.now(timezone.utc).isoformat(),
            "acl": {
                "owner":       acl.owner,
                "group":       acl.group,
                "users":       acl.users,
                "groups":      acl.groups,
                "public":      acl.public,
                "permissions": acl.permissions,
            },
        }
    }


def run_worker(batch_size: int = BATCH):
    consumer = KafkaConsumer(
        "documents-to-index",
        bootstrap_servers=KAFKA_BOOT,
        group_id="indexer-workers",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        max_poll_records=batch_size,
    )
    logging.info("Worker ACL démarré.")
    buffer, errors_total = [], 0

    for message in consumer:
        item, filepath, extension = message.value, message.value["filepath"], message.value.get("extension", "")
        if is_excluded(Path(filepath).name):
            logging.debug(f"[SKIP] Fichier temporaire ignoré : {filepath}")
            continue
        if is_archive(Path(filepath)):
            # Traité directement (extraction + indexation immédiate de
            # chaque membre, pas de mise en buffer bulk() ici) : le
            # fichier archive est sur le volume partagé /documents,
            # accessible depuis n'importe quel worker.
            try:
                index_archive(filepath)
            except Exception as e:
                logging.error(f"Erreur archive [{filepath}] : {e}")
            continue
        if extension not in SUPPORTED:
            continue

        doc_id = hashlib.md5(str(Path(filepath).resolve()).encode()).hexdigest()
        if es.exists(index="documents", id=doc_id):
            logging.debug(f"[SKIP] Déjà indexé : {filepath}")
            continue

        try:
            if extension == ".pst":
                from pst_extractor import index_pst
                index_pst(filepath)
                continue
            content, metadata = extract(filepath)
            buffer.append(build_action(filepath, content, metadata, extension))
            if len(buffer) >= batch_size:
                ok, errors = bulk(es, buffer, raise_on_error=False)
                errors_total += len(errors)
                logging.info(f"Lot : {ok} OK / {len(errors)} erreurs")
                buffer.clear()
        except Exception as e:
            logging.error(f"Erreur [{filepath}] : {e}")

    if buffer:
        bulk(es, buffer, raise_on_error=False)


if __name__ == "__main__":
    run_worker()
