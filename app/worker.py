# worker.py — Worker d'indexation avec ACL
# Mis à jour le 29/06/2026 — Tika 3.3.1.0 · ES 9.4.2 · ACL

import os
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
TIKA_SERVERS = os.getenv("TIKA_SERVERS", "http://localhost:9998").split(",")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
WORKER_BATCH_SIZE = int(os.getenv("WORKER_BATCH_SIZE", "200"))
DOCS_FOLDER = os.getenv("DOCS_FOLDER", "/documents")

import json
import time
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
from indexer import get_author, get_title, is_excluded, index_archive, ES_INDEX
from archive_extractor import is_archive
from filetype_config import is_allowed
from runtime_config import get_param

# WORKER_BATCH_SIZE (Kafka max_poll_records) reste fixé au démarrage :
# changer cette valeur nécessite de recréer le KafkaConsumer, donc un
# redémarrage du worker (./manage.sh set-config ne le couvre pas).
# En revanche le SEUIL DE FLUSH bulk() et l'intervalle de flush sont
# relus dynamiquement à chaque itération via runtime_config — voir
# la boucle de run_worker() ci-dessous.

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
        "_index":   ES_INDEX,
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


def _flush(buffer: list, errors_total: int) -> int:
    """Écrit le buffer courant dans ES et le vide. Retourne errors_total mis à jour."""
    if not buffer:
        return errors_total
    ok, errors = bulk(es, buffer, raise_on_error=False)
    errors_total += len(errors)
    logging.info(f"Lot flush\u00e9 : {ok} OK / {len(errors)} erreurs (buffer vid\u00e9)")
    buffer.clear()
    return errors_total


def run_worker(batch_size: int = BATCH):
    consumer = KafkaConsumer(
        "documents-to-index",
        bootstrap_servers=KAFKA_BOOT,
        group_id="indexer-workers",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        max_poll_records=batch_size,
        # Commit manuel : on ne marque un message comme consommé
        # qu'APRES l'avoir réellement écrit dans ES (bulk réussi).
        # Avec l'auto-commit (défaut), Kafka pouvait marquer des
        # messages comme traités alors qu'ils n'étaient encore que
        # dans le buffer en mémoire, jamais flushé vers ES.
        enable_auto_commit=False,
    )
    logging.info(
        f"Worker ACL démarré (max_poll_records={batch_size}, fixé au démarrage ; "
        f"seuil de flush et intervalle relus dynamiquement via runtime_config)."
    )
    buffer: list = []
    errors_total = 0
    last_flush = time.time()

    try:
        while True:
            # poll() avec timeout : contrairement à l'itérateur bloquant
            # "for message in consumer", ceci nous redonne la main
            # périodiquement même s'il n'arrive aucun message, ce qui
            # permet un flush basé sur le temps (voir plus bas) et évite
            # de rater les heartbeats du groupe de consumers pendant un
            # traitement Tika un peu long.
            records = consumer.poll(timeout_ms=2000, max_records=batch_size)

            for _tp, messages in records.items():
                for message in messages:
                    filepath  = message.value["filepath"]
                    extension = message.value.get("extension", "")

                    if is_excluded(Path(filepath).name):
                        logging.debug(f"[SKIP] Fichier temporaire ignoré : {filepath}")
                        continue

                    if is_archive(Path(filepath)):
                        # Traité directement (extraction + indexation
                        # immédiate de chaque membre, pas de mise en
                        # buffer bulk() ici) : le fichier archive est
                        # sur le volume partagé /documents, accessible
                        # depuis n'importe quel worker.
                        try:
                            index_archive(filepath)
                        except Exception as e:
                            logging.error(f"Erreur archive [{filepath}] : {e}")
                        continue

                    try:
                        size = Path(filepath).stat().st_size
                    except OSError:
                        continue
                    allowed, reason = is_allowed(extension, size)
                    if not allowed:
                        logging.info(f"[IGNORÉ] {filepath} — {reason}")
                        continue

                    doc_id = hashlib.md5(str(Path(filepath).resolve()).encode()).hexdigest()
                    if es.exists(index=ES_INDEX, id=doc_id):
                        logging.debug(f"[SKIP] Déjà indexé : {filepath}")
                        continue

                    try:
                        if extension == ".pst":
                            from pst_extractor import index_pst
                            index_pst(filepath)
                            continue
                        content, metadata = extract(filepath)
                        buffer.append(build_action(filepath, content, metadata, extension))
                    except Exception as e:
                        logging.error(f"Erreur [{filepath}] : {e}")

            now = time.time()
            # Seuil et intervalle de flush relus à chaque itération —
            # modifiables à chaud via ./manage.sh set-config, sans
            # redémarrer le worker (contrairement à max_poll_records,
            # qui reste fixé pour la durée de vie du KafkaConsumer).
            flush_threshold     = get_param("worker_batch_size")
            flush_interval_secs = get_param("worker_flush_interval")
            should_flush = buffer and (
                len(buffer) >= flush_threshold or (now - last_flush) >= flush_interval_secs
            )
            if should_flush:
                errors_total = _flush(buffer, errors_total)
                last_flush = now

            # Commit APRES flush réussi (ou s'il n'y avait rien à
            # flusher) — jamais avant, pour ne pas perdre de messages
            # en cas de crash entre la réception et l'écriture ES.
            if records:
                consumer.commit()

    finally:
        errors_total = _flush(buffer, errors_total)
        try:
            consumer.commit()
        except Exception:
            pass
        consumer.close()


if __name__ == "__main__":
    run_worker()
