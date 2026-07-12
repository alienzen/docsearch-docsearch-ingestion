# worker.py — Worker d'indexation avec ACL
# Mis à jour le 08/07/2026 — Tika 3.3.1.0 · ES 9.4.2 · ACL · multi-source

import os
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
TIKA_SERVERS = os.getenv("TIKA_SERVERS", "http://localhost:9998").split(",")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
WORKER_BATCH_SIZE = int(os.getenv("WORKER_BATCH_SIZE", "200"))

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
from indexer import get_author, get_title, get_keywords, is_excluded, index_archive, get_date_created, get_date_modified, compute_folder_fields
from archive_extractor import is_archive, archive_kind
from filetype_config import is_allowed
from runtime_config import get_param
from file_sources_config import Source, get_source, DEFAULT_SOURCE_NAME

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
    """Voir indexer.py:extract_text() pour l'explication complète —
    deux appels séparés (service="text" / service="meta") pour éviter
    que les métadonnées des ressources internes (ex: vignette d'un
    .docx) n'écrasent celles du document principal."""
    server = random.choice(TIKA_SVRS)
    content_result  = tika_parser.from_file(filepath, serverEndpoint=server, service="text")
    metadata_result = tika_parser.from_file(filepath, serverEndpoint=server, service="meta")
    content  = (content_result.get("content") or "").strip()
    metadata = metadata_result.get("metadata") or {}
    return content, metadata


def build_action(filepath: str, content: str, metadata: dict, extension: str, source: Source) -> dict:
    path     = Path(filepath)
    identity = str(Path(filepath).resolve())
    doc_id   = hashlib.md5(identity.encode()).hexdigest()

    # Extraction ACL
    acl = extract_acl(filepath)

    # date_created/date_modified/folder/folder_top : mêmes fonctions que
    # indexer.py (_index_document) — ce fichier construit son propre
    # document ES séparément (bulk via le pipeline producer/workers) et
    # ces champs y avaient été oubliés lors de leur ajout initial.
    folder, folder_top = compute_folder_fields(identity, source)

    return {
        "_op_type": "index",
        "_index":   source.es_index,
        "_id":      doc_id,
        "_source": {
            "filename":       path.name,
            "filepath":       identity,
            "extension":      extension,
            "type":           "document",
            "source":         source.name,
            "content":        content,
            "title":          get_title(metadata, path.stem),
            "author":         get_author(metadata),
            "keywords":       get_keywords(metadata),
            "date_created":   get_date_created(metadata, fallback_path=path if path.exists() else None),
            "date_modified":  get_date_modified(metadata, fallback_path=path if path.exists() else None),
            "folder":         folder,
            "folder_top":     folder_top,
            "size":           path.stat().st_size,
            "indexed_at":     datetime.now(timezone.utc).isoformat(),
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
    logging.info(f"Lot flushé : {ok} OK / {len(errors)} erreurs (buffer vidé)")
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
                    source_name = message.value.get("source", DEFAULT_SOURCE_NAME)

                    try:
                        source = get_source(source_name)
                    except KeyError as e:
                        # Source retirée du registre entre la publication
                        # et la consommation du message (course rare mais
                        # possible avec remove-file-source) : on abandonne ce
                        # message plutôt que de planter le worker.
                        logging.warning(f"[IGNORÉ] {filepath} — {e}")
                        continue

                    if is_excluded(Path(filepath).name):
                        logging.debug(f"[SKIP] Fichier temporaire ignoré : {filepath}")
                        continue

                    if is_archive(Path(filepath)):
                        # Re-vérification (défense en profondeur, la
                        # config a pu changer entre publication et
                        # consommation) avant d'extraire — évite de
                        # lancer une extraction potentiellement lourde
                        # pour une archive désormais désactivée.
                        try:
                            kind = archive_kind(Path(filepath))
                            size = Path(filepath).stat().st_size
                        except OSError:
                            continue
                        allowed, reason = is_allowed(kind, size, source.name)
                        if not allowed:
                            logging.info(f"[IGNORÉ] {filepath} — {reason}")
                            continue
                        # Traité directement (extraction + indexation
                        # immédiate de chaque membre, pas de mise en
                        # buffer bulk() ici) : le fichier archive est
                        # sur le volume partagé /sources, accessible
                        # depuis n'importe quel worker.
                        try:
                            index_archive(filepath, source)
                        except Exception as e:
                            logging.error(f"Erreur archive [{filepath}] : {e}")
                        continue

                    try:
                        size = Path(filepath).stat().st_size
                    except OSError:
                        continue
                    allowed, reason = is_allowed(extension, size, source.name)
                    if not allowed:
                        logging.info(f"[IGNORÉ] {filepath} — {reason}")
                        continue

                    doc_id = hashlib.md5(str(Path(filepath).resolve()).encode()).hexdigest()
                    if es.exists(index=source.es_index, id=doc_id):
                        logging.debug(f"[SKIP] Déjà indexé : {filepath}")
                        continue

                    try:
                        if extension == ".pst":
                            from pst_extractor import index_pst
                            index_pst(filepath, source)
                            continue
                        content, metadata = extract(filepath)
                        buffer.append(build_action(filepath, content, metadata, extension, source))
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
