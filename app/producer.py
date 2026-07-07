# producer.py — Producer Kafka pour l'indexation parallèle
#
# Rôle : parcourir DOCS_FOLDER et publier une référence de chaque
# fichier à indexer sur le topic Kafka "documents-to-index". Les
# workers (worker.py, plusieurs instances en parallèle) consomment
# ce topic et font le travail lourd (extraction Tika + indexation ES).
#
# C'est ce qui permet un débit d'indexation élevé : ce script est
# rapide (il ne fait que lister les fichiers), la charge réelle est
# distribuée sur N workers qui tournent simultanément.
#
# Usage :
#   python producer.py            # scan unique de DOCS_FOLDER
#
# Le nombre de partitions du topic doit être >= au nombre de workers
# pour que tous les workers reçoivent effectivement des messages —
# voir KAFKA_NUM_PARTITIONS dans docker-compose.yml (docsearch-infra).

import os
ES_HOST         = os.getenv("ES_HOST", "http://localhost:9200")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
DOCS_FOLDER     = os.getenv("DOCS_FOLDER", "/documents")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "documents-to-index")

import json
import time
import logging
from pathlib import Path

from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError

from indexer import is_excluded, create_index, optimize_for_bulk, restore_after_bulk, es
from archive_extractor import is_archive, archive_kind
from filetype_config import is_allowed
from path_filter import is_path_allowed, is_dir_excluded

KAFKA_WORKER_GROUP = "indexer-workers"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Producer] %(message)s"
)


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # acks=1 : suffisant ici (les messages perdus en cas de crash
        # du broker seront simplement rattrapés au prochain scan complet,
        # ce n'est pas une file critique nécessitant acks='all')
        acks=1,
        retries=3,
        linger_ms=20,       # regroupe les envois pour un meilleur débit
        batch_size=32768,
    )


def scan_and_produce(folder_path: str) -> tuple[int, int]:
    """
    Parcourt folder_path et publie une référence Kafka pour chaque
    fichier indexable (extension supportée ou archive).
    Retourne (nb_publies, nb_ignores).

    Les chemins comparés aux motifs d'inclusion/exclusion (path_filter)
    sont TOUJOURS relatifs à DOCS_FOLDER, jamais à folder_path — car
    folder_path peut être un simple sous-dossier ciblé (./manage.sh
    init finance) alors que les motifs sont définis relativement à la
    racine des documents.
    """
    producer = build_producer()
    published, skipped = 0, 0
    docs_root = Path(DOCS_FOLDER).resolve()

    for root, dirs, files in os.walk(folder_path):
        rel_root = os.path.relpath(root, docs_root)
        if rel_root == ".":
            rel_root = ""

        # Élaguer les sous-dossiers exclus AVANT qu'os.walk n'y descende —
        # gain de temps réel sur un sous-arbre volumineux entièrement
        # exclu (ex: un dossier "archives_2010" avec des milliers de
        # fichiers). Seule la liste NOIRE est utilisée pour l'élagage
        # (voir docstring de is_dir_excluded pour pourquoi la liste
        # blanche ne peut pas servir à élaguer sans risque).
        dirs[:] = [
            d for d in dirs
            if not is_dir_excluded(f"{rel_root}/{d}" if rel_root else d)
        ]

        for filename in files:
            filepath = os.path.join(root, filename)
            path = Path(filepath)
            rel_file = f"{rel_root}/{filename}" if rel_root else filename

            if is_excluded(path.name):
                skipped += 1
                continue

            allowed, reason = is_path_allowed(rel_file)
            if not allowed:
                logging.debug(f"[IGNORÉ] {filepath} — {reason}")
                skipped += 1
                continue

            extension = path.suffix.lower()
            archive = is_archive(path)

            try:
                size = path.stat().st_size
            except OSError:
                skipped += 1
                continue

            # Pour une archive, la clé de config est archive_kind()
            # (ex: "tar.gz"), PAS extension/path.suffix (qui vaudrait
            # ".gz" pour "x.tar.gz" — voir archive_extractor.py).
            check_key = archive_kind(path) if archive else extension
            allowed, reason = is_allowed(check_key, size)
            if not allowed:
                logging.debug(f"[IGNORÉ] {filepath} — {reason}")
                skipped += 1
                continue

            message = {
                "filepath":  str(path.resolve()),
                "extension": extension,
                "is_archive": archive,
            }
            try:
                producer.send(KAFKA_TOPIC, value=message)
                published += 1
                if published % 500 == 0:
                    logging.info(f"  ... {published} fichiers publiés")
            except KafkaError as e:
                logging.error(f"Erreur publication [{filepath}] : {e}")

    producer.flush(timeout=30)
    producer.close()
    return published, skipped


def wait_for_workers_to_catch_up(poll_interval: int = 10) -> None:
    """
    Bloque jusqu'à ce que le groupe de workers ait consommé tout le
    backlog Kafka publié par ce scan — la publication elle-même est
    rapide, le vrai travail (Tika + indexation ES) se fait ensuite en
    arrière-plan dans les workers. Nécessaire pour ne réactiver replicas
    et refresh_interval (restore_after_bulk) qu'une fois l'indexation
    réellement terminée, jamais juste après la publication sur Kafka.
    """
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)
    partitions = consumer.partitions_for_topic(KAFKA_TOPIC) or set()
    tps = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]
    if not tps:
        consumer.close()
        return
    consumer.assign(tps)
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)

    logging.info("⏳ Attente de la consommation complète par les workers...")
    try:
        while True:
            end_offsets = consumer.end_offsets(tps)
            committed = admin.list_consumer_group_offsets(group_id=KAFKA_WORKER_GROUP)
            lag = 0
            for tp in tps:
                offset_meta = committed.get(tp)
                consumed = offset_meta.offset if offset_meta and offset_meta.offset >= 0 else 0
                lag += end_offsets[tp] - consumed
            if lag <= 0:
                break
            logging.info(f"  ... {lag} message(s) restant(s) à traiter par les workers")
            time.sleep(poll_interval)
    finally:
        consumer.close()
        admin.close()
    logging.info("✅ Backlog Kafka entièrement consommé par les workers.")


if __name__ == "__main__":
    import sys

    # Argument optionnel : ne scanner qu'un sous-dossier de DOCS_FOLDER
    # (chemin relatif à DOCS_FOLDER, ou chemin absolu sous DOCS_FOLDER).
    #   python producer.py                    → scan complet
    #   python producer.py finance            → /documents/finance uniquement
    #   python producer.py /documents/finance → équivalent (absolu)
    target_folder = DOCS_FOLDER
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        candidate = Path(arg) if os.path.isabs(arg) else Path(DOCS_FOLDER) / arg
        candidate = candidate.resolve()
        docs_root = Path(DOCS_FOLDER).resolve()

        if docs_root != candidate and docs_root not in candidate.parents:
            logging.error(
                f"❌ '{candidate}' est en dehors de DOCS_FOLDER ({docs_root}) — abandon."
            )
            sys.exit(1)
        if not candidate.is_dir():
            logging.error(f"❌ Dossier introuvable : {candidate}")
            sys.exit(1)

        target_folder = str(candidate)

    logging.info(f"📂 Scan de {target_folder}...")
    create_index()   # s'assure que le mapping ES existe avant tout traitement
    optimize_for_bulk()
    try:
        published, skipped = scan_and_produce(target_folder)
        logging.info(
            f"✅ {published} fichier(s) publié(s) sur Kafka ({KAFKA_TOPIC}), "
            f"{skipped} ignoré(s). Les workers vont maintenant les traiter en parallèle."
        )
        wait_for_workers_to_catch_up()
    finally:
        # Toujours restaurer, même si le scan ou l'attente échoue — un
        # index laissé avec 0 replica / refresh désactivé est un risque
        # en production (perte de tolérance de panne, résultats non à
        # jour), pas seulement une perte de performance.
        restore_after_bulk()
