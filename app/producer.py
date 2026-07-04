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
import logging
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

from indexer import SUPPORTED, is_excluded, create_index, es
from archive_extractor import is_archive

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
    """
    producer = build_producer()
    published, skipped = 0, 0

    for root, _, files in os.walk(folder_path):
        for filename in files:
            filepath = os.path.join(root, filename)
            path = Path(filepath)

            if is_excluded(path.name):
                skipped += 1
                continue

            extension = path.suffix.lower()
            archive = is_archive(path)

            if not archive and extension not in SUPPORTED:
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
    published, skipped = scan_and_produce(target_folder)
    logging.info(
        f"✅ {published} fichier(s) publié(s) sur Kafka ({KAFKA_TOPIC}), "
        f"{skipped} ignoré(s). Les workers vont maintenant les traiter en parallèle."
    )
