# sql_worker.py — Ordonnanceur des sources SQL
#
# Relit le registre sql_sources_config.py à chaque tick (toutes les 5s)
# et déclenche run_source() (sql_indexer.py) pour chaque source dont
# poll_interval_seconds s'est écoulé depuis son dernier passage — une
# source ajoutée/retirée/modifiée via manage.sh est donc prise en compte
# sans redémarrage de ce conteneur, même principe que watcher.py pour
# les sources fichiers.
#
# Les passages sont exécutés dans un petit pool de threads : une requête
# SQL longue sur une source ne doit pas retarder le polling des autres.
# Une source dont le passage précédent est encore en cours au moment où
# son intervalle est de nouveau écoulé est simplement sautée pour ce
# tick (jamais deux passages concurrents de la même source — la
# réconciliation par diff d'ID suppose un seul passage à la fois).

import os
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor

from sql_sources_config import get_sources
from sql_indexer import run_source

TICK_SECONDS = 5
MAX_WORKERS = int(os.getenv("SQL_WORKER_MAX_PARALLEL", "4"))

HEARTBEAT_KEY = "docsearch:heartbeat:sql_worker"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SQLWorker] %(message)s"
)


def _write_heartbeat():
    try:
        import redis
        client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT,
            socket_connect_timeout=2, socket_timeout=2,
        )
        client.set(HEARTBEAT_KEY, json.dumps({"ts": time.time()}), ex=120)
    except Exception as e:
        logging.debug(f"[heartbeat] Redis injoignable : {e}")


def _run_and_log(name: str):
    try:
        run_source(get_sources()[name])
    except Exception as e:
        logging.error(f"[{name}] Erreur lors du passage : {e}")


def start_sql_worker():
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    in_flight: dict[str, object] = {}   # {name: Future}
    last_run: dict[str, float] = {}

    try:
        while True:
            now = time.time()
            _write_heartbeat()

            # Nettoie les futures terminées AVANT de décider quoi
            # (re)lancer ce tick — sinon une source dont le passage
            # précédent vient tout juste de se terminer resterait
            # sautée jusqu'au tick suivant.
            for name in list(in_flight):
                if in_flight[name].done():
                    in_flight.pop(name)

            for name, source in get_sources().items():
                if name in in_flight:
                    continue  # passage précédent encore en cours
                elapsed = now - last_run.get(name, 0)
                if elapsed >= source.poll_interval_seconds:
                    logging.info(f"⏱️  Déclenchement passage [{name}] (index '{source.es_index}')")
                    last_run[name] = now
                    in_flight[name] = executor.submit(_run_and_log, name)

            time.sleep(TICK_SECONDS)
    except KeyboardInterrupt:
        executor.shutdown(wait=True)


if __name__ == "__main__":
    start_sql_worker()
