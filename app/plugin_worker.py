# plugin_worker.py — Consommateur du topic `documents-ready`
#
# Reçoit les documents poussés par les modules complémentaires, les
# valide contre le contrat et le registre, puis les fait indexer par
# plugin_indexer.py. Rien d'autre : ni extraction, ni ordonnancement — un
# module complémentaire tourne à son propre rythme, dans son propre
# conteneur, et pousse quand il veut (même modèle que le crawler web, qui
# porte sa propre planification).
#
# ── Ce qui est refusé, et pourquoi ça ne s'arrête jamais ─────
#
# Un message invalide est journalisé et SAUTÉ ; la consommation continue.
# Un module fautif — version de contrat périmée, source inconnue, champ
# non déclaré, ACL vide après filtrage — ne doit jamais pouvoir bloquer
# l'indexation des autres. C'est aussi pour ça que le journal nomme
# toujours le module, la source et la raison : c'est le seul endroit où
# l'auteur du module verra ce qui cloche.
#
# ── Découpage en lots ────────────────────────────────────────
#
# Les documents sont accumulés par source et écrits par lots (le bulk ES
# est le coût dominant). Un lot est vidé quand il atteint
# PLUGIN_BATCH_SIZE, quand PLUGIN_FLUSH_INTERVAL s'est écoulé, ou quand
# un `run_end` arrive pour cette source — dans ce dernier cas AVANT la
# réconciliation, sans quoi on supprimerait des documents de la passe
# courante encore en tampon.

import os
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
PLUGIN_TOPIC = os.getenv("PLUGIN_TOPIC", "documents-ready")
PLUGIN_BATCH_SIZE = int(os.getenv("PLUGIN_BATCH_SIZE", "200"))
PLUGIN_FLUSH_INTERVAL = float(os.getenv("PLUGIN_FLUSH_INTERVAL", "5"))

import json
import time
import logging

from kafka import KafkaConsumer

import plugin_indexer
import plugin_sources_config
from docsearch_contract import documents as contract_documents
from docsearch_contract.erreurs import ContratInvalide

HEARTBEAT_KEY = "docsearch:heartbeat:plugin_worker"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PluginWorker] %(message)s"
)


def _write_heartbeat():
    """Même sonde que sql_worker/web_worker — c'est ce que lit le panneau
    d'administration pour dire si le worker tourne."""
    try:
        import redis
        client = redis.Redis(
            host=plugin_sources_config.REDIS_HOST,
            port=plugin_sources_config.REDIS_PORT,
            socket_connect_timeout=2, socket_timeout=2,
        )
        client.set(HEARTBEAT_KEY, json.dumps({"ts": time.time()}), ex=120)
    except Exception as e:
        logging.debug(f"[heartbeat] Redis injoignable : {e}")


class Tampon:
    """Documents en attente d'écriture, groupés par source.

    Une classe et non un dict nu : le vidage doit se faire au même
    endroit pour les trois déclencheurs (taille, temps, fin de passe), et
    l'oublier dans l'un des trois perdrait des documents en silence.
    """

    def __init__(self):
        self._par_source: dict[str, list] = {}
        self.taille = 0

    def ajouter(self, source_name: str, doc_id: str, document: dict) -> None:
        self._par_source.setdefault(source_name, []).append((doc_id, document))
        self.taille += 1

    def vider(self, source_name: str | None = None) -> tuple[int, int]:
        """Écrit dans ES le tampon d'une source (ou de toutes) et le vide."""
        noms = [source_name] if source_name else list(self._par_source)
        indexes = erreurs = 0
        for nom in noms:
            lot = self._par_source.pop(nom, [])
            if not lot:
                continue
            self.taille -= len(lot)
            try:
                source = plugin_sources_config.get_source(nom)
            except KeyError:
                # La source a été retirée du registre pendant que ses
                # documents attendaient. On les jette : les indexer dans
                # un index dont plus rien ne connaît la politique d'ACL
                # serait pire que de les perdre.
                logging.warning(f"[{nom}] Source retirée du registre — {len(lot)} document(s) abandonné(s).")
                continue
            ok, ko = plugin_indexer.indexer_documents(source, lot)
            indexes += ok
            erreurs += ko
        return indexes, erreurs


def traiter_message(brut, tampon: Tampon) -> None:
    """Valide un message et l'applique. Ne lève jamais : tout refus est
    journalisé et le message sauté (voir l'en-tête)."""
    try:
        message = contract_documents.valider_message(brut)
    except ContratInvalide as e:
        logging.warning(f"Message refusé : {e}")
        return

    nom_source = message["source"]
    try:
        source = plugin_sources_config.get_source(nom_source)
    except KeyError:
        logging.warning(
            f"[{message['plugin']}] Source '{nom_source}' absente du registre — message ignoré. "
            f"L'enregistrer : ./manage.sh add-plugin-source {nom_source} <module> <index_es> <politique>"
        )
        return

    try:
        contract_documents.verifier_emetteur(source, message)
    except ContratInvalide as e:
        logging.error(f"Message refusé : {e}")
        return

    if message["type"] == "document":
        try:
            doc_id, document, refuses = contract_documents.construire_document(
                source, message["document"], message["run_id"],
            )
        except ContratInvalide as e:
            logging.warning(f"[{nom_source}] Document refusé : {e}")
            return
        if refuses:
            # Bruyant à dessein : une ACL rétrécie en silence donne un
            # document introuvable sans rien à quoi le rattacher.
            logging.warning(
                f"[{nom_source}] Principaux écartés de l'ACL (hors liste blanche) : "
                f"{', '.join(refuses)}"
            )
        plugin_indexer.create_index(source)
        tampon.ajouter(nom_source, doc_id, document)

    elif message["type"] == "delete":
        tampon.vider(nom_source)
        plugin_indexer.supprimer(source, [message["doc_id"]])

    elif message["type"] == "run_end":
        # L'ordre compte : vider AVANT de réconcilier, sinon les
        # documents de la passe courante encore en tampon seraient
        # comptés comme périmés et supprimés.
        tampon.vider(nom_source)
        plugin_indexer.reconcilier(source, message["run_id"])


def run_plugin_worker(consumer=None):
    """Boucle de consommation.

    `consumer` n'est passé que par les tests : ils fournissent une
    doublure dont poll() rend des lots choisis, ce qui permet d'éprouver
    ce que fait la boucle À VIDE — le cas qui a justement échappé à la
    première version.
    """
    consumer = consumer or KafkaConsumer(
        PLUGIN_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="plugin-workers",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        max_poll_records=PLUGIN_BATCH_SIZE,
        # Validation puis écriture : on ne valide un message qu'après
        # l'avoir consommé, et un redémarrage doit rejouer ce qui n'a pas
        # été écrit. `enable_auto_commit` par défaut convient — un
        # document rejoué est simplement réindexé sur le même _id, l'écriture
        # est idempotente (c'est vrai des trois autres chemins aussi).
        auto_offset_reset="earliest",
    )
    logging.info(f"À l'écoute du topic '{PLUGIN_TOPIC}' (bootstrap {KAFKA_BOOTSTRAP})")

    tampon = Tampon()
    dernier_flush = time.time()
    dernier_heartbeat = 0.0

    try:
        while True:
            # poll() et NON `for message in consumer` : l'itération bloque
            # indéfiniment quand le topic est vide, et tout le corps de
            # boucle avec elle. Deux conséquences, constatées sur la pile
            # de dev le 2026-08-15 — le worker tournait depuis des heures
            # sans avoir exécuté une seule fois ce qui suit :
            #
            #   - aucun battement de cœur n'était écrit, donc le panneau
            #     d'administration déclarait mort un worker en bonne
            #     santé, aussi longtemps qu'aucun module ne poussait ;
            #   - le vidage du tampon SUR DÉLAI ne se déclenchait jamais.
            #     Un module qui pousse trois documents puis se tait les
            #     laissait en mémoire jusqu'au message suivant — et un
            #     module qui oublie son `run_end` les perdait en silence,
            #     exactement la panne que ce lot cherche à rendre
            #     impossible.
            #
            # Le délai d'attente vaut l'intervalle de vidage : à vide, la
            # boucle tourne à ce rythme et rien de plus.
            lots = consumer.poll(timeout_ms=int(PLUGIN_FLUSH_INTERVAL * 1000))
            for messages in lots.values():
                for message in messages:
                    try:
                        traiter_message(message.value, tampon)
                    except Exception as e:
                        # Filet de sécurité : traiter_message() ne doit pas
                        # lever, mais une panne d'ES ou de Redis peut
                        # remonter d'ailleurs. Le worker continue — sinon un
                        # module fautif ou un incident passager arrêterait
                        # l'indexation de tous les autres.
                        logging.error(f"Erreur non prévue sur un message : {e}")

            maintenant = time.time()
            if tampon.taille >= PLUGIN_BATCH_SIZE or (maintenant - dernier_flush) >= PLUGIN_FLUSH_INTERVAL:
                indexes, erreurs = tampon.vider()
                if indexes or erreurs:
                    logging.info(f"Lot écrit : {indexes} document(s), {erreurs} erreur(s)")
                dernier_flush = maintenant

            if (maintenant - dernier_heartbeat) >= 30:
                _write_heartbeat()
                dernier_heartbeat = maintenant
    except KeyboardInterrupt:
        logging.info("Arrêt demandé — vidage du tampon.")
        tampon.vider()
    finally:
        consumer.close()


if __name__ == "__main__":
    run_plugin_worker()
