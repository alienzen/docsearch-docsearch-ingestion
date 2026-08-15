# tests/test_plugin_worker.py — Traitement d'un message poussé par un
# module complémentaire.
#
# Kafka n'intervient pas : `traiter_message()` reçoit le message déjà
# désérialisé, ce que fait le consommateur. Ce qui est éprouvé ici est ce
# qui se passe ENTRE la réception et Elasticsearch — et Elasticsearch,
# lui, est réel.
#
# Le registre de sources est remplacé, et rien d'autre : c'est l'ENTRÉE
# de la fonction testée, pas sa logique. Le registre a ses propres
# contrôles (contract/tests/test_plugins.py), et le faire intervenir ici
# obligerait à écrire dans le Redis de configuration de l'installation de
# dev, que ces tests ne doivent jamais salir.

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))

import plugin_indexer  # noqa: E402
import plugin_sources_config  # noqa: E402
import plugin_worker  # noqa: E402
from conftest import requires_es  # noqa: E402
from docsearch_contract import CONTRACT_VERSION, documents as contract_documents  # noqa: E402

pytestmark = requires_es

es = plugin_indexer.es


@pytest.fixture
def registre(monkeypatch):
    """Registre en mémoire : {nom: PluginSource}."""
    sources: dict = {}

    def get_source(nom):
        if nom not in sources:
            raise KeyError(f"Source plugin inconnue : '{nom}'")
        return sources[nom]

    monkeypatch.setattr(plugin_sources_config, "get_source", get_source)
    return sources


def message(**surcharges) -> dict:
    base = {
        "contract_version": CONTRACT_VERSION,
        "plugin": "jira", "source": "tickets", "run_id": "passe-1",
        "type": "document", "document": {"id": "T-1", "title": "Un ticket"},
    }
    return {**base, **surcharges}


def test_document_valide_indexe(registre, fabrique_source):
    source = fabrique_source()
    registre["tickets"] = source

    tampon = plugin_worker.Tampon()
    plugin_worker.traiter_message(message(), tampon)
    assert tampon.taille == 1        # écrit au vidage, pas au message
    tampon.vider()
    es.indices.refresh(index=source.es_index)

    assert es.count(index=source.es_index)["count"] == 1


def test_source_inconnue_n_ecrit_rien(registre, fabrique_source, caplog):
    """Le registre fait foi : un module ne crée pas de source en poussant
    dessus. Sans ça, un nom mal orthographié produirait un index dont
    personne n'a déclaré la politique d'ACL."""
    source = fabrique_source()          # existe, mais n'est PAS enregistrée
    tampon = plugin_worker.Tampon()

    plugin_worker.traiter_message(message(source="inexistante"), tampon)

    assert tampon.taille == 0
    assert not es.indices.exists(index=source.es_index)
    assert "absente du registre" in caplog.text


def test_un_module_ne_pousse_pas_sur_la_source_d_un_autre(registre, fabrique_source, caplog):
    registre["tickets"] = fabrique_source()
    tampon = plugin_worker.Tampon()

    plugin_worker.traiter_message(message(plugin="confluence"), tampon)

    assert tampon.taille == 0
    assert "appartient à 'jira'" in caplog.text


def test_version_de_contrat_incompatible_ignoree(registre, fabrique_source, caplog):
    registre["tickets"] = fabrique_source()
    tampon = plugin_worker.Tampon()

    plugin_worker.traiter_message(message(contract_version="9.0.0"), tampon)

    assert tampon.taille == 0
    assert "Version de contrat" in caplog.text


def test_message_malforme_n_arrete_pas_le_worker(registre, fabrique_source):
    """Un module fautif ne doit jamais bloquer l'indexation des autres :
    tout refus est journalisé et sauté, jamais levé."""
    registre["tickets"] = fabrique_source()
    tampon = plugin_worker.Tampon()

    for mauvais in (None, "pas un objet", {}, {"type": "document"}, message(type="upsert")):
        plugin_worker.traiter_message(mauvais, tampon)

    assert tampon.taille == 0


def test_champ_non_declare_refuse_avant_es(registre, fabrique_source, caplog):
    registre["tickets"] = fabrique_source()
    tampon = plugin_worker.Tampon()

    plugin_worker.traiter_message(
        message(document={"id": "T-1", "extra": {"bureau": "Paris"}}), tampon,
    )

    assert tampon.taille == 0
    assert "non déclaré" in caplog.text


def test_acl_vide_apres_filtrage_refuse_le_document(registre, fabrique_source, caplog):
    """Plutôt qu'un document indexé mais invisible pour tout le monde —
    le genre de panne qu'on ne diagnostique jamais."""
    registre["tickets"] = fabrique_source(acl_policy="fournie", acl_principaux=["DL-SUPPORT"])
    tampon = plugin_worker.Tampon()

    plugin_worker.traiter_message(
        message(document={"id": "T-1", "acl": {"groups": ["DL-DIRECTION"]}}), tampon,
    )

    assert tampon.taille == 0
    assert "Aucun principal autorisé" in caplog.text


def test_principaux_hors_liste_blanche_journalises(registre, fabrique_source, caplog):
    """Écartés, mais bruyamment : une ACL rétrécie en silence donne un
    document introuvable sans rien à quoi le rattacher."""
    registre["tickets"] = fabrique_source(acl_policy="fournie", acl_principaux=["DL-SUPPORT"])
    tampon = plugin_worker.Tampon()

    plugin_worker.traiter_message(
        message(document={"id": "T-1", "acl": {"groups": ["DL-SUPPORT", "DL-DIRECTION"]}}), tampon,
    )

    assert tampon.taille == 1
    assert "DL-DIRECTION" in caplog.text


def test_suppression_explicite(registre, fabrique_source):
    source = fabrique_source()
    registre["tickets"] = source
    tampon = plugin_worker.Tampon()

    plugin_worker.traiter_message(message(), tampon)
    tampon.vider()
    es.indices.refresh(index=source.es_index)

    plugin_worker.traiter_message(
        message(type="delete", document=None,
                doc_id=contract_documents.doc_id_pour("tickets", "T-1")),
        tampon,
    )
    es.indices.refresh(index=source.es_index)

    assert es.count(index=source.es_index)["count"] == 0


def test_fin_de_passe_vide_le_tampon_avant_de_reconcilier(registre, fabrique_source):
    """L'ordre compte, et l'inverse serait indétectable : les documents
    de la passe courante encore en tampon porteraient le bon run_id mais
    ne seraient pas encore dans ES — la réconciliation les compterait
    donc comme absents, et le module verrait sa passe disparaître au
    moment même où il la termine."""
    source = fabrique_source()
    registre["tickets"] = source
    tampon = plugin_worker.Tampon()

    # Passe 1, écrite.
    plugin_worker.traiter_message(message(run_id="passe-1", document={"id": "VIEUX"}), tampon)
    tampon.vider()
    es.indices.refresh(index=source.es_index)

    # Passe 2 : le document est encore EN TAMPON quand le run_end arrive.
    plugin_worker.traiter_message(message(run_id="passe-2", document={"id": "NEUF"}), tampon)
    assert tampon.taille == 1
    plugin_worker.traiter_message(
        message(type="run_end", run_id="passe-2", document=None), tampon,
    )
    es.indices.refresh(index=source.es_index)

    restants = [
        h["_source"]["filename"]
        for h in es.search(index=source.es_index, query={"match_all": {}})["hits"]["hits"]
    ]
    assert restants == ["NEUF"]


def test_source_retiree_pendant_l_attente_abandonne_le_lot(registre, fabrique_source, caplog):
    """Les indexer dans un index dont plus rien ne connaît la politique
    d'ACL serait pire que de les perdre."""
    source = fabrique_source()
    registre["tickets"] = source
    tampon = plugin_worker.Tampon()

    plugin_worker.traiter_message(message(), tampon)
    del registre["tickets"]
    indexes, erreurs = tampon.vider()

    assert (indexes, erreurs) == (0, 0)
    assert "retirée du registre" in caplog.text


# ── La boucle à vide (correctif du 2026-08-15) ───────────────
#
# `for message in consumer` bloquait indéfiniment sur un topic vide, et
# tout le corps de boucle avec elle. Constaté sur la pile de dev : unité
# « active » depuis des heures, aucun battement de cœur écrit, et un
# tampon qui n'était jamais vidé sur délai. Ces trois tests éprouvent la
# boucle SANS message — le cas qui avait échappé.

class ConsommateurFactice:
    """Doublure de KafkaConsumer : rend les lots qu'on lui donne, puis
    interrompt la boucle comme le ferait un Ctrl-C."""

    def __init__(self, lots=(), tours_a_vide=2):
        self._lots = list(lots)
        self._tours_a_vide = tours_a_vide
        self.appels_poll = 0
        self.ferme = False

    def poll(self, timeout_ms=None):
        self.appels_poll += 1
        if self._lots:
            return self._lots.pop(0)
        self._tours_a_vide -= 1
        if self._tours_a_vide < 0:
            raise KeyboardInterrupt
        return {}

    def close(self):
        self.ferme = True


def test_le_battement_de_coeur_est_ecrit_sans_aucun_message(monkeypatch):
    """LE défaut corrigé : sans message, le panneau d'administration
    déclarait mort un worker en parfaite santé."""
    battements = []
    monkeypatch.setattr(plugin_worker, "_write_heartbeat", lambda: battements.append(1))

    plugin_worker.run_plugin_worker(consumer=ConsommateurFactice())

    assert battements, "aucun battement écrit alors que la boucle a tourné à vide"


def test_le_tampon_est_vide_sur_delai_sans_nouveau_message(monkeypatch):
    """Un module qui pousse puis se tait ne doit pas voir ses documents
    rester en mémoire : sans ça, un module qui oublie son `run_end` les
    perdait en silence.

    Le vidage est observé PENDANT la boucle, pas à l'arrêt — l'arrêt vide
    lui aussi le tampon, et s'en contenter aurait laissé passer le défaut
    exactement comme la première version.

    Elasticsearch n'intervient pas : ce qui est éprouvé est le
    DÉCLENCHEMENT du vidage, pas l'écriture, qui a ses propres tests.
    """
    monkeypatch.setattr(plugin_worker, "PLUGIN_FLUSH_INTERVAL", 0)
    monkeypatch.setattr(plugin_worker, "_write_heartbeat", lambda: None)

    consommateur = ConsommateurFactice(lots=[{"p0": [type("M", (), {"value": {}})()]}])
    vidages = []

    def vider_espion(self, source_name=None):
        vidages.append(consommateur.appels_poll)
        return 0, 0

    monkeypatch.setattr(plugin_worker.Tampon, "vider", vider_espion)
    monkeypatch.setattr(plugin_worker, "traiter_message", lambda brut, tampon: None)

    plugin_worker.run_plugin_worker(consumer=consommateur)

    # Au moins un vidage AVANT le dernier poll, celui qui interrompt.
    assert any(n < consommateur.appels_poll for n in vidages), (
        f"vidages observés aux polls {vidages}, interruption au poll "
        f"{consommateur.appels_poll} — le tampon n'a été vidé qu'à l'arrêt"
    )


def test_le_consommateur_est_ferme_a_l_arret():
    consommateur = ConsommateurFactice()
    plugin_worker.run_plugin_worker(consumer=consommateur)
    assert consommateur.ferme
