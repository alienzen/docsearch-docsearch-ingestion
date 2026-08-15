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
