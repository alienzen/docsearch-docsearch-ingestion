# tests/test_plugin_indexer.py — Écriture Elasticsearch des documents
# poussés par un module complémentaire.
#
# Elasticsearch est RÉEL : le mapping strict, le rattachement à l'alias
# fédéré et la réconciliation ne veulent rien dire simulés — ce sont
# précisément des comportements du moteur.

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))

import plugin_indexer  # noqa: E402
from conftest import requires_es  # noqa: E402
from docsearch_contract import documents as contract_documents  # noqa: E402
from file_sources_config import ES_SEARCH_ALIAS  # noqa: E402

pytestmark = requires_es

es = plugin_indexer.es


def _pousser(source, identifiants, run_id, **charge_commune):
    lot = []
    for ident in identifiants:
        doc_id, doc, _ = contract_documents.construire_document(
            source, {"id": ident, "title": f"Document {ident}", **charge_commune}, run_id,
        )
        lot.append((doc_id, doc))
    ok, erreurs = plugin_indexer.indexer_documents(source, lot)
    es.indices.refresh(index=source.es_index)
    return ok, erreurs


def test_index_cree_avec_le_mapping_du_coeur(fabrique_source):
    source = fabrique_source()
    plugin_indexer.create_index(source)

    mapping = next(iter(es.indices.get_mapping(index=source.es_index).values()))["mappings"]
    # Le module ne crée pas son index : c'est le cœur qui pose le schéma
    # commun, sans quoi la recherche fédérée ne saurait ni filtrer par
    # source, ni trier, ni afficher une carte de résultat.
    for champ in ("filename", "filepath", "content", "title", "source", "acl", "run_id"):
        assert champ in mapping["properties"]
    assert mapping["dynamic"] == "strict"


def test_index_rejoint_l_alias_federe(fabrique_source):
    """Sans l'alias, l'index existe et se remplit — et reste invisible à
    la recherche, sans aucune erreur. C'est le bug déjà documenté dans
    docsearch-infra/README.md."""
    source = fabrique_source()
    plugin_indexer.create_index(source)
    assert es.indices.exists_alias(name=ES_SEARCH_ALIAS, index=source.es_index)


def test_document_indexe_et_relisible(fabrique_source):
    source = fabrique_source()
    plugin_indexer.create_index(source)
    ok, erreurs = _pousser(source, ["T-1"], "passe-1")

    assert (ok, erreurs) == (1, 0)
    doc_id = contract_documents.doc_id_pour(source.name, "T-1")
    doc = es.get(index=source.es_index, id=doc_id)["_source"]
    assert doc["source"] == "tickets"
    assert doc["run_id"] == "passe-1"
    assert doc["acl"] == {"public": True}


def test_champ_declare_indexe_champ_inconnu_refuse_par_es(fabrique_source):
    """Seconde barrière : le contrat refuse déjà les champs non déclarés
    en amont, mais `dynamic: strict` tient même si un document arrive par
    un autre chemin. Un champ mappé au hasard ferait ensuite échouer les
    agrégations de facette, donc la recherche fédérée entière."""
    source = fabrique_source(fields=[{"nom": "bureau", "es_type": "keyword"}])
    plugin_indexer.create_index(source)

    doc_id, doc, _ = contract_documents.construire_document(
        source, {"id": "T-1", "extra": {"bureau": "Paris"}}, "passe-1",
    )
    ok, erreurs = plugin_indexer.indexer_documents(source, [(doc_id, doc)])
    assert (ok, erreurs) == (1, 0)

    # Le même document augmenté d'un champ que le registre ne déclare pas
    doc_hors_contrat = {**doc, "champ_surprise": "x"}
    ok, erreurs = plugin_indexer.indexer_documents(source, [("autre", doc_hors_contrat)])
    assert (ok, erreurs) == (0, 1)


def test_suppression_explicite(fabrique_source):
    source = fabrique_source()
    plugin_indexer.create_index(source)
    _pousser(source, ["T-1", "T-2"], "passe-1")

    supprimes = plugin_indexer.supprimer(source, [contract_documents.doc_id_pour(source.name, "T-1")])
    es.indices.refresh(index=source.es_index)

    assert supprimes == 1
    assert es.count(index=source.es_index)["count"] == 1


# ── Réconciliation ───────────────────────────────────────────

def test_reconciliation_supprime_les_documents_de_la_passe_precedente(fabrique_source):
    source = fabrique_source()
    plugin_indexer.create_index(source)
    _pousser(source, [f"T-{i}" for i in range(10)], "passe-1")

    # Deuxième passe : le module ne pousse plus que 8 documents sur 10.
    _pousser(source, [f"T-{i}" for i in range(8)], "passe-2")
    supprimes = plugin_indexer.reconcilier(source, "passe-2")
    es.indices.refresh(index=source.es_index)

    assert supprimes == 2
    assert es.count(index=source.es_index)["count"] == 8


def test_reconciliation_sans_rien_a_supprimer(fabrique_source):
    source = fabrique_source()
    plugin_indexer.create_index(source)
    _pousser(source, ["T-1", "T-2"], "passe-1")

    assert plugin_indexer.reconcilier(source, "passe-1") == 0
    assert es.count(index=source.es_index)["count"] == 2


def test_garde_fou_refuse_une_purge_massive(fabrique_source, caplog):
    """LE test de la réconciliation. Un module qui tombe au milieu de sa
    passe a poussé une fraction de ses documents : sans ce contrôle, son
    `run_end` effacerait tout le reste de la source, et la panne se
    lirait comme une source vidée plutôt que comme un module en échec."""
    source = fabrique_source()
    plugin_indexer.create_index(source)
    _pousser(source, [f"T-{i}" for i in range(30)], "passe-1")

    # Passe tronquée : seuls 5 documents sur 30 ont été poussés, donc 25
    # (83 %) seraient supprimés.
    _pousser(source, [f"T-{i}" for i in range(5)], "passe-2")
    supprimes = plugin_indexer.reconcilier(source, "passe-2")
    es.indices.refresh(index=source.es_index)

    assert supprimes == 0
    assert es.count(index=source.es_index)["count"] == 30
    assert "REFUSÉE" in caplog.text


def test_garde_fou_ne_s_applique_pas_a_un_petit_index(fabrique_source):
    """En dessous de RECONCILE_MIN_SAMPLE documents, le ratio ne veut
    rien dire : une source de 3 documents qui en perd 2 est un cas
    normal, pas un incident."""
    source = fabrique_source()
    plugin_indexer.create_index(source)
    _pousser(source, ["T-1", "T-2", "T-3"], "passe-1")
    _pousser(source, ["T-1"], "passe-2")

    assert plugin_indexer.reconcilier(source, "passe-2") == 2


def test_reconciliation_sur_index_absent_ne_leve_pas(fabrique_source):
    """Un `run_end` peut arriver avant tout document — module qui n'a
    rien trouvé à pousser. Ça ne doit pas faire tomber le worker."""
    source = fabrique_source()
    assert plugin_indexer.reconcilier(source, "passe-1") == 0


@pytest.mark.parametrize("politique,attendu", [
    ("public",  {"public": True}),
    ("groupes", {"public": False, "groups": ["DL-SUPPORT"]}),
])
def test_acl_ecrite_dans_es_selon_la_politique(fabrique_source, politique, attendu):
    """Bout en bout : ce qui est relu dans Elasticsearch est bien ce que
    la politique de la source impose, pas ce que le module proposait."""
    surcharges = {"acl_policy": politique}
    if politique == "groupes":
        surcharges["acl_groups"] = ["DL-SUPPORT"]
    source = fabrique_source(**surcharges)
    plugin_indexer.create_index(source)

    doc_id, doc, _ = contract_documents.construire_document(
        source,
        # Le module propose l'ouverture générale et un groupe qui n'est
        # pas le sien : les deux doivent être ignorés.
        {"id": "T-1", "acl": {"public": True, "groups": ["DL-TOUT-LE-MONDE"]}},
        "passe-1",
    )
    plugin_indexer.indexer_documents(source, [(doc_id, doc)])
    es.indices.refresh(index=source.es_index)

    assert es.get(index=source.es_index, id=doc_id)["_source"]["acl"] == attendu
