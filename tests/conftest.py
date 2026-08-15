# tests/conftest.py — Fixtures communes
#
# docsearch-ingestion n'avait aucun test : la CI ne faisait que `ruff` et
# `docker build`. Ce répertoire naît avec le connecteur de modules
# complémentaires, parce que c'est le premier chemin d'indexation qui
# accepte des documents venus de l'EXTÉRIEUR du produit — et qu'une règle
# d'ACL appliquée de travers y est irrattrapable.
#
# Deux principes, repris de docsearch-api/tests/conftest.py :
#
# 1. **Pas de test qui ne teste que des mocks.** Les écritures vont dans
#    un VRAI Elasticsearch (celui de la pile de dev), sur des index
#    jetables créés et détruits par test. Ce qui est remplacé, ce sont
#    les ENTRÉES — le registre de sources — jamais la logique éprouvée.
# 2. **Ne jamais salir l'environnement partagé.** Les index de test sont
#    tous préfixés `test-plugin-`, supprimés avant ET après chaque test,
#    et retirés de l'alias fédéré. Jamais de suppression par motif large.

import os
import sys
import uuid
from pathlib import Path

import pytest

APP_DIR = Path(__file__).resolve().parent.parent / "app"
sys.path.insert(0, str(APP_DIR))

# AVANT tout import des modules de l'application : plugin_indexer.py lit
# ES_HOST à l'import, comme tout le reste du dépôt. Le défaut "redis"/
# "elasticsearch" est le nom d'hôte du réseau de conteneurs, qui ne
# résout pas depuis la VM.
os.environ.setdefault("ES_HOST", "http://localhost:9200")
os.environ.setdefault("REDIS_HOST", "localhost")

from docsearch_contract import plugins as contract_plugins  # noqa: E402
import plugin_indexer  # noqa: E402

PREFIXE_INDEX_TEST = "test-plugin-"


def _es_joignable() -> bool:
    try:
        return bool(plugin_indexer.es.ping())
    except Exception:
        return False


requires_es = pytest.mark.skipif(
    not _es_joignable(),
    reason="Elasticsearch injoignable (ES_HOST) — test d'indexation sauté",
)


@pytest.fixture(autouse=True)
def index_de_test_allege(monkeypatch):
    """Crée les index JETABLES sans réplique et sans attendre l'allocation.

    Deux réglages, deux raisons, et les deux ont coûté cher le 2026-08-15 :

    - `wait_for_active_shards=0` — au-dessus du *high watermark* de disque
      (95 % sur cette VM), Elasticsearch n'alloue plus aucun shard neuf.
      `indices.create` reste alors bloqué en lecture socket, sans erreur
      ni délai d'attente : la suite paraît figée, et seul `faulthandler`
      montre où. Avec ce réglage, la création rend la main tout de suite
      et le test échoue franchement à l'écriture suivante — un échec
      lisible plutôt qu'un blocage.
    - `number_of_replicas: 0` — un seul nœud sur la VM de dev. Une
      réplique par index de test, c'est un shard `UNASSIGNED` de plus sur
      un cluster partagé, donc du bruit dans son état de santé.

    Volontairement AUTOUSE et borné au préfixe des index de test : une
    fixture qu'il faut penser à demander ne protège que les tests dont
    l'auteur y a pensé, et jamais un index réel ne doit passer par ici.

    ⚠️  La production garde `number_of_replicas: 1`
    (`plugin_indexer._build_mapping`), comme les trois autres indexeurs.
    Ce n'est pas ce mapping-là qu'on corrige ici.
    """
    creation_reelle = plugin_indexer.es.indices.create

    def creation_allegee(*, index, body=None, **options):
        if index.startswith(PREFIXE_INDEX_TEST) and body:
            body = {
                **body,
                "settings": {**body.get("settings", {}), "number_of_replicas": 0},
            }
            options.setdefault("wait_for_active_shards", 0)
        return creation_reelle(index=index, body=body, **options)

    monkeypatch.setattr(plugin_indexer.es.indices, "create", creation_allegee)


@pytest.fixture
def index_jetable():
    """Nom d'index unique, supprimé après le test — y compris son
    appartenance à l'alias fédéré, qu'un index de test ne doit jamais
    laisser derrière lui (la recherche de la pile de dev le verrait)."""
    nom = f"{PREFIXE_INDEX_TEST}{uuid.uuid4().hex[:8]}"
    yield nom
    try:
        if plugin_indexer.es.indices.exists(index=nom):
            plugin_indexer.es.indices.delete(index=nom)
    except Exception:
        pass


@pytest.fixture
def fabrique_source(index_jetable):
    """Construit une PluginSource valide visant l'index jetable.

    Passe par `valider_declaration` plutôt que par le constructeur : les
    tests éprouvent ainsi la même déclaration que celle qu'un
    administrateur enregistrerait, pas un objet fabriqué à la main qui
    contournerait la validation.
    """
    def _fabrique(nom="tickets", **surcharges):
        base = {"plugin": "jira", "es_index": index_jetable, "acl_policy": "public"}
        cfg = contract_plugins.valider_declaration({**base, **surcharges})
        return contract_plugins.depuis_dict(nom, cfg)

    return _fabrique
