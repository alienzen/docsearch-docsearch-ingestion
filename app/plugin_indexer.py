# plugin_indexer.py — Écriture Elasticsearch des documents poussés par un
# module complémentaire
#
# Le SEUL endroit du produit où un document de module complémentaire est
# écrit dans Elasticsearch. C'est ce qui rend tenable l'invariant 1 du
# plan (« rien ne contourne l'ACL ») : le module ne connaît ni l'URL du
# cluster ni le nom de l'index, il pousse sur Kafka et c'est ici que tout
# se décide — mapping, alias, ACL, identité du document.
#
#   1. `create_index()` pose le mapping du CŒUR (schéma DocSearch commun
#      + champs déclarés dans le registre) et rattache l'index à
#      ES_SEARCH_ALIAS. Un module qui créerait son index lui-même
#      reproduirait le bug déjà documenté dans docsearch-infra/README.md :
#      index auto-créé en mapping dynamique, sans alias, invisible à la
#      recherche fédérée, sans aucune erreur visible.
#   2. `indexer_documents()` écrit un lot, `supprimer()` retire un
#      document.
#   3. `reconcilier()` purge, à la fin d'une passe, les documents restés
#      sur une passe antérieure — voir plus bas.
#
# ── Réconciliation par run_id ────────────────────────────────
#
# Les connecteurs SQL et web relisent leur source en entier et comparent
# des ensembles d'identifiants. Ici, le cœur ne peut pas savoir ce que le
# module n'a PAS envoyé : chaque document porte donc le `run_id` de la
# passe qui l'a produit, et le message `run_end` supprime tout ce qui
# porte un autre run_id. Même garde-fou que sql_indexer/web_indexer : on
# refuse de supprimer plus de la moitié d'un index déjà significatif — un
# module qui échoue en cours de passe ne doit pas vider une source.

import os
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")

import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk as es_bulk

from docsearch_contract.plugins import PluginSource
from file_sources_config import ES_SEARCH_ALIAS

# Recopié d'indexer.py (ANALYSE / CHAMP_EXACT), comme l'ont fait
# sql_indexer.py et web_indexer.py avant lui et pour la même raison :
# importer indexer.py ferait entrer Tika et l'extraction d'archives dans
# un worker qui n'en a aucun usage. Toute évolution de la recherche
# exacte doit être répercutée dans les QUATRE.
ANALYSEUR_EXACT = {
    "tokenizer": "standard",
    "filter": ["lowercase", "asciifolding"],
}
CHAMP_EXACT = {"type": "text", "analyzer": "exact"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PluginIndexer] %(message)s"
)

es = Elasticsearch(
    ES_HOST,
    retry_on_timeout=True,
    max_retries=3,
    request_timeout=60,
)

RECONCILE_MAX_DELETE_RATIO = 0.5
RECONCILE_MIN_SAMPLE = 20


def _build_mapping(source: PluginSource) -> dict:
    # Schéma DocSearch commun — mêmes noms de champs que indexer.py,
    # sql_indexer.py et web_indexer.py, pour que la recherche fédérée
    # traite ces documents comme les autres (facettes, tri, carte de
    # résultat), sans cas particulier côté interface.
    properties = {
        "filename":      {"type": "keyword", "fields": {"exact": CHAMP_EXACT}},
        "filepath":      {"type": "keyword", "fields": {"text": {"type": "text"}, "exact": CHAMP_EXACT}},
        "extension":     {"type": "keyword"},
        "type":          {"type": "keyword"},
        "source":        {"type": "keyword"},
        "content":       {"type": "text", "analyzer": "french", "fields": {"exact": CHAMP_EXACT}},
        "title":         {"type": "text", "fields": {"exact": CHAMP_EXACT}},
        "author":        {"type": "keyword", "fields": {"text": {"type": "text"}, "exact": CHAMP_EXACT}},
        "keywords":      {"type": "keyword", "fields": {"text": {"type": "text"}, "exact": CHAMP_EXACT}},
        "date_created":  {"type": "date"},
        "date_modified": {"type": "date"},
        "indexed_at":    {"type": "date"},
        # Passe qui a produit le document — c'est le champ sur lequel
        # porte la réconciliation.
        "run_id":        {"type": "keyword"},
        "acl": {
            "properties": {
                "owner":  {"type": "keyword"},
                "users":  {"type": "keyword"},
                "groups": {"type": "keyword"},
                "public": {"type": "boolean"},
            }
        },
    }

    # Champs supplémentaires déclarés dans le registre — mêmes règles que
    # les colonnes d'une source SQL.
    for champ in source.fields:
        prop = {"type": champ.es_type}
        if champ.es_type == "text":
            if champ.analyzer:
                prop["analyzer"] = champ.analyzer
            prop["fields"] = {"exact": CHAMP_EXACT}
        properties[champ.nom] = prop

    return {
        # "dynamic": "strict" — un champ absent du mapping fait ÉCHOUER
        # l'indexation du document au lieu d'être inventé par ES. Même
        # choix que sql_indexer.py, et pour la même raison : un champ
        # mappé dynamiquement en `text` fait ensuite échouer toute
        # agrégation de facette, donc la recherche fédérée entière (voir
        # _verifier_shards() côté API). Le contrat refuse déjà les champs
        # non déclarés en amont — ceci est la seconde barrière, celle qui
        # tient même si un document arrive par un autre chemin.
        "mappings": {"dynamic": "strict", "properties": properties},
        "settings": {
            "number_of_shards":   1,
            "number_of_replicas": 1,
            "analysis": {
                "analyzer": {
                    "french": {
                        "tokenizer": "standard",
                        "filter": ["lowercase", "french_stop", "french_stemmer"]
                    },
                    "exact": ANALYSEUR_EXACT,
                },
                "filter": {
                    "french_stop":    {"type": "stop",    "stopwords": "_french_"},
                    "french_stemmer": {"type": "stemmer", "language": "light_french"},
                }
            }
        }
    }


def create_index(source: PluginSource) -> None:
    """Crée l'index s'il manque, met à jour son mapping sinon, et le
    rattache à l'alias fédéré.

    Appelée au premier message de chaque passe plutôt qu'au démarrage du
    worker : une source enregistrée pendant que le worker tourne doit
    fonctionner sans redémarrage, comme partout ailleurs dans le produit.
    """
    mapping = _build_mapping(source)
    if not es.indices.exists(index=source.es_index):
        es.indices.create(index=source.es_index, body=mapping)
        logging.info(f"Index '{source.es_index}' créé (source plugin '{source.name}').")
    else:
        try:
            es.indices.put_mapping(
                index=source.es_index,
                properties=mapping["mappings"]["properties"],
                dynamic=mapping["mappings"]["dynamic"],
            )
        except Exception as e:
            # Même arbitrage que sql_indexer.py : on journalise au lieu
            # de lever. Un champ déjà mappé ne peut pas changer de type,
            # et laisser l'exception remonter arrêterait l'indexation de
            # la source entière — y compris pour un champ divergent
            # inoffensif.
            logging.error(
                f"[{source.name}] Mapping de '{source.es_index}' non mis à jour : {e} — "
                f"un champ déjà mappé ne peut pas changer de type. Recréez l'index "
                f"(il sera repeuplé à la passe suivante) ou visez un nouvel es_index."
            )

    if not es.indices.exists_alias(name=ES_SEARCH_ALIAS, index=source.es_index):
        es.indices.put_alias(index=source.es_index, name=ES_SEARCH_ALIAS)


def indexer_documents(source: PluginSource, documents: list[tuple[str, dict]]) -> tuple[int, int]:
    """Écrit un lot de (doc_id, document). Rend (indexés, erreurs)."""
    if not documents:
        return 0, 0
    actions = [
        {"_op_type": "index", "_index": source.es_index, "_id": doc_id, "_source": doc}
        for doc_id, doc in documents
    ]
    ok, errors = es_bulk(es, actions, raise_on_error=False, chunk_size=500)
    if errors:
        # Le détail compte : `dynamic: strict` refuse un document entier
        # pour un seul champ inattendu, et le message d'ES nomme le champ.
        logging.error(f"[{source.name}] {len(errors)} erreur(s) d'indexation — première : {errors[0]}")
    return ok, len(errors)


def supprimer(source: PluginSource, doc_ids: list[str]) -> int:
    """Supprime des documents nommés par le module (message `delete`).

    Sans garde-fou de ratio, contrairement à la réconciliation : ici le
    module désigne explicitement ce qu'il veut retirer, ce n'est pas une
    déduction du cœur.
    """
    if not doc_ids:
        return 0
    actions = [{"_op_type": "delete", "_index": source.es_index, "_id": d} for d in doc_ids]
    ok, errors = es_bulk(es, actions, raise_on_error=False)
    if errors:
        # 404 compris : un document déjà absent n'est pas un incident,
        # mais il ne doit pas passer inaperçu si c'est systématique.
        logging.warning(f"[{source.name}] {len(errors)} suppression(s) sans effet (document déjà absent ?)")
    return ok


def reconcilier(source: PluginSource, run_id: str) -> int:
    """Supprime les documents de la source restés sur une passe
    antérieure à `run_id`. Rend le nombre de documents supprimés.

    ⚠️  Le garde-fou est la partie qui compte. Un module qui tombe au
    milieu de sa passe a poussé une fraction de ses documents ; sans ce
    contrôle, le `run_end` d'une passe tronquée effacerait tout le reste
    de la source — et la panne se verrait comme une source vidée, pas
    comme un module en échec.
    """
    if not es.indices.exists(index=source.es_index):
        return 0

    es.indices.refresh(index=source.es_index)
    total = es.count(index=source.es_index)["count"]
    if total == 0:
        return 0

    requete = {"bool": {"must_not": [{"term": {"run_id": run_id}}]}}
    perimes = es.count(index=source.es_index, query=requete)["count"]
    if perimes == 0:
        return 0

    if total >= RECONCILE_MIN_SAMPLE and (perimes / total) > RECONCILE_MAX_DELETE_RATIO:
        logging.error(
            f"[{source.name}] Réconciliation REFUSÉE par sécurité : {perimes}/{total} "
            f"documents seraient supprimés (> {int(RECONCILE_MAX_DELETE_RATIO * 100)}%) — plus "
            f"probablement le signe d'une passe tronquée (module en échec) que d'une "
            f"disparition réelle. Passe '{run_id}'."
        )
        return 0

    resultat = es.delete_by_query(
        index=source.es_index, query=requete,
        conflicts="proceed", refresh=True,
    )
    supprimes = resultat.get("deleted", 0)
    logging.info(f"[{source.name}] Réconciliation de la passe '{run_id}' : {supprimes} document(s) supprimé(s).")
    return supprimes
