# sql_indexer.py — Indexation de résultats de requêtes SQL (PostgreSQL/MySQL)
#
# Contrairement aux sources fichiers (indexer.py), une source SQL n'a pas
# de notion de "fichier modifié/supprimé" détectable événementiellement :
# faute de colonne de curseur fiable sur toutes les requêtes visées, ce
# module relit INTÉGRALEMENT le résultat de la requête à chaque passage
# (voir sql_worker.py pour la boucle de polling) :
#
#   1. Chaque ligne est upsertée (jamais de skip-if-exists : contrairement
#      à un fichier, une ligne SQL peut changer de contenu sans changer
#      d'identité).
#   2. Le même passage sert aussi de RÉCONCILIATION : l'ensemble des
#      doc_id attendus (calculé pendant le streaming, sans requête
#      séparée) est comparé à l'ensemble des _id actuellement dans
#      l'index ES de la source — tout _id présent dans ES mais absent
#      du nouveau résultat est supprimé (ligne supprimée côté SQL).
#
# Un garde-fou (_reconcile) refuse de supprimer plus de la moitié d'un
# index par ailleurs significatif en un seul passage — un résultat vide
# ou tronqué (DSN cassé, permissions révoquées, requête qui échoue
# silencieusement côté driver) ne doit jamais purger tout un index.

import os
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")

import hashlib
import logging
from datetime import datetime, timezone

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan as es_scan, bulk as es_bulk

from sources_config import ES_SEARCH_ALIAS
from sql_sources_config import SqlSource
import sql_dsn_registry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SQLIndexer] %(message)s"
)

es = Elasticsearch(
    ES_HOST,
    retry_on_timeout=True,
    max_retries=3,
    request_timeout=60,
)

# Préfixes de DSN SQLAlchemy attendus par db_type — sert à détecter tôt
# une incohérence entre le db_type déclaré dans le registre et le DSN
# réellement fourni via la variable d'environnement (connection_ref),
# plutôt que de laisser un driver échouer plus loin avec une erreur moins
# claire.
_DSN_PREFIXES = {
    "postgresql": ("postgresql://", "postgresql+"),
    "mysql":       ("mysql://", "mysql+"),
}

# Au-delà de ce ratio de suppression en une seule réconciliation (et
# seulement si l'index contient déjà un nombre significatif de documents,
# voir RECONCILE_MIN_SAMPLE), on refuse d'agir : plus probablement le
# signe d'une requête/connexion cassée que de vraies suppressions.
RECONCILE_MAX_DELETE_RATIO = 0.5
RECONCILE_MIN_SAMPLE = 20

# Un engine SQLAlchemy par source, réutilisé entre deux passages (évite
# de rouvrir une connexion à chaque cycle) — recréé si le DSN change
# (ex: rotation de mot de passe suivie d'un redémarrage du conteneur).
_engines: dict[str, tuple[str, object]] = {}


def _resolve_dsn(source: SqlSource) -> str:
    # Variable d'environnement d'abord, TOUJOURS prioritaire si elle existe
    # (compatibilité totale avec les déploiements existants — voir
    # docsearch-infra/.env). Repli sur le registre chiffré (Redis, ajouté
    # depuis le panneau d'administration) uniquement si ABSENTE ou VIDE.
    dsn = os.environ.get(source.connection_ref)
    origin = "variable d'environnement"
    if not dsn:
        dsn = sql_dsn_registry.resolve_dsn(source.connection_ref)
        origin = "DSN dynamique (panneau d'administration)"
    if not dsn:
        raise RuntimeError(
            f"Aucun DSN disponible pour '{source.connection_ref}' — impossible de se "
            f"connecter pour la source SQL '{source.name}'. Fournissez soit une variable "
            f"d'environnement '{source.connection_ref}' contenant le DSN complet (ex: "
            f"postgresql+psycopg2://user:pass@host:5432/db, jamais stocké dans Redis ni "
            f"dans manage.sh), soit un DSN enregistré sous ce nom depuis le panneau "
            f"d'administration (Sources SQL > DSN chiffrés)."
        )
    prefixes = _DSN_PREFIXES[source.db_type]
    if not dsn.startswith(prefixes):
        raise RuntimeError(
            f"Le DSN de '{source.connection_ref}' (résolu via {origin}) ne correspond pas "
            f"au db_type déclaré ('{source.db_type}') pour la source '{source.name}' — "
            f"attendu un préfixe parmi {prefixes}."
        )
    return dsn


def _get_engine(source: SqlSource):
    from sqlalchemy import create_engine
    dsn = _resolve_dsn(source)
    cached = _engines.get(source.name)
    if cached is not None and cached[0] == dsn:
        return cached[1]
    # pool_pre_ping : évite de servir une connexion morte après une
    # coupure réseau ou un redémarrage de la base entre deux cycles de
    # polling (l'intervalle peut être long, plusieurs minutes).
    engine = create_engine(dsn, pool_pre_ping=True, pool_recycle=1800)
    _engines[source.name] = (dsn, engine)
    return engine


def _coerce(value, es_type: str):
    if value is None:
        return None
    if es_type == "long":
        return int(value)
    if es_type == "double":
        return float(value)
    if es_type == "boolean":
        return bool(value)
    if es_type == "date":
        return value.isoformat() if hasattr(value, "isoformat") else value
    return str(value)  # keyword / text


def _build_mapping(source: SqlSource) -> dict:
    properties = {}
    for f in source.fields:
        prop = {"type": f.es_type}
        if f.es_type == "text" and f.analyzer:
            prop["analyzer"] = f.analyzer
        properties[f.es_field] = prop

    # Champs automatiques, communs à toutes les sources SQL — mêmes noms
    # que pour les sources fichiers (indexer.py) pour que la recherche
    # fédérée (alias ES_SEARCH_ALIAS) puisse filtrer/facetter par
    # `source` de façon uniforme. Ne les écrase pas si le mapping
    # explicite de la source les a déjà déclarés sous ces noms.
    properties.setdefault("source",     {"type": "keyword"})
    properties.setdefault("indexed_at", {"type": "date"})

    return {
        "mappings": {"properties": properties},
        "settings": {
            "number_of_shards":   1,
            "number_of_replicas": 1,
            # Analyseur "french" — identique à indexer.py, disponible
            # pour tout champ text qui déclare "analyzer": "french" dans
            # son mapping, harmless sinon (simplement inutilisé).
            "analysis": {
                "analyzer": {
                    "french": {
                        "tokenizer": "standard",
                        "filter": ["lowercase", "french_stop", "french_stemmer"]
                    }
                },
                "filter": {
                    "french_stop":    {"type": "stop",    "stopwords": "_french_"},
                    "french_stemmer": {"type": "stemmer", "language": "light_french"},
                }
            }
        }
    }


def create_index(source: SqlSource):
    if not es.indices.exists(index=source.es_index):
        es.indices.create(index=source.es_index, body=_build_mapping(source))
        logging.info(f"Index '{source.es_index}' créé (source SQL '{source.name}').")

    # Rejoint l'alias fédéré — même principe que les sources fichiers :
    # docsearch-api peut chercher sur cet index sans configuration
    # séparée. Alias en lecture uniquement (pas de is_write_index).
    if not es.indices.exists_alias(name=ES_SEARCH_ALIAS, index=source.es_index):
        es.indices.put_alias(index=source.es_index, name=ES_SEARCH_ALIAS)


def _stream_rows(engine, source: SqlSource):
    """
    Exécute la requête de `source` avec un curseur serveur (stream_results)
    plutôt que de charger tout le résultat en mémoire — nécessaire
    puisqu'on relit la requête en ENTIER à chaque cycle (pas de curseur
    incrémental disponible, voir docstring du module).
    """
    from sqlalchemy import text
    with engine.connect() as conn:
        result = conn.execution_options(stream_results=True).execute(text(source.query))
        mapped = result.mappings()
        expected_columns = {f.column for f in source.fields}
        checked = False
        for row in mapped:
            if not checked:
                missing = expected_columns - set(row.keys())
                if missing:
                    raise RuntimeError(
                        f"Colonne(s) mappée(s) absente(s) du résultat de la requête "
                        f"(source '{source.name}') : {', '.join(sorted(missing))} — "
                        f"vérifier 'fields' et la requête SQL enregistrées pour cette source."
                    )
                checked = True
            yield row


def _reconcile(source: SqlSource, expected_ids: set[str]) -> int:
    es.indices.refresh(index=source.es_index)
    es_ids = {
        hit["_id"]
        for hit in es_scan(es, index=source.es_index, query={"query": {"match_all": {}}}, _source=False)
    }

    orphans = es_ids - expected_ids
    if not orphans:
        return 0

    if len(es_ids) >= RECONCILE_MIN_SAMPLE and (len(orphans) / len(es_ids)) > RECONCILE_MAX_DELETE_RATIO:
        logging.error(
            f"[{source.name}] Réconciliation REFUSÉE par sécurité : {len(orphans)}/{len(es_ids)} "
            f"documents seraient supprimés (> {int(RECONCILE_MAX_DELETE_RATIO * 100)}%) — plus "
            f"probablement le signe d'une requête ou d'une connexion cassée que de vraies "
            f"suppressions côté base. Vérifier la source avant toute purge manuelle."
        )
        return 0

    actions = [{"_op_type": "delete", "_index": source.es_index, "_id": doc_id} for doc_id in orphans]
    ok, errors = es_bulk(es, actions, raise_on_error=False)
    if errors:
        logging.error(f"[{source.name}] {len(errors)} erreur(s) de suppression lors de la réconciliation")
    return ok


def run_source(source: SqlSource) -> dict:
    """
    Passage complet pour une source SQL : upsert de toutes les lignes
    actuelles + réconciliation (suppression des documents ES dont l'ID
    n'apparaît plus dans le résultat). Voir docstring du module pour le
    raisonnement complet.
    """
    create_index(source)
    engine = _get_engine(source)
    expected_ids: set[str] = set()

    def actions():
        for row in _stream_rows(engine, source):
            id_value = row[source.id_column]
            if id_value is None:
                logging.warning(
                    f"[{source.name}] ligne ignorée : id_column '{source.id_column}' est NULL"
                )
                continue
            doc_id = hashlib.md5(f"{source.name}:{id_value}".encode()).hexdigest()
            expected_ids.add(doc_id)

            doc = {f.es_field: _coerce(row[f.column], f.es_type) for f in source.fields}
            doc["source"]     = source.name
            doc["indexed_at"] = datetime.now(timezone.utc).isoformat()

            yield {"_op_type": "index", "_index": source.es_index, "_id": doc_id, "_source": doc}

    ok, errors = es_bulk(es, actions(), raise_on_error=False, chunk_size=500)
    if errors:
        logging.error(f"[{source.name}] {len(errors)} erreur(s) d'indexation")

    deleted = _reconcile(source, expected_ids)
    logging.info(
        f"[{source.name}] {ok} document(s) upserté(s), {deleted} supprimé(s) "
        f"(réconciliation), {len(errors)} erreur(s)."
    )
    return {"upserted": ok, "deleted": deleted, "errors": len(errors)}


if __name__ == "__main__":
    import sys
    from sql_sources_config import get_source

    if len(sys.argv) < 2:
        logging.error("Usage : python sql_indexer.py <nom_source>")
        sys.exit(1)

    try:
        target = get_source(sys.argv[1])
    except KeyError as e:
        logging.error(f"❌ {e}")
        sys.exit(1)

    run_source(target)
