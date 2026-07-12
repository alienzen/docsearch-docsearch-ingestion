# web_indexer.py — Transformation des sources web vers le schéma DocSearch
#
# Une source web est crawlée par Elastic Open Web Crawler dans un index ES
# intermédiaire (`crawl_index`, schéma propre au crawler : url, title, body,
# headings, meta_description, last_crawled_at...) — ce module relit
# INTÉGRALEMENT cet index à chaque passage (voir web_worker.py pour la
# boucle de polling) et transforme chaque document crawlé vers le schéma
# DocSearch commun (mêmes noms de champs que indexer.py/sql_indexer.py :
# filename, filepath, content, title, acl, source, indexed_at), dans
# `es_index` (rejoint ES_SEARCH_ALIAS pour la recherche fédérée).
#
#   1. Chaque page crawlée est upsertée (jamais de skip-if-exists : le
#      contenu d'une page peut changer sans que son URL ne change).
#   2. Le même passage sert aussi de RÉCONCILIATION : l'ensemble des
#      doc_id attendus (calculé pendant le scan, sans requête séparée) est
#      comparé à l'ensemble des _id actuellement dans `es_index` — tout
#      _id présent dans es_index mais absent du crawl_index est supprimé
#      (page disparue du site). Le crawler gère déjà lui-même la purge de
#      SON index (pages non revues lors de deux passages consécutifs), ce
#      module se contente de répercuter cette disparition sur l'index
#      DocSearch final.
#
# Un garde-fou (_reconcile) refuse de supprimer plus de la moitié d'un
# index par ailleurs significatif en un seul passage — un crawl_index vide
# ou tronqué (crawler en échec, mauvaise config output_index) ne doit
# jamais purger tout un index DocSearch.

import os
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")

import hashlib
import logging
from datetime import datetime, timezone

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan as es_scan, bulk as es_bulk

from file_sources_config import ES_SEARCH_ALIAS
from web_sources_config import WebSource

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WebIndexer] %(message)s"
)

es = Elasticsearch(
    ES_HOST,
    retry_on_timeout=True,
    max_retries=3,
    request_timeout=60,
)

# Au-delà de ce ratio de suppression en une seule réconciliation (et
# seulement si l'index contient déjà un nombre significatif de documents,
# voir RECONCILE_MIN_SAMPLE), on refuse d'agir — même logique que
# sql_indexer.py, pour la même raison (un crawl_index anormalement vide
# est plus probablement un crawl cassé qu'un site réellement vidé).
RECONCILE_MAX_DELETE_RATIO = 0.5
RECONCILE_MIN_SAMPLE = 20


def _build_mapping() -> dict:
    # Mêmes noms de champs que indexer.py/sql_indexer.py — permet à la
    # recherche fédérée (ES_SEARCH_ALIAS) de traiter une page web comme
    # n'importe quel autre document (filtre par source, affichage titre/
    # extrait, tri par date_modified...).
    return {
        "mappings": {
            "properties": {
                "filename":    {"type": "keyword"},
                "filepath":    {
                    "type": "keyword",
                    "fields": {"text": {"type": "text"}},
                },
                "extension":     {"type": "keyword"},
                "type":          {"type": "keyword"},
                "source":        {"type": "keyword"},
                "content":       {"type": "text", "analyzer": "french"},
                "title":         {"type": "text"},
                "date_modified": {"type": "date"},
                "indexed_at":    {"type": "date"},
                "acl": {
                    "properties": {
                        "public": {"type": "boolean"},
                    }
                },
            }
        },
        "settings": {
            "number_of_shards":   1,
            "number_of_replicas": 1,
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


def create_index(source: WebSource):
    if not es.indices.exists(index=source.es_index):
        es.indices.create(index=source.es_index, body=_build_mapping())
        logging.info(f"Index '{source.es_index}' créé (source web '{source.name}').")

    if not es.indices.exists_alias(name=ES_SEARCH_ALIAS, index=source.es_index):
        es.indices.put_alias(index=source.es_index, name=ES_SEARCH_ALIAS)


def _scan_crawled_docs(source: WebSource):
    """Parcourt l'index de crawl brut — échoue explicitement si l'index
    n'existe pas encore (le crawler n'a jamais tourné) plutôt que de
    traiter un index absent comme "0 page, tout supprimer"."""
    if not es.indices.exists(index=source.crawl_index):
        raise RuntimeError(
            f"Index de crawl '{source.crawl_index}' introuvable (source web "
            f"'{source.name}') — Elastic Open Web Crawler n'a probablement "
            f"jamais tourné pour ce site. Vérifier sa config (output_index)."
        )
    yield from es_scan(es, index=source.crawl_index, query={"query": {"match_all": {}}})


def _reconcile(source: WebSource, expected_ids: set[str]) -> int:
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
            f"probablement le signe d'un crawl cassé ou tronqué que de vraies pages disparues. "
            f"Vérifier '{source.crawl_index}' avant toute purge manuelle."
        )
        return 0

    actions = [{"_op_type": "delete", "_index": source.es_index, "_id": doc_id} for doc_id in orphans]
    ok, errors = es_bulk(es, actions, raise_on_error=False)
    if errors:
        logging.error(f"[{source.name}] {len(errors)} erreur(s) de suppression lors de la réconciliation")
    return ok


def run_source(source: WebSource) -> dict:
    """
    Passage complet pour une source web : upsert de toutes les pages
    actuellement dans `crawl_index` + réconciliation (suppression des
    documents de `es_index` dont l'URL n'apparaît plus dans le crawl).
    """
    create_index(source)
    expected_ids: set[str] = set()

    def actions():
        for hit in _scan_crawled_docs(source):
            raw = hit["_source"]
            url = raw.get("url")
            if not url:
                logging.warning(f"[{source.name}] document de crawl ignoré : champ 'url' absent")
                continue

            doc_id = hashlib.md5(url.encode()).hexdigest()
            expected_ids.add(doc_id)

            doc = {
                "filename":      raw.get("title") or url,
                "filepath":      url,
                "extension":     "html",
                "type":          "web",
                "source":        source.name,
                "content":       raw.get("body") or "",
                "title":         raw.get("title") or "",
                "date_modified": raw.get("last_crawled_at"),
                "indexed_at":    datetime.now(timezone.utc).isoformat(),
                "acl":           {"public": source.acl_public},
            }

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
    from web_sources_config import get_source

    if len(sys.argv) < 2:
        logging.error("Usage : python web_indexer.py <nom_source>")
        sys.exit(1)

    try:
        target = get_source(sys.argv[1])
    except KeyError as e:
        logging.error(f"❌ {e}")
        sys.exit(1)

    run_source(target)
