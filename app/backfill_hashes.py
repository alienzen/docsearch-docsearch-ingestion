#!/usr/bin/env python3
"""Rétro-remplissage de l'empreinte de contenu sur les documents déjà indexés.

    ./manage.sh backfill-hashes [source]          # simulation
    ./manage.sh backfill-hashes [source] --apply  # écriture

Les documents indexés avant l'ajout de `content_sha256` n'en ont pas :
sans ce rattrapage, le rapport de doublons ne couvre que les documents
arrivés depuis, ce qui le rend illisible pendant des mois.

Relit les fichiers sur disque et les hache — **sans jamais appeler
Tika** : c'est de l'entrée/sortie pure, sans extraction, donc sans
commune mesure avec une réindexation. Un rescan ordinaire ne ferait de
toute façon rien ici, `worker.py` sautant tout document déjà présent.

Garde-fous :
  - simulation par défaut, comme la purge d'index ;
  - ne touche QUE les documents dépourvus de `content_sha256` — d'où sa
    reprise naturelle après une interruption : relancer continue ;
  - ne réécrit aucun autre champ ;
  - un fichier disparu ou illisible est compté et laissé tel quel, jamais
    marqué d'une empreinte vide qui le ferait passer pour le doublon de
    tous les autres fichiers disparus.

⚠️ Les MEMBRES D'ARCHIVE sont ignorés : leur `filepath` ressemble à
« /docs/a.zip::note.pdf » et ne désigne aucun fichier sur disque. Les
hacher supposerait de réextraire chaque archive, ce qui n'est plus de
l'entrée/sortie pure — leur empreinte se remplira à la prochaine
réindexation de l'archive.
"""

import os
import sys

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk as es_bulk, scan as es_scan

from file_sources_config import get_sources, get_source
from indexer import content_sha256

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")

# Écriture par lots : un update par document ferait un aller-retour ES
# par fichier, pour une opération qui en compte potentiellement des
# millions.
TAILLE_LOT = 500


def traiter(es: Elasticsearch, index: str, appliquer: bool) -> dict:
    compte = {"vus": 0, "a_completer": 0, "illisibles": 0, "archives": 0, "ecrits": 0}
    lot = []

    documents = es_scan(
        es,
        index=index,
        query={"query": {"bool": {"must_not": [{"exists": {"field": "content_sha256"}}]}}},
        _source=["filepath"],
        size=TAILLE_LOT,
    )

    for document in documents:
        compte["vus"] += 1
        chemin = document["_source"].get("filepath", "")
        if not chemin:
            continue
        if "::" in chemin:
            compte["archives"] += 1
            continue

        empreinte = content_sha256(chemin)
        if empreinte is None:
            compte["illisibles"] += 1
            continue

        compte["a_completer"] += 1
        if not appliquer:
            continue

        lot.append({
            "_op_type": "update",
            "_index": index,
            "_id": document["_id"],
            "doc": {"content_sha256": empreinte},
        })
        if len(lot) >= TAILLE_LOT:
            ok, _ = es_bulk(es, lot, raise_on_error=False)
            compte["ecrits"] += ok
            lot.clear()

    if appliquer and lot:
        ok, _ = es_bulk(es, lot, raise_on_error=False)
        compte["ecrits"] += ok

    return compte


def main() -> int:
    arguments = [a for a in sys.argv[1:] if not a.startswith("--")]
    appliquer = "--apply" in sys.argv
    # Créé ici et non au chargement du module : importer ce fichier (un
    # test, un autre script) ne doit pas ouvrir de connexion.
    es_client = Elasticsearch(ES_HOST, request_timeout=120)

    sources = {arguments[0]: get_source(arguments[0])} if arguments else get_sources()

    titre = "Rétro-remplissage des empreintes de contenu"
    print(titre + ("" if appliquer else "  [SIMULATION — rien n'est écrit]"))
    print("Relit les fichiers sur disque, sans appeler Tika.\n")

    for nom, source in sources.items():
        if not es_client.indices.exists(index=source.es_index):
            print(f"  {nom:16} → index absent, ignoré")
            continue
        compte = traiter(es_client, source.es_index, appliquer)
        detail = []
        if compte["archives"]:
            detail.append(f"{compte['archives']} membre(s) d'archive ignoré(s)")
        if compte["illisibles"]:
            detail.append(f"{compte['illisibles']} fichier(s) illisible(s)")
        suffixe = (" — " + ", ".join(detail)) if detail else ""
        if appliquer:
            print(f"  {nom:16} → {compte['ecrits']} document(s) complété(s){suffixe}")
        else:
            print(f"  {nom:16} → {compte['a_completer']} document(s) à compléter"
                  f" sur {compte['vus']} sans empreinte{suffixe}")

    if not appliquer:
        print("\nRelancer avec --apply pour écrire.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
