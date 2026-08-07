# version.py — Identité de la livraison en cours d'exécution.
#
# Deux niveaux, volontairement distincts :
#
#   - VERSION : la version PRODUIT, déclarée à la main dans le fichier
#     VERSION à la racine du dépôt et IDENTIQUE dans les trois dépôts
#     construits en images (docsearch-api, docsearch-ingestion,
#     docsearch-ui-vue). C'est ce que voit l'utilisateur.
#   - COMMIT / BUILD_DATE : l'estampille de build, propre à CHAQUE image.
#     C'est la seule valeur qui ne ment pas sur ce qui tourne réellement
#     sur une machine — les trois dépôts se déployant indépendamment, la
#     dérive entre composants est une situation normale, pas un incident.
#
# Les trois valeurs sont injectées à la construction de l'image par
# ./manage.sh build (voir docsearch-infra/manage.sh, build_one), qui les
# lit depuis git : le dépôt .git n'est PAS copié dans l'image, et ne doit
# pas l'être.
#
# Copie conforme de docsearch-api/app/version.py — même convention
# que path_filter.py ou filetype_config.py, dupliqués entre les deux
# dépôts plutôt que partagés par un paquet commun.

import os
from pathlib import Path

_INCONNU = "inconnu"


def _version_du_fichier() -> str:
    """Repli quand DOCSEARCH_VERSION n'est pas positionnée.

    Deux cas : hors conteneur (script lancé à la main depuis app/, où
    VERSION est un cran au-dessus), et dans une image construite sans les
    --build-arg (le fichier VERSION y est copié à côté du code, justement
    pour que la version produit ne soit jamais « inconnue »).
    """
    ici = Path(__file__).resolve().parent
    for chemin in (ici / "VERSION", ici.parent / "VERSION"):
        try:
            return chemin.read_text(encoding="utf-8").strip()
        except OSError:
            continue
    return _INCONNU


VERSION = os.getenv("DOCSEARCH_VERSION") or _version_du_fichier()
COMMIT = os.getenv("DOCSEARCH_COMMIT") or _INCONNU
BUILD_DATE = os.getenv("DOCSEARCH_BUILD_DATE") or _INCONNU


def infos() -> dict:
    """Bloc d'identité.

    Les processus d'ingestion n'ayant aucune surface HTTP, il voyage dans
    le battement de cœur écrit par le watcher (watcher.py), que
    docsearch-api relit déjà pour /admin/status.
    """
    return {"version": VERSION, "commit": COMMIT, "build_date": BUILD_DATE}
