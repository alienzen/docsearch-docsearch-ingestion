# archive_extractor.py — Extraction sécurisée d'archives pour indexation
#
# Formats supportés nativement (bibliothèque standard Python, zéro
# dépendance) : .zip, .tar, .tar.gz/.tgz, .tar.bz2/.tbz2, .tar.xz/.txz
# Format optionnel : .7z (nécessite le paquet pip py7zr)
#
# Sécurité :
#   - Protection contre le "zip slip" (chemins ../../ dans l'archive)
#   - Limite du nombre de fichiers et de la taille décompressée totale
#     (protection contre les "zip bombs")
#   - tarfile utilise filter="data" (Python >= 3.12) qui neutralise
#     nativement les permissions dangereuses et les liens symboliques
#     pointant hors de la destination

import os
import logging
import tarfile
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)
from runtime_config import get_param

# ── Limites (protection zip bomb) ─────────────────────────────────
# Lues dynamiquement à chaque appel via runtime_config (modifiables à
# chaud sans redémarrage — voir ./manage.sh set-config).
def _max_files() -> int:
    return get_param("archive_max_files")

def _max_total_size_bytes() -> int:
    return get_param("archive_max_total_size_mb") * 1024 * 1024

def _max_total_mb() -> int:
    return get_param("archive_max_total_size_mb")

def max_depth() -> int:
    return get_param("archive_max_depth")

try:
    import py7zr
    HAS_7Z = True
except ImportError:
    HAS_7Z = False
    logger.info("py7zr non installé — les archives .7z seront ignorées "
                "(pip install py7zr pour les supporter)")


class ArchiveExtractionError(Exception):
    """Levée quand une archive est refusée (trop volumineuse, chemin
    suspect, format non supporté, ou dépendance manquante)."""


def is_archive(path: Path) -> bool:
    """Détecte une archive par son nom de fichier (gère les doubles
    extensions comme .tar.gz)."""
    name = path.name.lower()
    if name.endswith((".tar.gz", ".tar.bz2", ".tar.xz")):
        return True
    return path.suffix.lower() in {".zip", ".tar", ".tgz", ".tbz2", ".txz", ".7z"}


def _safe_join(base: Path, member_name: str) -> Path:
    """
    Empêche le 'zip slip' : calcule le chemin cible et vérifie qu'il
    reste strictement à l'intérieur de `base` une fois résolu.
    """
    target = (base / member_name).resolve()
    base_resolved = base.resolve()
    if base_resolved != target and base_resolved not in target.parents:
        raise ArchiveExtractionError(
            f"Chemin suspect dans l'archive (zip slip) : {member_name!r}"
        )
    return target


def _extract_zip(archive_path: Path, dest: Path) -> list[Path]:
    extracted = []
    total_size = 0
    with zipfile.ZipFile(archive_path) as zf:
        infos = [i for i in zf.infolist() if not i.is_dir()]
        max_files = _max_files()
        if len(infos) > max_files:
            raise ArchiveExtractionError(
                f"{len(infos)} fichiers dans l'archive (limite {max_files})"
            )
        for info in infos:
            total_size += info.file_size
            if total_size > _max_total_size_bytes():
                raise ArchiveExtractionError(
                    f"Taille décompressée > {_max_total_mb()} Mo — extraction interrompue"
                )
            target = _safe_join(dest, info.filename)
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info) as src, open(target, "wb") as out:
                out.write(src.read())
            extracted.append(target)
    return extracted


def _extract_tar(archive_path: Path, dest: Path) -> list[Path]:
    extracted = []
    total_size = 0
    # mode "r:*" détecte automatiquement gzip/bz2/xz ou l'absence de compression
    with tarfile.open(archive_path, mode="r:*") as tf:
        members = [m for m in tf.getmembers() if m.isfile()]
        max_files = _max_files()
        if len(members) > max_files:
            raise ArchiveExtractionError(
                f"{len(members)} fichiers dans l'archive (limite {max_files})"
            )
        for member in members:
            total_size += member.size
            if total_size > _max_total_size_bytes():
                raise ArchiveExtractionError(
                    f"Taille décompressée > {_max_total_mb()} Mo — extraction interrompue"
                )
            # filter="data" (Python 3.12+) neutralise nativement le path
            # traversal, les liens symboliques dangereux et les permissions
            # setuid/setgid — recommandation officielle Python pour tarfile.
            tf.extract(member, path=dest, filter="data")
            extracted.append(dest / member.name)
    return extracted


def _extract_7z(archive_path: Path, dest: Path) -> list[Path]:
    if not HAS_7Z:
        raise ArchiveExtractionError(
            "py7zr non installé — impossible d'extraire ce fichier .7z"
        )
    with py7zr.SevenZipFile(archive_path, mode="r") as z:
        names = z.getnames()
        max_files = _max_files()
        if len(names) > max_files:
            raise ArchiveExtractionError(
                f"{len(names)} fichiers dans l'archive (limite {max_files})"
            )
        z.extractall(path=dest)

    extracted = [p for p in dest.rglob("*") if p.is_file()]
    total_size = sum(p.stat().st_size for p in extracted)
    if total_size > _max_total_size_bytes():
        raise ArchiveExtractionError(
            f"Taille décompressée > {_max_total_mb()} Mo"
        )
    return extracted


def safe_extract_archive(archive_path: Path, dest: Path) -> list[tuple[Path, str]]:
    """
    Extrait une archive dans `dest` de façon sécurisée.

    Retourne une liste de tuples (chemin_reel_extrait, chemin_relatif)
    où chemin_relatif est la position du fichier À L'INTÉRIEUR de
    l'archive (utilisé pour construire l'identité du document indexé).

    Lève ArchiveExtractionError si l'archive est refusée (trop grosse,
    trop de fichiers, chemin suspect, format ou dépendance manquante).
    """
    name = archive_path.name.lower()

    if name.endswith(".zip"):
        files = _extract_zip(archive_path, dest)
    elif name.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")):
        files = _extract_tar(archive_path, dest)
    elif name.endswith(".7z"):
        files = _extract_7z(archive_path, dest)
    else:
        raise ArchiveExtractionError(f"Format d'archive non supporté : {archive_path.name}")

    return [(f, str(f.relative_to(dest))) for f in files]
