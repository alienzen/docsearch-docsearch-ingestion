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

# ── Limites configurables (protection zip bomb) ──────────────────
ARCHIVE_MAX_FILES     = int(os.getenv("ARCHIVE_MAX_FILES", "5000"))
ARCHIVE_MAX_TOTAL_MB  = int(os.getenv("ARCHIVE_MAX_TOTAL_SIZE_MB", "1000"))
ARCHIVE_MAX_TOTAL_SIZE = ARCHIVE_MAX_TOTAL_MB * 1024 * 1024

# Profondeur maximale d'archives imbriquées (zip dans un zip, etc.)
ARCHIVE_MAX_DEPTH = int(os.getenv("ARCHIVE_MAX_DEPTH", "1"))

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
        if len(infos) > ARCHIVE_MAX_FILES:
            raise ArchiveExtractionError(
                f"{len(infos)} fichiers dans l'archive (limite {ARCHIVE_MAX_FILES})"
            )
        for info in infos:
            total_size += info.file_size
            if total_size > ARCHIVE_MAX_TOTAL_SIZE:
                raise ArchiveExtractionError(
                    f"Taille décompressée > {ARCHIVE_MAX_TOTAL_MB} Mo — extraction interrompue"
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
        if len(members) > ARCHIVE_MAX_FILES:
            raise ArchiveExtractionError(
                f"{len(members)} fichiers dans l'archive (limite {ARCHIVE_MAX_FILES})"
            )
        for member in members:
            total_size += member.size
            if total_size > ARCHIVE_MAX_TOTAL_SIZE:
                raise ArchiveExtractionError(
                    f"Taille décompressée > {ARCHIVE_MAX_TOTAL_MB} Mo — extraction interrompue"
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
        if len(names) > ARCHIVE_MAX_FILES:
            raise ArchiveExtractionError(
                f"{len(names)} fichiers dans l'archive (limite {ARCHIVE_MAX_FILES})"
            )
        z.extractall(path=dest)

    extracted = [p for p in dest.rglob("*") if p.is_file()]
    total_size = sum(p.stat().st_size for p in extracted)
    if total_size > ARCHIVE_MAX_TOTAL_SIZE:
        raise ArchiveExtractionError(
            f"Taille décompressée > {ARCHIVE_MAX_TOTAL_MB} Mo"
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
