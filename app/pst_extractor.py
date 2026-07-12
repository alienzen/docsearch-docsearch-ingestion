# pst_extractor.py — Extraction des emails depuis les archives PST
# Mis à jour le 05/07/2026 — extraction pypff isolée en sous-processus
#
# pypff (paquet apt python3-pypff) est compilé contre le Python
# SYSTÈME Debian (/usr/bin/python3), PAS contre le Python 3.12 fourni
# par l'image de base python:3.12-slim (/usr/local/bin/python3), sous
# lequel tourne le reste de l'application (elasticsearch, tika...).
# Ces deux interpréteurs sont totalement indépendants : une extension C
# compilée pour l'un n'est PAS chargeable par l'autre — "import pypff"
# échouait silencieusement ici alors que l'installation apt réussissait
# (voir https://github.com/docker-library/python/issues/671).
#
# La lecture du PST est donc déléguée à pst_worker.py, exécuté
# explicitement avec /usr/bin/python3 (sans autre dépendance que
# pypff), qui retourne une ligne JSON par email sur stdout. Ce module
# (sous Python 3.12) parse cette sortie et se charge du reste (ACL,
# indexation ES) — inchangé par rapport à avant.

import os
ES_HOST     = os.getenv("ES_HOST", "http://localhost:9200")

import json
import hashlib
import logging
import subprocess
from pathlib import Path
from datetime import datetime, timezone

from elasticsearch import Elasticsearch
from acl_extractor import extract_acl
from file_sources_config import Source, get_source, DEFAULT_SOURCE_NAME

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PST] %(message)s")

es = Elasticsearch(ES_HOST, retry_on_timeout=True, max_retries=3, request_timeout=60)

# Python SYSTÈME Debian — PAS le "python3" du PATH (qui résout vers
# /usr/local/bin/python3, le Python 3.12 de l'image de base, incompatible
# avec le module pypff compilé par apt contre le Python système).
SYSTEM_PYTHON = "/usr/bin/python3"
PST_WORKER    = str(Path(__file__).parent / "pst_worker.py")


def index_email(email: dict, pst_filename: str, pst_acl, source: Source) -> None:
    unique_str = f"{email.get('subject','')}{email.get('sender_email','')}{email.get('date','')}"
    doc_id = hashlib.md5(unique_str.encode()).hexdigest()

    if es.exists(index=source.es_index, id=doc_id):
        return

    doc = {
        "filename":        pst_filename,
        "filepath":        pst_filename,
        "extension":       ".pst",
        "type":            "email",
        "source":          source.name,
        "folder":          email.get("folder", ""),
        "title":           email.get("subject", ""),
        "author":          email.get("sender", ""),
        "sender_email":    email.get("sender_email", ""),
        "recipients":      email.get("recipients", []),
        "content":         email.get("body_text") or email.get("body_html") or "",
        "attachments":     email.get("attachments", []),
        "has_attachments": email.get("has_attachments", False),
        "date":            email.get("date"),
        "indexed_at":      datetime.now(timezone.utc).isoformat(),
        "doc_hash":        doc_id,
        # ACL héritées du fichier PST source (un PST = une boîte mail = un propriétaire)
        "acl": {
            "owner":       pst_acl.owner,
            "group":       pst_acl.group,
            "users":       pst_acl.users,
            "groups":      pst_acl.groups,
            "public":      pst_acl.public,
            "permissions": pst_acl.permissions,
        },
    }
    es.index(index=source.es_index, id=doc_id, document=doc)


def index_pst(pst_path: str, source: Source | None = None) -> None:
    """
    Indexe l'intégralité d'un fichier PST, ACL héritées du fichier
    source. La lecture réelle du PST (pypff) se fait dans un
    sous-processus utilisant le Python système — voir l'en-tête de
    ce fichier pour l'explication complète.
    """
    source = source if source is not None else get_source(DEFAULT_SOURCE_NAME)
    logging.info(f"📂 Ouverture : {pst_path} (source '{source.name}')")

    pst_acl = extract_acl(pst_path)

    try:
        proc = subprocess.run(
            [SYSTEM_PYTHON, PST_WORKER, pst_path],
            capture_output=True, text=True, timeout=600,
        )
    except subprocess.TimeoutExpired:
        logging.error(f"pst_worker.py a dépassé le délai (600s) pour {pst_path}")
        return
    except FileNotFoundError:
        logging.error(
            f"{SYSTEM_PYTHON} introuvable — vérifier que l'image contient "
            f"bien le Python système Debian (dépendance de python3-pypff)."
        )
        return

    if proc.returncode != 0:
        logging.error(f"pst_worker.py a échoué ({pst_path}) : {proc.stderr.strip()[:500]}")
        return

    count = 0
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            email = json.loads(line)
        except json.JSONDecodeError:
            continue
        try:
            index_email(email, pst_path, pst_acl, source)
            count += 1
        except Exception as e:
            logging.error(f"  [ERREUR] Email dans {pst_path} : {e}")

    for err_line in proc.stderr.splitlines():
        if err_line.strip():
            logging.warning(f"  pst_worker: {err_line.strip()[:300]}")

    logging.info(f"✅ PST indexé : {pst_path} ({count} email(s))")
