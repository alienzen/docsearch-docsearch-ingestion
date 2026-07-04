# pst_extractor.py — Extraction des emails depuis les archives PST
# Mis à jour le 29/06/2026 — ACL intégrées, ES 9.4.2

import os
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
DOCS_FOLDER = os.getenv("DOCS_FOLDER", "/documents")

import hashlib
import logging
from datetime import datetime, timezone

# python3-libpff (paquet apt Debian/Ubuntu) expose le module "pff"
# et non "pypff". Si vous compilez libpff depuis les sources,
# le module s'appelle "pypff" — adapter l'import en conséquence.
try:
    import pff as pypff          # apt : python3-libpff
except ImportError:
    import pypff                 # compilation depuis les sources

from elasticsearch import Elasticsearch
from acl_extractor import extract_acl

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PST] %(message)s")

ES_HOST = ES_HOST
es = Elasticsearch(ES_HOST, retry_on_timeout=True, max_retries=3, request_timeout=60)


def safe_str(value) -> str:
    if value is None:
        return ""
    return value if isinstance(value, str) else value.decode("utf-8", errors="replace")


def format_date(pypff_date):
    try:
        return pypff_date.isoformat() if pypff_date else None
    except Exception:
        return None


def extract_attachments(message) -> list[dict]:
    attachments = []
    for i in range(message.number_of_attachments):
        try:
            att  = message.get_attachment(i)
            name = safe_str(att.name) or f"attachment_{i}"
            attachments.append({
                "filename":     name,
                "size":         att.size,
                "content_type": safe_str(getattr(att, "mime_type", "")),
            })
        except Exception as e:
            logging.warning(f"    Pièce jointe ignorée : {e}")
    return attachments


def index_email(message, pst_filename: str, folder_path: str, pst_acl) -> None:
    subject      = safe_str(message.subject)
    sender       = safe_str(message.sender_name)
    sender_email = safe_str(message.sender_email_address)
    body_text    = safe_str(message.plain_text_body)
    body_html    = safe_str(message.html_body)

    recipients = []
    try:
        for i in range(message.number_of_recipients):
            r = message.get_recipient(i)
            recipients.append({
                "name":  safe_str(r.display_name),
                "email": safe_str(r.email_address),
            })
    except Exception:
        pass

    unique_str = f"{subject}{sender_email}{message.delivery_time}"
    doc_id = hashlib.md5(unique_str.encode()).hexdigest()

    if es.exists(index="documents", id=doc_id):
        return

    doc = {
        "filename":        pst_filename,
        "filepath":        pst_filename,
        "extension":       ".pst",
        "type":            "email",
        "folder":          folder_path,
        "title":           subject,
        "author":          sender,
        "sender_email":    sender_email,
        "recipients":      recipients,
        "content":         body_text or body_html,
        "attachments":     extract_attachments(message),
        "has_attachments": message.number_of_attachments > 0,
        "date":            format_date(message.delivery_time),
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
    es.index(index="documents", id=doc_id, document=doc)


def walk_folder(folder, pst_filename: str, pst_acl, folder_path: str = "") -> None:
    current_path = f"{folder_path}/{safe_str(folder.name)}"
    logging.info(f"  📁 {current_path} ({folder.number_of_sub_messages} emails)")

    for i in range(folder.number_of_sub_messages):
        try:
            message = folder.get_sub_message(i)
            index_email(message, pst_filename, current_path, pst_acl)
        except Exception as e:
            logging.error(f"    [ERREUR] Email {i} : {e}")

    for i in range(folder.number_of_sub_folders):
        walk_folder(folder.get_sub_folder(i), pst_filename, pst_acl, current_path)


def index_pst(pst_path: str) -> None:
    """Indexe l'intégralité d'un fichier PST, ACL héritées du fichier source."""
    logging.info(f"📂 Ouverture : {pst_path}")

    # Les emails héritent des ACL du fichier .pst lui-même
    pst_acl = extract_acl(pst_path)

    pst = pypff.file()
    pst.open(pst_path)
    walk_folder(pst.get_root_folder(), pst_path, pst_acl)
    pst.close()

    logging.info(f"✅ PST indexé : {pst_path}")
