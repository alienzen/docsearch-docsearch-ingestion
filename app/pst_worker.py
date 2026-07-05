#!/usr/bin/env python3
# pst_worker.py — Extraction brute d'un fichier PST via pypff
#
# Exécuté explicitement avec /usr/bin/python3 (le Python SYSTÈME
# Debian), JAMAIS avec le "python3" par défaut du PATH — l'image de
# base python:3.12-slim installe son propre Python dans /usr/local/,
# totalement séparé de celui contre lequel apt compile python3-pypff
# (Debian bookworm/trixie, /usr/bin/python3). Un module C compilé pour
# l'un n'est pas chargeable par l'autre (voir pst_extractor.py, qui
# invoque ce script via subprocess, et
# https://github.com/docker-library/python/issues/671).
#
# Ce script n'a AUCUNE dépendance hors bibliothèque standard + pypff —
# volontairement minimal, pour ne pas avoir à réinstaller elasticsearch/
# tika/etc. sous le Python système en plus du Python principal 3.12.
#
# Usage : /usr/bin/python3 pst_worker.py <chemin.pst>
# Sortie : un objet JSON par ligne (un par email) sur stdout.
#          Les erreurs par email individuel vont sur stderr (ne
#          bloquent pas le traitement des emails suivants).

import sys
import json

import pypff


def safe_str(value) -> str:
    if value is None:
        return ""
    return value if isinstance(value, str) else value.decode("utf-8", errors="replace")


def format_date(pypff_date):
    try:
        return pypff_date.isoformat() if pypff_date else None
    except Exception:
        return None


def extract_attachments(message) -> list:
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
        except Exception:
            pass  # pièce jointe illisible : ignorée sans bloquer l'email
    return attachments


def message_to_dict(message, folder_path: str) -> dict:
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

    return {
        "folder":          folder_path,
        "subject":         safe_str(message.subject),
        "sender":          safe_str(message.sender_name),
        "sender_email":    safe_str(message.sender_email_address),
        "body_text":       safe_str(message.plain_text_body),
        "body_html":       safe_str(message.html_body),
        "recipients":      recipients,
        "attachments":     extract_attachments(message),
        "has_attachments": message.number_of_attachments > 0,
        "date":            format_date(message.delivery_time),
    }


def walk_folder(folder, folder_path: str = "") -> None:
    current_path = f"{folder_path}/{safe_str(folder.name)}"

    for i in range(folder.number_of_sub_messages):
        try:
            message = folder.get_sub_message(i)
            print(json.dumps(message_to_dict(message, current_path)), flush=True)
        except Exception as e:
            print(json.dumps({"folder": current_path, "error": str(e)}),
                  file=sys.stderr, flush=True)

    for i in range(folder.number_of_sub_folders):
        walk_folder(folder.get_sub_folder(i), current_path)


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: pst_worker.py <chemin.pst>", file=sys.stderr)
        sys.exit(1)

    pst_path = sys.argv[1]
    pst = pypff.file()
    pst.open(pst_path)
    try:
        walk_folder(pst.get_root_folder())
    finally:
        pst.close()


if __name__ == "__main__":
    main()
