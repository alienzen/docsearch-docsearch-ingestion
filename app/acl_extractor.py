# acl_extractor.py — Extraction des ACL POSIX et étendues
# Intégré le 29/06/2026

import os

import re
import stat
import grp
import pwd
import subprocess
import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class FileACL:
    owner:       str
    group:       str
    users:       list[str] = field(default_factory=list)
    groups:      list[str] = field(default_factory=list)
    public:      bool      = False
    permissions: str       = "---"


def extract_acl(filepath: str) -> FileACL:
    """ACL POSIX de base via os.stat()."""
    try:
        st   = os.stat(filepath)
        mode = st.st_mode
        perms = oct(stat.S_IMODE(mode))[-3:]

        try:
            owner = pwd.getpwuid(st.st_uid).pw_name
        except KeyError:
            owner = str(st.st_uid)

        try:
            group = grp.getgrgid(st.st_gid).gr_name
        except KeyError:
            group = str(st.st_gid)

        world_readable = bool(mode & stat.S_IROTH)

        acl = FileACL(
            owner=owner,
            group=group,
            users=[owner],
            groups=[group],
            public=world_readable,
            permissions=perms,
        )

        # Enrichissement avec les ACL étendues si disponibles
        ext = extract_extended_acl(filepath)
        acl.users  = list(set(acl.users  + ext.get("users",  [])))
        acl.groups = list(set(acl.groups + ext.get("groups", [])))

        return acl

    except (PermissionError, FileNotFoundError) as e:
        logger.warning(f"ACL inaccessible pour {filepath} : {e}")
        return FileACL(owner="unknown", group="unknown", public=False)


def extract_extended_acl(filepath: str) -> dict:
    """ACL étendues Linux via getfacl (setfacl/getfacl requis)."""
    try:
        result = subprocess.run(
            ["getfacl", "--omit-header", filepath],
            capture_output=True, text=True, timeout=5
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {"users": [], "groups": []}

    users, groups = [], []
    for line in result.stdout.splitlines():
        # user:dupont:r-- → accès lecture explicite
        m = re.match(r"user:([^:]+):([rwx-]+)", line)
        if m and m.group(1) and "r" in m.group(2):
            users.append(m.group(1).lower())

        # group:finance:r-x
        m = re.match(r"group:([^:]+):([rwx-]+)", line)
        if m and m.group(1) and "r" in m.group(2):
            groups.append(m.group(1).lower())

    return {"users": users, "groups": groups}
