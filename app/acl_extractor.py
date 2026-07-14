# acl_extractor.py — Extraction des ACL POSIX et étendues
# Intégré le 29/06/2026
# Extraction ACL NTFS (partages CIFS/Windows) ajoutée le 14/07/2026

import os

import re
import json
import stat
import grp
import pwd
import subprocess
import logging
import functools
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


# ── Détection des points de montage CIFS (via /proc/mounts) ──────────
# Nécessaire pour choisir la bonne méthode d'extraction : un partage
# Windows natif n'a pas d'extensions Unix en SMB2/3 (contrairement à un
# vieux serveur SMB1 avec CIFS Unix Extensions) — getfacl n'y renvoie
# donc jamais rien d'exploitable, il faut passer par getcifsacl (ACL
# NTFS, voir extract_windows_acl()). lru_cache : /proc/mounts ne change
# pas pour un point de montage déjà actif au démarrage du processus.
@functools.lru_cache(maxsize=1)
def _cifs_mountpoints() -> tuple[str, ...]:
    mounts = []
    try:
        with open("/proc/mounts") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 3 and parts[2] == "cifs":
                    mounts.append(parts[1])
    except OSError as e:
        logger.warning(f"Lecture /proc/mounts impossible ({e}) — détection CIFS désactivée")
    # Plus long préfixe d'abord, pour matcher le montage le plus spécifique.
    return tuple(sorted(mounts, key=len, reverse=True))


def _is_cifs_path(filepath: str) -> bool:
    abspath = os.path.abspath(filepath)
    return any(
        abspath == mp or abspath.startswith(mp.rstrip("/") + "/")
        for mp in _cifs_mountpoints()
    )


# ── SID Windows → identité LDAP (partages CIFS uniquement) ───────────
# SID bien connus, universels (identiques sur tout domaine/poste
# Windows) — à la différence des SID de compte/groupe ci-dessous,
# propres à CHAQUE déploiement.
_WELL_KNOWN_PUBLIC_SIDS = {
    "S-1-1-0",   # Everyone
    "S-1-5-11",  # Authenticated Users
}

# SID de domaine/local → identité LDAP correspondante ("user:<login>"
# ou "group:<nom>", mêmes identifiants que ceux comparés par
# get_user_groups()/resolve_user() côté docsearch-api — voir
# search_query.py:build_acl_filter). SMB ne fournit aucune passerelle
# automatique SID → compte LDAP (ça demanderait un service
# supplémentaire type winbind, hors périmètre ici) : cette table est
# TOUJOURS à renseigner manuellement par déploiement. Un SID absent de
# la table est ignoré (log debug) plutôt que de faire échouer
# l'extraction du fichier.
CIFS_SID_MAP: dict[str, str] = json.loads(os.getenv("CIFS_SID_MAP", "{}"))

# Une ligne DACL ressemble à "ACL:<SID>:ALLOWED/<flags>/<masque>".
_ACE_RE = re.compile(r"^ACL:(?P<sid>\S+):(?P<type>ALLOWED|DENIED)/[^/]*/(?P<mask>\S+)$")


def extract_windows_acl(filepath: str) -> FileACL:
    """ACL NTFS via getcifsacl, pour les fichiers sur un partage CIFS.

    Ne traite que les ACE de type ALLOWED, sans distinction fine du
    masque (lecture/écriture/contrôle total traités identiquement) : la
    présence d'un ALLOWED pour une identité qu'on sait traduire suffit à
    la considérer visible, même niveau de granularité que
    extract_extended_acl() côté POSIX (qui ne regarde que le "r" et
    ignore write/execute). Les ACE DENIED ne sont PAS modélisées (pas de
    priorité deny-avant-allow) — limitation assumée, pas une évaluation
    NTFS complète.
    """
    try:
        result = subprocess.run(
            ["getcifsacl", filepath],
            capture_output=True, text=True, timeout=5,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        logger.warning(f"getcifsacl indisponible pour {filepath} : {e}")
        return FileACL(owner="unknown", group="unknown", public=False, permissions="cifs-acl")

    owner = "unknown"
    users:  list[str] = []
    groups: list[str] = []
    public = False

    for raw_line in result.stdout.splitlines():
        line = raw_line.strip()

        if line.startswith("OWNER:"):
            mapped = CIFS_SID_MAP.get(line.split(":", 1)[1].strip())
            if mapped and mapped.startswith("user:"):
                owner = mapped.split(":", 1)[1]
            continue

        m = _ACE_RE.match(line)
        if not m or m.group("type") != "ALLOWED":
            continue

        sid = m.group("sid")
        if sid in _WELL_KNOWN_PUBLIC_SIDS:
            public = True
            continue

        mapped = CIFS_SID_MAP.get(sid)
        if not mapped:
            logger.debug(f"SID {sid} sans correspondance CIFS_SID_MAP ({filepath}) — ACE ignoré")
            continue

        kind, _, name = mapped.partition(":")
        if kind == "user":
            users.append(name.lower())
        elif kind == "group":
            groups.append(name.lower())

    if owner != "unknown" and owner not in users:
        users.append(owner)

    return FileACL(
        owner=owner,
        group=(groups[0] if groups else "unknown"),
        users=users,
        groups=groups,
        public=public,
        permissions="cifs-acl",
    )


def extract_acl(filepath: str) -> FileACL:
    """ACL POSIX de base via os.stat() — ou, sur un partage CIFS (voir
    extract_windows_acl()), ACL NTFS via getcifsacl. getfacl n'y renvoie
    jamais rien d'utile : un partage Windows natif n'expose pas
    d'extensions Unix en SMB2/3, donc pas d'uid/gid réel par fichier
    (uid/gid identiques partout, imposés par les options de montage
    forceuid/forcegid — voir docker-compose.yml)."""
    if _is_cifs_path(filepath):
        return extract_windows_acl(filepath)

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
