# ── docsearch-ingestion — Image Python ────────────────────────
# Indexation initiale, workers Kafka, watcher (surveillance dossier)
# Python 3.12 · pypff via apt (extraction PST) · ACL POSIX

FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    # ACL étendues POSIX (getfacl / setfacl)
    acl \
    # ACL NTFS sur partages CIFS/Windows (getcifsacl / setcifsacl) —
    # getfacl seul ne renvoie rien d'exploitable sur ce type de partage,
    # voir acl_extractor.py:extract_windows_acl()
    cifs-utils \
    # Binding Python pour libpff (archives PST Outlook) — le paquet
    # s'appelle python3-pypff (PAS python3-libpff, qui n'existe pas),
    # disponible directement via apt sur Debian 12, jamais publié sur
    # PyPI. Module Python fourni : "pypff".
    #
    # IMPORTANT : ce paquet compile pypff contre le Python SYSTÈME
    # Debian (/usr/bin/python3), qui est un exécutable DIFFÉRENT du
    # Python 3.12 fourni par cette image de base (/usr/local/bin/python3,
    # utilisé par le reste de l'application via pip). Une extension C
    # compilée pour l'un n'est PAS chargeable par l'autre — d'où
    # l'installation explicite du paquet système "python3" ci-dessous,
    # et l'usage exclusif du chemin /usr/bin/python3 (jamais "python3"
    # nu, qui résoudrait vers /usr/local/bin/python3 via le PATH) pour
    # tout ce qui a besoin de pypff (voir pst_extractor.py/pst_worker.py
    # et https://github.com/docker-library/python/issues/671).
    python3 \
    python3-pypff \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Vérification EXPLICITE avec /usr/bin/python3 — "python3" seul
# résoudrait vers /usr/local/bin/python3 (3.12, l'image de base) où
# pypff n'est PAS installé, et cette vérification échouerait à tort.
RUN /usr/bin/python3 -c "import pypff; print('pypff OK (Python système)')"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .

# UID configurable pour correspondre au propriétaire du volume
# /documents monté depuis l'hôte (voir README.md)
ARG DOCKER_UID=1000
RUN useradd -m -u ${DOCKER_UID} appuser 2>/dev/null || useradd -m appuser && \
    chown -R appuser /app
USER appuser

# Pas de CMD par défaut : le service (indexer.py / worker.py / watcher.py)
# est choisi via la directive "command:" du docker-compose qui utilise
# cette image (voir docsearch-infra).
