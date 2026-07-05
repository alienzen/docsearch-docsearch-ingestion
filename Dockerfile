# ── docsearch-ingestion — Image Python ────────────────────────
# Indexation initiale, workers Kafka, watcher (surveillance dossier)
# Python 3.12 · pypff via apt (extraction PST) · ACL POSIX

FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    # ACL étendues POSIX (getfacl / setfacl)
    acl \
    # Binding Python pour libpff (archives PST Outlook) — le paquet
    # s'appelle python3-pypff (PAS python3-libpff, qui n'existe pas),
    # disponible directement via apt sur Debian 12, jamais publié sur
    # PyPI. Module Python fourni : "pypff" (voir vérification ci-dessous).
    python3-pypff \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Vérification que le module pypff est bien disponible
RUN python3 -c "import pypff; print('pypff OK')"

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
