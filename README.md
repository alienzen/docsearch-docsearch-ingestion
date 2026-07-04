# docsearch-ingestion

Composant d'indexation de **DocSearch** : extraction de contenu, calcul des
ACL, indexation initiale, indexation continue (Kafka) et surveillance de
dossier en temps réel.

Fait partie de l'écosystème DocSearch, découpé en plusieurs dépôts :

| Dépôt | Rôle |
|---|---|
| **docsearch-ingestion** (ce dépôt) | Extraction, ACL, indexation |
| [docsearch-api](../docsearch-api) | API de recherche (FastAPI) |
| [docsearch-ui](../docsearch-ui) | Interface web statique |
| [docsearch-infra](../docsearch-infra) | Orchestration Docker Compose |
| [docsearch-docs](../docsearch-docs) | Documents commerciaux |

## Contenu

```
app/
├── indexer.py         # Indexation initiale (parcours + ACL + Tika)
├── worker.py           # Workers Kafka — indexation continue
├── watcher.py           # Surveillance temps réel (PollingObserver)
├── acl_extractor.py     # Extraction ACL POSIX + getfacl
└── pst_extractor.py     # Indexation des emails PST (Outlook)
```

## Dépendances externes (fournies par docsearch-infra)

- **Elasticsearch** — cluster cible de l'indexation
- **Apache Tika** (×4) — extraction de texte, appelé via HTTP
- **Kafka** (KRaft) — file de messages pour l'indexation continue
- Volume `/documents` — dossier source monté en lecture seule

## Format des documents indexés

```json
{
  "filename": "rapport.pdf",
  "filepath": "/documents/finance/rapport.pdf",
  "content": "...",
  "acl": {
    "owner": "dupont",
    "group": "finance",
    "users": ["dupont"],
    "groups": ["finance"],
    "public": false,
    "permissions": "640"
  }
}
```

`doc_id` est le hash MD5 du chemin **normalisé** (`Path(filepath).resolve()`)
— identique pour l'indexation et la suppression, y compris après que le
fichier a été supprimé du disque.

## Lancer en local (nécessite un ES + Kafka + Tika déjà démarrés)

```bash
cp .env.example .env
docker build -t docsearch-ingestion .

# Indexation initiale (one-shot)
docker run --rm --env-file .env -v /chemin/documents:/documents:ro \
  --network docsearch-infra_docsearch-net \
  docsearch-ingestion python indexer.py

# Watcher (démon)
docker run -d --env-file .env -v /chemin/documents:/documents:ro \
  --network docsearch-infra_docsearch-net \
  docsearch-ingestion python watcher.py
```

En pratique, ces conteneurs sont orchestrés par `docsearch-infra` — voir
son README pour le déploiement complet.

## Points d'attention

- **doc_id harmonisé** : `indexer.py`, `worker.py` et `watcher.py` doivent
  toujours calculer `doc_id` de la même façon
  (`md5(str(Path(filepath).resolve()))`). Toute divergence casse la
  suppression et la détection de doublons.
- **CIFS/NFS** : le watcher utilise `PollingObserver`, pas `Observer`
  (inotify ne fonctionne pas sur les montages réseau).
- **PST** : `pst_extractor.py` importe `pff` (paquet apt `python3-libpff`),
  jamais `pypff` (non publié sur PyPI).
