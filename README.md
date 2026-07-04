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
├── archive_extractor.py # Extraction sécurisée d'archives (zip, tar.*, 7z)
└── pst_extractor.py     # Indexation des emails PST (Outlook)
```

## Archives supportées

Le contenu des archives est indexé automatiquement — chaque fichier
supporté à l'intérieur devient un document, avec les **ACL héritées de
l'archive elle-même** (comme pour les emails d'un fichier PST) :

| Format | Dépendance |
|---|---|
| `.zip` | Bibliothèque standard (`zipfile`) |
| `.tar`, `.tar.gz`/`.tgz`, `.tar.bz2`/`.tbz2`, `.tar.xz`/`.txz` | Bibliothèque standard (`tarfile`) |
| `.7z` | `py7zr` (inclus dans `requirements.txt`) |

Le document indexé porte l'identité `chemin/archive.zip::dossier/fichier.pdf`
dans son champ `filepath` — il n'existe pas de fichier disque réel à cette
adresse (extraction dans un dossier temporaire, nettoyé après indexation).
L'aperçu (`/api/preview`) n'est donc pas disponible pour ces documents,
seule la recherche l'est.

**Sécurité** — protection contre les archives malveillantes :
- **Zip slip** : chemins `../../` dans l'archive détectés et bloqués
- **Zip bomb** : limites configurables sur le nombre de fichiers
  (`ARCHIVE_MAX_FILES`, défaut 5000) et la taille décompressée totale
  (`ARCHIVE_MAX_TOTAL_SIZE_MB`, défaut 1000 Mo)
- **Archives imbriquées** : profondeur limitée par `ARCHIVE_MAX_DEPTH`
  (défaut 1 — une archive dans une archive, pas plus)

La suppression d'une archive supprime automatiquement tous ses membres
de l'index (recherche par préfixe sur le champ `filepath`).

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

## Architecture d'indexation — producer / workers

```
producer.py                Kafka                  worker.py ×N (parallèle)
──────────────    scan     ──────────    consume   ─────────────────────────
Parcourt              →   documents-   →           Extraction Tika (I/O)
DOCS_FOLDER,               to-index                + calcul ACL
publie chaque               topic                  + indexation ES (bulk)
référence de                (16 partitions)
fichier (rapide,
non bloquant)
```

`producer.py` remplace l'ancienne indexation séquentielle (un seul
processus qui parcourt puis indexe fichier par fichier). Il se contente
de lister les fichiers et de publier leur chemin sur Kafka — opération
rapide qui ne dépend pas d'Elasticsearch ni de Tika.

Le vrai travail (extraction Tika, qui est l'opération la plus lente,
suivi de l'indexation ES) est fait par plusieurs **réplicas** du service
`worker`, qui consomment le topic Kafka en parallèle. C'est ce qui donne
un débit d'indexation élevé — la charge est distribuée sur N workers au
lieu d'un seul processus séquentiel.

### Augmenter le débit

```bash
# Depuis docsearch-infra :
./manage.sh scale-workers 12   # 12 workers en parallèle (recommandé
                                # pour de gros volumes en production)
```

**Le nombre de partitions du topic Kafka doit être ≥ au nombre de
workers** — sinon certains workers ne recevront jamais de messages
(le parallélisme d'un groupe de consumers Kafka est plafonné par le
nombre de partitions). C'est réglé via `KAFKA_NUM_PARTITIONS` dans
`docker-compose.yml` de `docsearch-infra` (16 par défaut).

Les 4 instances Tika (`tika1`-`tika4`) sont choisies aléatoirement par
chaque worker (`random.choice(TIKA_SERVERS)`) — elles absorbent la
charge de plusieurs workers simultanés sans configuration supplémentaire.

### Le watcher n'utilise pas ce pipeline

`watcher.py` (surveillance temps réel) appelle `index_file()` de
`indexer.py` **directement**, sans passer par Kafka — le volume de
fichiers modifiés en continu est généralement trop faible pour
justifier la complexité d'une file de messages. Le pipeline
producer/workers est réservé aux gros volumes (indexation initiale,
réindexation complète).

## Lancer en local (nécessite un ES + Kafka + Tika déjà démarrés)

```bash
cp .env.example .env
docker build -t docsearch-ingestion .

# Indexation initiale — scan + publication Kafka (rapide, non bloquant)
docker run --rm --env-file .env -v /chemin/documents:/documents:ro \
  --network docsearch-infra_docsearch-net \
  docsearch-ingestion python producer.py

# Workers — plusieurs instances en parallèle pour un débit élevé
# (le travail lourd — extraction Tika + indexation ES — se fait ici)
docker run -d --env-file .env -v /chemin/documents:/documents:ro \
  --network docsearch-infra_docsearch-net \
  docsearch-ingestion python worker.py
# → lancer plusieurs conteneurs de ce type pour paralléliser

# Watcher (démon) — indexation temps réel, appelle index_file()
# directement (pas via Kafka, volume trop faible pour justifier le
# passage par une file de messages)
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
