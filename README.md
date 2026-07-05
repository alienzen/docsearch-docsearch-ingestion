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

## Inclusion / exclusion de sous-dossiers

Certains sous-dossiers de `DOCS_FOLDER` peuvent être exclus (liste
noire) ou, à l'inverse, être les **seuls** indexés (liste blanche) —
modifiable à chaud, sans redémarrage (même mécanisme Redis) :

```bash
./manage.sh exclude-path finance/confidentiel
./manage.sh exclude-path "*/tmp"          # guillemets nécessaires (le shell ne doit pas développer le *)
./manage.sh exclude-path "*.cache"
./manage.sh include-path finance           # bascule en liste blanche
./manage.sh list-path-filters
./manage.sh remove-path-filter finance/confidentiel
```

**Règles de correspondance** (voir `path_filter.py`) :
- Chemins toujours **relatifs à `DOCS_FOLDER`**, jamais absolus
- **L'exclusion est prioritaire** sur l'inclusion — un chemin exclu le
  reste même s'il correspond aussi à un motif inclus
- Motif **sans `/`** (`tmp`, `*.cache`) : correspond à un composant de
  chemin à n'importe quel niveau de profondeur (comme `.gitignore`)
- Motif **avec `/`** (`finance/confidentiel`) : ancré — correspond au
  chemin complet ou à un préfixe de dossier (exclure un dossier exclut
  automatiquement tout son contenu)

**Où le filtre s'applique** :
- `producer.py` **élague `os.walk`** — un dossier exclu n'est même pas
  parcouru (gain de temps réel sur un sous-arbre volumineux). Seule la
  liste noire sert à l'élagage (la liste blanche ne peut pas élaguer
  sans risque de sauter un ancêtre nécessaire — voir `is_dir_excluded`)
- `watcher.py` vérifie chaque événement (création, modification,
  déplacement) — y compris le cas d'un fichier **déplacé vers** un
  dossier désormais exclu (retiré de l'index plutôt que renommé) et le
  renommage d'un **dossier entier** vers une zone exclue
- `indexer.py` / `index_file()` : contrôle définitif, point commun

⚠️ **Les documents déjà indexés dans un dossier fraîchement exclu ne
sont pas supprimés automatiquement** — la règle ne s'applique qu'aux
nouveaux passages (scan ou événement temps réel). Utiliser
`./manage.sh purge-path <motif>` pour nettoyer l'existant (aperçu
avant confirmation, même syntaxe glob que `exclude-path`) :

```bash
./manage.sh exclude-path finance/confidentiel   # pour le futur
./manage.sh purge-path finance/confidentiel     # nettoie l'existant
```

`purge_path()` (dans `indexer.py`) utilise le scan/scroll ES (pas une
simple recherche limitée) pour rester correct même sur un index de
plusieurs millions de documents. Pour les membres d'archive, seul
l'emplacement de l'**archive elle-même** est comparé au motif (jamais
le chemin interne d'un membre) — cohérent avec la façon dont
`index_file()` filtre les archives à l'indexation.

## Paramètres opérationnels dynamiques

Au-delà des types de fichiers, les réglages suivants sont aussi
modifiables à chaud (même mécanisme Redis, voir `runtime_config.py`) :

| Paramètre | Défaut | Effectif après |
|---|---|---|
| `archive_max_files` | 5000 | Immédiat (relu à chaque archive ouverte) |
| `archive_max_total_size_mb` | 1000 | Immédiat |
| `archive_max_depth` | 1 | Immédiat |
| `worker_flush_interval` | 10 (s) | ≤ 10s (relu à chaque itération du worker) |
| `worker_batch_size` | 200 | ⚠️ Le seuil de flush bulk() est immédiat, mais `max_poll_records` (réglage bas niveau du consumer Kafka) reste fixé au démarrage — redémarrer le worker pour un effet complet |
| `watcher_poll_interval` | 10 (s) | ≤ 5s (le watcher détecte le changement et redémarre son propre observateur automatiquement, sans redémarrer le conteneur) |

```bash
./manage.sh get-config
./manage.sh set-config archive_max_depth 2
./manage.sh set-config worker_flush_interval 5
./manage.sh set-config watcher_poll_interval 3
```

**Résilience** : comme pour les types de fichiers, un Redis injoignable
fait retomber sur les valeurs des variables d'environnement (elles-mêmes
avec un défaut) — jamais d'arrêt de service pour un problème de config.

## Configuration dynamique des types de fichiers

Les extensions indexées et leur taille maximale sont modifiables à
chaud (Redis), **sans redémarrer** `producer.py`, `worker.py` ni
`watcher.py` :

```bash
# Depuis docsearch-infra :
./manage.sh get-filetypes
./manage.sh set-filetype jpg --enabled true --max-size 5
./manage.sh set-filetype pdf --max-size 100
./manage.sh set-filetype docx --enabled false

# Les formats d'archive sont couverts de la même façon :
./manage.sh set-filetype zip --enabled false      # désactive tout .zip
./manage.sh set-filetype tar.gz --max-size 200     # limite les .tar.gz à 200 Mo
```

⚠️ **Extensions composées** (`tar.gz`, `tar.bz2`, `tar.xz`) : la clé
de configuration correspond à `archive_extractor.archive_kind()`, pas
à l'extension de fichier brute — `Path("x.tar.gz").suffix` ne renvoie
que `.gz`, ce qui matcherait à tort une entrée générique. Toutes les
clés d'archive disponibles : `zip`, `tar`, `tar.gz`, `tgz`, `tar.bz2`,
`tbz2`, `tar.xz`, `txz`, `7z`.

Pour un fichier archive, `max_size_mb` limite la taille du **fichier
archive lui-même** avant toute extraction — distinct de
`archive_max_total_size_mb` (paramètre opérationnel, voir plus bas)
qui limite la taille **décompressée totale** de son contenu.

Le changement est pris en compte dans un délai maximal de
`FILETYPE_CONFIG_CACHE_TTL` secondes (10s par défaut) par tous les
conteneurs déjà démarrés — chacun relit Redis périodiquement plutôt
qu'une seule fois au démarrage.

**Où le contrôle s'applique** (voir `filetype_config.py`) :
- `producer.py` : avant publication sur Kafka (évite de publier des
  messages pour des fichiers qui seront rejetés de toute façon) —
  s'applique aussi aux archives elles-mêmes, pas seulement aux
  fichiers normaux
- `worker.py` : re-vérifié avant l'appel Tika / l'extraction d'archive
  (les opérations les plus coûteuses à éviter)
- `indexer.py` (`index_file`) : contrôle définitif de l'archive
  elle-même, point d'entrée commun utilisé aussi par `watcher.py`
- Chaque **membre d'archive** est en plus vérifié individuellement
  dans `_process_archive` (un fichier trop volumineux à l'intérieur
  d'une archive par ailleurs autorisée reste filtré)

**Résilience** : si Redis est injoignable, repli automatique sur une
configuration par défaut codée en dur (`DEFAULT_CONFIG` dans
`filetype_config.py`) — l'ingestion ne s'arrête jamais pour un problème
de configuration.

## Renommage/déplacement — pas de réextraction Tika

Renommer un fichier ou un **dossier entier** ne relance jamais Tika :
le contenu déjà extrait est copié vers la nouvelle identité en base ES
(réécriture du seul champ `filepath`), ce qui est quasi instantané
même pour un dossier contenant des milliers de fichiers.

Couvre trois cas :
- Renommage d'un fichier normal (`_copy_document`)
- Renommage d'un **dossier entier** — tous les documents dont le
  chemin commence par l'ancien préfixe sont mis à jour en une seule
  recherche ES (`_rename_prefix`)
- Renommage d'une **archive** — tous ses membres indexés
  (`archive.zip::membre`) sont mis à jour (`_rename_archive_members`)

Les ACL sont rafraîchies à cette occasion (`extract_acl`, opération
légère : `stat` + `getfacl`, sans rapport avec le coût de Tika).

⚠️ **Limite connue sur CIFS/NFS** : cette optimisation suppose que
watchdog détecte l'opération comme un véritable renommage (`on_moved`),
ce qui dépend de la stabilité des numéros d'inode sur le montage
réseau. Si le serveur CIFS ne les préserve pas de façon fiable,
watchdog peut percevoir un renommage comme une suppression suivie
d'une création — dans ce cas, le fichier est réindexé normalement
(avec Tika) car aucun événement de déplacement n'est jamais généré.
Si vous observez encore des réextractions complètes après cette mise
à jour, c'est probablement le signe de ce cas de figure.

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
