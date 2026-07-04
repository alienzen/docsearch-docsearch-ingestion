# Contribuer à DocSearch

## Mise en route

1. Cloner le dépôt : `git clone <url> && cd docsearch`
2. Copier la configuration : `cp .env.example .env`
3. Adapter `DOCS_PATH` dans `.env` vers un dossier de documents de test
4. Lancer le stack de dev : `chmod +x manage.sh && ./manage.sh start`
5. Indexer un échantillon : `./manage.sh init`

Voir `guide_install_virtualbox.docx` pour une installation pas à pas sur VM VirtualBox.

## Structure du projet

```
docsearch/
├── docker-compose.yml             # Stack complet (production)
├── docker-compose.override.yml    # Overrides développement (auto-appliqué)
├── .env.example                   # Modèle de configuration
├── manage.sh                      # Script de gestion (start/stop/init/logs...)
├── nginx/nginx.conf               # Reverse proxy + emplacement SSO
└── 
    ├── indexer.py                 # Indexation initiale + ACL POSIX
    ├── worker.py                  # Workers Kafka parallèles
    ├── watcher.py                 # Surveillance temps réel + ACL
    ├── search_api.py              # API FastAPI + filtrage ACL
    ├── acl_extractor.py           # Extraction ACL POSIX / getfacl
    ├── ldap_resolver.py           # Résolution groupes LDAP/AD
    ├── pst_extractor.py           # Indexation emails PST
    └── templates/                 # Interface web (index.html, chat.html)
```

## Branches

- `main` — code stable, déployable
- `develop` — intégration des fonctionnalités en cours

## Avant de proposer une Pull Request

```bash
# Linter Python
pip install ruff
ruff check 

# Valider la syntaxe Docker Compose
docker compose config --quiet

# Build de l'image applicative
docker build -t docsearch-app:test ./app
```

## Versions de référence

| Composant | Version |
|---|---|
| Elasticsearch | 9.4.2 |
| Apache Tika | 3.3.1.0 |
| Python | 3.12 |
| Java (image Tika) | 17 |

Penser à vérifier que ces versions sont toujours à jour avant chaque release majeure.

## Dépannage build — pypff / libpff

`pypff` n'est pas distribué sur PyPI. Le `Dockerfile` le compile depuis les
sources officielles [libyal/libpff](https://github.com/libyal/libpff) dans
un stage de build dédié (`pffbuilder`), avant de copier uniquement le
résultat compilé dans l'image finale.

**Si le build échoue avec une erreur réseau (proxy d'entreprise, accès
GitHub bloqué) :**

```bash
# Option 1 — utiliser un miroir interne du dépôt libpff
# Modifier l'URL dans Dockerfile :
#   git clone --depth 1 https://votre-miroir-interne/libpff.git /tmp/libpff

# Option 2 — pré-télécharger une release tar.gz et l'ajouter au contexte
# de build, puis remplacer le git clone par une extraction locale.

# Option 3 — désactiver temporairement le support PST
# Commenter l'import de pypff dans pst_extractor.py et retirer
# le bloc "STAGE 1" du Dockerfile. L'indexation des autres formats
# (PDF, DOCX, XLSX, PPTX, TXT) n'est pas affectée.
```

**Vérifier que pypff est bien installé dans l'image :**

```bash
docker compose build api
docker compose run --rm api python3 -c "import pypff; print(pypff.get_version())"
```

**Liste officielle des outils requis (source : [wiki libyal/libpff — Building](https://github.com/libyal/libpff/wiki/Building)) :**

```
git aclocal autoconf automake autopoint libtoolize pkg-config
```

Sur Debian/Ubuntu, ces commandes sont fournies par les paquets apt
`git autoconf automake autopoint libtool pkg-config` — c'est exactement
la liste installée dans le stage `pffbuilder` du `Dockerfile`. Le
Dockerfile vérifie explicitement la présence de chaque outil avant de
lancer la compilation (`command -v <tool>`), pour échouer avec un
message clair plutôt qu'au milieu de `autogen.sh`.

**Erreur "Unable to find: pkg-config" ou "Can't exec autopoint" :**

Ces deux outils sont fournis par des paquets apt distincts et
**ne sont pas inclus** dans `build-essential` ni dans `gettext` seul :
`autopoint` nécessite explicitement le paquet `autopoint`, `pkg-config`
le paquet `pkg-config` (ou `pkgconf` selon la version de Debian). Le
Dockerfile actuel installe déjà les deux — si l'erreur persiste,
vérifier qu'aucun cache Docker périmé n'est réutilisé :

```bash
docker compose build --no-cache api
```

**Erreur "Autoconf version 2.71 or higher is required" :**

Connue sur les images de base avec une version d'autoconf ancienne.
L'image `python:3.12-slim` (Debian 12 "bookworm") fournit autoconf
2.71+, donc ce cas ne devrait pas se produire avec le Dockerfile
fourni. Si vous avez modifié l'image de base vers une distribution
plus ancienne, mettre à jour autoconf manuellement ou revenir à
`python:3.12-slim`.

**Erreur "mkdir: Permission denied" sur `/install/usr/include` pendant
`make install` :**

Le stage `pffbuilder` s'exécute entièrement en `root` (aucune
instruction `USER` n'y est définie), donc ce n'est pas un problème de
droits Unix classique. La cause la plus fréquente est que certains
environnements Docker (BuildKit avec cache distant, politique de
sécurité du démon hôte type rootless Docker ou Podman) restreignent
l'écriture directe à la racine `/` du système de fichiers du conteneur
intermédiaire — même en root. Le Dockerfile installe désormais
explicitement sous `/build/install` (créé avec `mkdir -p` et des droits
`755` avant toute compilation) plutôt que sous `/install` à la racine.
Si l'erreur persiste avec ce chemin également, c'est le signe que le
moteur Docker hôte impose des restrictions plus larges — vérifier
notamment l'usage de Docker en mode rootless :

```bash
docker info | grep -i rootless
```

```bash
# Lancer un conteneur interactif basé sur la même image de base
docker run --rm -it python:3.12-slim bash

# Dans le conteneur : cloner et tenter la chaîne de build à la main
apt-get update && apt-get install -y git build-essential autoconf \
  automake libtool libtool-bin pkg-config pkgconf gettext python3-dev
git clone --depth 1 https://github.com/libyal/libpff.git /tmp/libpff
cd /tmp/libpff && ./synclibs.sh && ./autogen.sh
# Lire attentivement la sortie : autogen.sh liste explicitement
# chaque outil manquant avant d'échouer.
```
