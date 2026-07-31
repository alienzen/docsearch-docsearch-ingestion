# Contribuer à DocSearch

## Mise en route

1. Cloner le dépôt : `git clone <url> && cd docsearch`
2. Copier la configuration : `cp .env.example .env`
3. Adapter `DOCS_PATH` dans `.env` vers un dossier de documents de test
4. Lancer le stack de dev : `chmod +x manage.sh && sudo ./manage.sh start`
5. Indexer un échantillon : `sudo ./manage.sh init`

Voir `guide_install_virtualbox.docx` pour une installation pas à pas sur VM VirtualBox.

## Structure du projet

```
docsearch/
├── quadlet/                       # Unités systemd (Quadlet) — orchestration
│   ├── common/                    # Cible, réseau, modèles de configuration
│   ├── dev/                       # Pile complète sur une machine
│   └── roles/                     # Déploiement 8 machines, par rôle
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

# Valider les unités Quadlet (aucune erreur de génération attendue)
/usr/libexec/podman/quadlet -dryrun

# Build de l'image applicative
podman build -t localhost/docsearch/ingestion:test .
```

## Versions de référence

| Composant | Version |
|---|---|
| Elasticsearch | 9.4.2 |
| Apache Tika | 3.3.1.0 |
| Python | 3.12 |
| Java (image Tika) | 17 |

Penser à vérifier que ces versions sont toujours à jour avant chaque release majeure.

## Dépannage — pypff (extraction PST)

`pypff` n'est pas distribué sur PyPI. Le `Dockerfile` installe le
paquet apt `python3-pypff` (⚠️ **pas** `python3-libpff`, qui n'existe
pas — vérifié sur packages.debian.org).

**Le piège principal : deux Python différents dans la même image.**
L'image de base `python:3.12-slim` installe son propre Python dans
`/usr/local/bin/python3` (utilisé par `pip install` et tout le reste
de l'application). Le paquet apt `python3-pypff` compile l'extension
contre le **Python système Debian**, dans `/usr/bin/python3` — un
exécutable totalement différent. Une extension C compilée pour l'un
**n'est pas chargeable** par l'autre. Si `import pypff` échoue après
une installation apt apparemment réussie, c'est presque toujours ça :
le `python3` invoqué (via le PATH) résout vers `/usr/local/bin/python3`
(3.12, sans pypff) plutôt que `/usr/bin/python3` (Debian, avec pypff).

Référence : <https://github.com/docker-library/python/issues/671>

**Solution retenue** (voir `pst_extractor.py` / `pst_worker.py`) :
isoler tout ce qui touche à `pypff` dans un sous-processus invoqué
avec le chemin **explicite** `/usr/bin/python3`, jamais `python3` nu.
`pst_worker.py` est un script minimal (aucune dépendance hors stdlib +
pypff) qui lit le PST et retourne une ligne JSON par email sur stdout ;
`pst_extractor.py` (qui tourne sous Python 3.12 normal) parse cette
sortie et gère ACL + indexation ES comme avant.

**Vérifier que pypff est bien installé et accessible :**

```bash
podman build -t localhost/docsearch/ingestion:test .
podman run --rm localhost/docsearch/ingestion:test /usr/bin/python3 -c "import pypff; print(pypff.get_version())"
```

Si cette commande échoue alors que `apt-get install python3-pypff`
n'a signalé aucune erreur au build, vérifier qu'aucun code n'invoque
`python3` (sans chemin absolu) pour quoi que ce soit lié à pypff —
ça résoudrait silencieusement vers le mauvais interpréteur.
