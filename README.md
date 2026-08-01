# donnee2-pipeline

Pipeline de collecte, nettoyage, chargement et analyse de données de qualité de
l'air, basé sur l'API [OpenWeather Air Pollution](https://openweathermap.org/api/air-pollution).

Cinq villes sont suivies : **Amsterdam, Antananarivo, Beijing, Londres, Paris**.

## Sommaire

- [Architecture](#architecture)
- [Structure du dépôt](#structure-du-dépôt)
- [Prérequis](#prérequis)
- [Installation](#installation)
- [Configuration](#configuration)
- [Utilisation](#utilisation)
- [Orchestration avec Airflow](#orchestration-avec-airflow)
- [Démarrage rapide avec Docker Compose](#démarrage-rapide-avec-docker-compose)
- [Schéma de l'entrepôt de données](#schéma-de-lentrepôt-de-données)
- [Limitations connues](#limitations-connues)

## Architecture

Le pipeline suit un flux ETL classique :

```
API OpenWeather ──▶ raw/ (JSON brut par ville) ──▶ clean/qualite_air.csv ──▶ Postgres (star schema)
     │                                                      │
     │ (collecte horaire)                                   └──▶ EDA (analyse exploratoire, logs)
     └ (backfill historique, par mois)
```

- **Extraction** : `scripts/collect.py` (temps réel, horaire) et `scripts/Backfill.py`
  (historique, mois par mois) interrogent l'API et sauvegardent chaque réponse
  brute en JSON dans `raw/<ABBR>/`.
- **Nettoyage** : `scripts/Clean.py` relit tous les fichiers `raw/`, en extrait une
  ligne par mesure horaire, déduplique par (ville, timestamp), et produit
  `clean/qualite_air.csv`.
- **Chargement** : `scripts/transform.py` charge ce CSV dans un entrepôt Postgres
  organisé en schéma en étoile (voir plus bas).
- **Orchestration** : `scripts/fetch_current.py` enchaîne collecte → nettoyage →
  chargement → EDA en une seule commande. Un DAG Airflow (`airflow/dags/weather_dags.py`)
  automatise séparément un backfill de 12 mois.

## Structure du dépôt

```
donnee2-pipeline/
├── config/
│   └── cities.json          # Référence des villes suivies (non lue par le code actuellement)
├── scripts/
│   ├── commun.py             # Config partagée : villes, endpoints API, logging, retry HTTP
│   ├── collect.py            # Collecte temps réel (une exécution = un instantané par ville)
│   ├── Backfill.py           # Backfill historique, découpé par mois, reprenable (--force pour re-télécharger)
│   ├── Clean.py               # raw/ (JSON) → clean/qualite_air.csv
│   ├── transform.py           # clean/qualite_air.csv → Postgres (schéma en étoile)
│   ├── fetch_current.py       # Orchestrateur : collecte + nettoyage + chargement + EDA
│   └── separate_cities.py     # Découpe clean/qualite_air.csv en un CSV par ville (pratique pour Colab/Sheets)
├── airflow/
│   └── dags/
│       └── weather_dags.py    # DAG Airflow : backfill 12 mois → CSV (indépendant du pipeline Postgres ci-dessus)
├── data/
│   ├── raw/                   # JSON brut par ville (généré, gitignored en pratique)
│   ├── clean/                 # CSV nettoyé et dédupliqué
│   ├── separated_clean/       # CSV par ville
│   └── star_schema/
│       └── star_schema.sql    # DDL de l'entrepôt (dim_ville, dim_date, dim_heure, fait_qualite_air)
├── docker-compose.yml         # Postgres + Airflow (standalone) + conteneur pipeline
├── requirements.txt
├── .env.example
└── .gitignore
```

## Prérequis

- Python 3.10+
- Une clé API [OpenWeather](https://openweathermap.org/api) (plan gratuit suffisant)
- PostgreSQL (local, via Docker Compose, ou un service managé type Neon) si vous
  utilisez `transform.py`
- Docker + Docker Compose (optionnel, pour l'exécution conteneurisée)

## Installation

```bash
git clone https://github.com/N0mena/donnee2-pipeline.git
cd donnee2-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

> ⚠️ `requirements.txt` ne liste actuellement que `requests` et `python-dotenv`.
> `transform.py` a aussi besoin de `psycopg2-binary`, et `separate_cities.py` de
> `pandas`. En attendant la mise à jour du fichier, installez-les manuellement :
> ```bash
> pip install psycopg2-binary pandas
> ```

## Configuration

Copiez `.env.example` vers `.env` et renseignez vos valeurs :

```bash
cp .env.example .env
```

```dotenv
OPENWEATHER_API_KEY=votre_cle_api

GEOCODE_URL=https://api.openweathermap.org/geo/1.0/direct
AIR_POLLUTION_HISTORY_URL=https://api.openweathermap.org/data/2.5/air_pollution/history

NEON_CONNECTION_STRING=votre_chaine_de_connexion
```

Pour `transform.py` (connexion Postgres via `psycopg2`, hors chaîne de connexion
unique), les variables suivantes sont attendues séparément :

```dotenv
PGHOST=localhost
PGPORT=5432
PGDATABASE=qualite_air_dw
PGUSER=postgres
PGPASSWORD=postgres
```

## Utilisation

Toutes les commandes ci-dessous s'exécutent depuis `scripts/` (les imports
internes sont relatifs à ce dossier) :

```bash
cd scripts
```

**Collecte temps réel** (un instantané par ville, à planifier toutes les heures) :
```bash
python collect.py
```

**Backfill historique** (12 mois par défaut, reprenable) :
```bash
python Backfill.py                 # 12 mois, toutes les villes
python Backfill.py --mois 6        # 6 mois
python Backfill.py --ville TNR     # une seule ville
python Backfill.py --force         # re-télécharge même si déjà présent
```

**Nettoyage** (reconstruit `clean/qualite_air.csv` à partir de tout `raw/`) :
```bash
python Clean.py
```

**Chargement dans l'entrepôt Postgres** :
```bash
python transform.py                # charge clean/qualite_air.csv
python transform.py --reset        # vide les tables avant de recharger
python transform.py --csv chemin/vers/autre.csv
```

**Pipeline complet** (collecte → nettoyage → chargement → EDA) :
```bash
python fetch_current.py
python fetch_current.py --eda-only   # relance uniquement l'analyse exploratoire
```

**Export par ville** (pour analyse dans Colab, Google Sheets, etc.) :
```bash
python separate_cities.py
```

## Orchestration avec Airflow

`airflow/dags/weather_dags.py` définit `openweather_aqi_history_backfill` :
géocodage des villes → récupération de 12 mois d'historique → export CSV dans
`/opt/airflow/data/aqi_history/`. Déclenchement manuel uniquement (`schedule=None`).

Variables Airflow requises (Admin → Variables) :
- `OPENWEATHER_API_KEY`
- `aqi_cities` *(optionnel, liste JSON de villes ; retombe sur les 5 villes par défaut si absente)*

> Ce DAG écrit uniquement un CSV — il n'alimente pas (encore) le schéma en
> étoile Postgres décrit plus bas. C'est un pipeline parallèle à
> `fetch_current.py` / `transform.py`, pas un remplacement.

## Démarrage rapide avec Docker Compose

```bash
docker compose up -d
```

Cela démarre :
- **postgres** — Postgres 15, base `qualite_air_dw` par défaut, expose le port `5432`
- **airflow** — Airflow en mode `standalone`, UI sur [http://localhost:8080](http://localhost:8080)
- **pipeline_runner** — conteneur prêt à exécuter les scripts de `scripts/` manuellement

## Schéma de l'entrepôt de données

Schéma en étoile défini dans `data/star_schema/star_schema.sql` :

- **`dim_ville`** — ville, code, pays, latitude/longitude
- **`dim_date`** — date, jour, mois, trimestre, année, jour de la semaine
- **`dim_heure`** — heure exacte (HH:MM), heure, minute, période de la journée (nuit/matin/après-midi/soirée)
- **`fait_qualite_air`** — AQI et concentrations (CO, NO, NO₂, O₃, SO₂, PM2.5, PM10, NH₃), une ligne par (ville, date, heure)
- **`vue_qualite_air_complete`** — vue dénormalisée joignant les 4 tables, pratique pour l'analyse ou le branchement d'un outil de BI

## Limitations connues

- `config/cities.json` n'est pas lu par le code : la liste des villes suivies
  vit uniquement dans `VILLES` (`scripts/commun.py`). Les deux fichiers
  peuvent diverger silencieusement.
- Incohérence de nommage : `commun.py` utilise **« Londres »**, tandis que le
  DAG Airflow utilise **« London »** — à garder en tête si vous rapprochez les
  deux sources.
- `dim_heure` est unique sur l'heure **exacte** (`heure_utc`, avec les
  minutes). Comme les exécutions ne tombent pas toujours à la même minute,
  cette dimension grossit plus vite qu'une dimension « heure de la journée »
  classique (24 lignes réutilisables) ne le ferait.
- `requirements.txt` est incomplet (voir [Installation](#installation)).
- Le DAG Airflow (backfill CSV) et le pipeline `transform.py` (chargement
  Postgres) sont deux chemins de données indépendants qui ne se rejoignent pas
  encore.