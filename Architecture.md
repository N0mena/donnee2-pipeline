# Architecture du pipeline — donnee2-pipeline

## Vue d'ensemble

```
OpenWeatherMap (Air Pollution API)
        │  collecte horaire (collect.py) + backfill (backfill.py)
        ▼
   ORCHESTRATEUR (Airflow)
        ▼
   STOCKAGE
     raw/    fichiers JSON bruts, un par ville et par appel, jamais modifiés
     clean/  qualite_air.csv, reconstruit à chaque run depuis raw/ (clean.py)
        ▼
   DATA WAREHOUSE — PostgreSQL, schéma en étoile (transform.py)
     fait_qualite_air + dim_ville + dim_date + dim_heure
```

## Choix techniques

### Source de données
**Choix** : OpenWeatherMap — Air Pollution API (endpoint temps réel `/data/2.5/air_pollution` et endpoint historique `/data/2.5/air_pollution/history`).
**Justification** : API gratuite couvrant les 5 villes via coordonnées lat/lon, renvoie l'AQI (échelle 1-5) et 8 polluants (co, no, no2, o3, so2, pm2_5, pm10, nh3) en un seul appel par ville, avec un endpoint historique dédié permettant le backfill.

### Villes retenues
**Choix** : Amsterdam (NL), Antananarivo (MG), Beijing (CN), Londres (GB), Paris (FR).
**Justification** : diversité géographique et de niveaux de pollution (Europe, Asie, Afrique), incluant une ville locale à l'équipe (Antananarivo) pour faciliter la vérification manuelle des résultats.

### Langage et bibliothèques de collecte
**Choix** : Python, avec `requests` et `python-dotenv`.
**Justification** : appels HTTP simples à écrire et à tester, lecture de la clé API directement depuis les variables d'environnement sans dépendance lourde.

### Structure du code de collecte
**Choix** : logique commune centralisée dans `commun.py` (config, retries, sauvegarde), séparée de `collect.py` (collecte horaire temps réel).
**Justification** : évite la duplication de code entre la collecte horaire et le futur script de backfill, qui pourront réutiliser les mêmes fonctions utilitaires.

### Gestion des erreurs réseau
**Choix** : retry automatique avec backoff exponentiel (3 tentatives maximum), pause d'une seconde entre chaque ville, poursuite sur les villes restantes en cas d'échec.
**Justification** : une panne ponctuelle de l'API sur une ville ne doit pas interrompre la collecte des 4 autres, et le backoff évite de solliciter l'API en boucle immédiate en cas d'instabilité.

### Journalisation
**Choix** : logs écrits à la fois dans un fichier (`logs/qualite_air.log`) et affichés en console.
**Justification** : permet de garder une trace persistante des runs (utile comme preuve d'exécution) tout en facilitant le débogage en direct pendant le développement.

### Orchestrateur
**Choix** : Apache Airflow.
**Justification** : permet de définir explicitement les dépendances entre étapes (collecte → clean → warehouse), avec retries automatiques et une interface de suivi visuelle de l'historique des exécutions.

### Stockage brut (raw/)
**Choix** : un fichier JSON par ville et par appel, organisé par sous-dossier (`raw/<ville>/<ville>_<timestamp>.json`), jamais modifié après écriture.
**Justification** : conserve la réponse API telle quelle, garantit que `clean/` peut être entièrement reconstruit sans perte d'information même après une erreur de traitement.

### Nettoyage et zone clean/
**Choix** : script Python (`clean.py`) qui reconstruit un CSV unique à chaque exécution, avec dédoublonnage par clé `(ville, timestamp)`.
**Justification** : un seul fichier trié et sans doublon à chaque run, sans dépendre d'un état précédent ni risquer d'accumuler des doublons au fil des runs.

### Data warehouse
**Choix** : PostgreSQL, avec modélisation en schéma en étoile : une table de faits (`fait_qualite_air`) et trois dimensions (`dim_ville`, `dim_date`, `dim_heure`).
**Justification** : séparer `dim_date` et `dim_heure` (plutôt qu'une seule dimension temps) simplifie les regroupements courants (par jour, par période de la journée) sans introduire de hiérarchie entre dimensions — le schéma reste une étoile, pas un flocon. PostgreSQL permet un chargement en masse performant (`execute_values`) et une base accessible à distance, vérifiable par un tiers.

### Backfill
**Choix** : script Python (`backfill.py`) qui découpe la période demandée en plages mensuelles et interroge l'endpoint historique d'OpenWeatherMap pour chacune, avec sauts automatiques des fichiers déjà téléchargés.
**Justification** : rejouable sans re-télécharger inutilement (option `--force` pour forcer), et paramétrable par nombre de mois ou par ville via la ligne de commande.

### Chargement du warehouse
**Choix** : script Python (`transform.py`) qui charge les dimensions puis la table de faits à partir du CSV de `clean/`, avec correspondance par dictionnaires (`abbr → ville_id`, `date → date_id`, `heure → heure_id`).
**Justification** : rejouable, respecte les clés étrangères en chargeant systématiquement les dimensions avant les faits ; une option `--reset` permet de repartir d'une base vide si besoin.

### Gestion des secrets
**Choix** : clé API et identifiants PostgreSQL (`PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`) stockés dans un fichier `.env` local, jamais commité (exclu via `.gitignore`).
**Justification** : évite toute clé API ou identifiant de base en dur dans le code ou l'historique Git, conformément aux règles du projet.