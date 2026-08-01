from scripts.collect import collecter_toutes_les_villes
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator

DAG_DIR = Path(_file_).resolve().parent

def _find_project_root() -> Path:
    candidates = [Path.cwd(), DAG_DIR, *DAG_DIR.parents, Path("/opt/airflow")]
    for candidate in candidates:
        root = candidate.resolve()
        if (root / "scripts" / "_init_.py").is_file():
            return root
    raise RuntimeError(
        "Impossible de localiser le paquet 'scripts' (scripts/__init__.py) "
        f"à partir du répertoire du DAG : {DAG_DIR}"
    )

PROJECT_ROOT = _find_project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from scripts.Clean import CLEAN_FILE, construire_clean
from scripts.collect import collecter_par_heure
from scripts.separate_cities import separer_par_ville
from scripts.sync_neon import sync


def extract_task_fn(**kwargs):
    heure = kwargs["data_interval_start"]
    if heure.tzinfo is None:
        heure = heure.replace(tzinfo=timezone.utc)
    return collecter_par_heure(heure)


def transform_task_fn(**kwargs):
    construire_clean()
    return str(CLEAN_FILE)

def load_task_fn(**kwargs):
    fichiers = separer_par_ville(CLEAN_FILE)
    return [str(f) for f in fichiers]

def split_task_fn(**kwargs):
    fichiers = separer_par_ville(CLEAN_FILE)
    return [str(f) for f in fichiers]


def load_neon_task_fn(**kwargs):
    return sync(CLEAN_FILE)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "weather_air_quality_hourly_etl",
    default_args=default_args,
    description=(
        "ETL horaire de la qualité de l'air pour les 5 villes : "
        "collecte (API OpenWeather) -> nettoyage (clean/) -> CSV par ville (separated_clean/) "
        "-> chargement incrémental dans Neon (star schema)"
    ),
    schedule="@hourly",
    start_date=datetime(2026, 8, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_collecte_horaire",
        python_callable=extract_task_fn,
    )
    transform_task = PythonOperator(
        task_id="transform_construction_clean",
        python_callable=transform_task_fn,
    )
    load_task = PythonOperator(
        task_id="load_entrepot",
        python_callable=load_task_fn,
    )
    split_task = PythonOperator(
        task_id="split_par_ville",
        python_callable=split_task_fn,
    )
    load_neon_task = PythonOperator(
        task_id="load_neon_star_schema",
        python_callable=load_neon_task_fn,
    )

    extract_task >> transform_task >> load_task >> split_task >> load_neon_task