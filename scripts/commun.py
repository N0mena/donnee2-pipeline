import os
import json
import time
import logging
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv() 
API_KEY = os.environ.get("OPENWEATHER_API_KEY")

URL_TEMPS_REEL = "https://api.openweathermap.org/data/2.5/air_pollution"
URL_HISTORIQUE = "https://api.openweathermap.org/data/2.5/air_pollution/history"

RACINE_PROJET = Path(__file__).resolve().parent.parent
RAW_DIR = RACINE_PROJET / "raw"
LOG_FILE = Path(__file__).resolve().parent / "logs" / "qualite_air.log"

TIMEOUT_SECONDS = 15
MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 2
PAUSE_ENTRE_APPELS = 1 

VILLES = [
    {"nom": "Amsterdam",     "abbr": "AMS", "lat": 52.3676,  "lon": 4.9041,   "pays": "NL"},
    {"nom": "Antananarivo",  "abbr": "TNR", "lat": -18.8792, "lon": 47.5079,  "pays": "MG"},
    {"nom": "Beijing",       "abbr": "BJS", "lat": 39.9042,  "lon": 116.4074, "pays": "CN"},
    {"nom": "Londres",       "abbr": "LON", "lat": 51.5074,  "lon": -0.1278,  "pays": "GB"},
    {"nom": "Paris",         "abbr": "PAR", "lat": 48.8566,  "lon": 2.3522,   "pays": "FR"},
]

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),       
    ],
)
logger = logging.getLogger(__name__)

def appel_api_avec_retry(url: str, params: dict, contexte: str) -> dict | None:
    for tentative in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=TIMEOUT_SECONDS)
            response.raise_for_status()  # lève une erreur si code HTTP >= 400
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning(
                "Echec appel (%s) tentative %d/%d : %s",
                contexte, tentative, MAX_RETRIES, e,
            )
            if tentative < MAX_RETRIES:
                time.sleep(BACKOFF_BASE_SECONDS ** tentative)
            else:
                logger.error("Abandon (%s) après %d tentatives", contexte, MAX_RETRIES)
                return None


def sauvegarder_raw(ville_abbr: str, data: dict, nom_fichier: str) -> Path:
    dossier_ville = RAW_DIR / ville_abbr
    dossier_ville.mkdir(parents=True, exist_ok=True)
    fichier = dossier_ville / nom_fichier
    with open(fichier, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return fichier