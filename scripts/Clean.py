import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from commun import RACINE_PROJET, RAW_DIR, logger

CLEAN_DIR = RACINE_PROJET / "clean"
CLEAN_FILE = CLEAN_DIR / "qualite_air.csv"

COLONNES = [
    "ville", "abbr", "pays", "latitude", "longitude",
    "timestamp_utc", "date_utc", "heure_utc",
    "aqi",
    "co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3",
]

POLLUANTS = ["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]


def lister_fichiers_raw() -> list[Path]:
    if not RAW_DIR.exists():
        logger.warning("Dossier raw/ introuvable : %s", RAW_DIR.resolve())
        return []
    return sorted(RAW_DIR.glob("*/*.json"))


def extraire_lignes(fichier: Path) -> list[dict]:
    """Extrait une ligne par entrée de 'list' dans un fichier raw/."""
    try:
        with open(fichier, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Fichier illisible, ignoré : %s (%s)", fichier, e)
        return []

    meta = data.get("_meta", {})
    entrees = data.get("list", [])
    if not entrees:
        logger.warning("Aucune donnée 'list' dans : %s", fichier)
        return []

    lignes = []
    for entree in entrees:
        dt_unix = entree.get("dt")
        if dt_unix is None:
            continue
        horodatage = datetime.fromtimestamp(dt_unix, tz=timezone.utc)
        composants = entree.get("components", {})

        ligne = {
            "ville": meta.get("ville"),
            "abbr": meta.get("abbr"),
            "pays": meta.get("pays"),
            "latitude": meta.get("lat"),
            "longitude": meta.get("lon"),
            "timestamp_utc": horodatage.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "date_utc": horodatage.strftime("%Y-%m-%d"),
            "heure_utc": horodatage.strftime("%H:%M"),
            "aqi": entree.get("main", {}).get("aqi"),
        }
        for polluant in POLLUANTS:
            ligne[polluant] = composants.get(polluant)

        lignes.append(ligne)

    return lignes


def construire_clean() -> None:
    fichiers = lister_fichiers_raw()
    logger.info("Reconstruction de clean/ à partir de %d fichier(s) raw/", len(fichiers))

    lignes_par_cle: dict[tuple, dict] = {}

    for fichier in fichiers:
        for ligne in extraire_lignes(fichier):
            cle = (ligne["abbr"], ligne["timestamp_utc"])
            lignes_par_cle[cle] = ligne

    lignes_triees = sorted(
        lignes_par_cle.values(),
        key=lambda l: (l["abbr"], l["timestamp_utc"]),
    )

    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    with open(CLEAN_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES)
        writer.writeheader()
        writer.writerows(lignes_triees)

    logger.info(
        "clean/%s reconstruit : %d ligne(s) unique(s) (ville x heure)",
        CLEAN_FILE.name, len(lignes_triees),
    )


if __name__ == "__main__":
    construire_clean()