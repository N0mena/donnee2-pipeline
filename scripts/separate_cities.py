import csv
from pathlib import Path

from .commun import RACINE_PROJET, VILLES, logger

CLEAN_FILE = RACINE_PROJET / "data" / "clean" / "qualite_air.csv"
SEPARATED_DIR = RACINE_PROJET / "data" / "separated_clean"


def separer_par_ville(
    clean_file: Path = CLEAN_FILE,
    sortie_dir: Path = SEPARATED_DIR,
) -> list[Path]:
    """Sépare clean/qualite_air.csv en un CSV par ville dans data/separated_clean/."""
    if not clean_file.exists():
        logger.error("Fichier clean introuvable : %s", clean_file.resolve())
        return []

    with open(clean_file, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            logger.error("CSV vide ou sans en-tête : %s", clean_file.resolve())
            return []
        lignes = list(reader)

    sortie_dir.mkdir(parents=True, exist_ok=True)
    fichiers = []

    for ville in VILLES:
        nom, abbr = ville["nom"], ville["abbr"]
        lignes_ville = [l for l in lignes if l["ville"] == nom]
        if not lignes_ville:
            logger.warning("[LOAD] Aucune ligne pour %s (%s) dans le CSV", nom, abbr)
            continue

        fichier = sortie_dir / f"{nom.lower()}_qa.csv"
        with open(fichier, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(lignes_ville)

        logger.info("[LOAD] %s (%s) : %d lignes -> %s", nom, abbr, len(lignes_ville), fichier)
        fichiers.append(fichier)

    return fichiers


if __name__ == "__main__":
    for fichier in separer_par_ville():
        print(fichier)
