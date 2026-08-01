import csv
import os
import argparse
import logging
from datetime import datetime, date, time
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from commun import RACINE_PROJET, logger

CLEAN_FILE = RACINE_PROJET / "clean" / "qualite_air.csv"

NOMS_JOUR = ["dimanche", "lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi"]
NOMS_MOIS = [
    "", "janvier", "fevrier", "mars", "avril", "mai", "juin",
    "juillet", "aout", "septembre", "octobre", "novembre", "decembre",
]


def periode_journee(heure: int) -> str:
    if heure < 6:
        return "nuit"
    elif heure < 12:
        return "matin"
    elif heure < 18:
        return "apres-midi"
    else:
        return "soiree"


def get_connexion():
    champs = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    manquants = [c for c in champs if not os.environ.get(c)]
    if manquants:
        raise EnvironmentError(
            "Variables d'environnement manquantes : " + ", ".join(manquants)
            + "\nCopiez .env.example en .env et remplissez les valeurs."
        )
    return psycopg2.connect(
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
    )


def reset_tables(conn):
    cur = conn.cursor()
    cur.execute("TRUNCATE fait_qualite_air, dim_heure, dim_date, dim_ville RESTART IDENTITY CASCADE;")
    conn.commit()
    cur.close()
    logger.info("Tables vidées avec succès.")


def charger_dim_ville(conn, lignes: list[dict]) -> dict[str, int]:
    villes: dict[str, dict] = {}
    for ligne in lignes:
        abbr = ligne["abbr"]
        if abbr not in villes:
            villes[abbr] = {
                "ville": ligne["ville"],
                "abbr": abbr,
                "pays": ligne["pays"],
                "latitude": float(ligne["latitude"]),
                "longitude": float(ligne["longitude"]),
            }

    cur = conn.cursor()
    abbrs = sorted(villes.keys())
    valeurs = [(v["ville"], v["abbr"], v["pays"], v["latitude"], v["longitude"])
               for v in [villes[a] for a in abbrs]]

    execute_values(
        cur,
        "INSERT INTO dim_ville (ville, abbr, pays, latitude, longitude) VALUES %s",
        valeurs,
    )

    cur.execute("SELECT abbr, ville_id FROM dim_ville")
    mapping = dict(cur.fetchall())
    conn.commit()
    cur.close()
    logger.info("dim_ville chargée : %d villes.", len(mapping))
    return mapping


def charger_dim_date(conn, lignes: list[dict]) -> dict[date, int]:
    dates_uniques: dict[str, dict] = {}
    for ligne in lignes:
        d = ligne["date_utc"]
        if d not in dates_uniques:
            dt = datetime.strptime(d, "%Y-%m-%d").date()
            dates_uniques[d] = {
                "date_utc": dt,
                "jour": dt.day,
                "mois": dt.month,
                "trimestre": (dt.month - 1) // 3 + 1,
                "annee": dt.year,
                "jour_semaine": dt.weekday(),
                "nom_jour": NOMS_JOUR[dt.weekday()],
                "nom_mois": NOMS_MOIS[dt.month],
            }

    cur = conn.cursor()
    cles = sorted(dates_uniques.keys())
    valeurs = [
        (dates_uniques[c]["date_utc"], dates_uniques[c]["jour"], dates_uniques[c]["mois"],
         dates_uniques[c]["trimestre"], dates_uniques[c]["annee"], dates_uniques[c]["jour_semaine"],
         dates_uniques[c]["nom_jour"], dates_uniques[c]["nom_mois"])
        for c in cles
    ]

    execute_values(
        cur,
        "INSERT INTO dim_date (date_utc, jour, mois, trimestre, annee, jour_semaine, nom_jour, nom_mois) VALUES %s",
        valeurs,
    )

    cur.execute("SELECT date_utc, date_id FROM dim_date")
    mapping = dict(cur.fetchall())
    conn.commit()
    cur.close()
    logger.info("dim_date chargée : %d dates.", len(mapping))
    return mapping


def charger_dim_heure(conn, lignes: list[dict]) -> dict[time, int]:
    heures_uniques: dict[str, dict] = {}
    for ligne in lignes:
        h = ligne["heure_utc"]
        if h not in heures_uniques:
            ht = datetime.strptime(h, "%H:%M").time()
            heures_uniques[h] = {
                "heure_utc": ht,
                "heure": ht.hour,
                "minute": ht.minute,
                "periode_journee": periode_journee(ht.hour),
            }

    cur = conn.cursor()
    cles = sorted(heures_uniques.keys())
    valeurs = [
        (heures_uniques[c]["heure_utc"], heures_uniques[c]["heure"],
         heures_uniques[c]["minute"], heures_uniques[c]["periode_journee"])
        for c in cles
    ]

    execute_values(
        cur,
        "INSERT INTO dim_heure (heure_utc, heure, minute, periode_journee) VALUES %s",
        valeurs,
    )

    cur.execute("SELECT heure_utc, heure_id FROM dim_heure")
    mapping = dict(cur.fetchall())
    conn.commit()
    cur.close()
    logger.info("dim_heure chargée : %d heures.", len(mapping))
    return mapping


def charger_fait(conn, lignes: list[dict], map_ville: dict, map_date: dict, map_heure: dict) -> int:
    cur = conn.cursor()
    batch = []
    for ligne in lignes:
        dt = datetime.strptime(ligne["date_utc"], "%Y-%m-%d").date()
        ht = datetime.strptime(ligne["heure_utc"], "%H:%M").time()

        ville_id = map_ville[ligne["abbr"]]
        date_id = map_date[dt]
        heure_id = map_heure[ht]

        batch.append((
            ville_id, date_id, heure_id,
            int(ligne["aqi"]),
            float(ligne["co"])   if ligne["co"]   else None,
            float(ligne["no"])   if ligne["no"]   else None,
            float(ligne["no2"])  if ligne["no2"]  else None,
            float(ligne["o3"])   if ligne["o3"]   else None,
            float(ligne["so2"])  if ligne["so2"]  else None,
            float(ligne["pm2_5"]) if ligne["pm2_5"] else None,
            float(ligne["pm10"]) if ligne["pm10"] else None,
            float(ligne["nh3"])  if ligne["nh3"]  else None,
        ))

    execute_values(
        cur,
        """INSERT INTO fait_qualite_air
           (ville_id, date_id, heure_id, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3)
           VALUES %s""",
        batch,
        page_size=5000,
    )

    count = len(batch)
    conn.commit()
    cur.close()
    logger.info("fait_qualite_air chargée : %d mesures.", count)
    return count


def charger_csv(chemin: Path) -> list[dict]:
    with open(chemin, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        lignes = list(reader)
    logger.info("CSV lu : %d lignes depuis %s", len(lignes), chemin)
    return lignes


def charger_lignes(lignes: list[dict], reset: bool = False) -> int:
    if not lignes:
        logger.info("Aucune ligne à charger.")
        return 0

    conn = get_connexion()
    try:
        if reset:
            reset_tables(conn)

        map_ville = charger_dim_ville(conn, lignes)
        map_date = charger_dim_date(conn, lignes)
        map_heure = charger_dim_heure(conn, lignes)
        count = charger_fait(conn, lignes, map_ville, map_date, map_heure)
        logger.info("Chargement incrémental terminé : %d mesures.", count)
        return count
    except Exception as e:
        conn.rollback()
        logger.error("Erreur lors du chargement : %s", e)
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Charge les données dans l'entrepôt (star schema).")
    parser.add_argument("--csv", type=Path, default=CLEAN_FILE, help="Chemin vers le CSV nettoyé.")
    parser.add_argument("--reset", action="store_true", help="Vider les tables avant chargement.")
    args = parser.parse_args()

    if not args.csv.exists():
        logger.error("Fichier CSV introuvable : %s", args.csv.resolve())
        return

    lignes = charger_csv(args.csv)
    charger_lignes(lignes, reset=args.reset)


if __name__ == "__main__":
    main()
