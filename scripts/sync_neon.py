import argparse
import os
from datetime import datetime
from pathlib import Path

import requests

from .commun import RACINE_PROJET, logger
from .load_warehouse import CLEAN_FILE, charger_csv, NOMS_JOUR, NOMS_MOIS, periode_journee

TAILLE_LOT = 3000


def lit(v):
    """Échappe une valeur pour un littéral SQL."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


class NeonHTTP:
    """Client minimal vers l'API SQL-over-HTTPS de Neon."""

    def __init__(self):
        champs = ["PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"]
        manquants = [c for c in champs if not os.environ.get(c)]
        if manquants:
            raise EnvironmentError(
                "Variables d'environnement manquantes : " + ", ".join(manquants)
                + "\nRenseignez PGHOST/PGDATABASE/PGUSER/PGPASSWORD dans .env"
            )
        self.host = os.environ["PGHOST"]
        self.url = f"https://{self.host}/sql"
        self.headers = {
            "Content-Type": "application/json",
            "Neon-Connection-String": (
                f"postgresql://{os.environ['PGUSER']}:{os.environ['PGPASSWORD']}"
                f"@{self.host}/{os.environ['PGDATABASE']}"
            ),
        }

    def exec(self, sql: str) -> dict:
        r = requests.post(self.url, headers=self.headers, json={"query": sql}, timeout=180)
        j = r.json()
        if r.status_code != 200 or "command" not in j:
            raise RuntimeError(f"Neon HTTP {r.status_code} : {j.get('message', j)}")
        return j

    def lignes(self, sql: str) -> list[dict]:
        return self.exec(sql)["rows"]

    def inserer_lots(self, entete: str, valeurs: list[str], suffixe: str) -> int:
        """Découpe une INSERT massive en lots et exécute chaque lot."""
        total = 0
        for i in range(0, len(valeurs), TAILLE_LOT):
            lot = valeurs[i : i + TAILLE_LOT]
            sql = entete + ", ".join(lot) + suffixe
            j = self.exec(sql)
            total += j.get("rowCount") or 0
        return total


DDL_DIMS = [
    """
    CREATE TABLE IF NOT EXISTS dim_ville (
        ville_id    SERIAL PRIMARY KEY,
        ville       VARCHAR(100) NOT NULL,
        abbr        VARCHAR(5)   NOT NULL UNIQUE,
        pays        VARCHAR(5)   NOT NULL,
        latitude    DOUBLE PRECISION NOT NULL,
        longitude   DOUBLE PRECISION NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id       SERIAL PRIMARY KEY,
        date_utc      DATE NOT NULL UNIQUE,
        jour          SMALLINT NOT NULL,
        mois          SMALLINT NOT NULL,
        trimestre     SMALLINT NOT NULL,
        annee         SMALLINT NOT NULL,
        jour_semaine  SMALLINT NOT NULL,
        nom_jour      VARCHAR(10) NOT NULL,
        nom_mois      VARCHAR(10) NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_heure (
        heure_id          SERIAL PRIMARY KEY,
        heure_utc         TIME NOT NULL UNIQUE,
        heure             SMALLINT NOT NULL,
        minute            SMALLINT NOT NULL,
        periode_journee   VARCHAR(20) NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fait_qualite_air (
        fait_id     SERIAL PRIMARY KEY,
        ville_id    INT NOT NULL REFERENCES dim_ville(ville_id),
        date_id     INT NOT NULL REFERENCES dim_date(date_id),
        heure_id    INT NOT NULL REFERENCES dim_heure(heure_id),
        aqi         SMALLINT NOT NULL,
        co          DOUBLE PRECISION,
        no          DOUBLE PRECISION,
        no2         DOUBLE PRECISION,
        o3          DOUBLE PRECISION,
        so2         DOUBLE PRECISION,
        pm2_5       DOUBLE PRECISION,
        pm10        DOUBLE PRECISION,
        nh3         DOUBLE PRECISION,
        UNIQUE (ville_id, date_id, heure_id)
    )
    """,
]

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_fait_ville ON fait_qualite_air(ville_id)",
    "CREATE INDEX IF NOT EXISTS idx_fait_date  ON fait_qualite_air(date_id)",
    "CREATE INDEX IF NOT EXISTS idx_fait_heure ON fait_qualite_air(heure_id)",
    "CREATE INDEX IF NOT EXISTS idx_fait_aqi   ON fait_qualite_air(aqi)",
]

DDL_VIEW = """
CREATE OR REPLACE VIEW vue_qualite_air_complete AS
SELECT
    f.fait_id,
    v.ville,
    v.abbr,
    v.pays,
    v.latitude,
    v.longitude,
    d.date_utc,
    d.jour,
    d.mois,
    d.trimestre,
    d.annee,
    d.nom_jour,
    d.nom_mois,
    h.heure_utc,
    h.heure,
    h.periode_journee,
    f.aqi,
    f.co,
    f.no,
    f.no2,
    f.o3,
    f.so2,
    f.pm2_5,
    f.pm10,
    f.nh3
FROM fait_qualite_air f
JOIN dim_ville  v ON f.ville_id  = v.ville_id
JOIN dim_date   d ON f.date_id   = d.date_id
JOIN dim_heure  h ON f.heure_id  = h.heure_id
"""


def ensure_schema(client: NeonHTTP) -> None:
    for ddl in DDL_DIMS + DDL_INDEXES:
        client.exec(ddl)
    client.exec(DDL_VIEW)
    logger.info("Schéma (dimensions + fait + vue) assuré.")


def charger_dim_ville(client: NeonHTTP, lignes: list[dict]) -> dict[str, int]:
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

    valeurs = [
        f"({lit(v['ville'])}, {lit(v['abbr'])}, {lit(v['pays'])}, {v['latitude']}, {v['longitude']})"
        for v in villes.values()
    ]
    client.inserer_lots(
        "INSERT INTO dim_ville (ville, abbr, pays, latitude, longitude) VALUES ",
        valeurs,
        " ON CONFLICT (abbr) DO NOTHING",
    )
    mapping = {r["abbr"]: r["ville_id"] for r in client.lignes("SELECT abbr, ville_id FROM dim_ville")}
    logger.info("dim_ville chargée : %d villes.", len(mapping))
    return mapping


def charger_dim_date(client: NeonHTTP, lignes: list[dict]) -> dict[str, int]:
    dates_uniques: dict[str, dict] = {}
    for ligne in lignes:
        d = ligne["date_utc"]
        if d not in dates_uniques:
            dt = datetime.strptime(d, "%Y-%m-%d").date()
            dates_uniques[d] = {
                "date_utc": d,
                "jour": dt.day,
                "mois": dt.month,
                "trimestre": (dt.month - 1) // 3 + 1,
                "annee": dt.year,
                "jour_semaine": dt.weekday(),
                "nom_jour": NOMS_JOUR[dt.weekday()],
                "nom_mois": NOMS_MOIS[dt.month],
            }

    valeurs = [
        f"({lit(v['date_utc'])}, {v['jour']}, {v['mois']}, {v['trimestre']}, {v['annee']}, "
        f"{v['jour_semaine']}, {lit(v['nom_jour'])}, {lit(v['nom_mois'])})"
        for v in dates_uniques.values()
    ]
    client.inserer_lots(
        "INSERT INTO dim_date (date_utc, jour, mois, trimestre, annee, jour_semaine, nom_jour, nom_mois) VALUES ",
        valeurs,
        " ON CONFLICT (date_utc) DO NOTHING",
    )
    mapping = {r["date_utc"]: r["date_id"] for r in client.lignes("SELECT date_utc::text, date_id FROM dim_date")}
    logger.info("dim_date chargée : %d dates.", len(mapping))
    return mapping


def charger_dim_heure(client: NeonHTTP, lignes: list[dict]) -> dict[str, int]:
    heures_uniques: dict[str, dict] = {}
    for ligne in lignes:
        h = ligne["heure_utc"]
        if h not in heures_uniques:
            ht = datetime.strptime(h, "%H:%M").time()
            heures_uniques[h] = {
                "heure_utc": h,
                "heure": ht.hour,
                "minute": ht.minute,
                "periode_journee": periode_journee(ht.hour),
            }

    valeurs = [
        f"({lit(v['heure_utc'])}, {v['heure']}, {v['minute']}, {lit(v['periode_journee'])})"
        for v in heures_uniques.values()
    ]
    client.inserer_lots(
        "INSERT INTO dim_heure (heure_utc, heure, minute, periode_journee) VALUES ",
        valeurs,
        " ON CONFLICT (heure_utc) DO NOTHING",
    )
    mapping = {r["heure_utc"]: r["heure_id"] for r in client.lignes("SELECT to_char(heure_utc, 'HH24:MI') AS heure_utc, heure_id FROM dim_heure")}
    logger.info("dim_heure chargée : %d heures.", len(mapping))
    return mapping


def charger_fait(
    client: NeonHTTP,
    lignes: list[dict],
    map_ville: dict,
    map_date: dict,
    map_heure: dict,
) -> int:
    valeurs = []
    for ligne in lignes:
        ville_id = map_ville[ligne["abbr"]]
        date_id = map_date[ligne["date_utc"]]
        heure_id = map_heure[ligne["heure_utc"]]

        def flot(x):
            return float(x) if x else None

        valeurs.append(
            f"({ville_id}, {date_id}, {heure_id}, {int(ligne['aqi'])}, "
            f"{lit(flot(ligne['co']))}, {lit(flot(ligne['no']))}, {lit(flot(ligne['no2']))}, "
            f"{lit(flot(ligne['o3']))}, {lit(flot(ligne['so2']))}, {lit(flot(ligne['pm2_5']))}, "
            f"{lit(flot(ligne['pm10']))}, {lit(flot(ligne['nh3']))})"
        )

    total = client.inserer_lots(
        "INSERT INTO fait_qualite_air "
        "(ville_id, date_id, heure_id, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3) VALUES ",
        valeurs,
        " ON CONFLICT (ville_id, date_id, heure_id) DO NOTHING",
    )
    logger.info("fait_qualite_air chargée : %d mesures.", total)
    return total


def sync(chemin: Path = CLEAN_FILE, reset: bool = False) -> int:
    if not chemin.exists():
        logger.error("CSV introuvable : %s", chemin.resolve())
        return -1

    lignes = charger_csv(chemin)
    if not lignes:
        logger.warning("CSV vide, aucun chargement effectué.")
        return 0

    client = NeonHTTP()
    ensure_schema(client)
    if reset:
        client.exec("TRUNCATE fait_qualite_air, dim_heure, dim_date, dim_ville RESTART IDENTITY CASCADE")

    map_ville = charger_dim_ville(client, lignes)
    map_date = charger_dim_date(client, lignes)
    map_heure = charger_dim_heure(client, lignes)
    count = charger_fait(client, lignes, map_ville, map_date, map_heure)
    logger.info("Synchronisation terminée : %d mesures dans Neon.", count)
    return count


def main():
    parser = argparse.ArgumentParser(
        description="Synchronise le CSV nettoyé vers Neon (SQL over HTTPS)."
    )
    parser.add_argument(
        "--csv", type=Path, default=CLEAN_FILE,
        help="Chemin vers le CSV nettoyé (défaut : data/clean/qualite_air.csv).",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Vider les tables avant chargement (rebuild complet).",
    )
    parser.add_argument(
        "--check-only", action="store_true",
        help="Aucun chargement (réservé aux appels planifiés).",
    )
    args = parser.parse_args()

    if args.check_only:
        return
    sync(args.csv, reset=args.reset)


if __name__ == "__main__":
    main()
