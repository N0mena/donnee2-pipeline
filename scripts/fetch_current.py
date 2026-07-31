import csv
import json
import argparse
from datetime import datetime, timezone
from pathlib import Path

from commun import (
    API_KEY,
    VILLES,
    URL_TEMPS_REEL,
    RACINE_PROJET,
    RAW_DIR,
    logger,
    appel_api_avec_retry,
    sauvegarder_raw,
)
from Clean import extraire_lignes, CLEAN_DIR, CLEAN_FILE, COLONNES, POLLUANTS
from transform import get_connexion, charger_lignes


def collecter() -> int:
    """Collecte les données horaires courantes pour toutes les villes.
    Retourne le nombre de fichiers raw sauvegardés."""
    if not API_KEY:
        logger.error("OPENWEATHER_API_KEY non définie. Arrêt.")
        return 0

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    succes = 0

    for ville in VILLES:
        logger.info("[EXTRACT] Collecte %s (%s)", ville["nom"], ville["abbr"])

        data = appel_api_avec_retry(
            URL_TEMPS_REEL,
            {"lat": ville["lat"], "lon": ville["lon"], "appid": API_KEY},
            ville["nom"],
        )
        if data is None:
            continue

        data["_meta"] = {
            "ville": ville["nom"], "abbr": ville["abbr"], "pays": ville["pays"],
            "lat": ville["lat"], "lon": ville["lon"],
            "type": "collecte_horaire", "collecte_le": timestamp,
        }

        fichier = sauvegarder_raw(ville["abbr"], data, f"{ville['abbr']}_{timestamp}.json")
        logger.info("[EXTRACT] OK -> %s", fichier)
        succes += 1

    logger.info("[EXTRACT] Terminé : %d/%d villes collectées", succes, len(VILLES))
    return succes


def nettoyer() -> list[dict]:
    fichiers = sorted(RAW_DIR.glob("*/*.json"))
    logger.info("[CLEAN] Reconstruction à partir de %d fichier(s) raw/", len(fichiers))

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

    logger.info("[CLEAN] %d lignes uniques (ville x heure)", len(lignes_triees))
    return lignes_triees



def transformer_charger(lignes: list[dict]) -> int:
    logger.info("[LOAD] Chargement de %d mesures dans l'entrepôt...", len(lignes))
    count = charger_lignes(lignes)
    logger.info("[LOAD] %d mesures insérées.", count)
    return count



def eda():
    logger.info("═══ DÉBUT EDA ═══")
    conn = get_connexion()
    cur = conn.cursor()

    # 1. Nombre total de mesures
    cur.execute("SELECT COUNT(*) FROM fait_qualite_air")
    total = cur.fetchone()[0]
    logger.info("[EDA] Total mesures en base : %d", total)

    # 2. Nombre de mesures par ville
    cur.execute("""
        SELECT v.ville, v.abbr, COUNT(*) AS nb_mesures
        FROM fait_qualite_air f
        JOIN dim_ville v ON f.ville_id = v.ville_id
        GROUP BY v.ville, v.abbr
        ORDER BY v.ville
    """)
    logger.info("[EDA] ─── Mesures par ville ───")
    for row in cur.fetchall():
        logger.info("[EDA]   %s (%s) : %d mesures", row[0], row[1], row[2])

    # 3. Plage temporelle
    cur.execute("""
        SELECT MIN(d.date_utc), MAX(d.date_utc)
        FROM fait_qualite_air f JOIN dim_date d ON f.date_id = d.date_id
    """)
    min_d, max_d = cur.fetchone()
    logger.info("[EDA] ─── Période couverte ───")
    logger.info("[EDA]   Du %s au %s", min_d, max_d)

    # 4. Dernières mesures par ville (AQI + polluants principaux)
    cur.execute("""
        SELECT v.ville, v.abbr, d.date_utc, h.heure_utc,
               f.aqi, f.pm2_5, f.pm10, f.no2, f.o3
        FROM fait_qualite_air f
        JOIN dim_ville  v ON f.ville_id  = v.ville_id
        JOIN dim_date   d ON f.date_id   = d.date_id
        JOIN dim_heure  h ON f.heure_id  = h.heure_id
        WHERE (f.ville_id, f.date_id, f.heure_id) IN (
            SELECT f2.ville_id, MAX(f2.date_id), MAX(f2.heure_id)
            FROM fait_qualite_air f2
            GROUP BY f2.ville_id
        )
        ORDER BY v.ville
    """)
    logger.info("[EDA] ─── Dernières mesures par ville ───")
    for row in cur.fetchall():
        logger.info(
            "[EDA]   %s | %s %s | AQI=%d  PM2.5=%.1f  PM10=%.1f  NO2=%.1f  O3=%.1f",
            row[1], row[2], row[3], row[4], row[5] or 0, row[6] or 0, row[7] or 0, row[8] or 0,
        )   

    # 5. Moyennes de polluants par ville
    polluants_sql = ", ".join(
        f"ROUND(AVG(f.{p})::numeric, 2) AS avg_{p}" for p in POLLUANTS
    )
    cur.execute(f"""
        SELECT v.ville, v.abbr, {polluants_sql}
        FROM fait_qualite_air f
        JOIN dim_ville v ON f.ville_id = v.ville_id
        GROUP BY v.ville, v.abbr
        ORDER BY v.ville
    """)
    cols = [desc[0] for desc in cur.description]
    logger.info("[EDA] ─── Moyennes de polluants par ville (µg/m³) ───")
    logger.info("[EDA]   %s", " | ".join(cols))
    for row in cur.fetchall():
        logger.info("[EDA]   %s", " | ".join(str(v) for v in row))

    # 6. Distribution AQI par ville
    cur.execute("""
        SELECT v.ville, v.abbr, f.aqi, COUNT(*) AS nb
        FROM fait_qualite_air f
        JOIN dim_ville v ON f.ville_id = v.ville_id
        GROUP BY v.ville, v.abbr, f.aqi
        ORDER BY v.ville, f.aqi
    """)
    logger.info("[EDA] ─── Distribution AQI par ville ───")
    for row in cur.fetchall():
        logger.info("[EDA]   %s | AQI=%d : %d mesures", row[1], row[2], row[3])

    # 7. Jours avec la pire qualité d'air (AQI moyen le plus élevé)
    cur.execute("""
        SELECT v.ville, d.date_utc, AVG(f.aqi)::numeric(3,1) AS aqi_moyen
        FROM fait_qualite_air f
        JOIN dim_ville v ON f.ville_id = v.ville_id
        JOIN dim_date  d ON f.date_id  = d.date_id
        GROUP BY v.ville, d.date_utc
        HAVING AVG(f.aqi) >= 3
        ORDER BY aqi_moyen DESC
        LIMIT 10
    """)
    logger.info("[EDA] ─── Top 10 jours avec pire AQI moyen (≥3) ───")
    for row in cur.fetchall():
        logger.info("[EDA]   %s | %s | AQI moyen=%s", row[0], row[1], row[2])

    # 8. Répartition par période de la journée
    cur.execute("""
        SELECT h.periode_journee, COUNT(*) AS nb, ROUND(AVG(f.aqi)::numeric, 2) AS aqi_moy
        FROM fait_qualite_air f
        JOIN dim_heure h ON f.heure_id = h.heure_id
        GROUP BY h.periode_journee
        ORDER BY aqi_moy DESC
    """)
    logger.info("[EDA] ─── AQI moyen par période de la journée ───")
    for row in cur.fetchall():
        logger.info("[EDA]   %-15s : %6d mesures | AQI moyen = %s", row[0], row[1], row[2])

    # 9. Corrélation PM2.5 vs PM10 (approximation via moyenne par ville)
    cur.execute("""
        SELECT v.ville,
               ROUND(CORR(f.pm2_5, f.pm10)::numeric, 3) AS corr_pm
        FROM fait_qualite_air f
        JOIN dim_ville v ON f.ville_id = v.ville_id
        WHERE f.pm2_5 IS NOT NULL AND f.pm10 IS NOT NULL
        GROUP BY v.ville
        ORDER BY v.ville
    """)
    logger.info("[EDA] ─── Corrélation PM2.5 / PM10 par ville ───")
    for row in cur.fetchall():
        logger.info("[EDA]   %s : r = %s", row[0], row[1])

    cur.close()
    conn.close()
    logger.info("═══ FIN EDA ═══")


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline complet : collecte → nettoyage → chargement → EDA"
    )
    parser.add_argument("--eda-only", action="store_true", help="Exécuter uniquement l'EDA sur les données existantes.")
    args = parser.parse_args()

    debut = datetime.now(timezone.utc)
    logger.info("═══════════════════════════════════════════")
    logger.info("Pipeline démarré à %s", debut.strftime("%Y-%m-%d %H:%M:%S UTC"))
    logger.info("═══════════════════════════════════════════")

    if not args.eda_only:
        collecter()
        lignes = nettoyer()
        transformer_charger(lignes)
    else:
        logger.info("Mode --eda-only : collecte et chargement ignorés.")

    eda()

    fin = datetime.now(timezone.utc)
    duree = (fin - debut).total_seconds()
    logger.info("Pipeline terminé en %.1f secondes.", duree)


if __name__ == "__main__":
    main()
