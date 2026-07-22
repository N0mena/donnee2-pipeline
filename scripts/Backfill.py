import argparse
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from commun import (
    API_KEY,
    VILLES,
    URL_HISTORIQUE,
    RAW_DIR,
    PAUSE_ENTRE_APPELS,
    logger,
    appel_api_avec_retry,
    sauvegarder_raw,
)


def generer_plages_mensuelles(nb_mois: int) -> list[tuple[datetime, datetime]]:
    plages = []
    aujourdhui = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    curseur = aujourdhui - timedelta(seconds=1)  # = hier 23:59:59 UTC

    for _ in range(nb_mois):
        fin_mois = curseur
        debut_mois = curseur.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        plages.append((debut_mois, fin_mois))
        curseur = debut_mois - timedelta(seconds=1)

    return list(reversed(plages)) 


def backfill(nb_mois: int, villes_cibles: list[dict], force: bool = False) -> None:
    """
    Récupère l'historique de pollution sur `nb_mois` mois, pour chaque
    ville de `villes_cibles`, et sauvegarde chaque réponse dans raw/.
    """
    if not API_KEY:
        logger.error("OPENWEATHER_API_KEY non définie dans l'environnement. Arrêt.")
        return

    plages = generer_plages_mensuelles(nb_mois)
    logger.info(
        "Backfill démarré : %d ville(s) x %d mois = %d appels prévus",
        len(villes_cibles), len(plages), len(villes_cibles) * len(plages),
    )

    succes, echecs, ignores = 0, 0, 0

    for ville in villes_cibles:
        for debut, fin in plages:
            nom_fichier = (
                f"{ville['abbr']}_history_"
                f"{debut.strftime('%Y%m%d')}_{fin.strftime('%Y%m%d')}.json"
            )
            fichier_attendu = RAW_DIR / ville["abbr"] / nom_fichier
            if fichier_attendu.exists() and not force:
                logger.info("Déjà présent, ignoré : %s", fichier_attendu)
                ignores += 1
                continue

            logger.info("Backfill %s : %s -> %s", ville["nom"], debut.date(), fin.date())

            data = appel_api_avec_retry(
                URL_HISTORIQUE,
                {
                    "lat": ville["lat"], "lon": ville["lon"],
                    "start": int(debut.timestamp()), "end": int(fin.timestamp()),
                    "appid": API_KEY,
                },
                f"{ville['nom']} {debut.date()}-{fin.date()}",
            )
            if data is None:
                echecs += 1
                continue

            data["_meta"] = {
                "ville": ville["nom"], "abbr": ville["abbr"], "pays": ville["pays"],
                "lat": ville["lat"], "lon": ville["lon"], "type": "backfill",
                "plage_debut": debut.isoformat(), "plage_fin": fin.isoformat(),
            }

            fichier = sauvegarder_raw(ville["abbr"], data, nom_fichier)
            logger.info("OK -> %s", fichier)
            succes += 1
            time.sleep(PAUSE_ENTRE_APPELS)

    logger.info(
        "Backfill terminé : %d succès, %d échecs, %d ignorés (déjà présents)",
        succes, echecs, ignores,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill de l'historique qualité de l'air (API OpenWeather)"
    )
    parser.add_argument(
        "--mois", type=int, default=12,
        help="Nombre de mois à récupérer (défaut : 12)",
    )
    parser.add_argument(
        "--ville", type=str, default=None,
        help="Code d'une ville (ex: TNR) pour limiter le backfill à une seule ville",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-télécharge même si le fichier existe déjà dans raw/",
    )
    args = parser.parse_args()

    if args.ville:
        cibles = [v for v in VILLES if v["abbr"].upper() == args.ville.upper()]
        if not cibles:
            logger.error(
                "Ville inconnue : %s (codes valides : %s)",
                args.ville, ", ".join(v["abbr"] for v in VILLES),
            )
            raise SystemExit(1)
    else:
        cibles = VILLES

    backfill(args.mois, cibles, args.force)