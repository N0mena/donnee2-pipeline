from datetime import datetime, timezone, timedelta

from .commun import (
    API_KEY,
    VILLES,
    URL_TEMPS_REEL,
    URL_HISTORIQUE,
    logger,
    appel_api_avec_retry,
    sauvegarder_raw,
)


def collecter_par_heure(heure: datetime) -> tuple[int, int]:
    """Récupère les données de pollution pour une heure précise (fenêtre de 1h)
    via l'API historique, pour obtenir une mesure alignée sur l'heure."""
    if not API_KEY:
        logger.error("OPENWEATHER_API_KEY non définie dans l'environnement.")
        return 0, 0

    debut = heure.replace(minute=0, second=0, microsecond=0)
    fin = debut + timedelta(hours=1)
    suffixe_fichier = debut.strftime("%Y%m%dT%H%M%SZ")

    succes, echecs = 0, 0

    for ville in VILLES:
        logger.info(
            "Collecte horaire %s pour %s (%s)",
            debut.isoformat(), ville["nom"], ville["abbr"],
        )

        data = appel_api_avec_retry(
            URL_HISTORIQUE,
            {
                "lat": ville["lat"], "lon": ville["lon"],
                "start": int(debut.timestamp()), "end": int(fin.timestamp()),
                "appid": API_KEY,
            },
            f"{ville['nom']} {debut.isoformat()}",
        )
        if data is None:
            echecs += 1
            continue
        data["_meta"] = {
            "ville": ville["nom"], "abbr": ville["abbr"], "pays": ville["pays"],
            "lat": ville["lat"], "lon": ville["lon"],
            "type": "collecte_horaire",
            "heure_debut": debut.isoformat(), "heure_fin": fin.isoformat(),
        }

        fichier = sauvegarder_raw(ville["abbr"], data, f"{ville['abbr']}_{suffixe_fichier}.json")
        logger.info("OK -> %s", fichier)
        succes += 1

    logger.info(
        "Collecte horaire %s terminée : %d succès, %d échecs sur %d villes",
        debut.isoformat(), succes, echecs, len(VILLES),
    )
    return succes, echecs


def collecter_toutes_les_villes() -> None:
    if not API_KEY:
        logger.error("OPENWEATHER_API_KEY non définie dans l'environnement.")
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    succes, echecs = 0, 0

    for ville in VILLES:
        logger.info("Collecte en cours pour %s (%s)", ville["nom"], ville["abbr"])

        data = appel_api_avec_retry(
            URL_TEMPS_REEL,
            {"lat": ville["lat"], "lon": ville["lon"], "appid": API_KEY},
            ville["nom"],
        )
        if data is None:
            echecs += 1
            continue
        data["_meta"] = {
            "ville": ville["nom"], "abbr": ville["abbr"], "pays": ville["pays"],
            "lat": ville["lat"], "lon": ville["lon"],
            "type": "collecte_horaire", "collecte_le": timestamp,
        }

        fichier = sauvegarder_raw(ville["abbr"], data, f"{ville['abbr']}_{timestamp}.json")
        logger.info("OK -> %s", fichier)
        succes += 1

    logger.info(
        "Collecte terminée : %d succès, %d échecs sur %d villes",
        succes, echecs, len(VILLES),
    )


if __name__ == "__main__":
    collecter_toutes_les_villes()