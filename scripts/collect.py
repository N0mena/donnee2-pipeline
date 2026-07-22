from datetime import datetime, timezone

from commun import (
    API_KEY,
    VILLES,
    URL_TEMPS_REEL,
    logger,
    appel_api_avec_retry,
    sauvegarder_raw,
)


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