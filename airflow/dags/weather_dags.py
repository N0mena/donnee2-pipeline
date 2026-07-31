from __future__ import annotations

import csv
import json
import os
import time
from datetime import datetime, timedelta, timezone

import requests
from airflow.sdk import Variable, dag, task
from scripts.collect import collecter_toutes_les_villes


DEFAULT_CITIES = ["Amsterdam", "Antananarivo", "Beijing", "London", "Paris"]
OUTPUT_DIR = "/opt/airflow/data/aqi_history"
GEOCODING_URL = "https://api.openweathermap.org/geo/1.0/direct"
AIR_POLLUTION_HISTORY_URL = "https://api.openweathermap.org/data/2.5/air_pollution/history"

MONTHS_BACK = 12

AQI_LABELS = {
    1: "Good",
    2: "Fair",
    3: "Moderate",
    4: "Poor",
    5: "Very Poor",
}

default_args = {
    "owner": "pipeline-aqi",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def month_ranges(months_back: int) -> list[tuple[datetime, datetime]]:
    ranges = []
    now = datetime.now(timezone.utc)
    end = now

    for _ in range(months_back):
        start = (end.replace(day=1) - timedelta(days=1)).replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        ranges.append((start, end))
        end = start

    return list(reversed(ranges))


@dag(
    dag_id="openweather_aqi_history_backfill",
    description="Backfill des 12 derniers mois d'historique AQI depuis OpenWeather",
    schedule=None,  # déclenchement manuel
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["aqi", "openweather", "pipeline-aqi", "backfill"],
)
def openweather_aqi_history_backfill():

    @task
    def get_cities() -> list[str]:
        raw = Variable.get("aqi_cities", default_var=json.dumps(DEFAULT_CITIES))
        try:
            cities = json.loads(raw)
            if not isinstance(cities, list) or not cities:
                raise ValueError
            return cities
        except (json.JSONDecodeError, ValueError):
            return DEFAULT_CITIES

    @task
    def geocode_cities(cities: list[str]) -> list[dict]:
        """Résout chaque nom de ville en lat/lon via l'API de géocodage OpenWeather."""
        api_key = Variable.get("openweather_api_key")
        results = []

        for city in cities:
            resp = requests.get(
                GEOCODING_URL,
                params={"q": city, "limit": 1, "appid": api_key},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            if not data:
                continue
            results.append(
                {
                    "city": city,
                    "lat": data[0]["lat"],
                    "lon": data[0]["lon"],
                }
            )
        return results

    @task
    def fetch_aqi_history(city_coords: list[dict]) -> list[dict]:
        api_key = Variable.get("openweather_api_key")
        all_rows = []

        for loc in city_coords:
            for start, end in month_ranges(MONTHS_BACK):
                resp = requests.get(
                    AIR_POLLUTION_HISTORY_URL,
                    params={
                        "lat": loc["lat"],
                        "lon": loc["lon"],
                        "start": int(start.timestamp()),
                        "end": int(end.timestamp()),
                        "appid": api_key,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()

                for item in data.get("list", []):
                    components = item["components"]
                    aqi_value = item["main"]["aqi"]
                    all_rows.append(
                        {
                            "city": loc["city"],
                            "lat": loc["lat"],
                            "lon": loc["lon"],
                            "aqi_index": aqi_value,
                            "aqi_label": AQI_LABELS.get(aqi_value, "Unknown"),
                            "co": components.get("co"),
                            "no": components.get("no"),
                            "no2": components.get("no2"),
                            "o3": components.get("o3"),
                            "so2": components.get("so2"),
                            "pm2_5": components.get("pm2_5"),
                            "pm10": components.get("pm10"),
                            "nh3": components.get("nh3"),
                            "measured_at": datetime.fromtimestamp(
                                item["dt"], tz=timezone.utc
                            ).isoformat(),
                        }
                    )
                time.sleep(0.2)

        return all_rows

    @task
    def load_to_csv(records: list[dict]) -> str:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        filepath = os.path.join(OUTPUT_DIR, f"aqi_history_12months_{run_date}.csv")

        if not records:
            return filepath

        fieldnames = list(records[0].keys())
        with open(filepath, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        return filepath

    cities = get_cities()
    coords = geocode_cities(cities)
    history = fetch_aqi_history(coords)
    load_to_csv(history)


openweather_aqi_history_backfill()
