"""
This flow fetches 5-day weather forecasts for specified locations from the OpenWeather API,
reduces the data to one record per day, and ingests it into a DuckDB table.
"""

import logging
import os
from datetime import date, datetime

import httpx
import pyarrow as pa
from prefect import flow, task, get_run_logger

from flows.lib.db import DbManager, Connection

OPENWEATHER_API_KEY = os.environ["OPENWEATHER_API_KEY"]
FORECAST_BASE_URL = "http://api.openweathermap.org/data/2.5/forecast"

LOCATIONS = [
    {"lat": "-22.9068", "lon": "-43.1729"},  # Rio de Janeiro, BR
    {"lat": "54.9783", "lon": "-1.6174"},    # Newcastle Upon Tyne, UK
]

SCHEMA = 'gizmosql_duck.series'
LANDING_TABLE = 'forecast_data'


def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[OPENWEATHER FORECAST] {msg}", kwargs

    return PrefixLoggerAdapter(get_run_logger(), {"flow_name": "OPENWEATHER FORECAST"})


@task(name="fetch-forecast")
def fetch_forecast(lat: str, lon: str) -> list[dict] | None:
    logger = get_flow_logger()
    params = {"lat": lat, "lon": lon, "units": "metric", "lang": "pt_br", "appid": OPENWEATHER_API_KEY}
    response = httpx.get(FORECAST_BASE_URL, params=params, timeout=30)

    if response.status_code != 200:
        logger.error(f"Error fetching forecast data: {response.json().get('message', 'Unknown error')}")
        return None

    data = response.json()
    fetched_at = datetime.utcnow()

    grouped = _group_forecast_by_date(data["list"])
    records = _reduce_forecast_by_date(grouped, fetched_at)

    logger.info(f"Fetched {len(records)} days of forecast")
    return records


def _group_forecast_by_date(data: list) -> list[list]:
    grouped: dict[str, list] = {}
    for item in data:
        day = datetime.fromtimestamp(item["dt"]).strftime("%Y-%m-%d")
        grouped.setdefault(day, []).append(item)
    return list(grouped.values())


def _reduce_forecast_by_date(groups: list[list], fetched_at: datetime) -> list[dict]:
    records = []
    for items in groups:
        mid = items[len(items) // 2]
        records.append({
            "date": date.fromisoformat(datetime.fromtimestamp(items[0]["dt"]).strftime("%Y-%m-%d")),
            "timestamp": mid["dt"],
            "temperature": round(sum(i["main"]["temp"] for i in items) / len(items), 2),
            "weather": mid["weather"][0]["main"],
            "description": mid["weather"][0]["description"],
            "icon": mid["weather"][0]["icon"],
            "fetched_at": fetched_at,
        })
    return records


def convert_forecast_to_arrow(records: list[dict]) -> pa.Table:
    schema = pa.schema([
        ("date", pa.date32()),
        ("timestamp", pa.int64()),
        ("temperature", pa.float32()),
        ("weather", pa.string()),
        ("description", pa.string()),
        ("icon", pa.string()),
        ("fetched_at", pa.timestamp("us")),
    ])
    keys = ["date", "timestamp", "temperature", "weather", "description", "icon", "fetched_at"]
    return pa.table({k: [r[k] for r in records] for k in keys}, schema=schema)


@task(name="ingest-forecast", persist_result=False)
def ingest_forecast(table: pa.Table, conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        rows_loaded = cur.adbc_ingest(
            table_name=LANDING_TABLE,
            data=table,
            mode="replace",
            catalog_name="gizmosql_duck",
            db_schema_name="series",
        )
        logger.info(f"Loaded {rows_loaded} rows into `{LANDING_TABLE}` table")


@flow(name="openweather_forecast")
def openweather_forecast(locations: list[dict] | None = None) -> None:
    if locations is None:
        locations = LOCATIONS
    logger = get_flow_logger()
    logger.info(f"Starting OpenWeather forecast pipeline for {len(locations)} location(s)")

    with DbManager.from_env() as conn:
        forecast_records = []
        for loc in locations:
            f = fetch_forecast(loc["lat"], loc["lon"])
            if f:
                forecast_records.extend(f)

        if forecast_records:
            ingest_forecast(convert_forecast_to_arrow(forecast_records), conn)
        else:
            logger.warning("No forecast data fetched, skipping ingest.")


if __name__ == "__main__":
    openweather_forecast()
