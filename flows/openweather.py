'''
How to deploy

$ prefect deploy /app/flows/openweather.py:openweather_pipeline \
  --name openweather-prod \
  --pool my-pool \
  --cron "*/10 * * * *"
'''

import logging
import os
from datetime import date, datetime

import httpx
import pyarrow as pa
from prefect import flow, task, get_run_logger

from flows.lib.db import DbManager, Connection

OPENWEATHER_API_KEY = os.environ["OPENWEATHER_API_KEY"]
WEATHER_BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
FORECAST_BASE_URL = "http://api.openweathermap.org/data/2.5/forecast"
LAT = "-22.9068"
LON = "-43.1729"


def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[OPENWEATHER] {msg}", kwargs

    return PrefixLoggerAdapter(get_run_logger(), {"flow_name": "OPENWEATHER"})


@task(name="create-weather-table", persist_result=False)
def create_weather_table(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gizmosql_duck.series.openweather (
              city VARCHAR,
              country VARCHAR,
              temperature FLOAT,
              feels_like FLOAT,
              min_temp FLOAT,
              max_temp FLOAT,
              humidity INTEGER,
              pressure INTEGER,
              wind_speed FLOAT,
              wind_deg INTEGER,
              weather VARCHAR,
              description VARCHAR,
              icon VARCHAR,
              sunrise BIGINT,
              sunset BIGINT,
              timestamp BIGINT,
              timezone INTEGER,
              fetched_at TIMESTAMP,
              PRIMARY KEY (city, country, timestamp)
            )
        """)
    logger.info("Ensured `series.openweather` table exists")


@task(name="fetch-weather")
def fetch_weather(lat: str, lon: str) -> dict | None:
    logger = get_flow_logger()
    params = {"lat": lat, "lon": lon, "units": "metric", "lang": "pt_br", "appid": OPENWEATHER_API_KEY}
    response = httpx.get(WEATHER_BASE_URL, params=params, timeout=30)

    if response.status_code != 200:
        logger.error(f"Error fetching weather data: {response.json().get('message', 'Unknown error')}")
        return None

    data = response.json()
    fetched_at = datetime.utcnow()

    record = {
        "city": data["name"],
        "country": data["sys"]["country"],
        "temperature": data["main"]["temp"],
        "feels_like": data["main"]["feels_like"],
        "min_temp": data["main"]["temp_min"],
        "max_temp": data["main"]["temp_max"],
        "humidity": data["main"]["humidity"],
        "pressure": data["main"]["pressure"],
        "wind_speed": data["wind"]["speed"],
        "wind_deg": data["wind"].get("deg"),
        "weather": data["weather"][0]["main"],
        "description": data["weather"][0]["description"],
        "icon": data["weather"][0]["icon"],
        "sunrise": data["sys"]["sunrise"],
        "sunset": data["sys"]["sunset"],
        "timestamp": data["dt"],
        "timezone": data["timezone"],
        "fetched_at": fetched_at,
    }

    logger.info(f"Fetched weather for '{record['city']}': {record['temperature']}°C, {record['description']}")
    return record


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


def convert_weather_to_arrow(record: dict) -> pa.Table:
    schema = pa.schema([
        ("city", pa.string()),
        ("country", pa.string()),
        ("temperature", pa.float32()),
        ("feels_like", pa.float32()),
        ("min_temp", pa.float32()),
        ("max_temp", pa.float32()),
        ("humidity", pa.int32()),
        ("pressure", pa.int32()),
        ("wind_speed", pa.float32()),
        ("wind_deg", pa.int32()),
        ("weather", pa.string()),
        ("description", pa.string()),
        ("icon", pa.string()),
        ("sunrise", pa.int64()),
        ("sunset", pa.int64()),
        ("timestamp", pa.int64()),
        ("timezone", pa.int32()),
        ("fetched_at", pa.timestamp("us")),
    ])
    return pa.table({k: [v] for k, v in record.items()}, schema=schema)


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


@task(name="ingest-weather", persist_result=False)
def ingest_weather(table: pa.Table, conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        rows_loaded = cur.adbc_ingest(
            table_name="openweather_landing",
            data=table,
            mode="replace",
            catalog_name="gizmosql_duck",
            db_schema_name="series",
        )
        logger.info(f"Loaded {rows_loaded} rows into `openweather_landing` table")


@task(name="upsert-weather", persist_result=False)
def upsert_weather(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO gizmosql_duck.series.openweather
            SELECT * FROM gizmosql_duck.series.openweather_landing
            ON CONFLICT (city, country, timestamp)
            DO UPDATE SET
              temperature = EXCLUDED.temperature,
              feels_like = EXCLUDED.feels_like,
              min_temp = EXCLUDED.min_temp,
              max_temp = EXCLUDED.max_temp,
              humidity = EXCLUDED.humidity,
              pressure = EXCLUDED.pressure,
              wind_speed = EXCLUDED.wind_speed,
              wind_deg = EXCLUDED.wind_deg,
              weather = EXCLUDED.weather,
              description = EXCLUDED.description,
              icon = EXCLUDED.icon,
              sunrise = EXCLUDED.sunrise,
              sunset = EXCLUDED.sunset,
              timezone = EXCLUDED.timezone,
              fetched_at = EXCLUDED.fetched_at
        """)
        rows_loaded = cur.fetchone()[0]
        logger.info(f"Upserted {rows_loaded} rows into `openweather` table")


@task(name="clear-weather-landing", persist_result=False)
def clear_weather_landing(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM gizmosql_duck.series.openweather_landing")
        logger.info("Cleared `openweather_landing` table")


@task(name="ingest-forecast", persist_result=False)
def ingest_forecast(table: pa.Table, conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        rows_loaded = cur.adbc_ingest(
            table_name="forecast_data",
            data=table,
            mode="replace",
            catalog_name="gizmosql_duck",
            db_schema_name="series",
        )
        logger.info(f"Loaded {rows_loaded} rows into `forecast_data` table")


@flow(name="openweather_pipeline")
def openweather_pipeline() -> None:
    logger = get_flow_logger()
    logger.info(f"Starting OpenWeather pipeline for lat={LAT}, lon={LON}")

    with DbManager.from_env() as conn:
        create_weather_table(conn)

        weather_record = fetch_weather(LAT, LON)
        forecast_records = fetch_forecast(LAT, LON)

        if weather_record:
            ingest_weather(convert_weather_to_arrow(weather_record), conn)
            upsert_weather(conn)
            clear_weather_landing(conn)
        else:
            logger.warning("No weather data fetched, skipping weather ingest.")

        if forecast_records:
            ingest_forecast(convert_forecast_to_arrow(forecast_records), conn)
        else:
            logger.warning("No forecast data fetched, skipping forecast ingest.")


if __name__ == "__main__":
    openweather_pipeline()
