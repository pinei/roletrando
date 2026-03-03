"""
This flow fetches current weather data for specified locations from the OpenWeather API and ingests it into a DuckDB table.
"""

import logging
import os
from datetime import datetime

import httpx
import pyarrow as pa
from prefect import flow, task, get_run_logger

from flows.lib.db import DbManager, Connection

OPENWEATHER_API_KEY = os.environ["OPENWEATHER_API_KEY"]
WEATHER_BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

LOCATIONS = [
    {"lat": "-22.9068", "lon": "-43.1729"},  # Rio de Janeiro, BR
    {"lat": "54.9783", "lon": "-1.6174"},    # Newcastle Upon Tyne, UK
]


def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[OPENWEATHER WEATHER] {msg}", kwargs

    return PrefixLoggerAdapter(get_run_logger(), {"flow_name": "OPENWEATHER WEATHER"})


@task(name="create-weather-table", persist_result=False)
def create_weather_table(conn: Connection) -> None:
    logger = get_flow_logger()

    conn.execute_ddl("""
        CREATE TABLE IF NOT EXISTS gizmosql_duck.series.openweather_weather (
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


def convert_weather_to_arrow(records: list[dict]) -> pa.Table:
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
    return pa.table({k: [r[k] for r in records] for k in schema.names}, schema=schema)


@task(name="ingest-weather", persist_result=False)
def ingest_weather(table: pa.Table, conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        rows_loaded = cur.adbc_ingest(
            table_name="openweather_weather_landing",
            data=table,
            mode="replace",
            catalog_name="gizmosql_duck",
            db_schema_name="series",
        )
        logger.info(f"Loaded {rows_loaded} rows into `openweather_weather_landing` table")


@task(name="upsert-weather", persist_result=False)
def upsert_weather(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO gizmosql_duck.series.openweather_weather
            SELECT * FROM gizmosql_duck.series.openweather_weather_landing
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
        logger.info(f"Upserted {rows_loaded} rows into `openweather_weather` table")


@task(name="clear-weather-landing", persist_result=False)
def clear_weather_landing(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM gizmosql_duck.series.openweather_weather_landing")
        logger.info("Cleared `openweather_weather_landing` table")


@flow(name="openweather_weather")
def openweather_weather(locations: list[dict] | None = None) -> None:
    if locations is None:
        locations = LOCATIONS
    logger = get_flow_logger()
    logger.info(f"Starting OpenWeather weather pipeline for {len(locations)} location(s)")

    with DbManager.from_env() as conn:
        create_weather_table(conn)

        weather_records = []
        for loc in locations:
            w = fetch_weather(loc["lat"], loc["lon"])
            if w:
                weather_records.append(w)

        if weather_records:
            ingest_weather(convert_weather_to_arrow(weather_records), conn)
            upsert_weather(conn)
            clear_weather_landing(conn)
        else:
            logger.warning("No weather data fetched, skipping ingest.")


if __name__ == "__main__":
    openweather_weather()
