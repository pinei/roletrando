'''
How to deploy

$ prefect deploy /app/flows/tuya_weather.py:tuya_weather_pipeline \
  --name tuya-weather-prod \
  --pool my-pool \
  --cron "*/5 * * * *"
'''

import logging
import os
from datetime import datetime

import pyarrow as pa
import tinytuya
from prefect import flow, task, get_run_logger

from flows.lib.db import DbManager, Connection

TUYA_DEVICE_ID = os.environ["TUYA_DEVICE_ID"]
TUYA_DEVICE_IP = os.environ["TUYA_DEVICE_IP"]
TUYA_DEVICE_KEY = os.environ["TUYA_DEVICE_KEY"]


def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[TUYA WEATHER] {msg}", kwargs

    return PrefixLoggerAdapter(get_run_logger(), {"flow_name": "TUYA WEATHER"})


@task(name="create-tuya-weather-table", persist_result=False)
def create_table(conn: Connection) -> None:
    logger = get_flow_logger()

    conn.execute_ddl("""
        CREATE TABLE IF NOT EXISTS gizmosql_duck.series.tuya_weather (
          timestamp BIGINT,
          temperature_indoor FLOAT,
          humidity_indoor FLOAT,
          temperature_outdoor FLOAT,
          humidity_outdoor FLOAT,
          PRIMARY KEY (timestamp)
        )
    """)
    logger.info("Ensured `series.tuya_weather` table exists")


@task(name="fetch-tuya-weather")
def fetch_tuya_weather() -> dict | None:
    logger = get_flow_logger()

    device = tinytuya.Device(
        dev_id=TUYA_DEVICE_ID,
        address=TUYA_DEVICE_IP,
        local_key=TUYA_DEVICE_KEY,
        version=3.3,
    )

    data = device.status()
    logger.info(f'Data: {data}')

    if not data:
        logger.error("Failed to fetch Tuya Weather Station data")
        return None

    dps = data.get("dps", {})
    temperature_indoor = dps.get("131")
    humidity_indoor = dps.get("132")
    temperature_outdoor = dps.get("133")
    humidity_outdoor = dps.get("134")

    if temperature_indoor is None or temperature_outdoor is None:
        logger.warning("Tuya Weather Station data incomplete, skipping")
        return None

    record = {
        "timestamp": int(datetime.now().timestamp() * 1000),
        "temperature_indoor": float(temperature_indoor) / 10.0,
        "humidity_indoor": float(humidity_indoor) if humidity_indoor is not None else None,
        "temperature_outdoor": float(temperature_outdoor) / 10.0,
        "humidity_outdoor": float(humidity_outdoor) if humidity_outdoor is not None else None,
    }

    logger.info(
        f"Fetched weather: indoor {record['temperature_indoor']}°C / {record['humidity_indoor']}%, "
        f"outdoor {record['temperature_outdoor']}°C / {record['humidity_outdoor']}%"
    )
    return record


def convert_to_arrow(record: dict) -> pa.Table:
    schema = pa.schema([
        ("timestamp", pa.int64()),
        ("temperature_indoor", pa.float32()),
        ("humidity_indoor", pa.float32()),
        ("temperature_outdoor", pa.float32()),
        ("humidity_outdoor", pa.float32()),
    ])
    return pa.table({
        "timestamp": [record["timestamp"]],
        "temperature_indoor": [record["temperature_indoor"]],
        "humidity_indoor": [record["humidity_indoor"]],
        "temperature_outdoor": [record["temperature_outdoor"]],
        "humidity_outdoor": [record["humidity_outdoor"]],
    }, schema=schema)


@task(name="ingest-tuya-weather", persist_result=False)
def ingest_tuya_weather(table: pa.Table, conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        rows_loaded = cur.adbc_ingest(
            table_name="tuya_weather_landing",
            data=table,
            mode="replace",
            catalog_name="gizmosql_duck",
            db_schema_name="series",
        )
        logger.info(f"Loaded {rows_loaded} rows into `tuya_weather_landing` table")


@task(name="upsert-tuya-weather", persist_result=False)
def upsert_tuya_weather(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO gizmosql_duck.series.tuya_weather
            SELECT * FROM gizmosql_duck.series.tuya_weather_landing
            ON CONFLICT (timestamp)
            DO UPDATE SET
              temperature_indoor = EXCLUDED.temperature_indoor,
              humidity_indoor = EXCLUDED.humidity_indoor,
              temperature_outdoor = EXCLUDED.temperature_outdoor,
              humidity_outdoor = EXCLUDED.humidity_outdoor
        """)
        rows_loaded = cur.fetchone()[0]
        logger.info(f"Upserted {rows_loaded} rows into `tuya_weather` table")


@task(name="clear-tuya-weather-landing", persist_result=False)
def clear_landing_table(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM gizmosql_duck.series.tuya_weather_landing")
        logger.info("Cleared `tuya_weather_landing` table")


@flow(name="tuya_weather_pipeline")
def tuya_weather_pipeline() -> None:
    logger = get_flow_logger()
    logger.info("Starting Tuya Weather Station pipeline")

    with DbManager.from_env() as conn:
        create_table(conn)

        record = fetch_tuya_weather()

        if not record:
            logger.warning("No weather data fetched, skipping ingest.")
            return

        table = convert_to_arrow(record)
        ingest_tuya_weather(table, conn)
        upsert_tuya_weather(conn)
        clear_landing_table(conn)


if __name__ == "__main__":
    tuya_weather_pipeline()
