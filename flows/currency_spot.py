'''
How to deploy

$ prefect deploy /app/flows/currency_spot.py:currency_spot_pipeline \
  --name currency-spot-prod \
  --pool my-pool \
  --cron "*/15 * * * *"
'''

from datetime import datetime
import logging

import httpx
import pyarrow as pa
from prefect import flow, task, get_run_logger

from flows.lib.db import DbManager, Connection

AWESOMEAPI_URL = "https://economia.awesomeapi.com.br/json/last/USD-BRL,EUR-BRL,GBP-BRL"


def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[CURRENCY SPOT] {msg}", kwargs

    return PrefixLoggerAdapter(get_run_logger(), {})


@task(name="create-currency-spot-table", persist_result=False)
def create_currency_spot_table(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gizmosql_duck.series.currency_spot (
                name       VARCHAR,
                base_name  VARCHAR,
                fetch_time TIMESTAMP,
                value      FLOAT,
                PRIMARY KEY (name, base_name)
            )
        """)
    logger.info("Ensured `series.currency_spot` table exists")


@task(name="fetch-currency-spot")
def fetch_currency_spot() -> list[tuple] | None:
    logger = get_flow_logger()
    fetched_at = datetime.now()
    response = httpx.get(AWESOMEAPI_URL, timeout=30)
    response.raise_for_status()

    data = response.json()
    if not data:
        logger.warning("Empty response from AwesomeAPI, skipping.")
        return None

    records = [
        (item["code"], item["codein"], fetched_at, float(item["bid"]))
        for item in data.values()
    ]
    logger.info(f"Fetched spot rates for: {[r[0] for r in records]}")
    return records


@task(name="ingest-currency-spot", persist_result=False)
def ingest_currency_spot(table: pa.Table, conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        rows_loaded = cur.adbc_ingest(
            table_name="currency_spot_landing",
            data=table,
            mode="replace",
            catalog_name="gizmosql_duck",
            db_schema_name="series",
        )
        logger.info(f"Loaded {rows_loaded} rows into `currency_spot_landing` table")


@task(name="upsert-currency-spot", persist_result=False)
def upsert_currency_spot(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO gizmosql_duck.series.currency_spot (name, base_name, fetch_time, value)
            SELECT name, base_name, fetch_time, value
            FROM gizmosql_duck.series.currency_spot_landing
            ON CONFLICT (name, base_name) DO UPDATE SET
                fetch_time = EXCLUDED.fetch_time,
                value = EXCLUDED.value
        """)
        logger.info(f"Upserted {cur.rowcount} rows into `currency_spot` table")


@flow(name="currency_spot_pipeline")
def currency_spot_pipeline() -> None:
    logger = get_flow_logger()
    logger.info("Starting currency spot pipeline")

    with DbManager.from_env() as conn:
        create_currency_spot_table(conn)

        records = fetch_currency_spot()
        if not records:
            logger.warning("No records fetched, aborting.")
            return

        cols = list(zip(*records))
        schema = pa.schema([
            ("name", pa.string()),
            ("base_name", pa.string()),
            ("fetch_time", pa.timestamp("us")),
            ("value", pa.float32()),
        ])
        table = pa.table(cols, names=["name", "base_name", "fetch_time", "value"], schema=schema)

        ingest_currency_spot(table, conn)
        upsert_currency_spot(conn)


if __name__ == "__main__":
    currency_spot_pipeline()
