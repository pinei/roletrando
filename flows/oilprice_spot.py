'''
How to deploy

$ prefect deploy /app/flows/oilprice_spot.py:oilprice_spot_pipeline \
  --name oilprice-spot-prod \
  --pool my-pool \
  --cron "*/15 * * * *"
'''

import os
from datetime import datetime
import logging

import httpx
import pyarrow as pa
from prefect import flow, task, get_run_logger

from flows.lib.db import DbManager, Connection

OILPRICE_API_URL = "https://api.oilpriceapi.com/v1/prices/latest"

OIL_CODES = ["WTI_USD", "BRENT_CRUDE_USD", "DUBAI_CRUDE_USD"]

SCHEMA = 'gizmosql_duck.series'
SILVER_TABLE = 'oilprice_spot'
LANDING_TABLE = 'oilprice_spot_landing'


def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[OILPRICE SPOT] {msg}", kwargs

    return PrefixLoggerAdapter(get_run_logger(), {})


@task(name="create-oilprice-spot-table", persist_result=False)
def create_oilprice_spot_table(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{SILVER_TABLE} (
                code        VARCHAR,
                fetch_time  TIMESTAMP,
                price       DECIMAL(10, 4),
                PRIMARY KEY (code)
            )
        """)
    logger.info(f"Ensured `{SILVER_TABLE}` table exists")


@task(name="fetch-oilprice-spot")
def fetch_oilprice_spot() -> list[tuple] | None:
    logger = get_flow_logger()
    fetch_time = datetime.now()

    response = httpx.get(
        OILPRICE_API_URL,
        params={"by_code": ",".join(OIL_CODES)},
        headers={
            "Authorization": f"Token {os.environ['OILPRICE_API_KEY']}",
            "Content-Type": "application/json",
        },
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()
    if data.get("status") != "success":
        logger.warning(f"Unexpected API response status: {data.get('status')}")
        return None

    records = [
        (item["code"], fetch_time, float(item["price"]))
        for item in data["data"]["prices"]
    ]
    logger.info(f"Fetched spot prices for: {[r[0] for r in records]}")
    return records


@task(name="ingest-oilprice-spot", persist_result=False)
def ingest_oilprice_spot(table: pa.Table, conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        rows_loaded = cur.adbc_ingest(
            table_name=LANDING_TABLE,
            data=table,
            mode="replace",
            catalog_name="gizmosql_duck",
            db_schema_name="series",
        )
        logger.info(f"Loaded {rows_loaded} rows into `{LANDING_TABLE}`")


@task(name="upsert-oilprice-spot", persist_result=False)
def upsert_oilprice_spot(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {SCHEMA}.{SILVER_TABLE} (code, fetch_time, price)
            SELECT code, fetch_time, price
            FROM {SCHEMA}.{LANDING_TABLE}
            ON CONFLICT (code) DO UPDATE SET
                fetch_time = EXCLUDED.fetch_time,
                price      = EXCLUDED.price
        """)
        logger.info(f"Upserted {cur.rowcount} rows into `{SILVER_TABLE}`")


@flow(name="oilprice_spot_pipeline")
def oilprice_spot_pipeline() -> None:
    logger = get_flow_logger()
    logger.info("Starting oil price spot pipeline")

    with DbManager.from_env() as conn:
        create_oilprice_spot_table(conn)

        records = fetch_oilprice_spot()
        if not records:
            logger.warning("No records fetched, aborting.")
            return

        cols = list(zip(*records))
        table = pa.table(
            cols,
            schema=pa.schema([
                ("code",       pa.string()),
                ("fetch_time", pa.timestamp("s")),
                ("price",      pa.float64()),
            ]),
        )

        ingest_oilprice_spot(table, conn)
        upsert_oilprice_spot(conn)


if __name__ == "__main__":
    oilprice_spot_pipeline()
