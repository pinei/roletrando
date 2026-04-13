'''
How to deploy

$ prefect deploy /app/flows/oilprice_history.py:oilprice_history_pipeline \
  --name oilprice-history-prod \
  --pool my-pool \
  --cron "0 9 * * 1-5"
'''

import os
from datetime import date
import logging

import httpx
import pyarrow as pa
from prefect import flow, task, get_run_logger

from flows.lib.db import DbManager, Connection

OILPRICE_API_URL = "https://api.oilpriceapi.com/v1/prices/past_month"

OIL_CODES = ["WTI_USD", "BRENT_CRUDE_USD", "DUBAI_CRUDE_USD"]

SCHEMA = 'gizmosql_duck.series'
SILVER_TABLE = 'oilprice_history'
LANDING_TABLE = 'oilprice_history_landing'


def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[OILPRICE HISTORY] {msg}", kwargs

    return PrefixLoggerAdapter(get_run_logger(), {})


@task(name="create-oilprice-history-table", persist_result=False)
def create_oilprice_history_table(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{SILVER_TABLE} (
                code   VARCHAR,
                date   DATE,
                price  DECIMAL(10, 4),
                PRIMARY KEY (code, date)
            )
        """)
    logger.info(f"Ensured `{SILVER_TABLE}` table exists")


@task(name="fetch-oilprice-history")
def fetch_oilprice_history() -> list[tuple] | None:
    logger = get_flow_logger()

    response = httpx.get(
        OILPRICE_API_URL,
        params={"by_code": ",".join(OIL_CODES), "interval": "1d", "by_type": "spot_price"},
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
        (item["code"], date.fromisoformat(item["created_at"][:10]), float(item["price"]))
        for item in data["data"]["prices"]
    ]
    logger.info(f"Fetched {len(records)} historical records for {OIL_CODES}")
    return records


@task(name="ingest-oilprice-history", persist_result=False)
def ingest_oilprice_history(table: pa.Table, conn: Connection) -> None:
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


@task(name="upsert-oilprice-history", persist_result=False)
def upsert_oilprice_history(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {SCHEMA}.{SILVER_TABLE} (code, date, price)
            SELECT code, date, price
            FROM {SCHEMA}.{LANDING_TABLE}
            ON CONFLICT (code, date) DO UPDATE SET price = EXCLUDED.price
        """)
        logger.info(f"Upserted {cur.rowcount} rows into `{SILVER_TABLE}`")


@task(name="clear-oilprice-history-landing", persist_result=False)
def clear_landing_table(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {SCHEMA}.{LANDING_TABLE}")
    logger.info(f"Cleared `{LANDING_TABLE}` table")


@flow(name="oilprice_history_pipeline")
def oilprice_history_pipeline() -> None:
    logger = get_flow_logger()
    logger.info("Starting oil price history pipeline")

    with DbManager.from_env() as conn:
        create_oilprice_history_table(conn)

        records = fetch_oilprice_history()
        if not records:
            logger.warning("No records fetched, aborting.")
            return

        cols = list(zip(*records))
        table = pa.table(
            cols,
            schema=pa.schema([
                ("code",  pa.string()),
                ("date",  pa.date32()),
                ("price", pa.float64()),
            ]),
        )

        ingest_oilprice_history(table, conn)
        upsert_oilprice_history(conn)
        clear_landing_table(conn)


if __name__ == "__main__":
    oilprice_history_pipeline()
