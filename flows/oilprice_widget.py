'''
How to deploy

$ prefect deploy /app/flows/oilprice_widget.py:oilprice_widget_pipeline \
  --name oilprice-widget-prod \
  --pool my-pool \
  --cron "0 9 * * 1-5"
'''

import logging
from datetime import datetime

import httpx
import pyarrow as pa
from prefect import flow, get_run_logger, task

from flows.lib.db import Connection, DbManager

OIL_CODES = ["brent_crude_usd", "wti_crude_usd", "natural_gas_usd", "diesel_usd"]

SCHEMA = 'gizmosql_duck.series'
SILVER_TABLE = 'oilprice_widget'
LANDING_TABLE = 'oilprice_widget_landing'

_ARROW_SCHEMA = pa.schema([
    pa.field("code", pa.string()),
    pa.field("date", pa.date32()),
    pa.field("price", pa.float64()),
    pa.field("updated_at", pa.timestamp("us", tz="UTC")),
])


def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[OILPRICE WIDGET] {msg}", kwargs
    return PrefixLoggerAdapter(get_run_logger(), {})


@task(name="fetch-oilprice-widget", persist_result=False)
def fetch_oilprice_widget() -> pa.Table:
    logger = get_flow_logger()
    url = "https://api.oilpriceapi.com/v1/prices/widget"
    response = httpx.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()
    updated_at = datetime.fromisoformat(data["updated_at"].replace("Z", "+00:00"))
    date_val = updated_at.date()

    records = [(code, date_val, float(data[code]), updated_at) for code in OIL_CODES]
    logger.info(f"Fetched {len(records)} oil price records for date {date_val}")

    codes, dates, prices, timestamps = zip(*records)
    return pa.table(
        {
            "code": pa.array(codes, type=pa.string()),
            "date": pa.array(dates, type=pa.date32()),
            "price": pa.array(prices, type=pa.float64()),
            "updated_at": pa.array(timestamps, type=pa.timestamp("us", tz="UTC")),
        },
        schema=_ARROW_SCHEMA,
    )


@task(name="create-oilprice-widget-table", persist_result=False)
def create_oilprice_widget_table(conn: Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{SILVER_TABLE} (
                code       VARCHAR,
                date       DATE,
                price      DECIMAL(10, 4),
                updated_at TIMESTAMP,
                PRIMARY KEY (code, date)
            )
        """)


@task(name="ingest-oilprice-widget", persist_result=False)
def ingest_oilprice_widget(table: pa.Table, conn: Connection) -> None:
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


@task(name="upsert-oilprice-widget", persist_result=False)
def upsert_oilprice_widget(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {SCHEMA}.{SILVER_TABLE} (code, date, price, updated_at)
            SELECT code, date, price, updated_at
            FROM {SCHEMA}.{LANDING_TABLE}
            ON CONFLICT (code, date) DO UPDATE SET
                price      = EXCLUDED.price,
                updated_at = EXCLUDED.updated_at
        """)
        logger.info(f"Upserted {cur.rowcount} rows into `{SILVER_TABLE}`")


@task(name="clear-oilprice-widget-landing", persist_result=False)
def clear_landing_table(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {SCHEMA}.{LANDING_TABLE}")
    logger.info(f"Cleared `{LANDING_TABLE}`")


@flow(name="oilprice_widget_pipeline")
def oilprice_widget_pipeline() -> None:
    logger = get_flow_logger()
    logger.info("Iniciando pipeline de preços de petróleo")

    table = fetch_oilprice_widget()

    with DbManager.from_env() as conn:
        create_oilprice_widget_table(conn)
        ingest_oilprice_widget(table, conn)
        upsert_oilprice_widget(conn)
        clear_landing_table(conn)


if __name__ == "__main__":
    oilprice_widget_pipeline()
