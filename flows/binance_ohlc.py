'''
How to deploy

$ prefect deploy /app/flows/binance_ohlc.py:binance_ohlc_pipeline \
  --name binance-ohlc-5m-prod --pool my-pool \
  --cron "*/5 * * * *" --param interval=5m

$ prefect deploy /app/flows/binance_ohlc.py:binance_ohlc_pipeline \
  --name binance-ohlc-30m-prod --pool my-pool \
  --cron "*/30 * * * *" --param interval=30m

$ prefect deploy /app/flows/binance_ohlc.py:binance_ohlc_pipeline \
  --name binance-ohlc-4h-prod --pool my-pool \
  --cron "0 */4 * * *" --param interval=4h
'''

import logging
from datetime import datetime

import httpx
import pyarrow as pa
from prefect import flow, task, get_run_logger

from flows.lib.db import DbManager, Connection

BINANCE_BASE_URL = "https://api.binance.com/api/v3/klines"
SYMBOL = "BTCUSDT"

SCHEMA = 'gizmosql_duck.series'


def get_flow_logger(interval: str):
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[BINANCE OHLC {interval.upper()}] {msg}", kwargs

    return PrefixLoggerAdapter(get_run_logger(), {"flow_name": f"BINANCE OHLC {interval.upper()}"})


@task(name="create-binance-ohlc-table", persist_result=False)
def create_table(conn: Connection, interval: str) -> None:
    logger = get_flow_logger(interval)
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.binance_ohlc_{interval} (
            symbol VARCHAR,
            open_timestamp TIMESTAMP,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume FLOAT,
            close_timestamp TIMESTAMP,
            fetched_at TIMESTAMP,
            PRIMARY KEY (symbol, open_timestamp)
            )
        """)
    logger.info(f"Ensured `series.binance_ohlc_{interval}` table exists")


@task(name="fetch-binance-ohlc")
def fetch_ohlc(symbol: str, interval: str, limit: int = 100) -> list[tuple]:
    logger = get_flow_logger(interval)
    logger.info(f'Fetching {symbol} using "{interval}" interval')

    params = {"symbol": symbol, "interval": interval, "limit": limit}
    response = httpx.get(BINANCE_BASE_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    if not data:
        logger.warning(f"No data returned for {symbol} with interval {interval}")
        return []

    records = []
    for record in data:
        open_timestamp = datetime.fromtimestamp(int(record[0]) / 1000)
        close_timestamp = datetime.fromtimestamp(int(record[6]) / 1000)
        fetched_at = datetime.now()
        records.append((
            symbol,
            open_timestamp,
            float(record[1]),
            float(record[2]),
            float(record[3]),
            float(record[4]),
            float(record[5]),
            close_timestamp,
            fetched_at,
        ))

    logger.info(f"Fetched {len(records)} records for {symbol}")
    return records


def convert_to_arrow(records: list[tuple]) -> pa.Table:
    schema = pa.schema([
        ("symbol", pa.string()),
        ("open_timestamp", pa.timestamp("us")),
        ("open", pa.float32()),
        ("high", pa.float32()),
        ("low", pa.float32()),
        ("close", pa.float32()),
        ("volume", pa.float32()),
        ("close_timestamp", pa.timestamp("us")),
        ("fetched_at", pa.timestamp("us")),
    ])
    return pa.table({
        "symbol": [r[0] for r in records],
        "open_timestamp": [r[1] for r in records],
        "open": [r[2] for r in records],
        "high": [r[3] for r in records],
        "low": [r[4] for r in records],
        "close": [r[5] for r in records],
        "volume": [r[6] for r in records],
        "close_timestamp": [r[7] for r in records],
        "fetched_at": [r[8] for r in records],
    }, schema=schema)


@task(name="ingest-binance-ohlc", persist_result=False)
def ingest_ohlc(table: pa.Table, conn: Connection, interval: str) -> None:
    logger = get_flow_logger(interval)
    with conn.cursor() as cur:
        rows_loaded = cur.adbc_ingest(
            table_name=f"binance_ohlc_{interval}_landing",
            data=table,
            mode="replace",
            catalog_name="gizmosql_duck",
            db_schema_name="series",
        )
        logger.info(f"Loaded {rows_loaded} rows into `binance_ohlc_{interval}_landing` table")


@task(name="upsert-binance-ohlc", persist_result=False)
def upsert_ohlc(conn: Connection, interval: str) -> None:
    logger = get_flow_logger(interval)
    table_name = f"binance_ohlc_{interval}"
    landing_table_name = f"binance_ohlc_{interval}_landing"
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {SCHEMA}.{table_name}
            SELECT * FROM {SCHEMA}.{landing_table_name}
            ON CONFLICT (symbol, open_timestamp)
            DO UPDATE SET
              open = EXCLUDED.open,
              high = EXCLUDED.high,
              low = EXCLUDED.low,
              close = EXCLUDED.close,
              volume = EXCLUDED.volume,
              close_timestamp = EXCLUDED.close_timestamp,
              fetched_at = EXCLUDED.fetched_at
        """)
        logger.info(f"Upserted {cur.rowcount} rows into `{table_name}` table")


@task(name="clear-binance-ohlc-landing", persist_result=False)
def clear_landing_table(conn: Connection, interval: str) -> None:
    logger = get_flow_logger(interval)
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {SCHEMA}.binance_ohlc_{interval}_landing")
        logger.info(f"Cleared `binance_ohlc_{interval}_landing` table")


@flow(name="binance_ohlc_pipeline")
def binance_ohlc_pipeline(interval: str = "5m") -> None:
    logger = get_flow_logger(interval)
    logger.info(f"Starting OHLC pipeline for {SYMBOL} ({interval})")

    with DbManager.from_env() as conn:
        create_table(conn, interval)

        records = fetch_ohlc(SYMBOL, interval, 100)

        if not records:
            logger.warning(f"No records fetched for {SYMBOL}, skipping ingest.")
            return

        table = convert_to_arrow(records)
        ingest_ohlc(table, conn, interval)
        upsert_ohlc(conn, interval)
        clear_landing_table(conn, interval)


if __name__ == "__main__":
    binance_ohlc_pipeline()
