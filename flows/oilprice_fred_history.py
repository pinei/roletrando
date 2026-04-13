'''
How to deploy

$ prefect deploy /app/flows/oilprice_fred_history.py:oilprice_fred_history_pipeline \
  --name oilprice-fred-history-prod \
  --pool my-pool \
  --cron "0 9 * * 1-5"
'''

import os
from datetime import date, datetime, timedelta
import logging

import httpx
import pyarrow as pa
from prefect import flow, task, get_run_logger

from flows.lib.db import DbManager, Connection

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

OIL_SERIES = {
    "WTI":   "DCOILWTICO",
    "Brent": "DCOILBRENTEU",
}

SCHEMA = 'gizmosql_duck.series'
SILVER_TABLE = 'oilprice_fred_history'
LANDING_TABLE = 'oilprice_fred_history_landing'

def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[OILPRICE HISTORY] {msg}", kwargs

    return PrefixLoggerAdapter(get_run_logger(), {})


@task(name="fetch-oilprice-data")
def fetch_oilprice_data(
    name: str,
    series_id: str,
    start_date: str,
    end_date: str,
) -> list[tuple]:
    """
    Fetches daily oil price observations from FRED and forward-fills missing dates.

    Parameters:
    name (str): Human-readable name (e.g. "WTI", "Brent")
    series_id (str): FRED series identifier
    start_date (str): Start date in 'YYYY-MM-DD' format
    end_date (str): End date in 'YYYY-MM-DD' format

    Returns:
    List of (name, date, value) tuples with all dates filled.
    """
    logger = get_flow_logger()

    response = httpx.get(
        FRED_BASE_URL,
        params={
            "series_id": series_id,
            "observation_start": start_date,
            "observation_end": end_date,
            "api_key": os.environ["FRED_API_KEY"],
            "file_type": "json",
        },
        timeout=30,
    )
    response.raise_for_status()

    # FRED uses "." to represent missing observations — skip those
    raw = [
        {"date": date.fromisoformat(obs["date"]), "value": float(obs["value"])}
        for obs in response.json()["observations"]
        if obs["value"] != "."
    ]

    if not raw:
        logger.warning(f"No observations returned for {name} ({series_id})")
        return []

    # Forward-fill missing dates (weekends, holidays)
    records: list[tuple] = []
    last_date: date | None = None
    last_value: float | None = None

    for item in raw:
        while last_date is not None and last_date + timedelta(days=1) < item["date"]:
            last_date += timedelta(days=1)
            records.append((name, last_date, last_value))

        last_date = item["date"]
        last_value = item["value"]
        records.append((name, last_date, last_value))

    logger.info(f"Fetched {len(records)} records for {name} ({series_id})")
    return records


@task(name="create-oilprice-history-table", persist_result=False)
def create_oilprice_history_table(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{SILVER_TABLE} (
                name   VARCHAR,
                date   DATE,
                value  DECIMAL(10, 4),
                PRIMARY KEY (name, date)
            )
        """)
    logger.info(f"Ensured `{SILVER_TABLE}` table exists")


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
            INSERT INTO {SCHEMA}.{SILVER_TABLE} (name, date, value)
            SELECT name, date, value
            FROM {SCHEMA}.{LANDING_TABLE}
            ON CONFLICT (name, date) DO UPDATE SET value = EXCLUDED.value
        """)
        logger.info(f"Upserted {cur.rowcount} rows into `{SILVER_TABLE}`")


@task(name="clear-oilprice-history-landing", persist_result=False)
def clear_landing_table(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {SCHEMA}.{LANDING_TABLE}")
    logger.info(f"Cleared `{LANDING_TABLE}` table")


@flow(name="oilprice_fred_history_pipeline")
def oilprice_fred_history_pipeline() -> None:
    logger = get_flow_logger()

    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Fetching oil price history from {start_date} to {end_date}")

    with DbManager.from_env() as conn:
        create_oilprice_history_table(conn)

        for name, series_id in OIL_SERIES.items():
            records = fetch_oilprice_data(
                name=name,
                series_id=series_id,
                start_date=start_date,
                end_date=end_date,
            )

            if not records:
                logger.warning(f"No data for {name}, skipping ingest.")
                continue

            cols = list(zip(*records))
            table = pa.table(
                cols,
                schema=pa.schema([
                    ("name",  pa.string()),
                    ("date",  pa.date32()),
                    ("value", pa.float64()),
                ]),
            )
            ingest_oilprice_history(table, conn)

        upsert_oilprice_history(conn)
        clear_landing_table(conn)


if __name__ == "__main__":
    oilprice_fred_history_pipeline()
