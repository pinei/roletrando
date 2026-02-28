'''
How to deploy

$ prefect deploy /app/flows/currency_history.py:currency_history_pipeline \
  --name currency-history-prod \
  --pool my-pool \
  --cron "0 9 * * 1-5"
'''

import os
from datetime import date, datetime, timedelta

import httpx
import pyarrow as pa
import logging
from adbc_driver_flightsql import dbapi as gizmosql
from prefect import flow, task, get_run_logger

GIZMOSQL_URL = os.environ["GIZMOSQL_URL"]
GIZMOSQL_USERNAME = os.environ["GIZMOSQL_USERNAME"]
GIZMOSQL_PASSWORD = os.environ["GIZMOSQL_PASSWORD"]

CURRENCY_CODES = {
    "USD": "1",      # Dólar americano
    "EUR": "21619",  # Euro
    "GBP": "21623",  # Libra esterlina
}

def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[{self.extra['flow_name']}] {msg}", kwargs

    logger = PrefixLoggerAdapter(get_run_logger(), {"flow_name": "CURRENCY HISTORY"})
    return logger

@task(name="fetch-currency-data")
def fetch_currency_data(
    currency_name: str,
    currency_base: str,
    series_code: str,
    start_date: str,
    end_date: str,
) -> list[tuple]:
    logger = get_flow_logger()
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados"
        f"?formato=json&dataInicial={start_date}&dataFinal={end_date}"
    )
    response = httpx.get(url, timeout=30)
    response.raise_for_status()

    raw = [
        {
            "date": date(
                int(d["data"][6:]),
                int(d["data"][3:5]),
                int(d["data"][:2]),
            ),
            "value": float(d["valor"].replace(",", ".")),
        }
        for d in response.json()
    ]

    records: list[tuple] = []
    last_date: date | None = None
    last_value: float | None = None

    for item in raw:
        while last_date is not None and last_date + timedelta(days=1) < item["date"]:
            last_date += timedelta(days=1)
            records.append((currency_name, currency_base, last_date, last_value))

        last_date = item["date"]
        last_value = item["value"]
        records.append((currency_name, currency_base, last_date, last_value))

    logger.info(f"Fetched {len(records)} records for {currency_name}/{currency_base}")
    return records


@task(name="ingest-currency-history", persist_result=False)
def ingest_currency_history(table: pa.Table, conn: gizmosql.Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        rows_loaded = cur.adbc_ingest(
            table_name="currency_history_landing",
            data=table,
            mode="append",
            catalog_name="gizmosql_duck",
            db_schema_name="series",
        )
        logger.info(f"Loaded {rows_loaded} rows into `currency_history_landing` table")

@task(name="upsert-currency-history", persist_result=False)
def upsert_currency_history(conn: gizmosql.Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO series.currency_history (name, base_name, date, value)
            SELECT name, base_name, date, value
            FROM series.currency_history_landing
            ON CONFLICT (name, base_name, date) DO UPDATE SET value = EXCLUDED.value
            """
        )
        rows_loaded = cur.fetchone()[0]
        logger.info(f"Upserted {rows_loaded} rows into `currency_history` table")

@task(name="fetch_currency_history")
def fetch_currency_history(
    currency_name: str,
    series_code: str,
    currency_base: str = "BRL",
    start_date: str = (datetime.now() - timedelta(days=30)).strftime("%d/%m/%Y"),
    end_date: str = datetime.now().strftime("%d/%m/%Y"),
) -> pa.Table | None:
    logger = get_flow_logger()
    records = fetch_currency_data(
        currency_name=currency_name,
        currency_base=currency_base,
        series_code=series_code,
        start_date=start_date,
        end_date=end_date,
    )
    if not records:
        logger.warning(f"No data returned for {currency_name}, skipping.")
        return None

    columns = list(zip(*records))
    columns_names = ["name", "base_name", "date", "value"]

    return pa.table(columns, names=columns_names)

@task(name="clear_landing_table", persist_result=False)
def clear_landing_table(conn: gizmosql.Connection) -> None:
    logger = get_flow_logger()
    logger.info("Cleared `currency_history_landing` table")
    with conn.cursor() as cur:
        cur.execute("DELETE FROM series.currency_history_landing")


@flow(name="currency_history_pipeline")
def currency_history_pipeline() -> None:
    logger = get_flow_logger()
    # 30 days
    start_date = (datetime.now() - timedelta(days=30)).strftime("%d/%m/%Y")
    end_date = datetime.now().strftime("%d/%m/%Y")

    logger.info(f"Fetching currency history from {start_date} to {end_date}")

    with gizmosql.connect(
        uri=GIZMOSQL_URL,
        db_kwargs={
            "username": GIZMOSQL_USERNAME,
            "password": GIZMOSQL_PASSWORD,
        },
        autocommit=True,
    ) as conn:
        for currency_name, series_code in CURRENCY_CODES.items():
            table = fetch_currency_history(
                currency_name=currency_name,
                series_code=series_code,
                start_date=start_date,
                end_date=end_date,
            )

            if table is not None:
                ingest_currency_history(table, conn)

        upsert_currency_history(conn)
        clear_landing_table(conn)




if __name__ == "__main__":
    currency_history_pipeline()
