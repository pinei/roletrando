"""
Flow de histórico de cotações USD/BRL para despesas.

Responsável pela ingestão na tabela "despesas.historico_usd_brl".
Dados são usados para conversão de despesas de US$ para R$.

Não tem relação com o "currency_history" que tem seu próprio histórico de
cotações para uso no app `Carrossel`.
"""
from datetime import datetime, timedelta
import logging

import httpx
import pandas as pd
from prefect import flow, task, get_run_logger

from flows.lib.db import DbManager, Connection

BCB_SERIES_USD_BRL = "10813"

SCHEMA = 'gizmosql_duck.despesas'
SILVER_TABLE = 'historico_usd_brl'
LANDING_TABLE = 'historico_usd_brl_landing'


def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[DESPESAS HISTORICO USD] {msg}", kwargs

    return PrefixLoggerAdapter(get_run_logger(), {})


@task(name="fetch-usd-brl-data", persist_result=False)
def fetch_usd_brl_data(start_date: str, end_date: str) -> pd.DataFrame | None:
    """
    Obtém o histórico de cotações do dólar comercial (USD/BRL) do Banco Central do Brasil.

    Parâmetros:
    start_date (str): Data de início no formato 'YYYY-MM-DD'
    end_date (str): Data de fim no formato 'YYYY-MM-DD'

    Retorna:
    DataFrame com as colunas: data (datetime), preco (float)
    """
    logger = get_flow_logger()

    start_date_fmt = datetime.strptime(start_date, "%Y-%m-%d").strftime("%d/%m/%Y")
    end_date_fmt = datetime.strptime(end_date, "%Y-%m-%d").strftime("%d/%m/%Y")

    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{BCB_SERIES_USD_BRL}/dados"
    params = {"formato": "json", "dataInicial": start_date_fmt, "dataFinal": end_date_fmt}

    response = httpx.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    df = pd.DataFrame(data)

    df["data"] = pd.to_datetime(df["data"], dayfirst=True)
    df["valor"] = pd.to_numeric(df["valor"].str.replace(",", "."))
    df = df.rename(columns={"valor": "preco"})
    df = df[["data", "preco"]].sort_values("data").reset_index(drop=True)

    logger.info(f"Fetched {len(df)} records from BCB (USD/BRL series {BCB_SERIES_USD_BRL})")
    return df


def preencher_gaps_datas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Completa as datas ausentes com o último preço disponível (forward fill).
    Converte a coluna 'data' para string 'YYYY-MM-DD' ao final.
    """
    if df.empty:
        return df

    todas_datas = pd.date_range(start=df["data"].min(), end=df["data"].max(), freq="D")
    df_completo = pd.DataFrame({"data": todas_datas})
    df_completo = df_completo.merge(df, on="data", how="left")
    df_completo["preco"] = df_completo["preco"].ffill()
    df_completo["data"] = df_completo["data"].dt.strftime("%Y-%m-%d")

    return df_completo


@task(name="create-historico-usd-brl-table", persist_result=False)
def create_table_if_not_exists(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{SILVER_TABLE} (
                data    VARCHAR,
                preco   DECIMAL(10, 4),
                PRIMARY KEY (data)
            )
            """
        )
    logger.info(f"Table `{SILVER_TABLE}` ensured")


@task(name="ingest-historico-usd-brl", persist_result=False)
def ingest_historico_usd_brl(df: pd.DataFrame, conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        rows_loaded = cur.adbc_ingest(
            table_name=LANDING_TABLE,
            data=df,
            mode="replace",
            catalog_name="gizmosql_duck",
            db_schema_name="despesas",
        )
    logger.info(f"Loaded {rows_loaded} rows into `{LANDING_TABLE}`")


@task(name="upsert-historico-usd-brl", persist_result=False)
def upsert_historico_usd_brl(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {SCHEMA}.{SILVER_TABLE} (data, preco)
            SELECT data, preco
            FROM {SCHEMA}.{LANDING_TABLE}
            ON CONFLICT (data) DO UPDATE SET preco = EXCLUDED.preco
            """
        )
        logger.info(f"Upserted {cur.rowcount} rows into `{SILVER_TABLE}`")


@task(name="clear-historico-usd-brl-landing", persist_result=False)
def clear_landing_table(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {SCHEMA}.{LANDING_TABLE}")
    logger.info(f"Cleared `{LANDING_TABLE}` table")


@flow(name="despesas_historico_usd")
def despesas_historico_usd() -> None:
    logger = get_flow_logger()

    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Fetching USD/BRL history from {start_date} to {end_date}")

    df = fetch_usd_brl_data(start_date, end_date)

    if df is None or df.empty:
        logger.warning("Nenhum dado retornado pela API do BCB — interrompendo flow")
        return

    df = preencher_gaps_datas(df)

    with DbManager.from_env() as conn:
        create_table_if_not_exists(conn)
        ingest_historico_usd_brl(df, conn)
        upsert_historico_usd_brl(conn)
        clear_landing_table(conn)


if __name__ == "__main__":
    despesas_historico_usd()
