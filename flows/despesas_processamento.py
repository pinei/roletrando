"""
Flow de processamento de despesas.

Executa processamentos no banco de dados após a ingestão das tabelas landing.
Depende de: despesa_corrente_landing, despesa_arquivo_landing, historico_usd_brl
Produz: despesa, despesas_corrente_brl, despesas_parceladas
"""
from prefect import flow, task, get_run_logger
import logging

from flows.lib.db import DbManager, Connection

SCHEMA = 'gizmosql_duck.despesas'
TABLE_DESPESA = 'despesa'
TABLE_CORRENTE_BRL = 'despesas_corrente_brl'
TABLE_PARCELADAS = 'despesas_parceladas'
VIEW_CATEGORIA = 'vw_categoria'
VIEW_GRUPO = 'vw_grupo'
TABLE_CATEGORIA_GOLD = 'despesas_categoria_gold'


def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[DESPESAS PROCESSAMENTO] {msg}", kwargs

    return PrefixLoggerAdapter(get_run_logger(), {})


@task(name="create-despesa", persist_result=False)
def create_despesa(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE OR REPLACE TABLE {SCHEMA}.{TABLE_DESPESA} AS
            SELECT * FROM {SCHEMA}.despesa_corrente_landing
            UNION ALL
            SELECT * FROM {SCHEMA}.despesa_arquivo_landing
        """)
    logger.info(f"Created `{TABLE_DESPESA}` (corrente + arquivo)")


@task(name="create-despesas-corrente-brl", persist_result=False)
def create_despesas_corrente_brl(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE OR REPLACE TABLE {SCHEMA}.{TABLE_CORRENTE_BRL} AS
            SELECT
                d.mes,
                d.data,
                d.dia,
                d.categoria,
                d.grupo,
                d.descricao,
                d.beneficiario,
                CASE
                    WHEN d.valor IS NULL AND d.valor_usd IS NOT NULL THEN
                        ROUND(d.valor_usd * h.preco, 2)
                    WHEN d.valor IS NOT NULL THEN d.valor
                    ELSE NULL
                END AS valor,
                d.parcelas,
                d.meio,
                d.valor_usd,
                d.valor_gbp
            FROM {SCHEMA}.{TABLE_DESPESA} d
            LEFT JOIN {SCHEMA}.historico_usd_brl h ON d.data = h.data
        """)
    logger.info(f"Created `{TABLE_CORRENTE_BRL}`")


@task(name="create-despesas-parceladas", persist_result=False)
def create_despesas_parceladas(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE OR REPLACE TABLE {SCHEMA}.{TABLE_PARCELADAS} AS (
                WITH numbered_parcelas AS (
                    SELECT
                        CAST(SUBSTR(mes, 1, 4) AS INTEGER) AS ano,
                        CAST(SUBSTR(mes, 6, 2) AS INTEGER) AS mes,
                        data,
                        categoria,
                        grupo,
                        descricao,
                        beneficiario,
                        valor,
                        parcelas,
                        ROUND(valor / parcelas, 2) AS valor_parcela,
                        UNNEST(GENERATE_SERIES(1, parcelas)) AS parcela_num
                    FROM {SCHEMA}.{TABLE_CORRENTE_BRL}
                    WHERE parcelas > 1
                )

                SELECT
                    CAST(
                        ano + ((mes + parcela_num - 2) // 12) AS VARCHAR
                    ) || '-' ||
                    LPAD(
                        CAST(
                            CASE
                                WHEN (mes + parcela_num - 1) % 12 = 0 THEN 12
                                ELSE (mes + parcela_num - 1) % 12
                            END AS VARCHAR
                        ), 2, '0')
                    AS mes,
                    data,
                    categoria,
                    grupo,
                    descricao || ' (' || parcela_num || '/' || parcelas || ')' AS descricao,
                    beneficiario,
                    valor_parcela AS valor
                FROM numbered_parcelas

                UNION ALL

                SELECT
                    mes,
                    data,
                    categoria,
                    grupo,
                    descricao,
                    beneficiario,
                    valor
                FROM {SCHEMA}.{TABLE_CORRENTE_BRL}
                WHERE parcelas = 1

                ORDER BY mes, data, descricao
            )
        """)
    logger.info(f"Created `{TABLE_PARCELADAS}`")


@task(name="create-view-categoria", persist_result=False)
def create_view_categoria(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE OR REPLACE VIEW {SCHEMA}.{VIEW_CATEGORIA} AS
            SELECT DISTINCT categoria, grupo
            FROM {SCHEMA}.{TABLE_PARCELADAS}
            WHERE categoria IS NOT NULL
              AND LEN(TRIM(categoria)) > 0
              AND categoria NOT IN ('Bens')
        """)
    logger.info(f"Created view `{VIEW_CATEGORIA}`")


@task(name="create-view-grupo", persist_result=False)
def create_view_grupo(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE OR REPLACE VIEW {SCHEMA}.{VIEW_GRUPO} AS
            SELECT DISTINCT grupo
            FROM {SCHEMA}.{TABLE_PARCELADAS}
            WHERE grupo IS NOT NULL
              AND LEN(TRIM(grupo)) > 0
              AND grupo NOT IN ('#N/A', 'Bens')
        """)
    logger.info(f"Created view `{VIEW_GRUPO}`")


@task(name="create-despesas-categoria-gold", persist_result=False)
def create_despesas_categoria_gold(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE OR REPLACE TABLE {SCHEMA}.{TABLE_CATEGORIA_GOLD} AS
            SELECT
                d.mes,
                c.categoria,
                g.grupo,
                ROUND(SUM(d.valor), 2) AS valor_total
            FROM {SCHEMA}.{VIEW_GRUPO} g
            LEFT OUTER JOIN {SCHEMA}.{VIEW_CATEGORIA} c ON c.grupo = g.grupo
            LEFT OUTER JOIN {SCHEMA}.{TABLE_PARCELADAS} d ON c.categoria = d.categoria
            WHERE d.mes < strftime(current_date, '%Y-%m')
            GROUP BY ALL
            ORDER BY mes
        """)
    logger.info(f"Created `{TABLE_CATEGORIA_GOLD}`")


@flow(name="despesas_processamento")
def despesas_processamento() -> None:
    logger = get_flow_logger()
    logger.info("Iniciando processamento de despesas")

    with DbManager.from_env() as conn:
        create_despesa(conn)
        create_despesas_corrente_brl(conn)
        create_despesas_parceladas(conn)
        create_view_categoria(conn)
        create_view_grupo(conn)
        create_despesas_categoria_gold(conn)


if __name__ == "__main__":
    despesas_processamento()
