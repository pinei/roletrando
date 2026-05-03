import os
import time
import logging

import httpx
from prefect import flow, task, get_run_logger

from flows.lib.db import DbManager, Connection

SCHEMA = 'gizmosql_duck.noticias'
SILVER_TABLE = 'resumo_youtube_video'


def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[NOTICIA FLAZOEIRO MATRIX] {msg}", kwargs

    return PrefixLoggerAdapter(get_run_logger(), {})


@task(name="fetch-summary-by-url", persist_result=False)
def fetch_summary(url: str, conn: Connection) -> str | None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT summary FROM {SCHEMA}.{SILVER_TABLE} WHERE url = ?",
            [url],
        )
        row = cur.fetchone()
    if row is None:
        logger.warning(f"No summary found for {url}")
        return None
    return row[0]


@task(name="post-summary-matrix", persist_result=False)
def post_summary_to_matrix(url: str, summary: str) -> None:
    logger = get_flow_logger()
    homeserver = os.environ["LARANJEIRA_MATRIX_HOMESERVER"]
    room_id = os.environ["LARANJEIRA_MATRIX_ROOM_ID"]
    token = os.environ["LARANJEIRA_MATRIX_TOKEN"]

    body = f"{summary}\n\n{url}"
    txn_id = f"resumo_{int(time.time() * 1000)}"
    response = httpx.put(
        f"{homeserver}/_matrix/client/v3/rooms/{room_id}/send/m.room.message/{txn_id}",
        headers={"Authorization": f"Bearer {token}"},
        json={"msgtype": "m.text", "body": body},
        timeout=30,
    )
    response.raise_for_status()
    logger.info(f"Posted summary for {url} to Matrix room")


@flow(name="noticia_flazoeiro_matrix_pipeline")
def noticia_flazoeiro_matrix_pipeline(url: str) -> None:
    logger = get_flow_logger()
    logger.info(f"Iniciando pipeline Matrix para {url}")

    with DbManager.from_env() as conn:
        summary = fetch_summary(url, conn)

    if not summary:
        logger.warning("Resumo não encontrado — encerrando.")
        return

    post_summary_to_matrix(url, summary)


if __name__ == "__main__":
    import sys
    noticia_flazoeiro_matrix_pipeline(url=sys.argv[1])
