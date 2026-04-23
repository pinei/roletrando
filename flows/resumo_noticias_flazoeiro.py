'''
How to deploy

$ prefect deploy /app/flows/resumo_noticias_flazoeiro.py:resumo_noticias_flazoeiro_pipeline \
  --name resumo-noticias-flazoeiro-prod \
  --pool my-pool \
  --cron "0 */4 * * *"
'''

import os
import time
import logging

import google.generativeai as genai
import httpx
import pyarrow as pa
from prefect import flow, task, get_run_logger

from flows.lib.db import DbManager, Connection

SCHEMA = 'gizmosql_duck.noticias'
SOURCE_TABLE = 'youtube_video'
SILVER_TABLE = 'resumo_youtube_video'
LANDING_TABLE = 'resumo_youtube_video_landing'

SYSTEM_INSTRUCTION = """
Você é um assistente especializado em jornalismo esportivo, focado no Clube de Regatas do Flamengo. Sua tarefa é processar transcrições ou textos de notícias e gerar um resumo estruturado.

**Regras de Formatação e Conteúdo:**

1. Extensão: O resumo deve ter entre 3 e 5 parágrafos.
2. Limite: O texto total não deve ultrapassar 1200 caracteres.
3. Prioridade de Conteúdo:
   - Próximo Jogo: Detalhes de escalação, importância da partida, data e adversário (se presentes).
   - Mercado da Bola: Contratações concretizadas, especulações ou saídas (se presentes).
   - Geral: Outros temas relevantes como bastidores ou homenagens.
4. Tom: Informativo, direto e profissional.
5. Restrição: Não utilize bullet points; use apenas parágrafos de texto fluido.
"""


def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[RESUMO NOTICIAS FLAZOEIRO] {msg}", kwargs

    return PrefixLoggerAdapter(get_run_logger(), {})


@task(name="create-resumo-youtube-video-table", persist_result=False)
def create_resumo_table(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{SILVER_TABLE} (
                url     VARCHAR,
                summary VARCHAR,
                PRIMARY KEY (url)
            )
        """)
    logger.info(f"Ensured `{SILVER_TABLE}` table exists")


@task(name="fetch-pending-videos", persist_result=False)
def fetch_pending_videos(conn: Connection) -> list[dict]:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT v.url, v.title, v.description
            FROM {SCHEMA}.{SOURCE_TABLE} v
            LEFT JOIN {SCHEMA}.{SILVER_TABLE} r ON v.url = r.url
            WHERE r.url IS NULL
              AND v.description IS NOT NULL
              AND v.description != ''
        """)
        rows = cur.fetchall()

    records = [{"url": r[0], "title": r[1], "description": r[2]} for r in rows]
    logger.info(f"Found {len(records)} video(s) pending summarization")
    return records


def _generate_summary(title: str, description: str) -> str:
    genai.configure(api_key=os.environ["GEMINI_API_KEY"])
    model = genai.GenerativeModel(
        model_name="gemini-3-flash-preview",
        system_instruction=SYSTEM_INSTRUCTION,
    )
    prompt = f"Processe o texto de entrada abaixo e gere o resumo:\n\n-- TEXTO DE ENTRADA --\n{title}\n{description}"
    response = model.generate_content(prompt)
    return response.text


@task(name="summarize-videos")
def summarize_videos(pending: list[dict]) -> list[tuple]:
    logger = get_flow_logger()
    results = []
    for video in pending:
        try:
            summary = _generate_summary(video["title"], video["description"])
            results.append((video["url"], summary))
            logger.info(f"Summarized: {video['url']}")
        except Exception as e:
            logger.error(f"Failed to summarize {video['url']}: {e}")
            raise
    return results


@task(name="ingest-resumo-flazoeiro", persist_result=False)
def ingest_resumo(table: pa.Table, conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        rows_loaded = cur.adbc_ingest(
            table_name=LANDING_TABLE,
            data=table,
            mode="replace",
            catalog_name="gizmosql_duck",
            db_schema_name="noticias",
        )
    logger.info(f"Loaded {rows_loaded} row(s) into `{LANDING_TABLE}`")


@task(name="upsert-resumo-flazoeiro", persist_result=False)
def upsert_resumo(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {SCHEMA}.{SILVER_TABLE} (url, summary)
            SELECT url, summary
            FROM {SCHEMA}.{LANDING_TABLE}
            ON CONFLICT (url) DO UPDATE SET summary = EXCLUDED.summary
        """)
        logger.info(f"Upserted {cur.rowcount} row(s) into `{SILVER_TABLE}`")


@task(name="post-resumo-matrix")
def post_summaries_to_matrix(summaries: list[tuple]) -> None:
    logger = get_flow_logger()
    homeserver = os.environ["LARANJEIRA_MATRIX_HOMESERVER"]
    room_id = os.environ["LARANJEIRA_MATRIX_ROOM_ID"]
    token = os.environ["LARANJEIRA_MATRIX_TOKEN"]
    headers = {"Authorization": f"Bearer {token}"}

    for url, summary in summaries:
        body = f"{summary}\n\n{url}"
        txn_id = f"resumo_{int(time.time() * 1000)}"
        response = httpx.put(
            f"{homeserver}/_matrix/client/v3/rooms/{room_id}/send/m.room.message/{txn_id}",
            headers=headers,
            json={"msgtype": "m.text", "body": body},
            timeout=30,
        )
        response.raise_for_status()
        logger.info(f"Posted summary for {url} to Matrix room")


@task(name="clear-resumo-flazoeiro-landing", persist_result=False)
def clear_landing_table(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {SCHEMA}.{LANDING_TABLE}")
    logger.info(f"Cleared `{LANDING_TABLE}`")


@flow(name="resumo_noticias_flazoeiro_pipeline")
def resumo_noticias_flazoeiro_pipeline() -> None:
    logger = get_flow_logger()
    logger.info("Iniciando pipeline de resumo de notícias do Flazoeiro")

    with DbManager.from_env() as conn:
        create_resumo_table(conn)
        pending = fetch_pending_videos(conn)

    if not pending:
        logger.info("Nenhum vídeo pendente de resumo — encerrando.")
        return

    summaries = summarize_videos(pending)

    if not summaries:
        logger.warning("Nenhum resumo gerado — encerrando.")
        return

    cols = list(zip(*summaries))
    table = pa.table(cols, schema=pa.schema([
        ("url",     pa.string()),
        ("summary", pa.string()),
    ]))

    with DbManager.from_env() as conn:
        ingest_resumo(table, conn)
        upsert_resumo(conn)
        clear_landing_table(conn)

    post_summaries_to_matrix(summaries)


if __name__ == "__main__":
    resumo_noticias_flazoeiro_pipeline()
