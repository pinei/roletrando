'''
How to deploy

$ prefect deploy /app/flows/noticias_flazoeiro.py:noticias_flazoeiro_pipeline \
  --name noticias-flazoeiro-prod \
  --pool my-pool \
  --cron "0 */3 * * *"
'''

import os
from datetime import datetime
import logging

import httpx
import pyarrow as pa
from prefect import flow, task, get_run_logger

from flows.lib.db import DbManager, Connection

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
CHANNEL_HANDLE = "FLAZOEIRO"

SCHEMA = 'gizmosql_duck.noticias'
SILVER_TABLE = 'youtube_video'
LANDING_TABLE = 'youtube_video_landing'


def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[NOTICIAS FLAZOEIRO] {msg}", kwargs

    return PrefixLoggerAdapter(get_run_logger(), {})


def _api_key() -> str:
    return os.environ["YOUTUBE_API_KEY"]


@task(name="fetch-flazoeiro-latest-video")
def fetch_latest_video() -> dict | None:
    """
    Obtém o vídeo mais recente do canal @FLAZOEIRO via YouTube Data API v3.

    Fluxo:
    1. channels.list      — obtém uploads_playlist_id pelo handle do canal
    2. playlistItems.list — obtém o video_id mais recente da playlist
    3. videos.list        — obtém title, published_at e description completa
    """
    logger = get_flow_logger()

    # 1. Resolve uploads playlist do canal
    resp = httpx.get(
        f"{YOUTUBE_API_BASE}/channels",
        params={"part": "contentDetails", "forHandle": CHANNEL_HANDLE, "key": _api_key()},
        timeout=30,
    )
    resp.raise_for_status()
    items = resp.json().get("items", [])
    if not items:
        logger.warning(f"Canal @{CHANNEL_HANDLE} não encontrado")
        return None

    uploads_playlist_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
    logger.info(f"Uploads playlist: {uploads_playlist_id}")

    # 2. Obtém o vídeo mais recente da playlist
    resp = httpx.get(
        f"{YOUTUBE_API_BASE}/playlistItems",
        params={
            "part": "snippet",
            "playlistId": uploads_playlist_id,
            "maxResults": 1,
            "key": _api_key(),
        },
        timeout=30,
    )
    resp.raise_for_status()
    items = resp.json().get("items", [])
    if not items:
        logger.warning("Nenhum vídeo encontrado na playlist")
        return None

    video_id = items[0]["snippet"]["resourceId"]["videoId"]
    logger.info(f"Latest video ID: {video_id}")

    # 3. Obtém detalhes completos (descrição não é truncada em videos.list)
    resp = httpx.get(
        f"{YOUTUBE_API_BASE}/videos",
        params={"part": "snippet,contentDetails", "id": video_id, "key": _api_key()},
        timeout=30,
    )
    resp.raise_for_status()
    items = resp.json().get("items", [])
    if not items:
        logger.warning(f"Vídeo {video_id} não encontrado")
        return None

    item = items[0]
    snippet = item["snippet"]

    thumbnails = snippet.get("thumbnails", {})
    thumbnail_url = (
        thumbnails.get("maxres")
        or thumbnails.get("high")
        or thumbnails.get("medium")
        or thumbnails.get("default")
        or {}
    ).get("url")

    video = {
        "url":           f"https://www.youtube.com/watch?v={video_id}",
        "published_at":  datetime.fromisoformat(snippet["publishedAt"].replace("Z", "+00:00")),
        "channel_id":    snippet["channelId"],
        "channel_title": snippet["channelTitle"],
        "title":         snippet["title"],
        "description":   snippet["description"],
        "tags":          snippet.get("tags", []),
        "duration":      item["contentDetails"]["duration"],
        "thumbnail_url": thumbnail_url,
    }
    logger.info(f"Fetched: '{video['title']}' ({video['published_at'].date()})")
    return video


@task(name="create-youtube-video-table", persist_result=False)
def create_youtube_video_table(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS gizmosql_duck.noticias")
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{SILVER_TABLE} (
                url           VARCHAR,
                published_at  TIMESTAMP,
                channel_id    VARCHAR,
                channel_title VARCHAR,
                title         VARCHAR,
                description   VARCHAR,
                tags          VARCHAR[],
                duration      VARCHAR,
                thumbnail_url VARCHAR,
                PRIMARY KEY (url)
            )
        """)
    logger.info(f"Ensured `{SILVER_TABLE}` table exists")


@task(name="ingest-flazoeiro-video", persist_result=False)
def ingest_youtube_video(table: pa.Table, conn: Connection) -> None:
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


@task(name="upsert-flazoeiro-video", persist_result=False)
def upsert_youtube_video(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {SCHEMA}.{SILVER_TABLE}
                (url, published_at, channel_id, channel_title, title, description, tags, duration, thumbnail_url)
            SELECT
                url, published_at, channel_id, channel_title, title, description, tags, duration, thumbnail_url
            FROM {SCHEMA}.{LANDING_TABLE}
            ON CONFLICT (url) DO UPDATE SET
                published_at  = EXCLUDED.published_at,
                channel_id    = EXCLUDED.channel_id,
                channel_title = EXCLUDED.channel_title,
                title         = EXCLUDED.title,
                description   = EXCLUDED.description,
                tags          = EXCLUDED.tags,
                duration      = EXCLUDED.duration,
                thumbnail_url = EXCLUDED.thumbnail_url
        """)
        logger.info(f"Upserted {cur.rowcount} row(s) into `{SILVER_TABLE}`")


@task(name="clear-flazoeiro-landing", persist_result=False)
def clear_landing_table(conn: Connection) -> None:
    logger = get_flow_logger()
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {SCHEMA}.{LANDING_TABLE}")
    logger.info(f"Cleared `{LANDING_TABLE}`")


@flow(name="noticias_flazoeiro_pipeline")
def noticias_flazoeiro_pipeline() -> None:
    logger = get_flow_logger()
    logger.info(f"Buscando último vídeo do canal @{CHANNEL_HANDLE}")

    video = fetch_latest_video()
    if not video:
        logger.warning("Nenhum vídeo obtido — abortando.")
        return

    table = pa.table(
        {
            "url":           [video["url"]],
            "published_at":  [video["published_at"]],
            "channel_id":    [video["channel_id"]],
            "channel_title": [video["channel_title"]],
            "title":         [video["title"]],
            "description":   [video["description"]],
            "tags":          [video["tags"]],
            "duration":      [video["duration"]],
            "thumbnail_url": [video["thumbnail_url"]],
        },
        schema=pa.schema([
            ("url",           pa.string()),
            ("published_at",  pa.timestamp("s", tz="UTC")),
            ("channel_id",    pa.string()),
            ("channel_title", pa.string()),
            ("title",         pa.string()),
            ("description",   pa.string()),
            ("tags",          pa.list_(pa.string())),
            ("duration",      pa.string()),
            ("thumbnail_url", pa.string()),
        ]),
    )

    with DbManager.from_env() as conn:
        create_youtube_video_table(conn)
        ingest_youtube_video(table, conn)
        upsert_youtube_video(conn)
        clear_landing_table(conn)


if __name__ == "__main__":
    noticias_flazoeiro_pipeline()
