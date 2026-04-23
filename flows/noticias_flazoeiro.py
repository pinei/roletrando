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


def _parse_video(item: dict) -> dict:
    snippet = item["snippet"]
    thumbnails = snippet.get("thumbnails", {})
    thumbnail_url = (
        thumbnails.get("maxres")
        or thumbnails.get("high")
        or thumbnails.get("medium")
        or thumbnails.get("default")
        or {}
    ).get("url")
    return {
        "url":           f"https://www.youtube.com/watch?v={item['id']}",
        "published_at":  datetime.fromisoformat(snippet["publishedAt"].replace("Z", "+00:00")),
        "channel_id":    snippet["channelId"],
        "channel_title": snippet["channelTitle"],
        "title":         snippet["title"],
        "description":   snippet["description"],
        "tags":          snippet.get("tags", []),
        "duration":      item["contentDetails"]["duration"],
        "thumbnail_url": thumbnail_url,
    }


@task(name="fetch-flazoeiro-latest-videos", persist_result=False)
def fetch_latest_videos(conn: Connection) -> list[dict]:
    """
    Obtém os 5 vídeos mais recentes do canal @FLAZOEIRO, filtrando os que
    já existem na SILVER_TABLE.

    Fluxo:
    1. channels.list      — obtém uploads_playlist_id pelo handle do canal
    2. playlistItems.list — obtém os 5 video_ids mais recentes da playlist
    3. Filtra os já existentes na silver table
    4. videos.list        — obtém detalhes completos dos novos
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
        return []

    uploads_playlist_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
    logger.info(f"Uploads playlist: {uploads_playlist_id}")

    # 2. Obtém os 5 vídeos mais recentes da playlist
    resp = httpx.get(
        f"{YOUTUBE_API_BASE}/playlistItems",
        params={
            "part": "snippet",
            "playlistId": uploads_playlist_id,
            "maxResults": 5,
            "key": _api_key(),
        },
        timeout=30,
    )
    resp.raise_for_status()
    playlist_items = resp.json().get("items", [])
    if not playlist_items:
        logger.warning("Nenhum vídeo encontrado na playlist")
        return []

    video_ids = [i["snippet"]["resourceId"]["videoId"] for i in playlist_items]
    candidate_urls = {f"https://www.youtube.com/watch?v={vid}" for vid in video_ids}
    logger.info(f"Candidate video IDs: {video_ids}")

    # 3. Filtra os já existentes na silver table
    placeholders = ", ".join(f"'{url}'" for url in candidate_urls)
    with conn.cursor() as cur:
        cur.execute(f"SELECT url FROM {SCHEMA}.{SILVER_TABLE} WHERE url IN ({placeholders})")
        existing = {row[0] for row in cur.fetchall()}

    new_ids = [vid for vid in video_ids if f"https://www.youtube.com/watch?v={vid}" not in existing]
    logger.info(f"{len(new_ids)} new video(s) to ingest (skipping {len(existing)} existing)")
    if not new_ids:
        return []

    # 4. Obtém detalhes completos dos novos vídeos
    resp = httpx.get(
        f"{YOUTUBE_API_BASE}/videos",
        params={"part": "snippet,contentDetails", "id": ",".join(new_ids), "key": _api_key()},
        timeout=30,
    )
    resp.raise_for_status()
    items = resp.json().get("items", [])

    videos = [_parse_video(item) for item in items]
    for v in videos:
        logger.info(f"Fetched: '{v['title']}' ({v['published_at'].date()})")
    return videos


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
    logger.info(f"Buscando últimos vídeos do canal @{CHANNEL_HANDLE}")

    with DbManager.from_env() as conn:
        create_youtube_video_table(conn)
        videos = fetch_latest_videos(conn)

    if not videos:
        logger.info("Nenhum vídeo novo — abortando.")
        return

    schema = pa.schema([
        ("url",           pa.string()),
        ("published_at",  pa.timestamp("s", tz="UTC")),
        ("channel_id",    pa.string()),
        ("channel_title", pa.string()),
        ("title",         pa.string()),
        ("description",   pa.string()),
        ("tags",          pa.list_(pa.string())),
        ("duration",      pa.string()),
        ("thumbnail_url", pa.string()),
    ])
    table = pa.table(
        {
            "url":           [v["url"] for v in videos],
            "published_at":  [v["published_at"] for v in videos],
            "channel_id":    [v["channel_id"] for v in videos],
            "channel_title": [v["channel_title"] for v in videos],
            "title":         [v["title"] for v in videos],
            "description":   [v["description"] for v in videos],
            "tags":          [v["tags"] for v in videos],
            "duration":      [v["duration"] for v in videos],
            "thumbnail_url": [v["thumbnail_url"] for v in videos],
        },
        schema=schema,
    )

    with DbManager.from_env() as conn:
        ingest_youtube_video(table, conn)
        upsert_youtube_video(conn)
        clear_landing_table(conn)


if __name__ == "__main__":
    noticias_flazoeiro_pipeline()
