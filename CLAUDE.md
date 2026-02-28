# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**RoleTraNDO** is a Prefect-based data pipeline that fetches Brazilian currency exchange rates from Brazil's Central Bank (BCB) API and ingests them into a GizmoSQL data warehouse via Arrow Flight SQL.

## Commands

**Install dependencies:**
```bash
pip install -e '.[dev]'
```

**Run tests:**
```bash
pytest
```

**Lint and format:**
```bash
ruff check .
ruff format .
```

**Run the pipeline directly (local dev, no Prefect server):**
```bash
set -a && source .env && set +a && PREFECT_API_URL="" python flows/currency_history.py
```

## Architecture

All pipeline logic lives in [flows/currency_history.py](flows/currency_history.py). The flow follows a standard ETL pattern using Prefect `@task` and `@flow` decorators:

1. **Fetch** (`fetch_currency_data`) — Calls BCB API for each currency (USD, EUR, GBP), then forward-fills missing dates to produce a continuous daily time series.
2. **Ingest** (`ingest_currency_history`) — Writes fetched data into a GizmoSQL landing table using Arrow Flight SQL (via `adbc-driver-flightsql` + `pyarrow`).
3. **Upsert** (`upsert_currency_history`) — Merges the landing table into the main table with conflict resolution via a SQL `MERGE`/upsert statement.

## Environment Variables

Required (see [.env.example](.env.example)):
- `GIZMOSQL_URL` — gRPC connection string (e.g. `grpc://host:port`)
- `GIZMOSQL_USERNAME` / `GIZMOSQL_PASSWORD` — database credentials

## Currency Codes

BCB series codes used in the pipeline:
- USD → series `1`
- EUR → series `21619`
- GBP → series `21623`

## Production Deployment

The production setup ([docker-compose.prod.yml](docker-compose.prod.yml)) runs two services:
- `prefect-server` — Prefect API server on port 4200
- `prefect-worker` — Executes flows, built from [Dockerfile.worker](Dockerfile.worker)
