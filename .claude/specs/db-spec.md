# Database Specification

## Stack

- **GizmoSQL** — Arrow Flight SQL server acting as a gateway.
- **DuckDB** — storage engine behind GizmoSQL (`gizmosql_duck` catalog).
- **ADBC** — transport layer (`adbc-driver-gizmosql` + `adbc-driver-flightsql`).
- **`flows/lib/db.py`** — thin wrapper: `DbManager` (context manager) and `Connection`.

## Connecting

Always use `DbManager.from_env()` as a context manager. Credentials are read from environment variables.

```python
from flows.lib.db import DbManager, Connection

with DbManager.from_env() as conn:
    ...
```

Required environment variables: `GIZMOSQL_URL`, `GIZMOSQL_USERNAME`, `GIZMOSQL_PASSWORD`.

Never instantiate `DbManager` directly with hardcoded credentials.

## Cursor usage

Each database operation gets its own `with conn.cursor() as cur:` block.
Do not reuse a cursor across operations.

```python
with conn.cursor() as cur:
    cur.execute("SELECT ...")

with conn.cursor() as cur:
    cur.execute("INSERT ...")
```

## Fully qualified table names

Always use the three-part name in SQL: `catalog.schema.table`.

```sql
SELECT * FROM gizmosql_duck.series.currency_history
INSERT INTO gizmosql_duck.despesas.historico_usd_brl ...
DELETE FROM gizmosql_duck.series.currency_history_landing
```

The catalog is always **`gizmosql_duck`**.

## Schemas (db_schema_name)

| Schema | Domain |
|---|---|
| `series` | Market data: currency rates, crypto OHLC, spot prices |
| `despesas` | Personal expenses and related reference data |

When adding a new domain, define a new schema name and document it here.

## Landing table pattern

All ingestion follows a **landing → upsert → clear** pattern:

1. Write data to a `{table}_landing` table with `mode="replace"` (full replace on every run).
2. Upsert from landing into the main table using `INSERT ... ON CONFLICT`.
3. Delete all rows from the landing table.

This ensures idempotency and clean state between runs.

### Ingest into landing

```python
with conn.cursor() as cur:
    rows_loaded = cur.adbc_ingest(
        table_name="my_table_landing",
        data=table_or_dataframe,       # pa.Table or pd.DataFrame
        mode="replace",
        catalog_name="gizmosql_duck",
        db_schema_name="series",       # or "despesas", etc.
    )
```

- `mode="replace"` drops and recreates the landing table on every ingest — this is intentional.
- `data` accepts both `pa.Table` and `pd.DataFrame`.
- The landing table does not need to be created beforehand; `mode="replace"` handles it.

### Upsert (landing → main)

```python
with conn.cursor() as cur:
    cur.execute("""
        INSERT INTO gizmosql_duck.series.my_table (col1, col2)
        SELECT col1, col2
        FROM gizmosql_duck.series.my_table_landing
        ON CONFLICT (col1) DO UPDATE SET col2 = EXCLUDED.col2
    """)
```

- Always list columns explicitly — do not use `SELECT *` in the upsert.
- Conflict key must match the `PRIMARY KEY` of the main table.

### Clear landing

```python
with conn.cursor() as cur:
    cur.execute("DELETE FROM gizmosql_duck.series.my_table_landing")
```

## Creating tables

Use `CREATE TABLE IF NOT EXISTS` when the main table may not exist yet.
Always define a `PRIMARY KEY` to enable upsert conflict resolution.

```python
with conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gizmosql_duck.series.my_table (
            name   VARCHAR,
            date   DATE,
            value  DECIMAL(10, 4),
            PRIMARY KEY (name, date)
        )
    """)
```

DuckDB type reference for common columns:

| Type | DuckDB |
|---|---|
| String / date as text | `VARCHAR` |
| Date | `DATE` |
| Timestamp | `TIMESTAMP` |
| Price / rate | `DECIMAL(10, 4)` |
| Float | `FLOAT` |

## DDL note

GizmoSQL routes DDL through `CommandStatementUpdate` (DoPut). Use `cur.execute()` for DDL — it works correctly via the ADBC cursor. Do not use `cur.adbc_ingest` for DDL.

See: https://github.com/gizmodata/gizmosql/issues/134
