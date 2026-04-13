# Flow Specification

## File layout

- One flow per file, named after the flow: `currency_history.py` → `currency_history_pipeline`.
- Orchestrator flows (that call sub-flows) live in their own file and only contain the orchestration logic.
- `__main__` at the bottom always calls the flow function directly, enabling local execution without Prefect server.
- Add a deploy comment at the top of the file with the `prefect deploy` command for that flow.

## Naming conventions

### Flows — `@flow(name="snake_case")`

Flow names use **snake_case**. Function name matches the `name` argument.

```python
@flow(name="currency_history_pipeline")
def currency_history_pipeline() -> None: ...
```

### Tasks — `@task(name="kebab-case-prefix-subject")`

Task names use **kebab-case**. Use the following standard prefixes:

| Prefix | When to use |
|---|---|
| `fetch-` | Calls an external API or source to retrieve data |
| `create-` | DDL — creates a table if not exists |
| `ingest-` | Writes data into a landing table via `adbc_ingest` |
| `upsert-` | Merges landing table into main table via `INSERT ... ON CONFLICT` |
| `clear-` | Deletes rows from a landing table after upsert |

Examples: `fetch-currency-spot`, `create-binance-ohlc-table`, `ingest-despesas`, `upsert-historico-usd-brl`, `clear-binance-ohlc-landing`.

## `@task` vs plain function

Use `@task` when the function:
- Calls an external API or reads from an external source (I/O de rede)
- Executes DDL or DML against the database (I/O de banco)
- Writes to a landing table or upserts data

Do **not** use `@task` for:
- Pure in-memory transformations (e.g. `remap_dataframe`, `convert_to_arrow`)
- Functions that will never need retries or independent observability

### `persist_result`

Set `persist_result=False` on all tasks that do database I/O (ingest, upsert, create, clear).
Omit it on `fetch-` tasks (default behavior is acceptable).

## Logger

Each file defines its own `get_flow_logger()` with a bracketed prefix identifying the flow:

```python
def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[FLOW NAME] {msg}", kwargs
    return PrefixLoggerAdapter(get_run_logger(), {})
```

- `get_run_logger()` must be called **inside** a task or flow context — never at module level.
- Call `get_flow_logger()` at the top of each task/flow function body.
- The prefix should be UPPER CASE and descriptive: `[CURRENCY HISTORY]`, `[DESPESAS INGESTAO]`.

## Module-level table constants

Each flow file declares three module-level constants for table names:

```python
SCHEMA = 'gizmosql_duck.series'        # or 'gizmosql_duck.despesas', etc.
SILVER_TABLE = 'my_table'
LANDING_TABLE = 'my_table_landing'
```

Use these constants everywhere a table name appears — in SQL strings (as f-strings), `adbc_ingest` arguments, and log messages. Never hardcode table paths inline.

```python
cur.execute(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{SILVER_TABLE} ...")
cur.adbc_ingest(table_name=LANDING_TABLE, ...)
cur.execute(f"INSERT INTO {SCHEMA}.{SILVER_TABLE} ... FROM {SCHEMA}.{LANDING_TABLE} ...")
cur.execute(f"DELETE FROM {SCHEMA}.{LANDING_TABLE}")
```

## HTTP requests

Always use **`httpx`**, never `requests`.

```python
response = httpx.get(url, params=params, timeout=30)
response.raise_for_status()
```

## PyArrow vs Pandas

| Use | When |
|---|---|
| `pa.Table` (PyArrow) | Data comes from structured records (list of tuples). Schema is known upfront and can be declared explicitly. |
| `pd.DataFrame` (Pandas) | Data requires column renaming, type coercion, forward-fill, or other DataFrame operations before ingest. |

- Convert records → PyArrow via a **plain function** (no `@task`): `convert_to_arrow(records)`.
- Always declare the PyArrow schema explicitly when building a `pa.Table`.
- Both `pa.Table` and `pd.DataFrame` are accepted by `cur.adbc_ingest`.

## ETL task order

Standard sequence inside a flow:

```
create_table_if_not_exists(conn)   # only when table may not exist yet
fetch_*(...)                        # external I/O
ingest_*(data, conn)               # write to landing
upsert_*(conn)                     # merge landing → main
clear_landing_table(conn)          # clean up landing
```

`create_table_if_not_exists` can be omitted when the table is guaranteed to exist.

## Sub-flow orchestration

When one flow depends on another, use the **sub-flow pattern** (Option 1):

```python
@flow(name="despesas_pipeline")
def despesas_pipeline() -> None:
    despesas_ingestao()      # Prefect awaits completion before proceeding
    despesas_processamento() # only runs if the previous flow succeeded
```

- The orchestrator imports and calls sub-flows directly — no `run_deployment`.
- Only the orchestrator flow needs a schedule in production.
- Use `run_deployment` only when sub-flows need different workers or independent scheduling.
