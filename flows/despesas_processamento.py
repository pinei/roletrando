"""
Flow de processamento de despesas.

Executa processamentos no banco de dados após a ingestão das tabelas landing.
"""
from prefect import flow, get_run_logger
import logging


def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[DESPESAS PROCESSAMENTO] {msg}", kwargs

    return PrefixLoggerAdapter(get_run_logger(), {})


@flow(name="despesas_processamento")
def despesas_processamento() -> None:
    pass


if __name__ == "__main__":
    despesas_processamento()
