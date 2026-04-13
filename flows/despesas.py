"""
Flow orquestrador de despesas.

Executa despesas_ingestao seguido de despesas_processamento.
"""
from prefect import flow

from flows.despesas_ingestao import despesas_ingestao
from flows.despesas_historico_usd import despesas_historico_usd
from flows.despesas_processamento import despesas_processamento


@flow(name="despesas_pipeline")
def despesas_pipeline() -> None:
    despesas_ingestao()
    despesas_historico_usd()
    despesas_processamento()


if __name__ == "__main__":
    despesas_pipeline()
