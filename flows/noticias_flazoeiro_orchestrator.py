"""
Flow orquestrador de notícias do Flazoeiro.

Executa noticias_flazoeiro_pipeline seguido de resumo_noticias_flazoeiro_pipeline.
"""
from prefect import flow

from flows.noticias_flazoeiro import noticias_flazoeiro_pipeline
from flows.resumo_noticias_flazoeiro import resumo_noticias_flazoeiro_pipeline


@flow(name="noticias_flazoeiro_orchestrator")
def noticias_flazoeiro_orchestrator() -> None:
    noticias_flazoeiro_pipeline()
    resumo_noticias_flazoeiro_pipeline()


if __name__ == "__main__":
    noticias_flazoeiro_orchestrator()
