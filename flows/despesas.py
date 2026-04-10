"""
Flow de exemplo para despesas.

Cria um logger de fluxo e chama `despesas_pipeline()` quando executado como script.
"""
from prefect import flow, get_run_logger
import logging
from pathlib import Path
import json
import gspread
from google.oauth2.service_account import Credentials as ServiceAccountCredentials

import pandas as pd
import numpy as np


def get_flow_logger():
    class PrefixLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f"[DESPESAS] {msg}", kwargs

    return PrefixLoggerAdapter(get_run_logger(), {})


def load_credentials() -> dict | None:
    logger = get_flow_logger()
    cred_path = Path(__file__).parents[1] / "despesas" / "cashappy_service_account.json"
    try:
        with cred_path.open("r", encoding="utf-8") as file:
            creds_json_str = file.read()
        creds = json.loads(creds_json_str)
        logger.info(f"Loaded credentials from {cred_path}")
        return creds
    except FileNotFoundError:
        logger.error(f"Credentials file not found: {cred_path}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in credentials file: {e}")
        return None


def authenticate_google(creds: dict) -> gspread.client.Client | None:
    logger = get_flow_logger()

    try:
        SCOPES = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
        ]

        # Claude disse que está deprecated
        # credentials = ServiceAccountCredentials.from_service_account_info(creds, scopes=SCOPES)
        # client = gspread.authorize(credentials)

        client = gspread.service_account_from_dict(creds, scopes=SCOPES)

        logger.info("Authenticated with Google APIs via service account")
        return client
    except Exception as e:
        logger.error(f"Failed to authenticate Google client: {e}")
        return None


# Carrega os registros de uma aba da planilha para um dataframe
def load_records_as_dataframe(sheet_document, sheet_name):
    worksheet = sheet_document.worksheet(sheet_name)

    # get_all_values gives a list of rows.
    rows = worksheet.get_all_values()

    dataframe = pd.DataFrame.from_records(rows)

    # Use first row as column names
    dataframe.columns = dataframe.iloc[0]
    dataframe = dataframe[1:].reset_index(drop=True)

    return dataframe


def remap_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    # Normalize incoming column names to reduce mismatch risk
    df_raw = df_raw.rename(columns={c: (c.strip() if isinstance(c, str) else c) for c in df_raw.columns})

    # Mapeamento de nomes da planilha visando padronização
    mapa_colunas = {
        'Mês': 'mes',
        'Data': 'data',
        'Dia': 'dia',
        'Categoria': 'categoria',
        'Grupo': 'grupo',
        'Descrição': 'descricao',
        'Beneficiário': 'beneficiario',
        'Valor': 'valor',
        'Parcelas': 'parcelas',
        'Formula': 'formula',
        'Meio': 'meio',
        'Valor USD': 'valor_usd',
        'Valor GBP': 'valor_gbp',
        'Observação': 'observacao',
        'Conversão': 'conversao',
        'Classe': 'classe',
        'Soma Mes': 'soma_mes'
    }

    # Colunas de interesse para a base de dados
    colunas_interesse = ['mes', 'data', 'dia', 'categoria', 'grupo', 'descricao', 'beneficiario', 'valor', 'parcelas', 'meio', 'valor_usd', 'valor_gbp']


    # Renomeia colunas e garante que todas as colunas de interesse existam
    df = df_raw.rename(columns=mapa_colunas)

    for col in colunas_interesse:
        if col not in df.columns:
            df[col] = pd.NA

    # Seleciona e retorna apenas as colunas de interesse na ordem desejada
    return df[colunas_interesse]


def convert_valores(df):
    # Colunas para converter
    colunas_numericas = ['valor', 'valor_usd', 'valor_gbp']

    # Substitui strings vazias por NaN
    df[colunas_numericas] = df[colunas_numericas].replace('', np.nan)

    # Converte para numérico (coerce erros para NaN)
    for col in colunas_numericas:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Parcelas: tenta converter para inteiro, preenche NaN com 0
    if 'parcelas' in df.columns:
        df['parcelas'] = pd.to_numeric(df['parcelas'], errors='coerce').fillna(0).astype(int)

    return df


@flow(name="despesas_pipeline")
def despesas_pipeline() -> None:
    logger = get_flow_logger()
    logger.info("Iniciando pipeline de despesas")

    creds = load_credentials()
    if not creds:
        logger.warning("Credenciais não carregadas — interrompendo pipeline de despesas")
        return

    client = authenticate_google(creds)
    if not client:
        logger.warning("Falha na autenticação Google — interrompendo pipeline de despesas")
        return

    # Abrir a planilha e carregar duas abas como DataFrames
    try:
        # ID da planilha de Despesas
        SHEET_ID = '1k3ZCp6zakidS9Mk277O-Gj6QCF3RR16-BhRmOz-Hm00'

        spreadsheet = client.open_by_key(SHEET_ID)

        df_despesas_corrente_raw = load_records_as_dataframe(spreadsheet, 'Despesas')
        df_despesas_arquivo_raw = load_records_as_dataframe(spreadsheet, 'Arquivo Despesas')

        logger.info(
            f"Loaded sheets: Despesas={df_despesas_corrente_raw.shape[0]} rows, "
            f"Arquivo Despesas={df_despesas_arquivo_raw.shape[0]} rows"
        )

        # Remapear colunas e selecionar colunas de interesse
        df_despesas_corrente = remap_dataframe(df_despesas_corrente_raw)
        df_despesas_arquivo = remap_dataframe(df_despesas_arquivo_raw)

        # Converter valores numéricos e parcelas
        df_despesas_corrente = convert_valores(df_despesas_corrente)
        df_despesas_arquivo = convert_valores(df_despesas_arquivo)

        logger.info(
            f"Remapped sheets: Despesas={df_despesas_corrente.shape[0]} rows, "
            f"Arquivo Despesas={df_despesas_arquivo.shape[0]} rows"
        )

        # Exemplo: expor ou processar os dataframes (implementar transformações/ingestão conforme necessidade)
    except gspread.exceptions.SpreadsheetNotFound:
        logger.error("Planilha não encontrada. Verifique o ID e permissões de compartilhamento.")
        return
    except Exception as e:
        logger.error(f"Ocorreu um erro ao abrir ou ler a planilha: {e}")
        return


if __name__ == "__main__":
    despesas_pipeline()
