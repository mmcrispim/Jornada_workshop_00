import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from pandas import DataFrame

load_dotenv()


# Função para baixar os arquivos existentes no Google Drive para a máquina local
def baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)


# Função para listar arquivos csv no diretório especificado:
def listar_arquivos_csv(diretorio):
    arquivos_csv = []
    todos_os_arquivos = os.listdir(diretorio)
    for arquivo in todos_os_arquivos:
        if arquivo.endswith('.csv'):
            caminho_completo = os.path.join(diretorio, arquivo)
            arquivos_csv.append(caminho_completo)
    # print(arquivos_csv)
    return arquivos_csv


# Função para ler arquivo csv e retornar DataFrame
def ler_csv(caminho_do_arquivo):
    dataframe_duckdb = duckdb.read_csv(caminho_do_arquivo)
    print(dataframe_duckdb)
    return dataframe_duckdb


# Função para adicionar uma coluna de total de vendas
def transformar(df: DuckDBPyRelation) -> DataFrame:
    # Executa a consulta sql que inclui a nova coluna, operando sobre a tabela virtual
    df_transformado = duckdb.sql('Select *, quantidade * valor as total_vendas from df').df()
    # Remove o registro da tabela virtual para limpeza
    print(df_transformado)
    return df_transformado


# Função para converter o Duckdb em Pandas e salvar o DataFrame num banco que pode ser o PostgreSQL
def salvar_no_postgres(df_duckdb, tabela):
    # DATABASE_URL = 'postgresql://postgres:152408@localhost:5432/postgres'
    DATABASE_URL = os.getenv('DATABASE_URL')
    engine = create_engine(DATABASE_URL)
    # salvar o dataframe no PostgreSQL
    df_duckdb.to_sql(tabela, con=engine, if_exists='append', index=False)


if __name__ == '__main__':
    url_pasta = 'https://drive.google.com/drive/folders/1FhkNWpC9R3B2uma4Fv5WZKtd5X5ITai_'
    diretorio_local = './pasta_gdown'    
    # baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local)
    lista_de_arquivos = listar_arquivos_csv(diretorio_local)

    for caminho_do_arquivo in lista_de_arquivos:
        duckdb_db_df = ler_csv(caminho_do_arquivo)
        pandas_df_transformado = transformar(duckdb_db_df)
        salvar_no_postgres(pandas_df_transformado, "vendas_calculado")
