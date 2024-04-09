import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv


def baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)

if __name__ == '__main__':
    url_pasta = 'https://drive.google.com/drive/folders/1FhkNWpC9R3B2uma4Fv5WZKtd5X5ITai_'
    diretorio_local = './pasta_gdown'
    baixar_os_arquivos_do_google_drive(url_pasta, diretorio_local)
