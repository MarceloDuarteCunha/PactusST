import os
from dotenv import load_dotenv

load_dotenv()

URL_CONTA_AZUL = os.getenv("URL_CONTA_AZUL")
USUARIO_CONTA_AZUL = os.getenv("USUARIO_CONTA_AZUL")
SENHA_CONTA_AZUL = os.getenv("SENHA_CONTA_AZUL")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
BIGQUERY_JSON = os.getenv("BIGQUERY_JSON")
DOWNLOAD_PATH = os.getenv("DOWNLOAD_PATH")