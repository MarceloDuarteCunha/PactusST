import sys
import os
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import pandas_gbq

sys.path.insert(0, os.path.abspath(os.curdir))
from config.system import *
from db.database_interface import DatabaseInterface

class DatabaseBigQuery(DatabaseInterface):

    def __init__(self):
        self.database = os.environ.get('JSON')
        self.credencial = self.get_credential()
        self.client = self.get_client()

    def __str__(self) -> str:
        return f"BigQuery: {PROJECT_ID}.{DATASET_ID}"

    def get_credential(self):
        service = service_account.Credentials.from_service_account_info(self.database)
        return service

    def get_client(self):
        return bigquery.Client(credentials=self.credencial, project=PROJECT_ID)

    def data_load(self, dataframe:pd.DataFrame, destination_table: str, replace: bool = False) -> None:
        pandas_gbq.to_gbq(
            dataframe=dataframe,
            destination_table=DATASET_ID + "." + destination_table,
            credentials=self.credencial,
            project_id=PROJECT_ID,
            if_exists=("replace" if replace else "append"),
        )
