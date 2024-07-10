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
        self.credencial = self.get_credential()
        self.client = self.get_client()

    def __str__(self) -> str:
        return f"BigQuery: {PROJECT_ID}.{DATASET_ID}"

    def get_credential(self):
        # return service_account.Credentials.from_service_account_file(BIGQUERY_JSON)
        service = service_account.Credentials.from_service_account_info(
            {
                "type": "service_account",
                "project_id": "datametria",
                "private_key_id": "8258241b463d83f2c45b1f8650c0f4c04768d2f6",
                "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDd4iJpyDVpJqYG\nzYdGupqwFkADep1+G9xTDG4ZY/JF0TpG9vJ9burinCtk7LATwKQ7xeVPZdbSuFOL\nzl1hD6pjhEO6wnbmQljled+9YbEUDSx6YMDhuWuaOUWuQJ0KIesxZDOvTBesbcTh\nc5ItP15gruIIlCkAtd0kxSVoYeHu7GV7t6iDThu6f6ZXpwsBR47v7EEywW/O8TN5\nbGXFF4HLVrk0jLBHYANaWTh3lHOlsF+HH3iJj2j+71a6DSNIGaPbMJ92w0Vxw4Zv\nlbW+uLwmOysUUFsxoHxNkUtZJhdb6TOD4IQj/b75EG0gaQnCBOfRKrdv4hvTPeDL\nbM3xVdsrAgMBAAECggEAErgWQShj4Sgij6ZdSElL7Zki82gvy0jccKE21KuK7/rz\n2LLSyGRIKrAUNSKGxFPXRzet3eHwmrx0R/I0DJE+xcCN2j63uztTtqBukNKiFtf5\nraVmAGUdP9ifGVFYLleoOPWEKsDXqnNkTcTrSyfydWOrrB+byfSN4KFFQ6YmYnqk\n5wJ3ctCS83lA3GtlYry7kuYmIhHotI/0FxBcmk6RHnJDtfno/bZg9Ly/tt9wRk5x\n4a/Co8iMXMh4nMkN89sZnq+BedfKr/JoQbqHObuKpYfN0rwXtKuxw3rSS218FHQT\nzG+vUwfQMp4k4vLm4uuvWlh5xj0EBlztv4n5CREkIQKBgQD0duhy2bsaL+quwE0W\nB0zWuSkD1AdEP5fRTBVda9tGDKlnFdaeL/qTuuLp9K2rUSb16XFo6op+73avPFes\noM+IyExXWczKpCG7HjTLQNjI/sU/hf9mU0jl4TjhClRO9lhAROAB0stRpyqh8uQ4\nTABLlQk0xpmLtMTrW/TwtewIOwKBgQDoWnMkMZtCLP9aIrTa/soK4PqpMhc/B+7/\nyTcxVzCvRBmFCk9UXbX4KP2f4VlfGWBtXDn3757/k0CoW+DEcZ+x4xpaqfAco3J2\n/zQlDQDCiM+6c5qxsj6oSZegg+QYORwS8FBMHIvHK7rBoqvD5yboKuosvT/A5zDc\nyA45aoo50QKBgCC3pUZxs2O+0/bNT8PJ+FOodj/H6a0NocDmINne7HrEiQt91bpY\nC34n/eR31aJe+wrgUkhLU3vHiqfLOKSm5P+WZvkt0IWyT71ePoyRQjIsPrPQuCy2\nqsPxT452tm/W8lqvnRdBYeqsp70C595M9aEn8hJ5HeWfrNce+HoartsvAoGAenCH\nclAtt46MtQvK8AgJoffyPsylkVJ18BYxXm/KuOES7ZeEPDnxlSbMl76hYMq3lIlg\nBrYfvB/1l4JrDDmv5IZ/Mb2RSRYcUHalHM0gVGLRzDf1EaSS/g1huHUn8lfN4nD+\nb00vnzH9BOLazq31Q6yxoAfOG7H4fq9xR3JMCrECgYApA9vy1Yy+xe2OTwSoIGJv\nynd2Br0Zs0op0D2mmlgJYgOkflhEhMCWvMj2U0vkWXe/9hEOfM4rTrPd6V+wFfHU\nkvjth5YJYtyh5gCOpSXJ7fxRGUq66mWavQtLrtGYAo4N/rGTAiMoptez4XzH7zMx\npwqPUYCKnZVy0Mq0Dgst0Q==\n-----END PRIVATE KEY-----\n",
                "client_email": "datametria@datametria.iam.gserviceaccount.com",
                "client_id": "103699222612549853078",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/datametria%40datametria.iam.gserviceaccount.com",
                "universe_domain": "googleapis.com"
            }
        )

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
