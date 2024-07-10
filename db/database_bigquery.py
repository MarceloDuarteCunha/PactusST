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
                "private_key_id": "bf58cdc524643a28963027c45d684cb7149b4490",
                "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC83iC0XULKofyd\ndp9xKiIraCxo19gQFyXmsOS2ysPPFC47Tm1uVfoLc+XPu+CU67+AwVkyvQGykf8h\npDQvSGuQBMPlMGlPgv/K36we3QR0lWInxLpeys0Up44E4cDdF4Jevd9FVTdrlkCI\nGoJdLv54rtrz/oqeLFB8StA5FoAYuAZtFWJrkoCTl0jNcDV3jt3F4IKb6KQBGEgn\nKLKWJZwMp5PIDO6ppFJpLhCheCudnSRK9tWgK00RRJGnzIiQjj4X4WKcFaG8n6g6\nPNMCwAhNEOjROJutlnriUvi/9mLLy9jUqJL5x/N88A41yxQD2c8A3Cx14pIpWS3W\nAFu6ucVPAgMBAAECggEABb61/PaHRRWnUvzcPbt73ZkmHKxQILErkHhaBKtnSGHD\nRD+O+Ze46udEGs0KuEzxfs+i0HXKWu2w9ouBoNyRlQhoEw0lbus7r6adHDU54Pf4\nS+BikOaXVWUBX2mkjbUc3Jfm9jtw259u6BTyWTZIPFCJTXm1KrmLWzSFX+8sx1Oo\nNzlwNiOZ7EDM+lW6RrCwILhXf48+vffFzCKyDrM8UokdqsYMyVLMiXT1cDWpMwCY\nDzu+kE0oylm4Hc8GbCCqO54DLIT69S5ZTrCBXO0V50D2Domi7Fg8VwhwGNBDvr9n\nb7lZlylRXXm3LCwj4fYmEjpdRsle1IPaDcZ0JUU/nQKBgQD5DWgzgUKHLmJW0noJ\ntBj8EeWj/ji0yhVfHv526jo9HO2nUfWyUuAJk61nQ4Ki4tGnSpab9Fm4ah22JYKm\n33A9vkj8AqC3AkUaBd+lOYpwYTyD6vh/DC12l+dpcshJ7j5sr2QrwKI7Rut6e7oA\nLShc3IXi42D2OywSFsW/5cMakwKBgQDCIupRcxnWnOlHGoagUBUDnfXVUYEm20FZ\n7Cu/05AQLofAK+bg/Hp2Y6lBQ10tuZn5t4Yy2/JQuAmVIfUYsvfQq8A8qsXzgPnb\n0/JUMQqYdJyOki0s4LMXAVCjwHPR+jwL4VyiOsOrZVROI5XrDZZo6WbaR33euNwR\ndREj3H1T1QKBgCsCq7opWonp8sW3t6exWJc/p/74Sma+d45j1eJHdSNUuZ4pw1q2\ngUZmII4y1H642Sgg832fZ1c3zEXGmJelAsdG82fCNGsmkzR5SzRHiab6w+6sR5n5\nSmiutyI41moeIN24NH5x2BKQnt4aO/cH1W6kDuMuDXK4qVW+ZQxM67LNAoGAaoKl\ndBJmNqzX/7655fKJ1PYxDdwts5fPfvpn9lKoU3hz2ic8AGwE9DcxgTDoSlHt/GeI\nJRcvFsIh38RFqEtoUwAn9CDtge9dJhJefXp6ibJQwpfIWTrehtyd1XmdEbLshsgZ\n/penCr4JRuWXNuK5hs2nNFLM6AkcFCfaQ4aQ7BECgYB6HH/ijYVz0SOZ9eRPD/M2\nIHA/mOh+ZpD0Dg6XxduRXLjCueNItH+8PIlIGXQPbWwQ4ev+ZNVYuaLnARSJTpz9\nmw9GF3NjZUn22EIMfiSm9aNanc/LLmC4QorcFSLIGjbhaIt1m4DJ5H/QfPDfFRqP\n2T2bhhn9KP8TU4dTSdoyJQ==\n-----END PRIVATE KEY-----\n",
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
