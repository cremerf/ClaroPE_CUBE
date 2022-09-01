from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

class ConnectionBigQuery():
    def __init__(self, path_credentials: str, project_id: str) -> None:
        self.credentials = service_account.Credentials.from_service_account_file(path_credentials)
        self.project_id = project_id
        self.client = None
        self.result = None

    def connect(self):
        self.client = bigquery.Client(credentials = self.credentials, project = self.project_id)
        
    def simple_query(self, dataset:str, table:str, select:str) -> pd.DataFrame: # Incorporar docstrings para descripción
        """
        Pre processes data for modeling. Receives train and testing dataframes 
        for Home Credit Competition, and returns numpy ndarrays of cleaned up 
        dataframes with feature engineering already performed.
        
        Arguments:
            dataset: Facebook
            table: FBADS_AGE_GENDER_20220831
            select: str
            
        Returns:
            result: DataFrame
        """
        self.result = self.client.query(
            f"""
            SELECT {select}
            FROM {dataset}.{table}
            """
        )
        self.result = self.result.result().to_dataframe()
    
        return self.result
