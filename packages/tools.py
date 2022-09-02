from http.client import BAD_REQUEST
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from google.api_core.exceptions import BadRequest

class ConnectionBigQuery():
    def __init__(self, path_credentials: str, project_id: str) -> None:
        self.credentials = service_account.Credentials.from_service_account_file(path_credentials)
        self.project_id = project_id
        self.client = None
        self.result = None

    def connect(self):
        self.client = bigquery.Client(credentials = self.credentials, project = self.project_id)
        
    def simple_query(self, select:str, dataset:str, table:str) -> pd.DataFrame:
        """Simple query to BigQuery database (GCP). Returns Pandas DataFrame.

        Uses standard SQL structure.

        Structure of the query {params}:

        "SELECT {select} FROM {dataset}.{table}"

        i.e: "SELECT '*' FROM 'MyDataset'.'Users'"

        Parameters
        ----------
        select : str
            User's selection from table 

        dataset : str
            Dataset from Project
        
        table : str
            Table from Dataset

        Returns
        ------
        Pandas DataFrame

        ValueError
        -----------
        If any parameter is not a string.
        """ 
        
        if type(select or dataset or table) != str:
            raise ValueError('Parameter must be a string.')
        else:
            try:
                self.result = self.client.query(
                    f"""
                    SELECT {select}
                    FROM {dataset}.{table}
                    """
                )
                self.result = self.result.result().to_dataframe()
                
            except BadRequest as e:
                for e in self.result.errors:
                    print(f'Reason: {e["reason"]} \nERROR: {e["message"]}')
    

        return self.result

    def complex_query(self, select:str, dataset:str, table:str, where:str="", group_by:str="", having:str="", order_by:str="", limit:int=0, offset:int=0) -> pd.DataFrame:
        """Complex query to BigQuery database (GCP). Returns Pandas DataFrame.

        Uses standard SQL structure.

        Structure of the query {params}:

        SELECT {select}

        FROM {dataset}.{table}

        WHERE {where}

        GROUP BY {group_by}

        HAVING {having}

        ORDER BY {order_by}

        LIMIT {limit}

        OFFSET {offset}


        i.e: 
        
        "SELECT '*' FROM 'MyDataset'.'Users' 

                WHERE 'date < 02/09/2022'

                GROUP BY 'sales'

                HAVING 'count(products) > 5'

                ORDER BY 'products DESC'

                LIMIT 100

                OFFESET 10"
                

        Parameters
        ----------
        select : str
            User's selection from table 

        dataset : str
            Dataset from Project
        
        table : str
            Table from Dataset

        where : str

        group_by : str

        having : str

        order_by : str

        limit : int

        offset : int

        Returns
        ------
        Pandas DataFrame

        ValueError
        -----------
        If any parameter is not a string.
        """ 
        
        if type(select or dataset or table or where or group_by or having or order_by) != str:
            raise ValueError("Parameters 'select', 'dataset', 'table', 'where', 'group_by', 'having' and 'order_by' must be strings.")
        
        if type(limit or offset) != int:
            raise ValueError("Parameters 'limit' and 'offset' must be integers.")
        else:
            try:
                self.result = self.client.query(
                    f"""
                    SELECT {select}
                    FROM {dataset}.{table}
                    {f'WHERE {where}' if where else ""}
                    {f'GROUP BY {group_by}' if group_by else ""}
                    {f'HAVING {having}' if having else ""}
                    {f'ORDER BY {order_by}' if order_by else ""}
                    {f'LIMIT {str(limit)}' if limit else ""}
                    {f'OFFSET {str(offset)}' if offset else ""}
                    """
                )
                self.result = self.result.result().to_dataframe()
                
            except BadRequest as e:
                for e in self.result.errors:
                    print(f'Reason: {e["reason"]} \nERROR: {e["message"]}')
        

        return self.result


    def custom_query(self, query:str) -> pd.DataFrame:
        """Custom query to BigQuery database (GCP). Returns Pandas DataFrame.

        Uses standard SQL structure.

        Structure of the query {query} must be completely created by the user.

        
                

        Parameters
        ----------
        query : str
            User passes a query fully created by himself as if he had to type it in SQL

        Returns
        ------
        Pandas DataFrame

        ValueError
        -----------
        If parameter is not a string.
        """ 
        
        if type(query) != str:
            raise ValueError("Parameter 'query' must be a string")
        
        else:
            try:
                self.result = self.client.query(query)
                self.result = self.result.result().to_dataframe()
                
            except BadRequest as e:
                for e in self.result.errors:
                    print(f'Reason: {e["reason"]} \nERROR: {e["message"]}')
        

        return self.result
