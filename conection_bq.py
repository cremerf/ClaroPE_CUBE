from packages.tools import ConnectionBigQuery
import pandas as pd
import numpy as np

client = ConnectionBigQuery(path_credentials="C:/Users/Federico.Cremer/OneDrive - Havas/Desktop/Science/Claro Peru/credentials.json",project_id="claro-peru-trasabilidad")

client.connect()

query_job = client.query(dataset="Facebook", table="FBADS_AD_20220831",select="*").to_dataframe()

#response = query_job.result()

print(query_job)







