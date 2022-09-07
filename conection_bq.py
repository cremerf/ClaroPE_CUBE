from packages.tools import ConnectionBigQuery
from packages.claro_paths import ClaroPath
import pandas as pd
import numpy as np


client = ConnectionBigQuery(path_credentials="C:/Users/Federico.Cremer/OneDrive - Havas/Desktop/Science/Claro Peru/credentials.json",project_id="claro-peru-trasabilidad")

client.connect()
print("The program is going to start:")

list_datasets_1 = ["DV360", "Facebook", "GoogleAds"]

print("FIRST MERGE")
df_total_all_datasets_1 = client.merge_all_datasets(list_of_selected_datasets= list_datasets_1)

list_datasets_2 = ["LinkedIn", "Paid_Media", "Twitter"]

print("SECOND MERGE")
df_total_all_datasets_2 = client.merge_all_datasets(list_of_selected_datasets= list_datasets_2)

print("CONCAT BOTH MERGES")
df_total_final =  pd.merge(df_total_all_datasets_1, df_total_all_datasets_2, left_index=True, right_index=True, how='outer').reset_index()

print("TO CSV")
df_total_final.to_csv('cuboMaster.csv')

print("UPLOAD TO BIGQUERY")
client.client.load_table_from_dataframe(df_total_final, destination= 'claro-peru-trasabilidad.cubo_test1.cubov1')

print('The program has finished.')













