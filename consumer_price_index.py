# Databricks notebook source
import requests
import zipfile
import io
import os
import pandas as pd
from tempfile import gettempdir

INDICATOR = 'FP.CPI.TOTL'
URL = 'https://api.worldbank.org/v2/en/indicator/FP.CPI.TOTL?downloadformat=csv'

response = requests.get(URL)

if response.status_code != 200:
    print('Request returned non-200', response.status_code)
    exit

with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
    filenames = zip_file.namelist()
    csv_file_name = next((name for name in filenames if name.startswith(f'API_{INDICATOR}')), None)
    
    if not csv_file_name:
        print(f"No file starting with 'API_{INDICATOR}' found in the ZIP archive.")
        exit()
    
    with zip_file.open(csv_file_name) as csv_file:
        df = pd.read_csv(csv_file, skiprows=3)

columns_to_drop = [col for col in df.columns if col.startswith('Unnamed') or col.startswith('Indicator')]
df = df.drop(columns=columns_to_drop)
df = df.melt(id_vars=['Country Name', 'Country Code'], var_name='year', value_name='CPI', ignore_index=False)
df = df.astype({'year': int})

df.columns = df.columns.str.lower().str.replace('\W', '_', regex=True)

df

# COMMAND ----------

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").saveAsTable("prd_mega.indicator.consumer_price_index")
