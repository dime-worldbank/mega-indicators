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
    
with open(local_zip_path, 'wb') as zip_file:
        zip_file.write(response.content)

zip_file = zipfile.ZipFile(io.BytesIO(response.content))
temp_dir = gettempdir()
zip_file.extractall(temp_dir)

filenames = zip_file.namelist()
csv_file_name = next((name for name in filenames if name.startswith(f'API_{INDICATOR}')))

df = pd.read_csv(os.path.join(temp_dir, csv_file_name), skiprows=3)

columns_to_drop = [col for col in df.columns if col.startswith('Unnamed') or col.startswith('Indicator')]
df = df.drop(columns=columns_to_drop)
df = df.melt(id_vars=['Country Name', 'Country Code'], var_name='year', value_name='CPI', ignore_index=False)
df = df.astype({'year': int})

df.columns = df.columns.str.replace('\W', '', regex=True)

df

# COMMAND ----------

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").saveAsTable("indicator.consumer_price_index")
