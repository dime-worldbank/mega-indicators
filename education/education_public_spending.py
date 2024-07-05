# Databricks notebook source
import requests
import zipfile
import io
import os
import pandas as pd
from tempfile import gettempdir

# API has latest years while wbgapi doesn't for this indicator
INDICATOR = 'SE.XPD.TOTL.GD.ZS'
URL = f'https://api.worldbank.org/v2/en/indicator/{INDICATOR}?downloadformat=csv'

response = requests.get(URL)

if response.status_code != 200:
    print('Request returned non-200', response.status_code)
    exit

zip_file = zipfile.ZipFile(io.BytesIO(response.content))
temp_dir = gettempdir()
zip_file.extractall(temp_dir)

filenames = zip_file.namelist()
csv_file_name = next((name for name in filenames if name.startswith(f'API_{INDICATOR}')))
csv_file_name

# COMMAND ----------

df = pd.read_csv(os.path.join(temp_dir, csv_file_name), skiprows=3)

columns_to_drop = [col for col in df.columns if col.startswith('Unnamed') or col.startswith('Indicator')]
df = df.drop(columns=columns_to_drop)

col_name = 'edu_gov_spending_gdp_share'
df = df.melt(id_vars=['Country Name', 'Country Code'], var_name='year', value_name=col_name, ignore_index=False)
df[col_name] = df[col_name] / 100
df = df.astype({'year': int})
df = df.dropna()

df.columns = df.columns.str.lower().str.replace('\W', '_', regex=True)
df['data_source'] = 'UNESCO Institute for Statistics (UIS)'

df

# COMMAND ----------

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("indicator.edu_gov_spending")
