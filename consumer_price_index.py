# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

import requests
import zipfile
import io
import pandas as pd

INDICATOR = 'FP.CPI.TOTL'
URL = f'https://api.worldbank.org/v2/en/indicator/{INDICATOR}?downloadformat=csv'

response = http_get(URL, timeout=DEFAULT_TIMEOUT_SECONDS)
response.raise_for_status()

with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
    filenames = zip_file.namelist()
    csv_file_name = next((name for name in filenames if name.startswith(f'API_{INDICATOR}')), None)

    if not csv_file_name:
        raise ValueError(f"No file starting with 'API_{INDICATOR}' found in the ZIP archive: {filenames}")

    with zip_file.open(csv_file_name) as csv_file:
        df = pd.read_csv(csv_file, skiprows=3)

columns_to_drop = [col for col in df.columns if col.startswith('Unnamed') or col.startswith('Indicator')]
df = df.drop(columns=columns_to_drop)
df = df.melt(id_vars=['Country Name', 'Country Code'], var_name='year', value_name='CPI', ignore_index=False)
df = df.astype({'year': int})

df.columns = df.columns.str.lower().str.replace(r'\W', '_', regex=True)

df

# COMMAND ----------

write_table(df, 'consumer_price_index')
