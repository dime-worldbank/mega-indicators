# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd

# One shared download for every country whose subnational population comes from this WB
# database (btn/tun/chl/zaf/alb depend on this task and filter their own country_code
# out of the result, instead of each downloading and parsing the same file).
ZIP_URL = "https://databank.worldbank.org/data/download/Subnational-Population_EXCEL.zip"
POPULATION_INDICATOR_CODE = "SP.POP.TOTL"
COUNTRY_CODES = ["BTN", "TUN", "CHL", "ZAF", "ALB"]

response = requests.get(ZIP_URL)
response.raise_for_status()

with ZipFile(BytesIO(response.content), 'r') as zip_file:
    files = zip_file.namelist()
    assert len(files) == 1
    df = pd.read_excel(zip_file.open(files[0]))

# COMMAND ----------

df = df[df['Indicator Code'] == POPULATION_INDICATOR_CODE].copy()
df['country_code'] = df['Country Code'].str[:3]
df = df[df['country_code'].isin(COUNTRY_CODES)]
df['adm1_name'] = df['Country Name'].map(lambda x: x.split(',')[-1].strip())

# Drop each country's own total-population row: it has no comma in Country Name, so
# adm1_name (the post-comma remainder, or the whole string when there's no comma)
# comes out equal to Country Name — unlike every real admin1 row.
df = df[df['adm1_name'] != df['Country Name']]

selected_columns = ['country_code', 'adm1_name'] + list(df.columns[df.columns.str.isnumeric()])
df_long = df[selected_columns].melt(id_vars=['country_code', 'adm1_name'], var_name='year', value_name='population')
df_long['population'] = df_long['population'].astype('int')
df_long['year'] = df_long['year'].astype('int')

# COMMAND ----------

sdf = spark.createDataFrame(df_long)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{INDICATOR_SCHEMA}.wb_subnational_population_silver")
