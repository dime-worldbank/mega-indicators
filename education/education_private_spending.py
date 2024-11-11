# Databricks notebook source
import pandas as pd
import requests
from io import StringIO

URL = 'https://sdmx.oecd.org/archive/rest/data/OECD,DF_DP_LIVE/?format=csv'
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# OECD API returns 403: Forbidden if no headers
response = requests.get(URL, headers=HEADERS)
response.raise_for_status()

# COMMAND ----------

csv_data = StringIO(response.text)
df = pd.read_csv(csv_data)
df

# COMMAND ----------

df_edu_private_exp = df[df.SUBJECT.isin(['PRY_TRY', 'EARLYCHILDEDU'])].groupby(['LOCATION', 'TIME_PERIOD']).agg('sum')[['OBS_VALUE']].reset_index()
df_edu_private_exp['edu_private_spending_share_gdp'] = df_edu_private_exp.OBS_VALUE / 100
df_edu_private_exp.rename(columns={'LOCATION': 'country_code', 'TIME_PERIOD': 'year'}, inplace=True)
df_edu_private_exp['year'] = df_edu_private_exp['year'].astype(int)
df_edu_private_exp

# COMMAND ----------

gdp_df = spark.table("indicator.gdp").toPandas()
gdp_df

# COMMAND ----------

df_merged = pd.merge(df_edu_private_exp, gdp_df, on=['country_code', 'year'])[['country_name', 'country_code', 'year', 'edu_private_spending_share_gdp', 'gdp_current_lcu']]
df_merged['edu_private_spending_current_lcu'] = df_merged.edu_private_spending_share_gdp * df_merged.gdp_current_lcu
df_merged.drop(columns=['gdp_current_lcu'], inplace=True)
df_merged['data_source'] = 'WB & OECD National Accounts, OECD Education at a Glance'
df_merged

# COMMAND ----------

sdf = spark.createDataFrame(df_merged)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("indicator.edu_private_spending")
