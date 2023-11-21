# Databricks notebook source
# MAGIC %pip install wbgapi

# COMMAND ----------

import pandas as pd
import wbgapi as wb

# COMMAND ----------

df = wb.economy.DataFrame()
df

# COMMAND ----------


COL_NAME_MAP = {'id': 'country_code', 'name': 'country_name', 'lendingType': 'lending_type', 'incomeLevel': 'income_level', 'capitalCity': 'capital_city'}
COL_NAMES = ['country_code', 'country_name', 'longitude', 'latitude', 'region', 'lending_type', 'income_level', 'capital_city']
df_cleaned = df.reset_index().rename(columns=COL_NAME_MAP)[COL_NAMES]
df_cleaned

# COMMAND ----------

sdf = spark.createDataFrame(df_cleaned)
sdf.write.mode("overwrite").saveAsTable("indicator.country")

# COMMAND ----------

df.loc[df['aggregate'] == True]
