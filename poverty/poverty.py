# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

indicators = [
    'SI.POV.DDAY',
    'SI.POV.LMIC',
    'SI.POV.UMIC',
]
col_names = [
    'poor300',
    'poor420',
    'poor830',
]

df = wbgapi_fetch(indicators, col_names, extra_col_names_from_country_table=['income_level'])
df

# COMMAND ----------

mask = df['country_code'].isin(['LIC', 'LMC', 'UMC', 'HIC'])
df.loc[mask, 'income_level'] = df.loc[mask, 'country_code']

df['poverty_rate'] = df['poor300']
df.loc[df['income_level'] == 'LMC', 'poverty_rate'] = df['poor420']
df.loc[df['income_level'].isin(['UMC', 'HIC']), 'poverty_rate'] = df['poor830']
df

# COMMAND ----------

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{INDICATOR_SCHEMA}.poverty_rate")
