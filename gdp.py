# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

indicators = [
    'NY.GDP.MKTP.CN',
    'NY.GDP.MKTP.CD',
    'NY.GDP.MKTP.KD',
    'NY.GDP.MKTP.PP.CD',
    'NY.GDP.MKTP.PP.KD',
    'NY.GDP.PCAP.PP.KD'
]
col_names = [
    'gdp_current_lcu',
    'gdp_current_usd',
    'gdp_2015_usd',
    'gdp_current_ppp',
    'gdp_2021_ppp',
    'gdp_per_capita_2017_ppp'
]

df = wbgapi_fetch(indicators, col_names)

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{INDICATOR_SCHEMA}.gdp")
