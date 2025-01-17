# Databricks notebook source
# MAGIC %pip install wbgapi

# COMMAND ----------

from utils import wbgapi_fetch

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

data_source = 'WB & OECD National Accounts, WB International Comparison Program, Eurostat-OECD PPP Programme'

df = wbgapi_fetch(indicators, col_names, data_source)

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("prd_mega.indicator.gdp")
