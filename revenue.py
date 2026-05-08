# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

indicators = [
    'GC.REV.TOTL.GD'
]
col_names = [
    'total_revenue_pct_gdp'
]

data_source = 'World Bank - General Government Revenue Statistics'

df = wbgapi_fetch(indicators, col_names, data_source)

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("prd_mega.indicator.revenue")
