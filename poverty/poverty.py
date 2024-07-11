# Databricks notebook source
# MAGIC %pip install wbgapi

# COMMAND ----------

from utils import wbgapi_fetch

# COMMAND ----------

indicators = [
    'SI.POV.DDAY',
    'SI.POV.LMIC.GP',
    'SI.POV.UMIC',
]
col_names = [
    'poor215',
    'poor365',
    'poor685',
]

data_source = 'WB Poverty and Inequality Platform'

df = wbgapi_fetch(indicators, col_names, data_source)

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("indicator.poverty")
