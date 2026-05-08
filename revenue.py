# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

indicators = [
    'GC.REV.SOCL.ZS',
    'GC.REV.GOTR.CN',
    'GC.REV.XGRT.CN',
    'GC.REV.GOTR.ZS',
    'GC.TAX.OTHR.RV.ZS'
]
col_names = [
    'social_contributions_pct_revenue',
    'grants_other_revenue_current_lcu',
    'revenue_excluding_grants_current_lcu',
    'grants_other_revenue_pct_revenue',
    'other_taxes_pct_revenue'
]

data_source = 'Government Finance Statistics Yearbook and data files, International Monetary Fund (IMF)'

df = wbgapi_fetch(indicators, col_names, data_source)

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("prd_mega.indicator.revenue")
