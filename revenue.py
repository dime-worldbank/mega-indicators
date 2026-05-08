# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

indicators = [
    'GC.REV.TOTL.GD',
    'GC.TAX.TOTL.GD',
    'GC.TAX.YPKG.RV.ZS',
    'GC.TAX.INCD.RV.ZS',
    'GC.TAX.CORP.RV.ZS',
    'GC.TAX.GSRV.RV.ZS',
    'GC.TAX.EXGS.RV.ZS'
]
col_names = [
    'total_revenue_pct_gdp',
    'tax_revenue_pct_gdp',
    'tax_revenue_pct_gni',
    'income_tax_pct_total_tax',
    'corporate_income_tax_pct_total_tax',
    'goods_services_tax_pct_total_tax',
    'excise_tax_pct_total_tax'
]

data_source = 'World Bank - Government Revenue Statistics'

df = wbgapi_fetch(indicators, col_names, data_source)

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("prd_mega.indicator.revenue")
