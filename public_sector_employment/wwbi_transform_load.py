# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# public_sector_employment: the WWBI silver table joined to country, plus one row per
# region and year with the regional means (region codes are country codes of the
# aggregate rows in country). Plain pandas, run as a notebook task (it replaced a DLT pipeline).
import pandas as pd

countries = spark.table(f'{INDICATOR_SCHEMA}.country').select('country_name', 'country_code', 'region').toPandas()

employment = (spark.table(f'{INDICATOR_SCHEMA}.public_sector_employment_silver').toPandas()
    .rename(columns={'economy': 'country_code'})
    .merge(countries, on='country_code', how='inner'))

regional_means = (employment.groupby(['region', 'year'], as_index=False)
    .agg(wage_percent_gdp=('wage_percent_gdp', 'mean'),
         wage_percent_expenditure=('wage_percent_expenditure', 'mean'),
         wage_premium=('wage_premium', 'mean'),
         data_source=('data_source', 'first'))
    .rename(columns={'region': 'country_code'})
    .merge(countries, on='country_code', how='inner'))

# COMMAND ----------

spark.createDataFrame(pd.concat([employment, regional_means[employment.columns]], ignore_index=True)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{INDICATOR_SCHEMA}.public_sector_employment")