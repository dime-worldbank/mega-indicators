# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

# public_sector_employment: the WWBI silver table joined to country, plus one row per
# region and year with the regional means (region codes are country codes of the
# aggregate rows in country). Plain pandas on both sides (it replaced a DLT pipeline).
import pandas as pd

countries = read_table('country', columns=['country_name', 'country_code', 'region'])

employment = (read_table('public_sector_employment_silver')
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

write_table(pd.concat([employment, regional_means[employment.columns]], ignore_index=True), 'public_sector_employment')