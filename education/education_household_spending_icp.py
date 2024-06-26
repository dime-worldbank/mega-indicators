# Databricks notebook source
# MAGIC %pip install wbgapi

# COMMAND ----------

import wbgapi as wb
import pandas as pd

# COMMAND ----------

# wb.source.info()
# wb.db = 71 # ICP 2005: only 2005, 9120000:EDUCATION (Analytical Category)
# wb.db = 62 # ICP 2011: only 2011
# wb.db = 78 # ICP 2017: 2011 & 2017
# wb.db = 90 # ICP 2021: 2017 & 2021
# wb.series.info()

# COMMAND ----------

edu_household_exp_key = '9120000'
wb.db = 78
outcome_df = wb.data.DataFrame([edu_household_exp_key], skipBlanks=True).reset_index()
outcome_df

# COMMAND ----------

# TODO: add 2021 data

# COMMAND ----------

def create_long_df(outcome_df, classification, col_name, multi_factor):
    outcome_sub_df = outcome_df[outcome_df.classification == classification].drop(columns=['classification'])
    long_df = outcome_sub_df.melt(id_vars='economy', var_name='year', value_name=col_name)
    long_df['year'] = long_df['year'].str.replace('YR', '')
    long_df = long_df.astype({'year': 'int'})
    long_df = long_df.dropna(subset=[col_name]).sort_values(by=['economy', 'year'])
    long_df[col_name] = long_df[col_name]*multi_factor
    return long_df

# COMMAND ----------

long_df_conversion_args = [
    ('CN', 'edu_household_spending_current_lcu_icp', 1e9), # original data in billion, so convert to unit term 
    ('ZS', 'edu_household_spending_gdp_share', 0.01) # convert to [0, 1]
]

lcu_df = create_long_df(outcome_df, *long_df_conversion_args[0])
gdp_share_df = create_long_df(outcome_df, *long_df_conversion_args[1])

long_df = pd.merge(lcu_df, gdp_share_df, on=['economy', 'year'])
long_df['data_source'] = 'International Comparison Program (ICP)'
long_df

# COMMAND ----------

# quick check of data availability
countries = ['COL', 'PRY', 'KEN', 'MOZ', 'BFA', 'PAK', 'COD']
long_df[long_df.economy.isin(countries)]

# COMMAND ----------

country_df = spark.table(f'indicator.country').select('country_name', 'country_code', 'region').toPandas()
country_df

# COMMAND ----------

long_cols_without_economy = list(c for c in long_df.columns.to_list() if c != 'economy')
col_names = ['country_name', 'country_code', 'region'] + long_cols_without_economy
result_df = pd.merge(long_df, country_df, left_on='economy', right_on='country_code', how='inner')[col_names]
result_df

# COMMAND ----------

sdf = spark.createDataFrame(result_df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("indicator.edu_household_spending")
