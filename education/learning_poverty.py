# Databricks notebook source
# MAGIC %pip install wbgapi

# COMMAND ----------

import wbgapi as wb
import pandas as pd

wb.db = 12 # Education Stats

# COMMAND ----------

col_name = 'learning_poverty_rate'
outcome_series_to_col_name = {
    'SE.LPV.PRIM': col_name,
}

outcome_df = wb.data.DataFrame(outcome_series_to_col_name.keys(), skipBlanks=True).reset_index()
outcome_df

# COMMAND ----------

# quick check of data availability
countries = ['COL', 'PRY', 'KEN', 'MOZ', 'BFA', 'PAK', 'COD']
outcome_df[outcome_df.economy.isin(countries)]

# COMMAND ----------

first_key = list(outcome_series_to_col_name.keys())[0]
long_df = outcome_df.melt(id_vars='economy', var_name='year', value_name=outcome_series_to_col_name[first_key])
long_df['year'] = long_df['year'].str.replace('YR', '')
long_df = long_df.astype({'year': 'int'})
long_df['data_source'] = 'World Bank & UNESCO Institute for Statistics (UIS)'
long_df = long_df.dropna(subset=[col_name]).sort_values(by=['economy', 'year'])
long_df[col_name] = long_df[col_name]/100
long_df

# COMMAND ----------

country_df = spark.table(f'indicator.country').select('country_name', 'country_code', 'region').toPandas()
country_df

# COMMAND ----------

merged_df = pd.merge(long_df, country_df, left_on='economy', right_on='country_code', how='left')[['country_name', 'country_code', 'region', 'year', col_name, 'data_source']]
merged_df

# COMMAND ----------

sdf = spark.createDataFrame(merged_df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"indicator.{col_name}")
