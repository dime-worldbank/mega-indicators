# Databricks notebook source
# MAGIC %pip install wbgapi

# COMMAND ----------

import wbgapi as wb
import pandas as pd

# COMMAND ----------

indicators = ['SP.POP.TOTL', 'SP.POP.TOTL.FE.IN']
col_names = ['population', 'population_female']

# COMMAND ----------

long_dfs = []
for series, col_name in zip(indicators, col_names):
    df = wb.data.DataFrame(series, skipBlanks=True).reset_index()
    long_df = df.melt(id_vars='economy', var_name='year', value_name=col_name)
    long_df = long_df.dropna(subset=col_name)
    long_df['year'] = long_df['year'].str.replace('YR', '')
    long_df = long_df.astype({'year': 'int'})
    long_dfs.append(long_df)

merged_df = long_dfs[0]
for df in long_dfs[1:]:
    merged_df = pd.merge(merged_df, df, on=['economy', 'year'])

merged_df['data_source'] = 'UN & WB & Eurostat'
merged_df

# COMMAND ----------

country_df = spark.table(f'prd_mega.indicator.country').select('country_name', 'country_code', 'region').toPandas()
country_df

# COMMAND ----------

pop_df = pd.merge(merged_df, country_df, left_on='economy', right_on='country_code', how='left')[['country_name', 'country_code', 'region', 'year', *col_names, 'data_source']]
pop_df

# COMMAND ----------

pop_df[pop_df['country_name'] == 'Liberia']

# COMMAND ----------

sdf = spark.createDataFrame(pop_df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("prd_mega.indicator.population")
