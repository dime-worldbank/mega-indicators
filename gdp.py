# Databricks notebook source
# MAGIC %pip install wbgapi

# COMMAND ----------

import wbgapi as wb
import pandas as pd

# COMMAND ----------

wb.db = 2
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

merged_df['data_source'] = 'WB & OECD National Accounts, WB International Comparison Program, Eurostat-OECD PPP Programme'
merged_df

# COMMAND ----------

country_df = spark.table(f'indicator.country').select('country_name', 'country_code', 'region').toPandas()
country_df

# COMMAND ----------

gdp_df = pd.merge(merged_df, country_df, left_on='economy', right_on='country_code', how='left')[['country_name', 'country_code', 'region', 'year', *col_names, 'data_source']]
gdp_df

# COMMAND ----------

sdf = spark.createDataFrame(gdp_df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("indicator.gdp")
