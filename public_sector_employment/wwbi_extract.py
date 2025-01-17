# Databricks notebook source
# MAGIC %pip install wbgapi

# COMMAND ----------

import wbgapi as wb
import pandas as pd

wb.db = 64 # WWBI wb.source.info()

series_to_col_name = {
    'BI.WAG.TOTL.GD.ZS': 'wage_percent_gdp',
    'BI.WAG.TOTL.PB.ZS': 'wage_percent_expenditure',
    'BI.WAG.PREM.PB.GP': 'wage_premium',
}

# COMMAND ----------

df = wb.data.DataFrame(series_to_col_name.keys(), skipBlanks=True)
df

# COMMAND ----------

avg_df = df.groupby('series').mean()
avg_df['economy'] = 'WLD'
avg_df = avg_df.reset_index().set_index(['economy', 'series'])
avg_df

# COMMAND ----------

wide_df = df.append(avg_df).reset_index()
wide_df

# COMMAND ----------

first_key = list(series_to_col_name.keys())[0]
long_df = wide_df[wide_df.series == first_key].drop(columns=['series']).melt(id_vars='economy', var_name='year', value_name=series_to_col_name[first_key])[['economy', 'year']]

for series, col_name in series_to_col_name.items():
    long_df_w_val = wide_df[wide_df.series == series].drop(columns=['series']).melt(id_vars='economy', var_name='year', value_name=col_name, ignore_index=True)
    long_df = pd.merge(long_df, long_df_w_val, on=['economy', 'year'])

long_df['year'] = long_df['year'].str.replace('YR', '')
long_df = long_df.astype({'year': 'int'})
long_df['data_source'] = 'WWBI'
long_df

# COMMAND ----------

sdf = spark.createDataFrame(long_df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("prd_mega.indicator_intermediate.public_sector_employment")
