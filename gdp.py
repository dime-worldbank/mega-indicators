# Databricks notebook source
# MAGIC %pip install wbgapi

# COMMAND ----------

import wbgapi as wb
import pandas as pd

# COMMAND ----------

wb.db = 2
series = 'NY.GDP.MKTP.CN'

# COMMAND ----------

df = wb.data.DataFrame(series, skipBlanks=True).reset_index()
df

# COMMAND ----------

long_df = df.melt(id_vars='economy', var_name='year', value_name='gdp_current_lcu')
long_df = long_df.dropna(subset='gdp_current_lcu')
long_df['year'] = long_df['year'].str.replace('YR', '')
long_df = long_df.astype({'year': 'int'})
long_df['data_source'] = 'WB & OECD National Accounts'
long_df

# COMMAND ----------

country_df = spark.table(f'indicator.country').select('country_name', 'country_code', 'region').toPandas()
country_df

# COMMAND ----------

gdp_df = pd.merge(long_df, country_df, left_on='economy', right_on='country_code', how='left')[['country_name', 'country_code', 'region', 'year', 'gdp_current_lcu', 'data_source']]
gdp_df

# COMMAND ----------

sdf = spark.createDataFrame(gdp_df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("indicator.gdp")
