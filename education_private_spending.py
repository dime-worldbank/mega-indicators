# Databricks notebook source
import pandas as pd

URL = 'https://stats.oecd.org/sdmx-json/data/DP_LIVE/.EDUPRIVEXP.../OECD?contentType=csv&detail=code&separator=comma&csv-lang=en'

# COMMAND ----------

df = pd.read_csv(URL)
df

# COMMAND ----------

df_edu_private_exp = df[df.SUBJECT.isin(['PRY_TRY', 'EARLYCHILDEDU'])].groupby(['LOCATION', 'TIME']).agg('sum')[['Value']].reset_index()
df_edu_private_exp['edu_private_spending_share_gdp'] = df_edu_private_exp.Value / 100
df_edu_private_exp.rename(columns={'LOCATION': 'country_code', 'TIME': 'year'}, inplace=True)
df_edu_private_exp

# COMMAND ----------

gdp_df = spark.table("indicator.gdp").toPandas()
gdp_df

# COMMAND ----------

df_merged = pd.merge(df_edu_private_exp, gdp_df, on=['country_code', 'year'])[['country_name', 'country_code', 'year', 'edu_private_spending_share_gdp', 'gdp_current_lcu']]
df_merged['edu_private_spending_current_lcu'] = df_merged.edu_private_spending_share_gdp * df_merged.gdp_current_lcu
df_merged.drop(columns=['gdp_current_lcu'], inplace=True)
df_merged['data_source'] = 'WB & OECD National Accounts, OECD Education at a Glance'
df_merged

# COMMAND ----------

sdf = spark.createDataFrame(df_merged)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("indicator.edu_private_spending")
