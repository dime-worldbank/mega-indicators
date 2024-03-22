# Databricks notebook source
import pandas as pd
import requests

# COMMAND ----------

# MAGIC %md
# MAGIC ###### GHED_OOPSCHE_SHA2011: Out-of-pocket expenditure as percentage of current health expenditure (CHE) (%)
# MAGIC The share of revenues from out-of-pocket payments as percentage of current health expenditure indicates how much is funded directly by households out-of-pocket expenditure on health. High out-of-pocket payments are associated with catastrophic and impoverishing household spending. Out of pocket payments are not pooled and there is no sharing of risk among wider group of people other than the household. This indicator describes the role of household out-of-pocket payments in funding healthcare relative to government, external or other private domestic sources.
# MAGIC
# MAGIC ###### GHED_CHEGDP_SHA2011: Current health expenditure (CHE) as percentage of gross domestic product (GDP) (%)
# MAGIC Current health expenditure as a share of GDP provides an indication on the level of resources channelled to health relative to other uses. It shows the importance of the health sector in the whole economy and indicates the societal priority which health is given measured in monetary terms.
# MAGIC
# MAGIC ###### GHED_CHE_pc_US_SHA2011:Current health expenditure (CHE) per capita in US$
# MAGIC This indicator calculates the current health expenditure spent per person in USD currency. It contributes to understand the current health expenditure relative to the population size facilitating international comparison.
# MAGIC
# MAGIC ###### GHED_OOP_pc_US_SHA2011: Out-of-Pocket expenditure (OOP) per capita in US$
# MAGIC This indicator calculates the current health expenditure from out-of-pocket payments per person in USD currency. It indicates how much every person pays out-of-pocket on average in USD at the point of use. High out-of-pocket payments are associated with catastrophic and impoverishing household spending. Out of pocket payments are not pooled and there is no sharing of risk among wider group of people other than the household. This indicator describes the OOP expenditure in relation to the population size facilitating international comparison.
# MAGIC

# COMMAND ----------

indicators = {
    'GHED_OOPSCHE_SHA2011' : 'oop_percent_che',
    'GHED_CHEGDP_SHA2011': 'che_percent_gdp',
    'GHED_CHE_pc_US_SHA2011' : 'che_per_capita_usd',
    'GHED_OOP_pc_US_SHA2011' : 'oop_per_capita_usd',
}

df = pd.DataFrame()
for indicator, value in indicators.items():
    url = f"https://ghoapi.azureedge.net/api/{indicator}"
    resp = requests.get(url)
    ddf = pd.DataFrame(resp.json()['value'])
    ddf = ddf[ddf.SpatialDimType=='COUNTRY'][['SpatialDim', 'ParentLocationCode', 'TimeDim', 'NumericValue']]
    ddf.rename(columns={'NumericValue': value}, inplace=True)
    if df.empty:
        df = ddf
    else:
        df = df.merge(ddf, on=['SpatialDim', 'ParentLocationCode', 'TimeDim'], how='outer')

# rename columns
df.rename(columns={'SpatialDim': 'country_code', 'ParentLocationCode':'region_WHO', 'TimeDim':'year' }, inplace=True)
# append datasource name
df['data_source'] = 'https://ghoapi.azureedge.net/api/'

num_countries = df.country_code.nunique()
assert num_countries >= 192, f'Expected data from at least 192 countries, got {num_countries}'


# COMMAND ----------

# read data from the gdp table in indicator
df_gdp = spark.sql("SELECT * FROM indicator.gdp").toPandas()[['country_name', 'country_code', 'region',  'year', 'gdp_current_lcu']]
# merge to the previous dataframe
merged_df = pd.merge(df, df_gdp, on=['country_code', 'year'], how='left')
# to get CHE get the 'che_percent_gdp' percentage of the value in gdp_current_lcu
che = merged_df.apply(lambda x: 0.01 * x['che_percent_gdp'] * x['gdp_current_lcu'] if pd.notnull(x['che_percent_gdp']) and pd.notnull(x['gdp_current_lcu']) else None, axis=1)
merged_df['che'] = che
merged_df.sort_values(['country_name', 'year'], inplace=True)
cols = ['country_code', 'country_name', 'region_WHO', 'region', 'year',	'che', 'oop_percent_che', 'oop_per_capita_usd',  'che_percent_gdp', 'che_per_capita_usd','gdp_current_lcu', 'data_source']
merged_df = merged_df[cols]

# COMMAND ----------

merged_df.sample(3)

# COMMAND ----------

# Write to indicator
database_name = "indicator"

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")
sdf = spark.createDataFrame(merged_df)
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{database_name}.health_expenditure")
