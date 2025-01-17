# Databricks notebook source
!pip install openpyxl

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# census.gov subnational population 
URL = 'https://www2.census.gov/programs-surveys/international-programs/tables/time-series/pepfar/nigeria.xlsx'

df_raw = pd.read_excel(URL, sheet_name='2000 - 2020', skiprows=2, header=None)
# Read correct header
header = df_raw.iloc[1]
df_raw.columns = header
df_raw = df_raw.drop([0,1,2])


# COMMAND ----------

df_raw.sample(3)

# COMMAND ----------

# Extract Total population columns 
df_pop_wide = df_raw[df_raw.ADM_LEVEL==1][['COUNTRY', 'ADM1_NAME']+[x for x in header if 'BTOTL' in x]]
df_pop = pd.melt(df_pop_wide, id_vars=['COUNTRY', 'ADM1_NAME'], var_name='year', value_name='population')
df_pop['year'] = df_pop['year'].str.extract(r'(\d+)').astype(int)
df_pop.columns = ['country_name', 'adm1_name', 'year', 'population']

# Modifications to the admin1 and county name and add data_source
df_pop['country_name'] = df_pop['country_name'].str.title()
df_pop['adm1_name'] = df_pop['adm1_name'].str.replace(r'[-/]+', ' ', regex=True).str.title()
df_pop['data_source'] = URL
df_pop = df_pop.astype({'year': 'int', 'population': 'int'})
df_pop = df_pop.sort_values(['adm1_name', 'year'], ignore_index=True)

num_adm1_units = df_pop.adm1_name.nunique()

assert df_pop.shape[0] >= 777, f'Expect at least 777 rows, got {df_pop.shape[0]}'
assert all(df_pop.population.notnull()), f'Expect no missing values in population field, got {sum(df_pop.population.isnull())} null values'
# 36 states and 1 federal capital territory
assert num_adm1_units==37

# COMMAND ----------

# Write to indicator_intermediate

database_name = "prd_mega.indicator_intermediate"

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(df_pop)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.nga_subnational_population")

