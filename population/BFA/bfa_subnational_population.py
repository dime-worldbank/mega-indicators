# Databricks notebook source
! pip install openpyxl

# COMMAND ----------

import pandas as pd

# population data for 2015 and after from census.gov
URL = 'https://www2.census.gov/programs-surveys/international-programs/tables/time-series/pepfar/burkina-faso.xlsx'

df_raw = pd.read_excel(URL, sheet_name='2015 - 2030', skiprows=2, header=None)
# Read correct header
header = df_raw.iloc[1]
df_raw.columns = header
df_raw = df_raw.drop([0,1,2])

# Extract Total population columns 
df_pop_wide = df_raw[df_raw.ADM_LEVEL==1][['CNTRY_NAME', 'ADM1_NAME']+[x for x in header if 'BTOTL' in x]]
df_pop = pd.melt(df_pop_wide, id_vars=['CNTRY_NAME', 'ADM1_NAME'], var_name='year', value_name='population')
df_pop['year'] = df_pop['year'].str.extract(r'(\d+)').astype(int)
df_pop.columns = ['country_name', 'adm1_name', 'year', 'population']

# Modifications to the admin1 and county name and add data_source
df_pop['country_name'] = df_pop['country_name'].str.title()
df_pop['adm1_name'] = df_pop['adm1_name'].str.replace(r'[-/]+', ' ', regex=True).str.title()
df_pop['data_source'] = URL
df_pop = df_pop.astype({'year': 'int', 'population': 'int'})
df_pop = df_pop.sort_values(['adm1_name', 'year'], ignore_index=True)

# COMMAND ----------

num_adm1_units = df_pop.adm1_name.nunique()

assert df_pop.shape[0] >= 208, f'Expect at least 208 rows, got {df_pop.shape[0]}'
assert all(df_pop.population.notnull()), f'Expect no missing values in population field, got {sum(df_pop.population.isnull())} null values'
# Note that Federally Administrative Tribal Areas appears in some lists as an admin1 region and not in some
assert num_adm1_units==13

# COMMAND ----------

# Write to indicator_intermediate

database_name = "indicator_intermediate"

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(df_pop)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.bfa_subnational_population")

