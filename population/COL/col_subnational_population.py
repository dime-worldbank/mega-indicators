# Databricks notebook source
!pip install openpyxl

# COMMAND ----------

import numpy as np
import pandas as pd
import unicodedata

# COMMAND ----------

def normalize_cell(cell_value):
    if pd.notna(cell_value) and isinstance(cell_value, str):
        return ''.join(c for c in unicodedata.normalize('NFD', cell_value)
                       if unicodedata.category(c) != 'Mn')
    else:
        return cell_value

# COMMAND ----------

URL = 'https://www2.census.gov/programs-surveys/international-programs/tables/time-series/bha/Colombia.xlsx'

def usecols(col):
    return col in ('ADM1_NAME', 'ADM2_NAME', 'ADM_LEVEL', 'NSO_CODE') or col.startswith('BTOTL_')

df_raw = pd.read_excel(URL, sheet_name=-1, skiprows=3, usecols=usecols)
df_raw.columns = df_raw.columns.str.lower()
df_raw['adm1_name'] = df_raw.adm1_name.str.title().apply(normalize_cell)
df_raw

# COMMAND ----------

df_adm2_adm1_lookup = df_raw[df_raw.adm_level.isin([1, 2])][['adm1_name', 'adm2_name', 'nso_code']]
df_adm2_adm1_lookup.nso_code = df_adm2_adm1_lookup.nso_code.astype(int)
df_adm2_adm1_lookup

# COMMAND ----------

# Save the adm1 adm2 nso lookup table for reuse
database_name = "prd_mega.indicator_intermediate"

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(df_adm2_adm1_lookup)
sdf.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{database_name}.col_subnational_adm2_adm1_lookup")

# COMMAND ----------

df_pop_wide = df_raw[df_raw.adm_level == 1].drop(columns=['adm2_name', 'adm_level', 'nso_code'])
df_pop = pd.melt(df_pop_wide, id_vars=['adm1_name'], var_name='year', value_name='population')
df_pop['year'] = df_pop['year'].str.extract(r'(\d+)').astype(int)
df_pop['country_name'] = 'Colombia'
df_pop['data_source'] = URL
df_pop

# COMMAND ----------

# check column data types
for col in ['year', 'population']:
    assert df_pop[col].dtype == 'int64', f'Expect {col} to be of type integer, got {df_pop[col].dtype}'

# check number of adm1 observations per year
num_adm1_by_year = df_pop.groupby(['year'])['adm1_name'].count()
expected_num_adm1_units = 33
assert np.all(num_adm1_by_year.values == expected_num_adm1_units), f'Expect there to be {expected_num_adm1_units} across all years, but got {num_adm1_by_year}'

# COMMAND ----------

sdf_pop = spark.createDataFrame(df_pop)
sdf_pop.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{database_name}.col_subnational_population")
