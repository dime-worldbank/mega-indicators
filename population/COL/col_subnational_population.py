# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

# MAGIC %run ../../utils

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

def _read_col_census_excel(buf):
    def usecols(col):
        return col in ('ADM1_NAME', 'ADM2_NAME', 'ADM_LEVEL', 'NSO_CODE') or col.startswith('BTOTL_')
    df = pd.read_excel(buf, sheet_name=-1, skiprows=3, usecols=usecols)
    df.columns = df.columns.str.lower()
    return df

update_version = update_version_flag('census_population_update_version')
df_raw = versioned_dataframe(URL, 'col_census_raw', update_version, parse=_read_col_census_excel)
df_raw['adm1_name'] = df_raw.adm1_name.str.title().apply(normalize_cell)
df_raw

# COMMAND ----------

df_adm2_adm1_lookup = df_raw[df_raw.adm_level.isin([1, 2])][['adm1_name', 'adm2_name', 'nso_code']]
df_adm2_adm1_lookup.nso_code = df_adm2_adm1_lookup.nso_code.astype(int)
df_adm2_adm1_lookup

# COMMAND ----------

# Save the adm1 adm2 nso lookup table for reuse
write_table(df_adm2_adm1_lookup, 'col_subnational_adm2_adm1_lookup_silver')

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

write_table(df_pop, 'col_subnational_population_silver')
