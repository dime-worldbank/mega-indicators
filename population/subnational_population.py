# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

# subnational_population: the per-country silver tables stacked into one. Plain pandas on
# both sides (it replaced a DLT pipeline).
import pandas as pd

# Adding a new country requires adding the country here
country_codes = ['moz', 'pry', 'ken', 'pak', 'bfa', 'col', 'cod', 'tun', 'btn', 'chl', 'nga', 'bgd', 'alb', 'zaf', 'gha', 'lbr', 'tgo', 'bdi']

if COUNTRY_NAME:  # a one-country run only has that country's silver table
    country_codes = [code for code in country_codes if table_exists(f'{code}_subnational_population_silver')]

# COMMAND ----------

# Consolidating all the country specific dataframes
dfs = [read_table(f'{code}_subnational_population_silver') for code in country_codes]
columns = set(dfs[0].columns)
mismatched = [code for code, df in zip(country_codes, dfs) if set(df.columns) != columns]
assert not mismatched, f"columns differ from {country_codes[0]}'s in {mismatched}"  # pd.concat would fill NaN where the DLT unionByName failed
write_table(pd.concat(dfs, ignore_index=True), 'subnational_population')