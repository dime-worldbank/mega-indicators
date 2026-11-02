# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

# subnational_population: the per-country silver tables stacked into one. Plain pandas on
# both sides.
import pandas as pd

# Adding a new country requires adding the country here
country_codes = ['moz', 'pry', 'ken', 'pak', 'bfa', 'col', 'cod', 'tun', 'btn', 'chl', 'nga', 'bgd', 'alb', 'zaf', 'gha', 'lbr', 'tgo', 'bdi', 'mda']

if COUNTRY_NAME:  # a one-country run only has that country's silver table
    country_codes = [code for code in country_codes if table_exists(f'{code}_subnational_population_silver')]

# COMMAND ----------

# Consolidating all the country specific dataframes. Only these columns are kept: a country
# table may carry extras (ALB's per-row source_id), and read_table fails on a missing one.
COLUMNS = ['country_name', 'adm1_name', 'year', 'population']
dfs = [read_table(f'{code}_subnational_population_silver', columns=COLUMNS) for code in country_codes]
write_table(pd.concat(dfs, ignore_index=True), 'subnational_population')