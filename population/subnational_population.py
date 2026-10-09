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

# COMMAND ----------

# Consolidating all the country specific dataframes
dfs = [read_table(f'{code}_subnational_population_silver') for code in country_codes]
write_table(pd.concat(dfs, ignore_index=True), 'subnational_population')