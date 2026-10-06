# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

# Local counterpart of subnational_population_official_dlt.py, which unions the
# per-country silver tables on Databricks with DLT. Unions whichever of them exist
# in the local store. Keep the code list in sync with the DLT notebook.
import pandas as pd

country_codes = ['moz', 'pry', 'ken', 'pak', 'bfa', 'col', 'cod', 'tun', 'btn', 'chl', 'nga', 'bgd', 'alb', 'zaf', 'gha', 'lbr', 'tgo', 'bdi']

silver_tables = [f'{code}_subnational_population_silver' for code in country_codes]
present = [name for name in silver_tables if table_exists(name)]
if not present:
    raise FileNotFoundError(f'none of {silver_tables} exist; run a country notebook such as population/TGO/tgo_subnational_population.py first')

write_table(pd.concat([read_table(name) for name in present], ignore_index=True), 'subnational_population')
