# Databricks notebook source
# Staging schema redirect: DLT reads schema_suffix from its pipeline configuration.
_suffix = spark.conf.get("schema_suffix", "")
INDICATOR_SCHEMA = f"prd_mega.indicator{_suffix}"

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# Adding a new country requires adding the country here
country_codes = ['moz', 'pry', 'ken', 'pak', 'bfa', 'col', 'cod', 'tun', 'btn', 'chl', 'nga', 'bgd', 'alb', "zaf", 'chl', 'gha', 'lbr', 'tgo']

@dlt.table(name=f'subnational_population')
def subnational_population():
    # Consolidating all the country specific dataframes
    dfs = [spark.table(f'{INDICATOR_SCHEMA}.{code}_subnational_population_silver') for code in country_codes]
    result_df = dfs[0]
    for df in dfs[1:]:
        result_df = result_df.unionByName(df)
    return result_df
