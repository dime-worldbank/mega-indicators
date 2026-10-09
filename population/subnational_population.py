# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# subnational_population: the per-country silver tables stacked into one. Plain pandas, run
# as a notebook task (it replaced a DLT pipeline).
import pandas as pd

# Adding a new country requires adding the country here
country_codes = ['moz', 'pry', 'ken', 'pak', 'bfa', 'col', 'cod', 'tun', 'btn', 'chl', 'nga', 'bgd', 'alb', 'zaf', 'gha', 'lbr', 'tgo', 'bdi']

# COMMAND ----------

# Consolidating all the country specific dataframes
dfs = [spark.table(f'{INDICATOR_SCHEMA}.{code}_subnational_population_silver').toPandas() for code in country_codes]
spark.createDataFrame(pd.concat(dfs, ignore_index=True)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{INDICATOR_SCHEMA}.subnational_population")