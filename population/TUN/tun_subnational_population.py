# Databricks notebook source
# MAGIC %run ../../config

# COMMAND ----------

import pandas as pd

# Shared download+parse across countries (wb_subnational_population_extract.py).
ddf_pop = (
    spark.table(f'{INDICATOR_SCHEMA}.wb_subnational_population_silver')
    .where("country_code = 'TUN'")
    .drop('country_code')
    .toPandas()
)
ddf_pop['country_name'] = 'Tunisia'
ddf_pop['data_source'] = 'WB subnational population database'

# COMMAND ----------

assert ddf_pop.shape[0] >= 408, f'Expect at least 408 rows, got {ddf_pop.shape[0]}'
assert all(ddf_pop.population.notnull()), f'Expect no missing values in population field, got {sum(ddf_pop.population.isnull())} null values'
assert ddf_pop.adm1_name.nunique() == 24, f'Expect 24 adm1 regions (governorates), got {ddf_pop.adm1_name.nunique()}'


# COMMAND ----------

database_name = INDICATOR_SCHEMA

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(ddf_pop)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.tun_subnational_population_silver")
