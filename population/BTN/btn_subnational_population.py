# Databricks notebook source
# MAGIC %run ../../config

# COMMAND ----------

# Shared download+parse across countries (wb_subnational_population_extract.py).
ddf_pop = (
    spark.table(f'{INDICATOR_SCHEMA}.wb_subnational_population_silver')
    .where("country_code = 'BTN'")
    .drop('country_code')
    .toPandas()
)
ddf_pop['country_name'] = 'Bhutan'
ddf_pop['data_source'] = 'WB subnational population database'

# COMMAND ----------

assert ddf_pop.shape[0] >= 340, f'Expect at least 340 rows, got {ddf_pop.shape[0]}'
assert all(ddf_pop.population.notnull()), f'Expect no missing values in population field, got {sum(ddf_pop.population.isnull())} null values'
assert ddf_pop.adm1_name.nunique() == 20, f'Expect 20 adm1 regions (districts), got {ddf_pop.adm1_name.nunique()}'


# COMMAND ----------

database_name = INDICATOR_SCHEMA

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(ddf_pop)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.btn_subnational_population_silver")
