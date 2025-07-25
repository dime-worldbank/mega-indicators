# Databricks notebook source
# MAGIC %run ../subnational_population_extraction_from_census_gov

# COMMAND ----------

df_pop = get_pop_from_census_gov('bangladesh')

# normalize division names
name_map = {
    "Barishal": "Barisal",
    "Chattogram": "Chittagong",
    "Rājshāhi": "Rajshahi",
}
df_pop['adm1_name'] = (
    df_pop['adm1_name']
    .replace(name_map)
)

df_pop

# COMMAND ----------

# normalize division names
name_map = {
    "Barishal": "Barisal",
    "Chattogram": "Chittagong",
    "Rājshāhi": "Rajshahi",
}
df_pop['adm1_name'] = (
    df_pop['adm1_name']
    .replace(name_map)
)

# COMMAND ----------

assert df_pop.shape[0] >= 328, f'Expect at least 328 rows, got {df_pop.shape[0]}'
assert all(df_pop.population.notnull()), f'Expect no missing values in population field, got {sum(df_pop.population.isnull())} null values'

num_adm1_units = df_pop.adm1_name.nunique()
assert num_adm1_units==8

# COMMAND ----------

# Write to indicator_intermediate

database_name = "prd_mega.indicator_intermediate"

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(df_pop)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.bgd_subnational_population")
