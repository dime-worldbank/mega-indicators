# Databricks notebook source
# MAGIC %run ../subnational_population_extraction_from_census_gov

# COMMAND ----------

df_pop = get_pop_from_census_gov('togo')

# COMMAND ----------

expected_adm1_names = ['Centrale', 'Kara', 'Maritime', 'Plateaux', 'Savanes']

extracted_adm1_names = sorted(df_pop.adm1_name.unique().tolist())
assert extracted_adm1_names == expected_adm1_names, f'Expected {expected_adm1_names}, got {extracted_adm1_names}'

# COMMAND ----------

assert df_pop.shape[0] >= 80, f'Expect at least 80 rows, got {df_pop.shape[0]}'

assert all(df_pop.population.notnull()), f'Expect no missing values in population field, got {sum(df_pop.population.isnull())} null values'

num_adm1_units = df_pop.adm1_name.nunique()
assert num_adm1_units==5

# COMMAND ----------

# Write to indicator_intermediate
database_name = "prd_mega.indicator_intermediate"

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(df_pop)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.tgo_subnational_population")
