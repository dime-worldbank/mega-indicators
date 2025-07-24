# Databricks notebook source
# MAGIC %run ../subnational_population_extraction_from_census_gov

# COMMAND ----------

df_pop = get_pop_from_census_gov('pakistan')

# COMMAND ----------

expected_adm1_names = [
  'Azad Kashmir', 'Balochistan', 'Federally Administered Tribal Areas',
  'Gilgit Baltistan', 'Islamabad', 'Khyber Pakhtunkhwa', 'Punjab', 'Sindh'
]

extracted_adm1_names = sorted(df_pop.adm1_name.unique().tolist())
assert extracted_adm1_names == expected_adm1_names, f'Expected {expected_adm1_names}, got {extracted_adm1_names}'

# COMMAND ----------

assert df_pop.shape[0] >= 328, f'Expect at least 328 rows, got {df_pop.shape[0]}'
assert all(df_pop.population.notnull()), f'Expect no missing values in population field, got {sum(df_pop.population.isnull())} null values'
# Note that Federally Administrative Tribal Areas appears in some lists as an admin1 region and not in some
num_adm1_units = df_pop.adm1_name.nunique()
assert num_adm1_units==8

# COMMAND ----------

# Write to indicator_intermediate

database_name = "prd_mega.indicator_intermediate"

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(df_pop)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.pak_subnational_population")
