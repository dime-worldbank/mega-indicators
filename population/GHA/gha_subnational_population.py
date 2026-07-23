# Databricks notebook source
# MAGIC %run ../subnational_population_extraction_from_census_gov

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

df_pop = get_pop_from_census_gov('ghana')
df_pop['adm1_name'] = (
    df_pop['adm1_name']
    .str.replace(r'(?i)\bRegion\b', '', regex=True)
    .str.strip()
)

# COMMAND ----------

extracted_adm1_names = sorted(df_pop.adm1_name.unique().tolist())
expected_adm1_names = ['Ahafo', 'Ashanti', 'Bono', 'Bono East', 'Central', 'Eastern',
       'Greater Accra', 'North East', 'Northern', 'Oti', 'Savannah',
       'Upper East', 'Upper West', 'Volta', 'Western', 'Western North']
assert extracted_adm1_names == expected_adm1_names, extracted_adm1_names

# COMMAND ----------

assert df_pop.shape[0] >= 256, f'Expect at least 256 rows, got {df_pop.shape[0]}'
assert all(df_pop.population.notnull()), f'Expect no missing values in population field, got {sum(df_pop.population.isnull())} null values'

num_adm1_units = df_pop.adm1_name.nunique()
assert num_adm1_units == 16

# COMMAND ----------

database_name = INDICATOR_SCHEMA

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(df_pop)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.gha_subnational_population_silver")
