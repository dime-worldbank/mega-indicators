# Databricks notebook source
# MAGIC %run ../subnational_population_extraction_from_census_gov

# COMMAND ----------

df_pop = get_pop_from_census_gov('kenya', timeseries='prh')

# COMMAND ----------

expected_adm1_names = [
    'Baringo', 'Bomet', 'Bungoma', 'Busia', 'Elgeyo Marakwet', 'Embu',
    'Garissa', 'Homa Bay', 'Isiolo', 'Kajiado', 'Kakamega', 'Kericho',
    'Kiambu', 'Kilifi', 'Kirinyaga', 'Kisii', 'Kisumu', 'Kitui', 'Kwale',
    'Laikipia', 'Lamu', 'Machakos', 'Makueni', 'Mandera', 'Marsabit', 'Meru',
    'Migori', 'Mombasa', 'Murang’A', 'Nairobi City', 'Nakuru', 'Nandi',
    'Narok', 'Nyamira', 'Nyandarua', 'Nyeri', 'Samburu', 'Siaya',
    'Taita Taveta', 'Tana River', 'Tharaka Nithi', 'Trans Nzoia', 'Turkana',
    'Uasin Gishu', 'Vihiga', 'Wajir', 'West Pokot'
]

extracted_adm1_names = sorted(df_pop.adm1_name.unique().tolist())
assert extracted_adm1_names == expected_adm1_names, f'Expected {expected_adm1_names}, got {extracted_adm1_names}'

# COMMAND ----------

assert df_pop.shape[0] >= 1927, f'Expect at least 1927 rows, got {df_pop.shape[0]}'
assert all(df_pop.population.notnull()), f'Expect no missing values in population field, got {sum(df_pop.population.isnull())} null values'

num_counties = df_pop.adm1_name.nunique()
assert num_counties==47

# COMMAND ----------

# Write to indicator_intermediate

database_name = "prd_mega.indicator_intermediate"

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(df_pop)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.ken_subnational_population")
