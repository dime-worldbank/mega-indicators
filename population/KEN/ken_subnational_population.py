# Databricks notebook source
# MAGIC %run ../../config

# COMMAND ----------

# MAGIC %run ../../utils

# COMMAND ----------

if 'get_pop_from_census_gov' not in globals():  # off Databricks the %run cells above are comments
    from population.subnational_population_extraction_from_census_gov import get_pop_from_census_gov

# COMMAND ----------

update_version = update_version_flag('census_population_update_version')
df_pop = get_pop_from_census_gov('kenya', timeseries='prh', update_version=update_version)

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

write_table(df_pop, 'ken_subnational_population_silver')
