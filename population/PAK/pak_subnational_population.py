# Databricks notebook source
# MAGIC %run ../../config

# COMMAND ----------

# MAGIC %run ../../utils

# COMMAND ----------

update_version = update_version_flag('census_population_update_version')
df_pop = get_pop_from_census_gov('pakistan', update_version=update_version)

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

write_table(df_pop, 'pak_subnational_population_silver')
