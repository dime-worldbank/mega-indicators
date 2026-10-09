# Databricks notebook source
# MAGIC %run ../../config

# COMMAND ----------

# MAGIC %run ../../utils

# COMMAND ----------

if 'get_pop_from_census_gov' not in globals():  # off Databricks the %run cells above are comments
    from population.subnational_population_extraction_from_census_gov import get_pop_from_census_gov

# COMMAND ----------

update_version = update_version_flag('census_population_update_version')
df_pop = get_pop_from_census_gov('ghana', update_version=update_version)
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

write_table(df_pop, 'gha_subnational_population_silver')
