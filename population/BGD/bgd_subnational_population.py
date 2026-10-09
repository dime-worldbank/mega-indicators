# Databricks notebook source
# MAGIC %run ../subnational_population_extraction_from_census_gov

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

if 'get_pop_from_census_gov' not in globals():  # off Databricks the %run cells above are comments
    from population.subnational_population_extraction_from_census_gov import get_pop_from_census_gov

# COMMAND ----------

update_version = update_version_flag('census_population_update_version')
df_pop = get_pop_from_census_gov('bangladesh', update_version=update_version)

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

write_table(df_pop, 'bgd_subnational_population_silver')
