# Databricks notebook source
# MAGIC %run ../../config

# COMMAND ----------

# MAGIC %run ../../utils

# COMMAND ----------

# Shared download+parse across countries (wb_subnational_population_extract.py).
ddf_pop = read_table('wb_subnational_population_silver')
ddf_pop = ddf_pop[ddf_pop['country_code'] == 'ZAF'].drop(columns='country_code').reset_index(drop=True)
ddf_pop['country_name'] = 'South Africa'
ddf_pop['data_source'] = 'WB subnational population database'

# COMMAND ----------

ddf_pop.adm1_name.unique()

# COMMAND ----------

assert ddf_pop.shape[0] >= 153, f'Expect at least 153 rows, got {ddf_pop.shape[0]}'
assert all(ddf_pop.population.notnull()), f'Expect no missing values in population field, got {sum(ddf_pop.population.isnull())} null values'
assert ddf_pop.adm1_name.nunique() == 9, f'Expected 9 provinces, got {ddf_pop.adm1_name.nunique()}'

# COMMAND ----------

write_table(ddf_pop, 'zaf_subnational_population_silver')
