# Databricks notebook source
# MAGIC %run ../../config

# COMMAND ----------

# MAGIC %run ../../utils

# COMMAND ----------

# Shared download+parse across countries (wb_subnational_population_extract.py).
ddf_pop = read_table('wb_subnational_population_silver')
ddf_pop = ddf_pop[ddf_pop['country_code'] == 'TUN'].drop(columns='country_code').reset_index(drop=True)
ddf_pop['country_name'] = 'Tunisia'
ddf_pop['data_source'] = 'WB subnational population database'

# COMMAND ----------

assert ddf_pop.shape[0] >= 408, f'Expect at least 408 rows, got {ddf_pop.shape[0]}'
assert all(ddf_pop.population.notnull()), f'Expect no missing values in population field, got {sum(ddf_pop.population.isnull())} null values'
assert ddf_pop.adm1_name.nunique() == 24, f'Expect 24 adm1 regions (governorates), got {ddf_pop.adm1_name.nunique()}'

# COMMAND ----------

write_table(ddf_pop, 'tun_subnational_population_silver')
