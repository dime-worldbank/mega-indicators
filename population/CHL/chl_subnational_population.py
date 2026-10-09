# Databricks notebook source
# MAGIC %run ../../config

# COMMAND ----------

# MAGIC %run ../../utils

# COMMAND ----------

# Shared download+parse across countries (wb_subnational_population_extract.py).
ddf_pop = read_table('wb_subnational_population_silver')
ddf_pop = ddf_pop[ddf_pop['country_code'] == 'CHL'].drop(columns='country_code').reset_index(drop=True)
ddf_pop['country_name'] = 'Chile'
ddf_pop['data_source'] = 'WB subnational population database'

# COMMAND ----------

ddf_pop.sample(3)

# COMMAND ----------

name_correction = {
    "Araucania": "Araucanía",
    "Antofagasta": "Antofagasta",
    "Aysen": "Aysén",
    "Coquimbo": "Coquimbo",
    "Biobio": "Biobío",
    "Arica y Painacota": "Arica y Parinacota",
    "Los Lagos": "Los Lagos",
    "Metropolitana": "Región Metropolitana de Santiago",
    "Libertador Gral. Bernardo O'Higgins": "Libertador General Bernardo O'Higgins",
    "Magallanes": "Magallanes y la Antártica Chilena",
    "Atacama": "Atacama",
    "Tarapaca": "Tarapacá",
    "Los Rios": "Los Ríos",
    "Maule": "Maule",
    "Valparaiso": "Valparaíso"
}
ddf_pop["adm1_name"] = ddf_pop["adm1_name"].replace(name_correction)

# COMMAND ----------

# Currently the subnational population data ends in 2016
# TO DO: Find a source to extrapolate the population for years after 2016
assert ddf_pop.shape[0] >= 255, f'Expect at least 255 rows, got {ddf_pop.shape[0]}'
assert all(ddf_pop.population.notnull()), f'Expect no missing values in population field, got {sum(ddf_pop.population.isnull())} null values'
assert ddf_pop.adm1_name.nunique() >14, f'Expect 15 adm1 regions (districts) if data is from before 2018, got {ddf_pop.adm1_name.nunique()}'
if 2019 in ddf_pop.year.unique():
    assert ddf_pop.adm1_name.nunique() >15, f'Expect 16 adm1 regions (districts) if data is after 2018, got {ddf_pop.adm1_name.nunique()}'

# COMMAND ----------

write_table(ddf_pop, 'chl_subnational_population_silver')
