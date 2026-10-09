# Databricks notebook source
# MAGIC %run ../../config

# COMMAND ----------

# MAGIC %run ../../utils

# COMMAND ----------

# helper name correction map
adm1_name_map = {
    'kasai oriental':'kasai-oriental',
    'kasai occidental': 'kasai-occidental'
}
# Read global datalab table for Congo
df = read_table('global_data_lab_subnational_population')

ddf = df[df.ISO_Code=='COD'][['Country', 'Region', 'year', 'population_millions']]
ddf.columns = ['country_name', 'adm1_name', 'year', 'population']
ddf['population'] = ddf.population.map(lambda x: x*1_000_000)
# TODO: Find a different source to harmonize the changes in the adm1_names (redrawn in 2015)
ddf['adm1_name'] = ddf.adm1_name.map(lambda x: adm1_name_map.get(x.lower(), x.lower()))
pop = ddf[ddf.adm1_name!='total'].sort_values(['year', 'adm1_name'])
pop.country_name = 'Congo, Dem. Rep.'
pop['data_source'] = 'Global Data Lab'

# COMMAND ----------

ddf = df[df.ISO_Code=='COD'][['Country', 'Region', 'year', 'population_millions']]

# COMMAND ----------

write_table(pop, 'cod_subnational_population_silver')
