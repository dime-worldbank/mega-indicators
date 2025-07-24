# Databricks notebook source
# MAGIC %run ../subnational_population_extraction_from_census_gov

# COMMAND ----------

import pandas as pd
import wbgapi as wb

# Helper functions
def modify_name(name):
    if not name:
        return
    mod_name = name.split('_BF.')[0].split('BFA_')[-1]
    return mod_name.replace('_', ' ').replace('-', ' ').title()

# COMMAND ----------

# post 2015 population estimates from census.gov
df_pop = get_pop_from_census_gov('burkina-faso')

# Pre 2015 spopulation estimates from the World Bank subnational population database 
wb.db = 50 # Database ID
ddf_raw = wb.data.DataFrame('SP.POP.TOTL', skipBlanks=True)
ddf = ddf_raw[ddf_raw.index.map(lambda x: x.split('_')[0]=='BFA')][1:]
ddf['adm1_name'] = ddf.index.map(lambda x: modify_name(x))
ddf_pop = pd.melt(ddf, id_vars=['adm1_name'], var_name='year', value_name='population')
ddf_pop['year'] = ddf_pop.year.map(lambda x: int(x[2:]))
# select the years on or before 2014
ddf_pop = ddf_pop[ddf_pop.year<2015]
ddf_pop['country_name'] = 'Burkina Faso'
ddf_pop['data_source'] = "WB API Database ID 50"

# Final concatenated dataset
pop =  pd.concat([df_pop, ddf_pop]).sort_values(['adm1_name', 'year'])

# name changes to show correct regions in the Power BI viz
name_changes = {
    'Est': 'Est Region Burkina Faso',
    'Centre Sud': 'Centre Sud Region Burkina Faso'
}
pop['adm1_name'] = pop.adm1_name.map(lambda x: name_changes.get(x, x))
pop['population'] = pop.population.astype(int)

# COMMAND ----------

assert df_pop['year'].min() == 2015, f"Expected minimum year from census.gov to be 2015, got {df_pop['year'].min()}"
assert ddf_pop['year'].max() == 2014, f"Expected maximum year from wbgapi to be 2015, got {ddf_pop['year'].min()}"

num_adm1_units_src1 = df_pop.adm1_name.nunique()
num_adm1_units_src2 = ddf_pop.adm1_name.nunique()

assert df_pop.shape[0] >= 208, f'Expect at least 208 rows, got {df_pop.shape[0]}'
assert ddf_pop.shape[0] >= 195, f'Expect at least 195 rows, gor{ddf_pop.shape[0]}'
assert all(df_pop.population.notnull()), f'Expect no missing values in population field, got {sum(df_pop.population.isnull())} null values'
assert all(ddf_pop.population.notnull()), f'Expect no missing values in population field, got {sum(ddf_pop.population.isnull())} null values'

assert num_adm1_units_src1 == 13
assert num_adm1_units_src2 == 13

# COMMAND ----------

# Write to indicator_intermediate

database_name = "prd_mega.indicator_intermediate"

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(pop)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.bfa_subnational_population")
