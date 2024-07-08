# Databricks notebook source
! pip install openpyxl wbgapi

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

URL = 'https://www2.census.gov/programs-surveys/international-programs/tables/time-series/pepfar/burkina-faso.xlsx'

df_raw = pd.read_excel(URL, sheet_name='2015 - 2030', skiprows=2, header=None)
# Read correct header
header = df_raw.iloc[1]
df_raw.columns = header
df_raw = df_raw.drop([0,1,2])

# Extract Total population columns 
df_pop_wide = df_raw[df_raw.ADM_LEVEL==1][['CNTRY_NAME', 'ADM1_NAME']+[x for x in header if 'BTOTL' in x]]
df_pop = pd.melt(df_pop_wide, id_vars=['CNTRY_NAME', 'ADM1_NAME'], var_name='year', value_name='population')
df_pop['year'] = df_pop['year'].str.extract(r'(\d+)').astype(int)
df_pop.columns = ['country_name', 'adm1_name', 'year', 'population']

# Modifications to the admin1 and county name and add data_source
df_pop['country_name'] = df_pop['country_name'].str.title()
df_pop['adm1_name'] = df_pop['adm1_name'].str.replace(r'[-/]+', ' ', regex=True).str.title()
df_pop['data_source'] = URL
df_pop = df_pop.astype({'year': 'int', 'population': 'int'})
df_pop = df_pop.sort_values(['adm1_name', 'year'], ignore_index=True)

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



# COMMAND ----------

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

database_name = "indicator_intermediate"

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(pop)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.bfa_subnational_population")
