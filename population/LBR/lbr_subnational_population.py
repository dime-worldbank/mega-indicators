# Databricks notebook source
# MAGIC %run ../../config

# COMMAND ----------

# MAGIC %run ../../utils

# COMMAND ----------

import pandas as pd

# COMMAND ----------

def build_subnational_population(country_name:str, country_code:str, adm1_drop:list=[]):

    df = read_table('global_data_lab_subnational_population')

    ddf = df[df.ISO_Code==country_code.upper()][['Country', 'Region', 'year', 'population_millions']]
    ddf.columns = ['country_name', 'adm1_name', 'year', 'population']
    ddf['population'] = ddf.population.map(lambda x: x*1_000_000)
    ddf['adm1_name'] = ddf['adm1_name'].str.lower()
    ddf = ddf[ddf.adm1_name!='total']
    ddf['adm1_name'] = ddf['adm1_name'].str.strip().str.title()
    ddf = ddf[~ddf['adm1_name'].isin(adm1_drop)]

    pop = ddf.sort_values(['year', 'adm1_name'])
    pop.country_name = country_name
    pop['data_source'] = 'Global Data Lab'

    return pop

def write_subnational_population(pop:pd.DataFrame, country_code:str):

    write_table(pop, f'{country_code.lower()}_subnational_population_silver')

    return

# COMMAND ----------

country_code = 'LBR'
country_name = 'Liberia'
adm1_drop = ['North Central','North Western','Monrovia','South Eastern A','South Eastern B','South Central']
pop = build_subnational_population(country_name, country_code, adm1_drop)
write_subnational_population(pop, country_code)
