# Databricks notebook source
# MAGIC %run ../../config

# COMMAND ----------

import pandas as pd

# COMMAND ----------

def build_subnational_population(country_name:str, country_code:str, adm1_drop:list=[]):

    spark_df = spark.table(f'{INDICATOR_SCHEMA}.global_data_lab_subnational_population')
    df = spark_df.toPandas()

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

    database_name = INDICATOR_SCHEMA
    if not spark.catalog.databaseExists(database_name):
        print(f"Database '{database_name}' does not exist. Creating the database.")
        spark.sql(f"CREATE DATABASE {database_name}")

    sdf = spark.createDataFrame(pop)
    sdf.write.mode("overwrite").saveAsTable(f"{database_name}.{country_code.lower()}_subnational_population_silver")

    return

# COMMAND ----------

country_code = 'LBR'
country_name = 'Liberia'
adm1_drop = ['North Central','North Western','Monrovia','South Eastern A','South Eastern B','South Central']
pop = build_subnational_population(country_name, country_code, adm1_drop)
write_subnational_population(pop, country_code)
