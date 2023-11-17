# Databricks notebook source
import pandas as pd

# COMMAND ----------

URL = 'https://www.ine.gov.py/microdatos/cuadro/b7dc2DEP01-Paraguay-Poblacion-total-por-anio-calendario-segun-sexo-y-departamento-2000-2025.csv'
df_raw = pd.read_csv(URL, skip_blank_lines=True).rename(columns={'#': 'adm1_name'})
df_raw

# COMMAND ----------

df_pop_wide = df_raw.groupby('adm1_name').agg('max')\
        .drop(['Hombres', 'Mujeres', 'Total Pais'])\
        .filter(regex='^(?!Unnamed)')
df_pop_wide

# COMMAND ----------

df_pop = df_pop_wide.melt(var_name='year', value_name='population', ignore_index=False)\
    .reset_index()\
    .sort_values(['adm1_name', 'year'], ignore_index=True)

df_pop['country_name'] = 'Paraguay'
df_pop['data_source'] = URL

df_pop = df_pop.astype({'year': 'int', 'population': 'int'})

COL_NAMES_ORDERED = ['country_name', 'adm1_name', 'year', 'population', 'data_source']
df_pop = df_pop.reindex(columns=COL_NAMES_ORDERED)

df_pop

# COMMAND ----------

assert df_pop.shape[0] >= 468, f'Expect at least 468 rows, got {df_pop.shape[0]}'
assert all(df_pop.population.notnull()), f'Expect no missing values in population field, got {sum(df_pop.population.isnull())} null values'

num_departments = len(df_pop.adm1_name.unique())
assert num_departments == 18, f'Expect 18 distinct departments, got {num_departments}'

# COMMAND ----------

database_name = "indicator_intermediate"

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

# COMMAND ----------

sdf = spark.createDataFrame(df_pop)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.pry_subnational_population")

