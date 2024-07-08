# Databricks notebook source
import pandas as pd


# COMMAND ----------

# helper name correction map
adm1_name_map = {
    'kasai oriental':'kasai-oriental',
    'kasai occidental': 'kasai-occidental'
}
# Read global datalab table for Congo
spark_df = spark.table(f'indicator.global_data_lab_subnational_population')
df = spark_df.toPandas()

ddf = df[df.Country=='Congo Democratic Republic'][['Country', 'Region', 'year', 'population_millions']]
ddf.columns = ['country_name', 'adm1_name', 'year', 'population']
ddf['population'] = ddf.population.map(lambda x: x*1_000_000)
# TODO: Find a different source to harmonize the changes in the adm1_names (redrawn in 2015)
ddf['adm1_name'] = ddf.adm1_name.map(lambda x: adm1_name_map.get(x.lower(), x.lower()))
pop = ddf[ddf.adm1_name!='total'].sort_values(['year', 'adm1_name'])
pop.country_name = 'Congo, Dem. Rep.'
pop['data_source'] = 'Global Data Lab'

# COMMAND ----------

pop.sample(5)

# COMMAND ----------

# Write to indicator_intermediate
database_name = "indicator_intermediate"
if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(pop)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.cod_subnational_population")
