# Databricks notebook source
def process_to_indicator_intermediate(country_name,country_code):

    # Read global datalab table
    spark_df = spark.table(f'prd_mega.indicator.global_data_lab_subnational_population')
    df = spark_df.toPandas()

    ddf = df[df.ISO_Code==country_code.upper()][['Country', 'Region', 'year', 'population_millions']]
    ddf.columns = ['country_name', 'adm1_name', 'year', 'population']
    ddf['population'] = ddf.population.map(lambda x: x*1_000_000)
    ddf['adm1_name'] = ddf['adm1_name'].str.lower()
    ddf = ddf[ddf.adm1_name!='total']
    ddf['adm1_name'] = ddf['adm1_name'].str.strip().str.title()

    pop = ddf.sort_values(['year', 'adm1_name'])
    pop.country_name = country_name
    pop['data_source'] = 'Global Data Lab'

    return pop

def write_to_indicator_intermediate(pop,country_code):
    # Write to indicator_intermediate
    database_name = "prd_mega.indicator_intermediate"
    if not spark.catalog.databaseExists(database_name):
        print(f"Database '{database_name}' does not exist. Creating the database.")
        spark.sql(f"CREATE DATABASE {database_name}")

    sdf = spark.createDataFrame(pop)
    sdf.write.mode("overwrite").saveAsTable(f"{database_name}.{country_code.lower()}_subnational_population")

    return

# COMMAND ----------

country_code = 'LBR'
country_name = 'Liberia'
pop = process_to_indicator_intermediate(country_name,country_code)
write_to_indicator_intermediate(pop,country_code)
