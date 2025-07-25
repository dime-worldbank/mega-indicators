# Databricks notebook source
import requests
from zipfile import ZipFile
import pandas as pd
from io import BytesIO

# URL of the ZIP file
zip_url = "https://databank.worldbank.org/data/download/Subnational-Population_EXCEL.zip"
response = requests.get(zip_url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    with ZipFile(BytesIO(response.content), 'r') as zip_file:
        files = zip_file.namelist()
        assert len( zip_file.namelist())==1
        excel_file_name = files[0]
        
        # Read the Excel file into a pandas DataFrame
        df = pd.read_excel(zip_file.open(excel_file_name))
else:
    print(f"Failed to download the ZIP file. Status code: {response.status_code}")


# COMMAND ----------

# Filter the rows corresponding to Chile
ddf = df[(df['Country Code'].map(lambda x: x[:3]=='CHL'))&(df['Indicator Code']=='SP.POP.TOTL')]
ddf = ddf.copy()
ddf['adm1_name'] = ddf['Country Name'].map(lambda x: x.split(',')[-1].strip())

# Remove the row with adm1_name Chile -- this corresponds to the country population
ddf = ddf[ddf['adm1_name'] != 'Chile']
selected_columns = ddf.columns[(ddf.columns.str.isnumeric()) | (ddf.columns == 'adm1_name')]
ddf_selected = ddf[selected_columns]
ddf_pop = ddf_selected.melt(id_vars=['adm1_name'], var_name='year', value_name='population')

# Append additional information
ddf_pop['country_name'] = 'Chile'
ddf_pop['data_source'] = 'WB subnational population database'

# correct data types
ddf_pop['population'] = ddf_pop['population'].astype('int')
ddf_pop['year'] = ddf_pop['year'].astype('int')

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

# Write to indicator_intermediate

database_name = "prd_mega.indicator_intermediate"

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(ddf_pop)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.chl_subnational_population")

