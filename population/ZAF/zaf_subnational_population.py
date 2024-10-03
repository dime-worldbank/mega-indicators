# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

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

# Filter the rows corresponding to South Africa
ddf = df[(df['Country Code'].map(lambda x: x[:3]=='ZAF'))&(df['Indicator Code']=='SP.POP.TOTL')]
ddf = ddf.copy()
ddf['adm1_name'] = ddf['Country Name'].map(lambda x: x.split(',')[-1].strip())

# Remove the row with adm1_name South Africa -- this corresponds to the country population
ddf = ddf[ddf['adm1_name'] != 'South Africa']
selected_columns = ddf.columns[(ddf.columns.str.isnumeric()) | (ddf.columns == 'adm1_name')]
ddf_selected = ddf[selected_columns]
ddf_pop = ddf_selected.melt(id_vars=['adm1_name'], var_name='year', value_name='population')

# Append additional information
ddf_pop['country_name'] = 'South Africa'
ddf_pop['data_source'] = 'WB subnational population database'
# correct data types
ddf_pop['population'] = ddf_pop['population'].astype('int')
ddf_pop['year'] = ddf_pop['year'].astype('int')


# COMMAND ----------

ddf_pop.adm1_name.unique()

# COMMAND ----------

assert ddf_pop.shape[0] >= 153, f'Expect at least 153 rows, got {ddf_pop.shape[0]}'
assert all(ddf_pop.population.notnull()), f'Expect no missing values in population field, got {sum(ddf_pop.population.isnull())} null values'
assert ddf_pop.adm1_name.nunique() == 9, f'Expected 9 provinces, got {ddf_pop.adm1_name.nunique()}'

# COMMAND ----------

# Write to indicator_intermediate

database_name = "indicator_intermediate"

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(ddf_pop)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.zaf_subnational_population")
