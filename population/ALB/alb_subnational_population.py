# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

import requests
from zipfile import ZipFile
import pandas as pd
import unicodedata
import io

# Extract 2016 and earlier data from WB subnational Population
# URL of the ZIP file
zip_url_WB = "https://databank.worldbank.org/data/download/Subnational-Population_EXCEL.zip"
response = requests.get(zip_url_WB)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    with ZipFile(io.BytesIO(response.content), 'r') as zip_file:
        files = zip_file.namelist()
        assert len( zip_file.namelist())==1
        excel_file_name = files[0]
        
        # Read the Excel file into a pandas DataFrame
        df_wb = pd.read_excel(zip_file.open(excel_file_name))
else:
    print(f"Failed to download the ZIP file. Status code: {response.status_code}")

# Filter the rows corresponding to Albania
df_wb = df_wb[(df_wb['Country Code'].map(lambda x: x[:3]=='ALB'))&(df_wb['Indicator Code']=='SP.POP.TOTL')]
df_wb['adm1_name'] = df_wb['Country Name'].map(lambda x: x.split(',')[-1].strip())

# Remove the row with adm1_name Albania -- this corresponds to the country population
df_wb = df_wb[df_wb['adm1_name'] != 'Albania']
selected_columns = df_wb.columns[(df_wb.columns.str.isnumeric()) | (df_wb.columns == 'adm1_name')]
df_wb = df_wb[selected_columns]
df_wb_long = df_wb.melt(id_vars=['adm1_name'], var_name='year', value_name='population')

# Append additional information
df_wb_long['country_name'] = 'Albania'
df_wb_long['data_source'] = 'WB subnational population database'
# correct data types
df_wb_long['population'] = df_wb_long['population'].astype('int')
df_wb_long['year'] = df_wb_long['year'].astype('int')

assert df_wb_long.shape[0] >= 204, f'Expect at least 204 rows, got {df_wb_long.shape[0]}'
assert all(df_wb_long.population.notnull()), f'Expect no missing values in population field, got {sum(df_wb_long.population.isnull())} null values'
assert df_wb_long.adm1_name.nunique() == 12, f'Expected 12 counties, got {df_wb_long.adm1_name.nunique()}'


# COMMAND ----------

# Extract the data from 2018 and later from
url_instat = "https://www.instat.gov.al/media/9831/tab2.xlsx"

response = requests.get(url_instat)
response.raise_for_status()
df_instat = pd.read_excel(io.BytesIO(response.content), sheet_name=0, header=3)
# selecting the column containing the adm1_names as a variable in case the exact name changes in future iterations
adm1_name_column = [x for x in df_instat.columns if 'prefectures' in str(x).lower()][0]
df_instat = df_instat.rename(columns={adm1_name_column: 'adm1_name'})
selected_columns = ['adm1_name'] + [x for x in df_instat.columns if isinstance(x, int)]

def remove_accents(input_str: str) -> str:
    return ''.join(
        c for c in unicodedata.normalize('NFD', input_str)
        if unicodedata.category(c) != 'Mn'
    )

df_instat = (
    df_instat[selected_columns]
    .dropna()
    .loc[~df_instat['adm1_name'].str.contains('total', case=False, na=False)]
    .assign(adm1_name=lambda df: df['adm1_name'].apply(remove_accents))
)

df_instat_long = df_instat.melt(
    id_vars=['adm1_name'], 
    var_name='year', 
    value_name='population'
)

df_instat_long['country_name'] = 'Albania'
df_instat_long['data_source'] = 'instat.gov.al'

assert all(df_instat_long.population.notnull()), f'Expected no missing values in population field, got {sum(df_instat_long.population.isnull())} null values'
assert df_instat_long.adm1_name.nunique() == 12, f'Expected 12 counties, got {df_instat_long.adm1_name.nunique()}'

# COMMAND ----------

# combine the two data sources and impute the values for the missing year 2017
df = pd.concat([df_wb_long, df_instat_long])

df['year'] = pd.to_numeric(df['year'], errors='coerce')

pivot_df = df[df['year'].isin([2016, 2018])].pivot_table(
    index='adm1_name', 
    columns='year', 
    values='population'
)

pivot_df[2017] = ((pivot_df[2016] + pivot_df[2018]) / 2)

imputed_df = pivot_df.reset_index().melt(
    id_vars='adm1_name', 
    value_vars=[2016, 2017, 2018], 
    var_name='year', 
    value_name='population'
)
imputed_df.loc[imputed_df['year'] == 2017, 'country_name'] = 'Albania'
imputed_df.loc[imputed_df['year'] == 2017, 'data_source'] = 'Imputed from WB subnational population and instat.gov.al'

df_pop = pd.concat([df, imputed_df], ignore_index=True).drop_duplicates(subset=['adm1_name', 'year'])

df_pop = df_pop.sort_values(['adm1_name', 'year'])
df_pop['population'] = df_pop['population'].astype(int)

# COMMAND ----------

# Write to indicator_intermediate

database_name = "prd_mega.indicator_intermediate"

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(df_pop)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.alb_subnational_population")
