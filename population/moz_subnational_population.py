# Databricks notebook source
import unicodedata
import pandas as pd

# COMMAND ----------

POP_07_16_URL = 'https://raw.githubusercontent.com/weilu/mega-indicators/main/population/MOZ/moz_pop_2007-2016.csv'
pop_07_16 = pd.read_csv(POP_07_16_URL, usecols=['region', 'Date', 'Value'])
pop_07_16['data_source'] = 'https://mozambique.opendataforafrica.org/RDM2016'
pop_07_16

# COMMAND ----------

POP_17_50_URL = 'https://raw.githubusercontent.com/weilu/mega-indicators/main/population/MOZ/moz_pop_2017-2050.csv'
pop_17_50 = pd.read_csv(POP_17_50_URL, usecols=['província', 'Date', 'Value'])
pop_17_50.rename(columns={'província': 'region'}, inplace=True)
pop_17_50['data_source'] = 'https://mozambique.opendataforafrica.org/bumjrrg'
pop_17_50

# COMMAND ----------

def remove_accents(text):
    return ''.join(char for char in unicodedata.normalize('NFD', text) if unicodedata.category(char) != 'Mn')

df = pd.concat([pop_07_16, pop_17_50], ignore_index=True)
df['region'] = df['region'].apply(remove_accents)
df.rename(columns={'region': 'adm1_name_alt', 'Date': 'year', 'Value': 'population'}, inplace=True)

df['country_name'] = 'Mozambique'

columns_ordered = ['country_name', 'adm1_name_alt', 'year', 'population', 'data_source']
df = df.reindex(columns=columns_ordered)
df

# COMMAND ----------

# Check regions
num_regions = len(df.adm1_name_alt.unique())
assert num_regions == 11, f'Expect there to be 11 regions got {num_regions}'

# Check year coverage for each region
for region, group in df.groupby('adm1_name_alt')['year']:
    assert group.iloc[0] == 2007, f"Invalid start year for region {region}"
    assert group.iloc[-1] == 2050, f"Invalid end year for region {region}"
    assert all(y2 - y1 == 1 for y1, y2 in zip(group, group[1:])), f"Missing years in region {region}"

# COMMAND ----------

database_name = "indicator_intermediate"

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

# COMMAND ----------

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.moz_subnational_population")
