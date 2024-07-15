# Databricks notebook source
import pandas as pd
from pathlib import Path

# COMMAND ----------

# Source prevents programmatic data fetch: "https://www.energyinst.org/__data/assets/file/0015/1421601/2023-Country-Transition-Tracker-Consolidated-Format.csv"
current_dir = Path().resolve()
file_path = current_dir / '2023-Country-Transition-Tracker-Consolidated-Format.csv'
raw_df = pd.read_csv(file_path)
raw_df

# COMMAND ----------

df = raw_df[raw_df.Variable == 'Primary Renewable Energy Consumption']
df = df.drop(columns=["Unit", "Variable"])\
    .rename(columns={"Value": "primary_renewable_consumption_share"})

df_total = raw_df[raw_df.Variable == 'Primary Energy Consumption per Capita']
df_total = df_total.drop(columns=["Unit", "Variable"])\
    .rename(columns={"Value": "primary_consumption_gigajoules_per_capita"})

df = pd.merge(df, df_total, on=['Country', 'Year'], how='left')
df = df.rename(columns={"Country": "country_name", "Year": "year"})
df

# COMMAND ----------

name_map = {
    'China Hong Kong SAR': 'Hong Kong SAR, China',
    "Czech Republic": "Czechia",
    "Egypt": "Egypt, Arab Rep.",
    "Iran": "Iran, Islamic Rep.",
    "Slovakia": "Slovak Republic",
    "South Korea": "Korea, Rep.",
    "Trinidad & Tobago": "Trinidad and Tobago",
    "Turkey": "Turkiye",
    "US": "United States",
    "Venezuela": "Venezuela, RB",
    "Vietnam": "Viet Nam",
}
for old_name, new_name in name_map.items():
    df.loc[df.country_name == old_name, 'country_name'] = new_name

# COMMAND ----------

country_df = spark.table(f'indicator.country').select('country_name', 'country_code', 'region').toPandas()
country_df

# COMMAND ----------

df_merged = pd.merge(df, country_df, on="country_name")
df_merged['data_source'] = 'Energy Institute'
df_merged

# COMMAND ----------

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"indicator.energy_consumption")
