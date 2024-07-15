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

raw_df = raw_df.rename(columns={"Country": "country_name", "Year": "year"})

# Normalizing to WB economy names
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
    raw_df.loc[raw_df.country_name == old_name, 'country_name'] = new_name

# COMMAND ----------

df_renewable = raw_df[raw_df.Variable == 'Primary Renewable Energy Consumption']
df_renewable = df_renewable.drop(columns=["Unit", "Variable"])\
    .rename(columns={"Value": "primary_renewable_consumption_share"})

df_total = raw_df[raw_df.Variable == 'Primary Energy Consumption per Capita']
df_total = df_total.drop(columns=["Unit", "Variable"])\
    .rename(columns={"Value": "primary_consumption_gigajoules_per_capita"})

df = pd.merge(df_renewable, df_total, on=['country_name', 'year'], how='left')
df

# COMMAND ----------

country_df = spark.table(f'indicator.country').select('country_name', 'country_code', 'region').toPandas()
country_df

# COMMAND ----------

df_merged = pd.merge(df, country_df, on="country_name")
df_merged['data_source'] = 'Energy Institute'
df_merged

# COMMAND ----------

sdf = spark.createDataFrame(df_merged)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"indicator.energy_consumption")

# COMMAND ----------

df_mix = raw_df[raw_df.Unit == 'Share of Generation Mix']
df_mix = df_mix.drop(columns=['Unit'])\
    .rename(columns={"Variable": "energy_source", "Value": "generation_mix_share"})
df_mix

# COMMAND ----------

gen_df_merged = pd.merge(df_mix, country_df, on="country_name")
gen_df_merged['data_source'] = 'Energy Institute'
gen_df_merged

# COMMAND ----------

sdf = spark.createDataFrame(gen_df_merged)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"indicator.energy_generation")
