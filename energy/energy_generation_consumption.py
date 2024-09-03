# Databricks notebook source
import pandas as pd
from pathlib import Path

# COMMAND ----------

import requests

my_api_key = dbutils.secrets.get(scope="DIMEBOOSTKEYVAULT", key="ember_energy_key")
base_url = "https://api.ember-climate.org"
query_url = (
    f"{base_url}/v1/electricity-generation/yearly"
    + f"?is_aggregate_series=false&api_key={my_api_key}"
)

response = requests.get(query_url)

if response.status_code == 200:
    data = response.json()

# The renaming is necessary to preserve the consistency with the published energy data https://ember-climate.org/data-catalogue/yearly-electricity-data/
raw_df = pd.DataFrame(data["data"]).rename(
    columns={
        "date": "Year",
        "entity_code": "Country code",
        "series": "Variable",
        "entity": "Area",
        "share_of_generation_pct": "Value"
    },
)

# COMMAND ----------

# Commented out the following code which loads the dataset from our static dataset
# file_path ='https://raw.githubusercontent.com/weilu/mega-indicators/main/energy/2023-Country-Transition-Tracker-Consolidated-Format.csv'
# raw_df = pd.read_csv(file_path)
# raw_df = raw_df.loc[
#     (raw_df["Category"] == "Electricity generation") & (raw_df["Unit"] == "%")
# ]

# COMMAND ----------

# These areas were excluded by the merge. But none of them were recognized country
country_df = (
    spark.table(f"indicator.country")
    .select("country_name", "country_code", "region")
    .toPandas()
)
energy_df = raw_df.merge(country_df, left_on="Country code", right_on="country_code")

emmited_areas = [
    country
    for country in raw_df["Area"].unique()
    if country not in energy_df["Area"].unique()
]
emmited_areas

# COMMAND ----------

# Define the fuel types based on the data methodology documentation https://ember-climate.org/data-catalogue/yearly-electricity-data/|
PRIMARY_TYPES = [
    "Hydro",
    "Bioenergy",
    "Coal",
    "Gas",
    "Nuclear",
    "Other Fossil",
    "Other Renewables",
    "Solar",
    "Wind",
]
SECONDARY_TYPES = {
    "Wind and Solar": ["Solar", "Wind"],
    "Hydro, Bioenergy and Other Renewables": ["Hydro", "Bioenergy", "Other Renewables"],
    "Gas and Other Fossil": ["Gas", "Other Fossil"],
}
TERTIALLY_TYPES = {
    "Renewables": ["Solar", "Wind", "Hydro", "Bioenergy", "Other Renewables"],
    "Fossil": ["Gas", "Other Fossil", "Coal"],
}
QUATERNARY_TYPES = {
    "Clean": ["Solar", "Wind", "Hydro", "Bioenergy", "Other Renewables", "Nuclear"]
}


def categorize_fuel(row, type_dict):
    for key in type_dict:
        if row in type_dict[key]:
            return key

# COMMAND ----------

energy_df = energy_df[energy_df["Variable"].isin(PRIMARY_TYPES)]
energy_df["secondary_fuel_type"] = energy_df["Variable"].apply(
    categorize_fuel, type_dict=SECONDARY_TYPES
)
energy_df["tertialy_fuel_type"] = energy_df["Variable"].apply(
    categorize_fuel, type_dict=TERTIALLY_TYPES
)
energy_df["quaternary_fuel_type"] = energy_df["Variable"].apply(
    categorize_fuel, type_dict=QUATERNARY_TYPES
)

# COMMAND ----------

# Check for the countries whose share of the primary type fuel do not sum up to 100
quality_check = energy_df.groupby(["Area", "Year"]).sum("Variable").reset_index()

# Note that Other Fossil dataset is missing, causing the quality check to fail for many countries
quality_check[
    (quality_check.Value < 99)
    | (quality_check.Value > 101)
]

# COMMAND ----------

energy_df = energy_df[
    [
        "country_name",
        "country_code",
        "region",
        "Year",
        "Value",
        "Variable",
        "secondary_fuel_type",
        "tertialy_fuel_type",
        "quaternary_fuel_type",
    ]
]
energy_df.rename(
    columns={"Year": "year", "Variable": "primary_fuel_type", "Value": "share"},
    inplace=True,
)

# COMMAND ----------

energy_df

# COMMAND ----------

sdf = spark.createDataFrame(energy_df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"indicator.energy_generation"
)
