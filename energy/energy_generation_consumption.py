# Databricks notebook source
import pandas as pd
from pathlib import Path
import requests

# COMMAND ----------

# Load dataset from the API
my_api_key = dbutils.secrets.get(scope="DIMEBOOSTKEYVAULT", key="ember_energy_key")
base_url = "https://api.ember-climate.org"
query_url = (
    f"{base_url}/v1/electricity-generation/yearly"
    + f"?is_aggregate_series=false&api_key={my_api_key}"
)

response = requests.get(query_url)

if response.status_code == 200:
    data = response.json()
raw_df = pd.DataFrame(data["data"])

# COMMAND ----------

# These areas were excluded by the merge. But none of them were recognized country
country_df = (
    spark.table(f"indicator.country")
    .select("country_name", "country_code", "region")
    .toPandas()
)
energy_df = raw_df.merge(country_df, left_on="entity_code", right_on="country_code")

emmited_areas = [
    country
    for country in raw_df["entity"].unique()
    if country not in energy_df["entity"].unique()
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

energy_df = energy_df[energy_df["series"].isin(PRIMARY_TYPES)]
energy_df["secondary_fuel_type"] = energy_df["series"].apply(
    categorize_fuel, type_dict=SECONDARY_TYPES
)
energy_df["tertialy_fuel_type"] = energy_df["series"].apply(
    categorize_fuel, type_dict=TERTIALLY_TYPES
)
energy_df["quaternary_fuel_type"] = energy_df["series"].apply(
    categorize_fuel, type_dict=QUATERNARY_TYPES
)

# COMMAND ----------

# Check for the countries whose share of the primary type fuel do not sum up to 100
quality_check = energy_df.groupby(["entity", "date"]).sum("series").reset_index()

# Note that Other Fossil dataset is missing, causing the quality check to fail for many countries
quality_check[
    (quality_check.share_of_generation_pct < 99)
    | (quality_check.share_of_generation_pct > 101)
]

# COMMAND ----------

energy_df = energy_df[
    [
        "country_name",
        "country_code",
        "region",
        "date",
        "share_of_generation_pct",
        "series",
        "secondary_fuel_type",
        "tertialy_fuel_type",
        "quaternary_fuel_type",
    ]
]
energy_df.rename(
    columns={"date": "year", "series": "primary_fuel_type"},
    inplace=True,
)

# COMMAND ----------

sdf = spark.createDataFrame(energy_df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"indicator.energy_generation"
)
