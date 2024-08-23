# Databricks notebook source
import pandas as pd
from pathlib import Path

# COMMAND ----------

# Bank firewall prenvents programmatic data fetch: https://ember-climate.org/data/api/
# Curretnly requesting ITS to whitelist this request

file_path = 'https://raw.githubusercontent.com/weilu/mega-indicators/main/energy/2023-Country-Transition-Tracker-Consolidated-Format.csv'
raw_df = pd.read_csv(file_path)
country_df = (
    spark.table(f"indicator.country")
    .select("country_name", "country_code", "region")
    .toPandas()
)
energy_df = raw_df.merge(country_df, left_on="Country code", right_on="country_code")

# COMMAND ----------

# These areas were excluded by the merge. But none of them were recognized country

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

energy_df = energy_df.loc[
    (energy_df["Category"] == "Electricity generation") & (energy_df["Unit"] == "%")
]

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
quality_check = (
    energy_df.groupby(["country_name", "Year"]).sum("Variable").reset_index()
)
quality_check[(quality_check.Value < 99) | (quality_check.Value > 101)]

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
    f"indicator.energy_consumption"
)
