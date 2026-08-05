# Databricks notebook source
# MAGIC %run ../subnational_population_extraction_from_census_gov

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

import io
from zipfile import ZipFile

import pandas as pd
import requests

COUNTRY_NAME = "Burundi"
COUNTRY_CODE = "BDI"
POPULATION_INDICATOR_CODE = "SP.POP.TOTL"
EXPECTED_ADM1_COUNT = 18
EXPECTED_ROW_COUNT = EXPECTED_ADM1_COUNT * 26

WB_SUBNATIONAL_POPULATION_URL = (
    "https://databankfiles.worldbank.org/public/ddpext_download/"
    "Subnational-Population_EXCEL.zip"
)
CENSUS_GOV_COUNTRY_FILENAME = "burundi"
BOUNDARY_REGION_NAME_FIXES = {
    "Bujumbura Mairie": "Mairie de Bujumbura",
    "Bujumbura Rural": "Bujumbura",
}

# COMMAND ----------

# World Bank subnational population data through 2016.
response = requests.get(WB_SUBNATIONAL_POPULATION_URL, timeout=300)
response.raise_for_status()

with ZipFile(io.BytesIO(response.content)) as wb_zip:
    wb_excel_files = [
        name for name in wb_zip.namelist() if name.lower().endswith((".xls", ".xlsx"))
    ]
    assert len(wb_excel_files) == 1, wb_excel_files
    with wb_zip.open(wb_excel_files[0]) as wb_excel:
        df_wb = pd.read_excel(wb_excel)

df_wb_bdi = df_wb[
    (df_wb["Country Code"].map(lambda value: str(value)[:3] == COUNTRY_CODE))
    & (df_wb["Indicator Code"] == POPULATION_INDICATOR_CODE)
].copy()
df_wb_bdi["adm1_name"] = df_wb_bdi["Country Name"].map(
    lambda value: value.split(",")[-1].strip()
)
df_wb_bdi = df_wb_bdi[df_wb_bdi["adm1_name"] != COUNTRY_NAME]

wb_year_cols = [
    col for col in df_wb_bdi.columns if str(col).isdigit() and 2000 <= int(col) <= 2016
]

df_wb_long = df_wb_bdi.melt(
    id_vars=["adm1_name"],
    value_vars=wb_year_cols,
    var_name="year",
    value_name="population",
)
df_wb_long["country_name"] = COUNTRY_NAME
df_wb_long["data_source"] = "World Bank Subnational Population Database"
df_wb_long["year"] = df_wb_long["year"].astype(int)

# COMMAND ----------

# Census.gov population estimates use the current 18-province geography for
# the full 2000-2025 series.
df_census_long = get_pop_from_census_gov(CENSUS_GOV_COUNTRY_FILENAME)

# Align spelling differences to the labels used by admin1_boundaries_gold.
df_wb_long["adm1_name"] = df_wb_long["adm1_name"].replace(
    BOUNDARY_REGION_NAME_FIXES
)
df_census_long["adm1_name"] = df_census_long["adm1_name"].replace(
    BOUNDARY_REGION_NAME_FIXES
)

# COMMAND ----------

# The World Bank series predates Rumonge province and reports its population
# within Bururi. Preserve the World Bank Bururi total for 2000-2016, but split
# it between Bururi and Rumonge using their Census.gov population shares. This
# provides a consistent 18-province series without changing the historical
# combined population total.
historic_census_bururi_rumonge = df_census_long[
    (df_census_long["year"].between(2000, 2016))
    & (df_census_long["adm1_name"].isin(["Bururi", "Rumonge"]))
].pivot(index="year", columns="adm1_name", values="population")

historic_census_bururi_rumonge["bururi_share"] = (
    historic_census_bururi_rumonge["Bururi"]
    / (
        historic_census_bururi_rumonge["Bururi"]
        + historic_census_bururi_rumonge["Rumonge"]
    )
)

wb_bururi = df_wb_long[df_wb_long["adm1_name"] == "Bururi"][
    ["country_name", "year", "population"]
].merge(
    historic_census_bururi_rumonge[["bururi_share"]],
    on="year",
    how="left",
    validate="one_to_one",
)
wb_bururi["bururi_population"] = (
    wb_bururi["population"] * wb_bururi["bururi_share"]
).round().astype(int)
wb_bururi["rumonge_population"] = (
    wb_bururi["population"] - wb_bururi["bururi_population"]
)

allocation_source = (
    "World Bank Subnational Population Database; Bururi/Rumonge split using "
    "Census.gov provincial shares"
)
historic_bururi = wb_bururi[["country_name", "year", "bururi_population"]].rename(
    columns={"bururi_population": "population"}
)
historic_bururi["adm1_name"] = "Bururi"
historic_bururi["data_source"] = allocation_source

historic_rumonge = wb_bururi[
    ["country_name", "year", "rumonge_population"]
].rename(columns={"rumonge_population": "population"})
historic_rumonge["adm1_name"] = "Rumonge"
historic_rumonge["data_source"] = allocation_source

df_wb_18 = pd.concat(
    [
        df_wb_long[df_wb_long["adm1_name"] != "Bururi"],
        historic_bururi,
        historic_rumonge,
    ],
    ignore_index=True,
)
df_census_recent = df_census_long[df_census_long["year"].between(2017, 2025)]

assert set(df_wb_18["adm1_name"]) == set(df_census_recent["adm1_name"])
assert (
    wb_bururi["bururi_population"] + wb_bururi["rumonge_population"]
).equals(wb_bururi["population"])

df_pop = pd.concat([df_wb_18, df_census_recent], ignore_index=True)
df_pop = df_pop[["country_name", "adm1_name", "year", "population", "data_source"]]
df_pop = df_pop.sort_values(["adm1_name", "year"]).reset_index(drop=True)
df_pop["year"] = df_pop["year"].astype(int)
df_pop["population"] = df_pop["population"].astype(int)

assert df_wb_long["year"].max() == 2016
assert df_census_recent["year"].min() == 2017
assert df_pop.shape[0] == EXPECTED_ROW_COUNT, (
    f"Expected {EXPECTED_ROW_COUNT} rows, got {df_pop.shape[0]}"
)
assert df_pop["population"].notnull().all()
assert df_pop["adm1_name"].nunique() == EXPECTED_ADM1_COUNT
assert set(df_pop.groupby("adm1_name").size()) == {26}
assert not df_pop.duplicated(["adm1_name", "year"]).any()
assert "Rumonge" in set(df_pop["adm1_name"])
assert "Bujumbura Mairie" not in set(df_pop["adm1_name"])
assert "Bujumbura Rural" not in set(df_pop["adm1_name"])

# COMMAND ----------

database_name = INDICATOR_SCHEMA

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(df_pop)
sdf.write.mode("overwrite").saveAsTable(
    f"{database_name}.bdi_subnational_population_silver"
)
