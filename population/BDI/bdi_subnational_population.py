# Databricks notebook source
# MAGIC %run ../subnational_population_extraction_from_census_gov

# COMMAND ----------

import io
from zipfile import ZipFile

import pandas as pd
import requests

COUNTRY_NAME = "Burundi"
COUNTRY_CODE = "BDI"
POPULATION_INDICATOR_CODE = "SP.POP.TOTL"
EXPECTED_ADM1_COUNT = 17

WB_SUBNATIONAL_POPULATION_URL = (
    "https://databankfiles.worldbank.org/public/ddpext_download/"
    "Subnational-Population_EXCEL.zip"
)
CENSUS_GOV_COUNTRY_FILENAME = "burundi"

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

# Census.gov population estimates from 2017 onward.
df_census_long = get_pop_from_census_gov(CENSUS_GOV_COUNTRY_FILENAME)
df_census_long = df_census_long[df_census_long["year"].between(2017, 2025)]

# Census.gov reports Rumonge separately. Recombine it with Bururi to preserve
# the World Bank database's historical 17-province geography.
df_census_long["adm1_name"] = df_census_long["adm1_name"].replace(
    {"Rumonge": "Bururi"}
)
df_census_long = (
    df_census_long.groupby(
        ["country_name", "adm1_name", "year", "data_source"],
        as_index=False,
    )["population"]
    .sum()
    .sort_values(["adm1_name", "year"])
)

# COMMAND ----------

assert set(df_wb_long["adm1_name"]) == set(df_census_long["adm1_name"])

df_pop = pd.concat([df_wb_long, df_census_long], ignore_index=True)
df_pop = df_pop[["country_name", "adm1_name", "year", "population", "data_source"]]
df_pop = df_pop.sort_values(["adm1_name", "year"]).reset_index(drop=True)
df_pop["year"] = df_pop["year"].astype(int)
df_pop["population"] = df_pop["population"].astype(int)

assert df_wb_long["year"].max() == 2016
assert df_census_long["year"].min() == 2017
assert df_pop.shape[0] == 442, f"Expected 442 rows, got {df_pop.shape[0]}"
assert df_pop["population"].notnull().all()
assert df_pop["adm1_name"].nunique() == EXPECTED_ADM1_COUNT
assert not df_pop.duplicated(["adm1_name", "year"]).any()

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS prd_mega.indicator_intermediate")
spark.createDataFrame(df_pop).write.mode("overwrite").saveAsTable(
    "prd_mega.indicator_intermediate.bdi_subnational_population"
)
spark.createDataFrame(df_pop).write.mode("overwrite").saveAsTable(
    "prd_mega.indicator_intermediate.bdi_subnational_population"
)
