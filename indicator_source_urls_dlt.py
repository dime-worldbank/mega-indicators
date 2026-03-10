# Databricks notebook source
import json
import dlt
from pyspark.sql.types import StructType, StructField, StringType

# Metadata for each indicator table, shown on the dashboard.
# indicator_key must match the DLT gold table name.
# The metadata field is a JSON string containing whichever fields are
# relevant for each indicator (e.g. source_url, title, description,
# source_name, license_type, license_url).  Only include fields that
# come from the official source — don't invent descriptions.
INDICATOR_METADATA = [
    {
        "indicator_key": "global_data_lab_hd_index",
        "metadata": {
            "source_url": "https://globaldatalab.org/shdi/about/",
            "title": "Subnational Human Development Index",
            "source_name": "Global Data Lab",
        },
    },
    {
        "indicator_key": "learning_poverty_rate",
        "metadata": {
            "source_url": "https://datacatalog.worldbank.org/search/dataset/0038947",
            "title": "Learning Poverty Rate",
            "source_name": "World Bank",
        },
    },
    {
        "indicator_key": "subnational_poverty_rate",
        "metadata": {
            "source_url": "https://datacatalog.worldbank.org/search/dataset/0029053",
            "title": "Subnational Poverty Rate",
            "source_name": "World Bank",
        },
    },
    {
        "indicator_key": "universal_health_coverage_index_gho",
        "metadata": {
            "source_url": "https://www.who.int/data/gho/data/indicators/indicator-details/GHO/uhc-index-of-service-coverage",
            "title": "Universal Health Coverage Index",
            "source_name": "World Health Organization (GHO)",
        },
    },
    {
        "indicator_key": "pefa_by_pillar",
        "metadata": {
            "source_url": "https://www.pefa.org/assessments/batch-downloads",
            "framework_url": "https://www.pefa.org/resources/pefa-2016-framework",
            "title": "PEFA Assessment",
            "source_name": "PEFA Secretariat",
            "description": (
                "PEFA assessments use letter grades (A to D, with + "
                "modifiers). For this dashboard, grades are converted to "
                "numerical scores (A=4, B+=3.5, B=3, C+=2.5, C=2, D+=1.5, "
                "D=1). Pillar scores are the arithmetic mean of their "
                "constituent indicators, and the overall score is the mean "
                "of all pillar scores. "
                "Data covers both the 2011 framework (28 indicators, 6 "
                "pillars) and the 2016 framework (31 indicators, 7 pillars). "
            ),
        },
    },
    {
        "indicator_key": "edu_private_expenditure",
        "metadata": {
            "title": "Private Education Expenditure",
            "source_name": "World Bank International Comparison Program (ICP)",
            "description": (
                "Derived as total education spending from the ICP "
                "minus BOOST public education expenditure. ICP data "
                "covers benchmark years (2005, 2011, 2017, 2021)."
            ),
        },
    },
    {
        "indicator_key": "health_private_expenditure",
        "metadata": {
            "source_url": "https://www.who.int/data/gho/data/indicators/indicator-details/GHO/out-of-pocket-expenditure-(oop)-per-capita-in-us",
            "title": "Out-of-Pocket Health Expenditure",
            "source_name": "WHO Global Health Expenditure Database",
            "description": (
                "Computed as CHE (current health expenditure in local "
                "currency, derived from CHE % of GDP * GDP) multiplied "
                "by OOP % of CHE, then adjusted for inflation using CPI. "
                "Source indicators: GHED_CHEGDP_SHA2011 and "
                "GHED_OOPSCHE_SHA2011."
            ),
        },
    },
]

# Serialize metadata dicts to JSON strings for the DataFrame.
_ROWS = [
    {"indicator_key": row["indicator_key"], "metadata": json.dumps(row["metadata"])}
    for row in INDICATOR_METADATA
]

# COMMAND ----------

@dlt.table(name='indicator_metadata_bronze')
def indicator_metadata_bronze():
    schema = StructType([
        StructField("indicator_key", StringType()),
        StructField("metadata", StringType()),
    ])
    return spark.createDataFrame(_ROWS, schema=schema)
