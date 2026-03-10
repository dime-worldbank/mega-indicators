# Databricks notebook source
import dlt
from pyspark.sql.types import StructType, StructField, StringType

# Source URLs for each indicator table.
# indicator_key must match the DLT gold table name.
# Descriptive metadata (title, description, source_name) now lives in the
# dashboard repo at chart level — this table only stores canonical source URLs.
_ROWS = [
    {"indicator_key": "global_data_lab_hd_index", "source_url": "https://globaldatalab.org/shdi/about/"},
    {"indicator_key": "learning_poverty_rate", "source_url": "https://data360.worldbank.org/en/indicator/WB_LPGD_SE_LPV_PRIM_SD"},
    {"indicator_key": "subnational_poverty_rate", "source_url": "https://datacatalog.worldbank.org/search/dataset/0064796/subnational-poverty-and-inequality-database-spid"},
    {"indicator_key": "universal_health_coverage_index_gho", "source_url": "https://www.who.int/data/gho/data/indicators/indicator-details/GHO/uhc-index-of-service-coverage"},
    {"indicator_key": "pefa_by_pillar", "source_url": "https://www.pefa.org/resources/pefa-2016-framework"},
    {"indicator_key": "health_private_expenditure", "source_url": "https://www.who.int/data/gho/data/indicators/indicator-details/GHO/out-of-pocket-expenditure-(oop)-per-capita-in-us"},
]

# COMMAND ----------

@dlt.table(name='indicator_source_urls_bronze')
def indicator_source_urls_bronze():
    schema = StructType([
        StructField("indicator_key", StringType()),
        StructField("source_url", StringType()),
    ])
    return spark.createDataFrame(_ROWS, schema=schema)
