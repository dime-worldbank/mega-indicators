# Databricks notebook source
import dlt
from pyspark.sql.types import StructType, StructField, StringType

# Display URLs for each indicator table, shown on the dashboard.
# indicator_key must match the DLT gold table name.
SOURCE_URLS = [
    {"indicator_key": "global_data_lab_hd_index", "source_url": "https://globaldatalab.org/shdi/"},
    {"indicator_key": "learning_poverty_rate", "source_url": "https://datacatalog.worldbank.org/search/dataset/0038947"},
    {"indicator_key": "subnational_poverty_rate", "source_url": "https://datacatalog.worldbank.org/search/dataset/0029053"},
    {"indicator_key": "universal_health_coverage_index_gho", "source_url": "https://www.who.int/data/gho/data/indicators/indicator-details/GHO/uhc-index-of-service-coverage"},
    {"indicator_key": "pefa_by_pillar", "source_url": "https://www.pefa.org/assessments/batch-downloads"},
]

# COMMAND ----------

@dlt.table(name='indicator_source_urls_bronze')
def indicator_source_urls_bronze():
    schema = StructType([
        StructField("indicator_key", StringType()),
        StructField("source_url", StringType()),
    ])
    return spark.createDataFrame(SOURCE_URLS, schema=schema)
