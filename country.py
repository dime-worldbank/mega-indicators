# Databricks notebook source
# MAGIC %pip install wbgapi

# COMMAND ----------

import pandas as pd
import wbgapi as wb

# COMMAND ----------

df = wb.economy.DataFrame()
df

# COMMAND ----------

COL_NAME_MAP = {
    "id": "country_code",
    "name": "country_name",
    "lendingType": "lending_type",
    "incomeLevel": "income_level",
    "capitalCity": "capital_city",
    "aggregate": "is_aggregate",
}
COL_NAMES = [
    "country_code",
    "country_name",
    "longitude",
    "latitude",
    "region",
    "lending_type",
    "income_level",
    "capital_city",
    "is_aggregate"
]
df_cleaned = df.reset_index().rename(columns=COL_NAME_MAP)[COL_NAMES]
df_cleaned

# COMMAND ----------

UC_CATALOG = "prd_mega"
SCHEMA = "indicator"
TABLE = "country"

sdf = spark.createDataFrame(df_cleaned)

# Write to UC
spark.sql(f"USE CATALOG {UC_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{SCHEMA}")
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{UC_CATALOG}.{SCHEMA}.{TABLE}"
)

# COMMAND ----------

# Write to hive_metastore
spark.sql(f"USE hive_metastore.{SCHEMA}")
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)
