# Databricks notebook source
# MAGIC %pip install wbgapi
# MAGIC %pip install shapely

# COMMAND ----------

import pandas as pd
import wbgapi as wb
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, DoubleType
from shapely.geometry import shape, MultiPolygon, Polygon
import json

# COMMAND ----------

df = wb.economy.DataFrame()

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

# COMMAND ----------

countries = spark.createDataFrame(df_cleaned)

# COMMAND ----------

# add display_lat, display_lon and zoom to the coutnries table
zoom = {
    "Albania": 5.7,
    "Bangladesh": 5,
    "Bhutan": 6,
    "Burkina Faso": 4.7,
    "Colombia": 3.6,
    "Kenya": 4.35,
    "Mozambique": 3.35,
    "Nigeria": 4.2,
    "Pakistan": 3.7,
    "Paraguay": 4.4,
    "Tunisia": 4.5,
    "Chile" : 2.0,
    "Liberia": 5.5,
    "Togo": 5.0
}
def get_zoom(country):
    return float(zoom.get(country, 3.0))  # TODO: replace this dict by a function that can compute this from the boundaries
zoom_udf = udf(get_zoom, DoubleType())

def compute_country_centroid(boundaries_list):
    polygons = []
    for boundary_str in boundaries_list:
        boundary_json = json.loads(boundary_str)
        geom = shape(boundary_json)
        
        if isinstance(geom, Polygon):
            polygons.append(geom)
        elif isinstance(geom, MultiPolygon):
            polygons.extend(geom.geoms)

    if len(polygons) > 1:
        multi_polygon = MultiPolygon(polygons)
    else:
        multi_polygon = polygons[0]

    centroid = multi_polygon.centroid
    return (centroid.x, centroid.y) 

schema = StructType([
    StructField("display_lon", DoubleType(), False),
    StructField("display_lat", DoubleType(), False)
])

centroid_udf = F.udf(compute_country_centroid, schema)

admin1_boundaries = spark.table('indicator.admin1_boundaries_gold')
grouped_df = admin1_boundaries.groupBy("country_name").agg(F.collect_list("boundary").alias("all_boundaries"))

centroid_df = grouped_df.withColumn("centroid", centroid_udf(F.col("all_boundaries"))) \
                        .select(F.col("country_name"), F.col("centroid.display_lon"),  F.col("centroid.display_lat"))

sdf = countries.join(centroid_df, on="country_name", how="left"
            ).withColumn("zoom", zoom_udf(F.col("country_name")))

# COMMAND ----------

# --- Write to Catalog ---
CATALOG = "prd_mega"
SCHEMA = "indicator"
TABLE = "country"
spark.sql(f"USE {CATALOG}.{SCHEMA}")
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)
