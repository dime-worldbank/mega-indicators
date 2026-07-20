# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

URL = 'https://datacatalogfiles.worldbank.org/ddh-published/0038272/DR0095369/World%20Bank%20Official%20Boundaries%20(GeoJSON)/World%20Bank%20Official%20Boundaries%20-%20Admin%201.geojson'
DATA_DIR = '/Volumes/prd_mega/sboost4/vboost4/Workspace/auxiliary_data/admin1geoboundaries'
WB_ADM1_GEO_FILENAME = f'{DATA_DIR}/World Bank Official Boundaries - Admin 1.geojson'

# Prefer the mounted DDH volume; fall back to the URL (see ddh_bytes in utils).
with open(WB_ADM1_GEO_FILENAME, 'wb') as f:
    f.write(ddh_bytes(URL))
print(f"Wrote '{WB_ADM1_GEO_FILENAME}'")
