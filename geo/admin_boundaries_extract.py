# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

# The World Bank Official Boundaries GeoJSON files admin_boundaries_transform_load.py
# reads: Admin 1 (regions) and the Admin 0 "all layers" file (which carries the
# disputed areas). Prefer the mounted DDH volume; fall back to the URL (see ddh_bytes in utils).
DDH_FOLDER = 'https://datacatalogfiles.worldbank.org/ddh-published/0038272/DR0095369/World%20Bank%20Official%20Boundaries%20(GeoJSON)'
FILES = {
    f'{DDH_FOLDER}/World%20Bank%20Official%20Boundaries%20-%20Admin%201.geojson':
        f'{VOLUME_ROOT_PATH}/auxiliary_data/admin1geoboundaries/World Bank Official Boundaries - Admin 1.geojson',
    f'{DDH_FOLDER}/World%20Bank%20Official%20Boundaries%20-%20Admin%200_all_layers.geojson':
        f'{VOLUME_ROOT_PATH}/auxiliary_data/admin0geoboundaries/World Bank Official Boundaries - Admin 0_all_layers.geojson',
}

for url, path in FILES.items():
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as f:
        f.write(ddh_bytes(url))
    print(f"Wrote '{path}'")
