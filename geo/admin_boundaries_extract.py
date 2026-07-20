# Databricks notebook source
import shutil

# The WB DDH published URL 404s; the GeoJSON is served from the DDH volume, so copy it
# into the auxiliary_data volume the boundary DLT reads.
SOURCE = '/Volumes/prd_development_data/files/ddh/0038272/DR0095369/World Bank Official Boundaries (GeoJSON)/World Bank Official Boundaries - Admin 1.geojson'
DATA_DIR = '/Volumes/prd_mega/sboost4/vboost4/Workspace/auxiliary_data/admin1geoboundaries'
WB_ADM1_GEO_FILENAME = f'{DATA_DIR}/World Bank Official Boundaries - Admin 1.geojson'

shutil.copyfile(SOURCE, WB_ADM1_GEO_FILENAME)
print(f"Copied '{SOURCE}' to '{WB_ADM1_GEO_FILENAME}'")
