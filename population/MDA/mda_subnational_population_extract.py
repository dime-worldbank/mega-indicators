# Databricks notebook source
# MAGIC %run ../../utils

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

import os

VOLUME_PATH = f'{VOLUME_ROOT_PATH}/auxiliary_data/population/mda/'

# NBS population by locality: one cumulative workbook, a sheet per year. A new edition
# replaces it: update FILENAME here and NBS_FILENAME / LAST_YEAR in mda_subnational_population.py.
FILENAME = 'Populatia_sate_comune_orase_2014-2023.xlsx'
URL = f'https://statistica.gov.md/files/files/serii_de_timp/populatie/numarul_PRO_profil_teritorial/{FILENAME}'

EXCEL_CONTENT_TYPE = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

os.makedirs(VOLUME_PATH, exist_ok=True)

if os.path.exists(VOLUME_PATH + FILENAME):
    print(f"Skipping {FILENAME}, as it already exists")
else:
    resp = http_get(URL)
    resp.raise_for_status()
    content_type = resp.headers.get('content-type', '')
    if EXCEL_CONTENT_TYPE not in content_type:
        raise ValueError(f"Expected an xlsx, got {content_type}")
    with open(VOLUME_PATH + FILENAME, 'wb') as f:
        f.write(resp.content)
    print(f"✓ {FILENAME} ({len(resp.content)} bytes)")
