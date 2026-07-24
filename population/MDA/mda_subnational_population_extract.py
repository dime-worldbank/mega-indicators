# Databricks notebook source
# MAGIC %run ../../utils

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

import os
import requests

VOLUME_PATH = f'{VOLUME_ROOT_PATH}/auxiliary_data/population/mda/'

# Population with usual residence by locality (village/commune, town/municipality),
# published by the National Bureau of Statistics of the Republic of Moldova.
# One sheet per year. When the NBS publishes a workbook covering later years, add
# its URL here and update EXPECTED_YEARS in mda_subnational_population.py.
URLS = {
    'Populatia_sate_comune_orase_2014-2023.xlsx': 'https://statistica.gov.md/files/files/serii_de_timp/populatie/numarul_PRO_profil_teritorial/Populatia_sate_comune_orase_2014-2023.xlsx',
}

EXCEL_CONTENT_TYPE = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

os.makedirs(VOLUME_PATH, exist_ok=True)

for filename, url in URLS.items():
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    content_type = resp.headers.get('content-type', '')
    if EXCEL_CONTENT_TYPE not in content_type:
        raise ValueError(f"Expected an xlsx, got {content_type}")
    with open(VOLUME_PATH + filename, 'wb') as f:
        f.write(resp.content)
    print(f"✓ {filename} ({len(resp.content)} bytes)")
