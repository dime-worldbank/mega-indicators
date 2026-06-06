# Databricks notebook source
# MAGIC %run ../../utils

# COMMAND ----------

import os
import requests

VOLUME_ROOT_PATH = get_volume_root_path()
VOLUME_PATH = f'{VOLUME_ROOT_PATH}/auxiliary_data/official_finance_reports/togo/'

# Year → DGBFTG budget execution report URL. Filenames are constructed from the
# year so togo_revenue.extract_year() can recover it deterministically; do not
# rely on the server's Content-Disposition header.
URLS = {
    2024: 'https://togoreformes.gouv.tg/documents/download/609',
    2023: 'https://togoreformes.gouv.tg/documents/download/3',
    2022: 'https://togoreformes.gouv.tg/documents/download/449',
    2021: 'https://togoreformes.gouv.tg/documents/download/360',
    2020: 'https://togoreformes.gouv.tg/documents/download/320',
}

os.makedirs(VOLUME_PATH, exist_ok=True)

for year, url in URLS.items():
    filename = f'togo_budget_{year}.pdf'
    if os.path.exists(VOLUME_PATH + filename):
        print(f"Skipping {filename}, as it already exists")
        continue
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    if 'application/pdf' not in resp.headers.get('content-type', ''):
        raise ValueError(f"Expected PDF, got {resp.headers.get('content-type')}")
    with open(VOLUME_PATH + filename, 'wb') as f:
        f.write(resp.content)
    print(f"✓ {filename} ({len(resp.content)} bytes)")
