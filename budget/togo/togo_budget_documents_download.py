# Databricks notebook source
import os
import requests

VOLUME_PATH = '/Volumes/prd_mega/sboost4/vboost4/Workspace/auxiliary_data/buget/togo/'

# Year → DGBFTG budget execution report URL. Filenames are constructed from the
# year so togo_revenue.extract_year() can recover it deterministically; do not
# rely on the server's Content-Disposition header.
URLS = {
    2024: 'https://dgbftg.org/index.php/documentation/category-layout/category-style2?task=download.send&id=138&catid=12&m=0',
    2023: 'https://dgbftg.org/index.php/documentation/category-layout/category-style2?task=download.send&id=118&catid=12&m=0',
    2022: 'https://dgbftg.org/index.php/documentation/category-layout/category-style2?task=download.send&id=99&catid=12&m=0',
}

os.makedirs(VOLUME_PATH, exist_ok=True)

for year, url in URLS.items():
    filename = f'togo_budget_{year}.pdf'
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        with open(VOLUME_PATH + filename, 'wb') as f:
            f.write(resp.content)
        print(f"✓ {filename} ({len(resp.content)} bytes)")
    except Exception as e:
        print(f"✗ {filename}: {e}")
