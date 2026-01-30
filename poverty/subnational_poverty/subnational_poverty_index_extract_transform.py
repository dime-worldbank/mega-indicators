# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

import requests
import pandas as pd
import numpy as np
from io import BytesIO
import re

# COMMAND ----------

spid_resource_url = 'https://ddh-openapi.worldbank.org/resources/DR0092191'
response = requests.get(spid_resource_url)
if response.status_code == 200:
    resource_metadata = response.json()
    spid_url = resource_metadata['distribution']['url']
    print(f"Downloading SPID data from: {spid_url}")
    excel_response = requests.get(spid_url)
    if excel_response.status_code == 200:
        df_SPID = pd.read_excel(BytesIO(excel_response.content), sheet_name="Data")
    else:
        print(f"Failed to download the Excel file. Status code: {excel_response.status_code}")
        exit()
else:
    print(f"Failed to fetch resource metadata. Status code: {response.status_code}")
    exit()

df_SPID = df_SPID[df_SPID.data_group == 'ALL']
df_SPID

# COMMAND ----------

gsap_resource_url = 'https://ddh-openapi.worldbank.org/resources/DR0052555'
response = requests.get(gsap_resource_url)
if response.status_code == 200:
    resource_metadata = response.json()
    gsap_url = resource_metadata['distribution']['url']
    print(f"Downloading GSAP data from: {gsap_url}")
    excel_response = requests.get(gsap_url)
    if excel_response.status_code == 200:
        # expect the first sheet to be metadata, followed by the latest lineup data sheet
        df_GSAP = pd.read_excel(BytesIO(excel_response.content), sheet_name=1) # use latest lineup
    else:
        print(f"Failed to download the Excel file. Status code: {excel_response.status_code}")
        exit()
else:
    print(f"Failed to fetch resource metadata. Status code: {response.status_code}")
    exit()
df_GSAP

# COMMAND ----------

overlap = pd.merge(df_GSAP, df_SPID, left_on=['code', 'sample', 'lineupyear'], right_on=['code', 'sample', 'year'], how='inner')
overlap

# COMMAND ----------

unique_overlap_years = overlap.lineupyear.unique()
assert len(unique_overlap_years) == 1, f'expect there to be only 1 overlap year between GSAP and SPID, got {len(unique_overlap_years)}'

gsap_lineup_year = df_GSAP.lineupyear.max()
assert unique_overlap_years[0] == gsap_lineup_year, f'expect the only overlap year between GSAP and SPID to be {gsap_lineup_year}, got {unique_overlap_years[0]}'

# COMMAND ----------

inconsistent = overlap[~np.isclose(overlap.poor300, overlap.poor300_ln, atol=0.01, rtol=0.05, equal_nan=True)][['code', 'sample', 'poor300', 'poor300_ln']]
inconsistent['deviation'] = abs((overlap.poor300 - overlap.poor300_ln)/overlap.poor300)
# TODO: figure out what's up with these big diviations
inconsistent.sort_values(['deviation'], ascending=False)

# COMMAND ----------

COL_NAMES = ['code', 'sample', 'year', 'survname', 'poor300', 'poor420', 'poor830', 'data_source']
df_SPID_without_gsap_year = df_SPID.loc[df_SPID.year != gsap_lineup_year]
df_SPID_without_gsap_year['data_source'] = 'SPID'
df_SPID_to_merge = df_SPID_without_gsap_year[COL_NAMES]
df_SPID_to_merge

# COMMAND ----------

df_GSAP_renamed = df_GSAP.rename(columns=lambda x: re.sub('_ln', '', x)).rename(columns={'lineupyear': 'year'})
df_GSAP_renamed['data_source'] = 'GSAP'
df_GSAP_to_merge = df_GSAP_renamed[COL_NAMES]
df_GSAP_to_merge

# COMMAND ----------

# Check there is no overlap
merged = pd.merge(df_GSAP_to_merge, df_SPID_to_merge, on=['code', 'sample', 'year'], how='outer')
assert merged.shape[0] == df_SPID_to_merge.shape[0] + df_GSAP.shape[0]

# COMMAND ----------

df_combined = pd.concat([df_SPID_to_merge, df_GSAP_to_merge], ignore_index=True, sort=False)
df_combined.sort_values(['code', 'year', 'sample'], inplace=True)
df_combined.rename(columns={'code': 'country_code', 'sample': 'region_name'}, inplace=True)
df_combined

# COMMAND ----------

sdf = spark.createDataFrame(df_combined)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("prd_mega.indicator_intermediate.poverty_rate_SPID_GSAP")
