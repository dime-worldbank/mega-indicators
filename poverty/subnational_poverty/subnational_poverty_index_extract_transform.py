# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %run ../../config

# COMMAND ----------

# MAGIC %run ../../utils

# COMMAND ----------

import requests
import pandas as pd
import numpy as np
from io import BytesIO
import re

# COMMAND ----------

spid_resource_url = 'https://ddh-openapi.worldbank.org/resources/DR0092191'
response = requests.get(spid_resource_url)
response.raise_for_status()
spid_url = response.json()['distribution']['url']
# Prefer the mounted DDH volume; fall back to the URL (see ddh_bytes in utils).
df_SPID = pd.read_excel(BytesIO(ddh_bytes(spid_url)), sheet_name="Data")

df_SPID = df_SPID[df_SPID.data_group == 'ALL']
df_SPID

# COMMAND ----------

gsap_resource_url = 'https://ddh-openapi.worldbank.org/resources/DR0052555'
response = requests.get(gsap_resource_url)
response.raise_for_status()
gsap_url = response.json()['distribution']['url']
# expect the first sheet to be metadata, followed by the latest lineup data sheet
df_GSAP = pd.read_excel(BytesIO(ddh_bytes(gsap_url)), sheet_name=1) # use latest lineup
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

COL_NAMES = ['code', 'sample', 'year', 'survname', 'poor300', 'poor420', 'poor830']
df_SPID_without_gsap_year = df_SPID.loc[df_SPID.year != gsap_lineup_year]
df_SPID_to_merge = df_SPID_without_gsap_year[COL_NAMES]
df_SPID_to_merge

# COMMAND ----------

df_GSAP_renamed = df_GSAP.rename(columns=lambda x: re.sub('_ln', '', x)).rename(columns={'lineupyear': 'year'})
df_GSAP_to_merge = df_GSAP_renamed[COL_NAMES]
df_GSAP_to_merge

# COMMAND ----------

# Check there is no overlap
merged = pd.merge(df_GSAP_to_merge, df_SPID_to_merge, on=['code', 'sample', 'year'], how='outer')
assert merged.shape[0] == df_SPID_to_merge.shape[0] + df_GSAP.shape[0]

# COMMAND ----------

# Write each source to its own table (no data_source column — the source is the
# table's identity).
RENAME = {'code': 'country_code', 'sample': 'region_name'}
for df_source, table in [(df_SPID_to_merge, 'poverty_rate_spid_silver'),
                         (df_GSAP_to_merge, 'poverty_rate_gsap_silver')]:
    out = (df_source.rename(columns=RENAME)
           .sort_values(['country_code', 'year', 'region_name']))
    (spark.createDataFrame(out)
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{INDICATOR_SCHEMA}.{table}"))
