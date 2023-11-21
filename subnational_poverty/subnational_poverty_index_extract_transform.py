# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

import requests
import pandas as pd
import numpy as np
from io import BytesIO
import re

# COMMAND ----------

spid_url = 'https://datacatalogfiles.worldbank.org/ddh-published/0064796/DR0092191/subnational-poverty-inequality-spid-poverty.xlsx?versionId=2023-09-11T14:24:15.5456758Z'
response = requests.get(spid_url)
if response.status_code == 200:
    df_SPID = pd.read_excel(BytesIO(response.content))
else:
    print(f"Failed to download the file. Status code: {response.status_code}")
    exit
df_SPID

# COMMAND ----------

# Fix admlevel typo
df_SPID.loc[df_SPID.admlevel == 'AMD1', 'admlevel'] = 'ADM1'
df_SPID.admlevel.unique()

# COMMAND ----------

gsap_url = 'https://datacatalogfiles.worldbank.org/ddh-published/0042041/DR0052555/global-subnational-poverty-gsap-2019-data.xlsx?versionId=2023-09-11T14:26:49.3938437Z'
response = requests.get(gsap_url)
if response.status_code == 200:
    df_GSAP = pd.read_excel(BytesIO(response.content))
else:
    print(f"Failed to download the file. Status code: {response.status_code}")
    exit
df_GSAP

# COMMAND ----------

# TODO: normalize and populate admlevel for GSAP records
# TODO: normalize welfaretype
# df_GSAP.level.unique()
# df_GSAP_w_admlevel = pd.merge(df_GSAP, df_SPID[['code','sample','admlevel']].drop_duplicates(), on=['code', 'sample'], how='left')
# df_GSAP_w_admlevel[df_GSAP_w_admlevel.admlevel.isnull()]

# COMMAND ----------

overlap = pd.merge(df_GSAP, df_SPID, left_on=['code', 'sample', 'lineupyear'], right_on=['code', 'sample', 'year'], how='inner')
overlap

# COMMAND ----------

unique_overlap_years = overlap.lineupyear.unique()
assert len(unique_overlap_years) == 1, f'expect there to be only 1 overlap year between GSAP and SPID, got {len(unique_overlap_years)}'
assert unique_overlap_years[0] == 2019, f'expect the only overlap year between GSAP and SPID to be 2019, got {unique_overlap_years[0]}'

# COMMAND ----------

inconsistent = overlap[~np.isclose(overlap.poor215, overlap.poor215_ln, atol=0.01, rtol=0.05, equal_nan=True)][['code', 'sample', 'poor215', 'poor215_ln']]
inconsistent['deviation'] = abs((overlap.poor215 - overlap.poor215_ln)/overlap.poor215)
# TODO: figure out what's up with these big diviations
inconsistent.sort_values(['deviation'], ascending=False)

# COMMAND ----------

COL_NAMES = ['code', 'sample', 'year', 'survname', 'poor215', 'poor365', 'poor685', 'data_source']
df_SPID_no_2019 = df_SPID.loc[df_SPID.year != 2019]
df_SPID_no_2019['data_source'] = 'SPID'
df_SPID_to_merge = df_SPID_no_2019[COL_NAMES]
df_SPID_to_merge

# COMMAND ----------

df_GSAP_renamed = df_GSAP.rename(columns=lambda x: re.sub('_ln', '', x)).rename(columns={'lineupyear': 'year'})
df_GSAP_renamed['data_source'] = 'GSAP'
df_GSAP_to_merge = df_GSAP_renamed[COL_NAMES]
df_GSAP_to_merge

# COMMAND ----------

# Check there is no overlap
merged = pd.merge(df_GSAP_to_merge, df_SPID_no_2019, on=['code', 'sample', 'year'], how='outer')
assert merged.shape[0] == df_SPID_no_2019.shape[0] + df_GSAP.shape[0]

# COMMAND ----------

df_combined = pd.concat([df_SPID_to_merge, df_GSAP_to_merge], ignore_index=True, sort=False)
df_combined.sort_values(['code', 'year', 'sample'], inplace=True)
df_combined.rename(columns={'code': 'country_code', 'sample': 'region_name'}, inplace=True)
df_combined

# COMMAND ----------

sdf = spark.createDataFrame(df_combined)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("indicator_intermediate.poverty_index_SPID_GSAP")
