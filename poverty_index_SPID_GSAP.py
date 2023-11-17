# Databricks notebook source
import requests
import pandas as pd

SPID_id, GSAP_id = 'DR0092191', 'DR0052555'

SPID_url = f'https://datacatalogapi.worldbank.org/ddhxext/ResourceFileData?resource_unique_id={SPID_id}&rowLimit=100000'
GSAP_url = f'https://datacatalogapi.worldbank.org/ddhxext/ResourceFileData?resource_unique_id={GSAP_id}&rowLimit=100000'

SPID_resp = requests.get(SPID_url, timeout=30)
df_SPID = pd.DataFrame(SPID_resp.json()['Details'])
assert df_SPID.code.nunique() > 140
assert df_SPID.pip_reg.nunique() > 6

GSAP_resp = requests.get(GSAP_url, timeout=30)
df_GSAP = pd.DataFrame(GSAP_resp.json()['Details'])
assert df_GSAP.code.nunique()>140
assert df_GSAP.region.nunique()>6

GSAP_2_SPID_cols = {
    'region':'pip_reg',
    'code':'code',
    'lineupyear':'year',
    'survname':'survname',
    'level':'byvar',
    'sample':'sample',
    'vintage':'vintage',
    'geo_code2':'geo_code2',
    'geo_code2_new':'geo_code2_new',
    'poor215_ln':'poor215',
    'poor365_ln':'poor365',
    'poor685_ln':'poor685'
}

df_GSAP_mod = df_GSAP[GSAP_2_SPID_cols.keys()]
df_GSAP_mod.columns = [GSAP_2_SPID_cols[x] for x in df_GSAP_mod.columns]
SPID_regions_2019 = [tuple(x) for x in df_SPID[df_SPID.year=='2019'][['code', 'year', 'sample']].drop_duplicates().values.tolist()]
df_GSAP_mod_missing = df_GSAP_mod[df_GSAP_mod.apply(lambda x: (x['code'], x['year'], x['sample']) not in SPID_regions_2019, axis=1)]
df_GSAP_mod_missing['SPID_GSAP'] = ['GSAP']*df_GSAP_mod_missing.shape[0]
df_SPID['SPID_GSAP'] = ['SPID']*df_SPID.shape[0]

df_combined = pd.concat([df_SPID, df_GSAP_mod_missing], ignore_index=True, sort=False)
df_combined = df_combined.sort_values(['code', 'year', 'sample'])

sdf = spark.createDataFrame(df_combined)
sdf.write.mode("overwrite").saveAsTable("indicator.poverty_index_SPID_GSAP")
