
import requests
import pandas as pd
from io import BytesIO

spid_url = 'https://datacatalogfiles.worldbank.org/ddh-published/0064796/DR0092191/subnational-poverty-inequality-spid-poverty.xlsx?versionId=2023-09-11T14:24:15.5456758Z'
gsap_url = 'https://datacatalogfiles.worldbank.org/ddh-published/0042041/DR0052555/global-subnational-poverty-gsap-2019-data.xlsx?versionId=2023-09-11T14:26:49.3938437Z'

#
response = requests.get(spid_url)
if response.status_code == 200:
    df_SPID = pd.read_excel(BytesIO(response.content))
else:
    print(f"Failed to download the file. Status code: {response.status_code}")
    exit

response = requests.get(gsap_url)
if response.status_code == 200:
    df_GSAP = pd.read_excel(BytesIO(response.content))
else:
    print(f"Failed to download the file. Status code: {response.status_code}")
    exit


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
SPID_regions_2019 = [tuple(x) for x in df_SPID[df_SPID.year==2019][['code', 'year', 'sample']].drop_duplicates().values.tolist()]
df_GSAP_mod_missing = df_GSAP_mod[df_GSAP_mod.apply(lambda x: (x['code'], x['year'], x['sample']) not in SPID_regions_2019, axis=1)]

df_combined = pd.concat([df_SPID, df_GSAP_mod_missing], ignore_index=True, sort=False)
df_combined = df_combined.sort_values(['code', 'year', 'sample'])


sdf = spark.createDataFrame(df_combined)
sdf.write.mode("overwrite").saveAsTable("indicator.poverty_index_SPID_GSAP")