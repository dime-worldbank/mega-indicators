# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

# Subnational human development indices and school attendance from Global Data Lab, for
# every country, one request per dataset and year, as the R notebook made through the
# gdldata package: <base>/<dataset>/download/<year>/<indicator+indicator>/?format=csv&token=...
# Plain pandas; it replaced the R notebook. The token is the GDL_API_TOKEN secret
# (DIMEBOOSTKEYVAULT) on Databricks and the GDL_API_TOKEN environment variable otherwise.
import io
import os
from datetime import date

import pandas as pd
import requests

GDL_BASEURL = 'https://globaldatalab.org'
START_YEAR = 1990
END_YEAR = date.today().year
DATASETS = {
    'shdi': ['healthindex', 'edindex', 'incindex'],
    'education': ['lprimary', 'uprimary', 'lsecondary', 'usecondary'],  # attendance by school level
}
INDICATORS = [i for inds in DATASETS.values() for i in inds]

if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
    token = dbutils.secrets.get(scope='DIMEBOOSTKEYVAULT', key='GDL_API_TOKEN')
else:
    token = os.environ['GDL_API_TOKEN']


def gdl_download(dataset, indicators, year):
    url = f"{GDL_BASEURL}/{dataset}/download/{year}/{'+'.join(indicators)}/"
    resp = http_get(url, params={'format': 'csv', 'token': token, 'interpolation': 1},
                        headers={'Accept': 'text/csv'}, timeout=DEFAULT_TIMEOUT_SECONDS)
    resp.raise_for_status()
    if resp.text.lstrip().startswith('<'):  # errors (bad token, exhausted quota) come back as an HTML page
        raise RuntimeError(f'Global Data Lab returned an error page for {url}; check the token and the API quota')
    return pd.read_csv(io.StringIO(resp.text))


frames = []
for dataset, indicators in DATASETS.items():
    for year in range(START_YEAR, END_YEAR + 1):
        df = gdl_download(dataset, indicators, year)
        print(f'{dataset} {year}: {len(df)} rows')
        frames.append(df)
# The education download returns the nearest survey's rows for every requested year, so
# the same row comes back many times; the R notebook's merge(all = TRUE) collapsed those.
raw = pd.concat(frames, ignore_index=True).drop_duplicates(ignore_index=True)
spark.createDataFrame(raw).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{INDICATOR_SCHEMA}.global_data_lab_hd_index_bronze")
print(f'global_data_lab_hd_index_bronze nrow: {len(raw)}')

# COMMAND ----------

silver = raw.rename(columns={'Year': 'year'})
# Country names as the rest of the pipeline spells them
silver['Country'] = silver['Country'].replace({'Congo Democratic Republic': 'Congo, Dem. Rep.', 'Chili': 'Chile'})

# One row per region and year: the two datasets each contribute their own indicator
# columns, so take the first non-null value per column.
present = [i for i in INDICATORS if i in silver.columns]
silver = silver.groupby(['Country', 'ISO_Code', 'Region', 'year'], as_index=False)[present].first()
assert not silver.duplicated(['Country', 'Region', 'year']).any(), 'Some groups do not have exactly one observation'
# Attendance across the four school levels; the intervals are uniform so a plain mean works.
silver['attendance'] = silver[['lprimary', 'uprimary', 'lsecondary', 'usecondary']].mean(axis=1)
silver

# COMMAND ----------

spark.createDataFrame(silver).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{INDICATOR_SCHEMA}.global_data_lab_hd_index_silver")
print(f'global_data_lab_hd_index_silver nrow: {len(silver)}')
