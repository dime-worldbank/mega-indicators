# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

# Subnational human development indices and school attendance from Global Data Lab, for
# every country, one request per dataset and year. The URL scheme is the gdldata R
# package's: <base>/<dataset>/download/<year>/<indicator+indicator>/?format=csv&token=...
# Plain pandas on both sides; it replaced an R notebook. The token is the GDL_API_TOKEN
# secret (DIMEBOOSTKEYVAULT) on Databricks and the GDL_API_TOKEN environment variable off it.
import io
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

token = get_secret('DIMEBOOSTKEYVAULT', 'GDL_API_TOKEN')


def gdl_download(dataset, indicators, year):
    url = f"{GDL_BASEURL}/{dataset}/download/{year}/{'+'.join(indicators)}/"
    resp = requests.get(url, params={'format': 'csv', 'token': token, 'interpolation': 1},
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
raw = pd.concat(frames, ignore_index=True).rename(columns={'Year': 'year'})
write_table(raw, 'global_data_lab_hd_index_bronze')

# COMMAND ----------

# Country names as the rest of the pipeline spells them
raw['Country'] = raw['Country'].replace({'Congo Democratic Republic': 'Congo, Dem. Rep.', 'Chili': 'Chile'})

# One row per region and year: the two datasets each contribute their own indicator
# columns, so take the first non-null value per column.
present = [i for i in INDICATORS if i in raw.columns]
silver = raw.groupby(['Country', 'ISO_Code', 'Region', 'year'], as_index=False)[present].first()
assert not silver.duplicated(['Country', 'Region', 'year']).any(), 'Some groups do not have exactly one observation'
# Attendance across the four school levels; the intervals are uniform so a plain mean works.
silver['attendance'] = silver[['lprimary', 'uprimary', 'lsecondary', 'usecondary']].mean(axis=1)
silver

# COMMAND ----------

write_table(silver, 'global_data_lab_hd_index_silver')
