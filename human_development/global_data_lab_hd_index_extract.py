from utils import *

import io
import os
from datetime import date

import pandas as pd
import requests

# Subnational human development indices and school attendance from Global Data Lab,
# one request per dataset and year, as the gdldata R package does (its URL scheme is
# <base>/<dataset>/download/<year>/<indicator+indicator>/<ISO3>/?format=csv&token=...).
# Needs a free API token from https://globaldatalab.org in GDL_API_TOKEN.
GDL_BASEURL = 'https://globaldatalab.org'
COUNTRY_CODE = 'TGO'
START_YEAR = 1990
END_YEAR = date.today().year
DATASETS = {
    'shdi': ['healthindex', 'edindex', 'incindex'],
    'education': ['lprimary', 'uprimary', 'lsecondary', 'usecondary'],  # attendance by school level
}
INDICATORS = [i for inds in DATASETS.values() for i in inds]

token = os.environ.get('GDL_API_TOKEN')
if not token:
    raise RuntimeError('GDL_API_TOKEN is not set; create a free token at https://globaldatalab.org and export it')


def gdl_download(dataset, indicators, year):
    url = f"{GDL_BASEURL}/{dataset}/download/{year}/{'+'.join(indicators)}/{COUNTRY_CODE}/"
    resp = requests.get(url, params={'format': 'csv', 'token': token, 'interpolation': 1},
                        headers={'Accept': 'text/csv'}, timeout=DEFAULT_TIMEOUT_SECONDS)
    resp.raise_for_status()
    if resp.text.lstrip().startswith('<'):
        raise RuntimeError(f'Global Data Lab returned an error page for {url}; check GDL_API_TOKEN and the API quota')
    return pd.read_csv(io.StringIO(resp.text))


frames = []
for dataset, indicators in DATASETS.items():
    for year in range(START_YEAR, END_YEAR + 1):
        df = gdl_download(dataset, indicators, year)
        print(f'{dataset} {year}: {len(df)} rows')
        frames.append(df)
raw = pd.concat(frames, ignore_index=True).rename(columns={'Year': 'year'})
write_table(raw, 'global_data_lab_hd_index_bronze')

# One row per region and year: the two datasets each contribute their own indicator
# columns, so take the first non-null value per column.
present = [i for i in INDICATORS if i in raw.columns]
silver = raw.groupby(['Country', 'ISO_Code', 'Region', 'year'], as_index=False)[present].first()
assert not silver.duplicated(['Country', 'Region', 'year']).any()
# Attendance across the four school levels; the intervals are uniform so a plain mean works.
silver['attendance'] = silver[['lprimary', 'uprimary', 'lsecondary', 'usecondary']].mean(axis=1)

write_table(silver, 'global_data_lab_hd_index_silver')
