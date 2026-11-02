# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

# Subnational human development indices and school attendance from Global Data Lab, for
# every country, one request per dataset and year through the gdldata package's URL scheme
# (gdl_download in utils). Plain pandas on both sides.
from datetime import date

import pandas as pd

START_YEAR = 1990
END_YEAR = date.today().year
DATASETS = {
    'shdi': ['healthindex', 'edindex', 'incindex'],
    'education': ['lprimary', 'uprimary', 'lsecondary', 'usecondary'],  # attendance by school level
}
INDICATORS = [i for inds in DATASETS.values() for i in inds]

token = get_secret('DIMEBOOSTKEYVAULT', 'GDL_API_TOKEN')

frames = []
for dataset, indicators in DATASETS.items():
    for year in range(START_YEAR, END_YEAR + 1):
        df = gdl_download(token, dataset, indicators, year)
        print(f'{dataset} {year}: {len(df)} rows')
        frames.append(df)
# The education download returns the nearest survey's rows for every requested year, so
# the same row comes back many times; keep one copy of each.
raw = pd.concat(frames, ignore_index=True).drop_duplicates(ignore_index=True)
write_table(raw, 'global_data_lab_hd_index_bronze')
print(f'global_data_lab_hd_index_bronze nrow: {len(raw)}')

# COMMAND ----------

silver = raw.rename(columns={'Year': 'year'})
# Country names as the rest of the pipeline spells them
silver['Country'] = silver['Country'].replace(GDL_COUNTRY_RENAMES)

# One row per region and year: the two datasets each contribute their own indicator
# columns, so take the first non-null value per column.
present = [i for i in INDICATORS if i in silver.columns]
silver = silver.groupby(['Country', 'ISO_Code', 'Region', 'year'], as_index=False)[present].first()
assert not silver.duplicated(['Country', 'Region', 'year']).any(), 'Some groups do not have exactly one observation'
# Attendance across the four school levels; the intervals are uniform so a plain mean works.
silver['attendance'] = silver[['lprimary', 'uprimary', 'lsecondary', 'usecondary']].mean(axis=1)
silver

# COMMAND ----------

write_table(silver, 'global_data_lab_hd_index_silver')
print(f'global_data_lab_hd_index_silver nrow: {len(silver)}')
