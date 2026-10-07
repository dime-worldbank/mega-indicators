# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

# Regional population from Global Data Lab (dataset demographics, indicator regpopm, in
# millions) for every country, read by the Congo DR and Liberia notebooks. One request
# gives all years; the URL scheme is the gdldata R package's. Plain pandas on both sides;
# it replaced an R notebook. The token is the GDL_API_TOKEN secret (DIMEBOOSTKEYVAULT) on
# Databricks and the GDL_API_TOKEN environment variable off it.
import io

import pandas as pd
import requests

GDL_BASEURL = 'https://globaldatalab.org'
token = get_secret('DIMEBOOSTKEYVAULT', 'GDL_API_TOKEN')

# by default linear extrapolation for 3 years
# disabling extrapolation doesn't seem to work
resp = requests.get(f'{GDL_BASEURL}/demographics/download/regpopm/', params={'format': 'csv', 'token': token, 'interpolation': 1},
                    headers={'Accept': 'text/csv'}, timeout=DEFAULT_TIMEOUT_SECONDS)
resp.raise_for_status()
if resp.text.lstrip().startswith('<'):  # errors (bad token, exhausted quota) come back as an HTML page
    raise RuntimeError('Global Data Lab returned an error page; check the token and the API quota')
spop_merged = pd.read_csv(io.StringIO(resp.text))
print(list(spop_merged.columns))
print(f'nrow: {len(spop_merged)}')
write_table(spop_merged, 'global_data_lab_subnational_population_bronze')

# COMMAND ----------

df_population_bronze = spop_merged.rename(columns={'Year': 'year', 'regpopm': 'population_millions'})
# Country names as the rest of the pipeline spells them
df_population_bronze['Country'] = df_population_bronze['Country'].replace({'Congo Democratic Republic': 'Congo, Dem. Rep.', 'Chili': 'Chile'})

# Drop extrapolated years (first & last 3 years given Country, Region)
df = (df_population_bronze.dropna(subset=['population_millions'])
      .sort_values(['Country', 'Region', 'year'], ascending=[True, True, False]))
position = df.groupby(['Country', 'Region']).cumcount() + 1
count = df.groupby(['Country', 'Region'])['year'].transform('size')
df_no_extrapolation = df[(position > 3) & (position <= count - 3)].reset_index(drop=True)

# COMMAND ----------

write_table(df_no_extrapolation, 'global_data_lab_subnational_population')
print(f"global_data_lab_subnational_population nrow: {len(df_no_extrapolation)}")
