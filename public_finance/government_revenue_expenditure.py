# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# COMMAND ----------

# MAGIC %run ./imf_sdmx

# COMMAND ----------

if '_parse_payload' not in globals():  # off Databricks the %run above is a comment
    from public_finance.imf_sdmx import _parse_payload, _weo_annotate_forecast

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sources (IMF SDMX v3, national currency / XDC)
# MAGIC - **WEO** (General Government): `GGR` revenue, `GGX` expenditure
# MAGIC - **GFS_SOO** (Budgetary Central Government): `G1_T` revenue, `G2M_T` expense

# COMMAND ----------

SDMX_DATA_API = 'https://api.imf.org/external/sdmx/3.0/data/dataflow'
CHUNK_SIZE = 50

session = requests.Session()
session.mount('https://', HTTPAdapter(max_retries=Retry(
    total=5, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(['GET']),
)))

# COMMAND ----------

def fetch_sdmx(country_codes, flow, key_template, indicators, post_process=None):
    """Generic SDMX flow fetcher.

    Chunks countries, fetches each chunk, parses, and pivots indicators into columns.
    `post_process(chunk_records, payload)` is called per chunk if provided; any extra
    keys it adds to records (e.g., 'is_forecast' for WEO) become additional pivot
    index columns automatically.
    """
    indicator_key = '+'.join(indicators.keys())
    records = []

    for i in range(0, len(country_codes), CHUNK_SIZE):
        key = key_template.format(
            countries='+'.join(country_codes[i:i + CHUNK_SIZE]),
            indicators=indicator_key,
        )
        resp = session.get(
            f'{SDMX_DATA_API}/{flow}/{key}',
            params={
                'format': 'jsondata',
                'attributes': 'all',
                'detail': 'full',
                # Explicit `limit` is required for the API to include the
                # dimensionGroup attribute values (LATEST_ACTUAL_ANNUAL_DATA etc.)
                # — without it the metadata bucket is stripped from the response.
                'limit': max(1000, len(country_codes[i:i + CHUNK_SIZE]) * len(indicators) * 2),
            },
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        chunk_records = list(_parse_payload(payload))
        if post_process is not None:
            post_process(chunk_records, payload)
        records.extend(chunk_records)

    if not records:
        raise RuntimeError(f"{flow}: no records returned")

    # Any extra keys added by post_process become additional pivot index columns.
    extra_dims = sorted(set(records[0]) - {'country_code', 'year', 'indicator', 'value'})
    df = (pd.DataFrame(records)
            .pivot_table(index=['country_code', 'year', *extra_dims],
                         columns='indicator', values='value', aggfunc='first')
            .reset_index()
            .rename_axis(columns=None)
            .rename(columns=indicators))
    return df

# COMMAND ----------

SOURCES = [
    {
        'flow': 'IMF.RES/WEO/9.0.0',
        'key_template': '{countries}.{indicators}.A',#A = Annual
        'indicators': {'GGR': 'revenue_current_lcu', 'GGX': 'expenditure_current_lcu'},
        'table': 'government_revenue_expenditure_weo',
        'post_process': _weo_annotate_forecast,
    },
    {
        'flow': 'IMF.STA/GFS_SOO/12.0.0',
        'key_template': '{countries}.S1311B.*.{indicators}.XDC.*', #S1311B = Budgetary Central Government, XDC = Current LCU
        'indicators': {'G1_T': 'revenue_current_lcu', 'G2M_T': 'expenditure_current_lcu'},
        'table': 'government_revenue_expenditure_gfs',
    },
]

# COMMAND ----------

country_df = read_table('country', columns=['country_name', 'country_code', 'region', 'is_aggregate'])
# Drop regional/income aggregates (WLD, SSF, ...); a null flag counts as a country.
country_df = country_df[country_df['is_aggregate'].ne(True)].drop(columns='is_aggregate')
country_codes = country_df['country_code'].dropna().unique().tolist()

BASE_COLUMNS = ['country_name', 'country_code', 'region', 'year',
                'revenue_current_lcu', 'expenditure_current_lcu']

# COMMAND ----------

# Write each single-source split table (no source column — the source is the
# table's identity, via indicator_source). is_forecast is kept only for WEO.
split_dfs = {}
for source in SOURCES:
    df = fetch_sdmx(country_codes, source['flow'], source['key_template'],
                    source['indicators'], source.get('post_process'))
    columns = list(BASE_COLUMNS)
    if 'is_forecast' in df.columns:
        df['is_forecast'] = df['is_forecast'].fillna(False)
        columns.append('is_forecast')
    merged = (pd.merge(df, country_df, on='country_code', how='inner')[columns]
        .sort_values(['country_name', 'year']))
    write_table(merged, source['table'])
    split_dfs[source['table']] = merged

# COMMAND ----------

# Combined table built from the split tables, adding a clean source_id FK (replaces
# the free-text data_source). GFS has no is_forecast, so it defaults to False.
COMBINED_COLUMNS = ['country_name', 'country_code', 'region', 'year', 'is_forecast',
                    'revenue_current_lcu', 'expenditure_current_lcu', 'source_id']
combined_df = pd.concat([
    split_dfs['government_revenue_expenditure_weo'].assign(source_id='imf_weo'),
    split_dfs['government_revenue_expenditure_gfs'].assign(is_forecast=False, source_id='imf_gfs'),
], ignore_index=True)[COMBINED_COLUMNS]
combined_df.sample(5)

# COMMAND ----------

write_table(combined_df, 'government_revenue_expenditure')
