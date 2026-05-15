# Databricks notebook source
import pandas as pd
import requests
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

SOURCES = [
    {
        'flow': 'IMF.RES/WEO/9.0.0',
        'key_template': '{countries}.{indicators}.A',
        'indicators': {'GGR': 'revenue_current_lcu', 'GGX': 'expenditure_current_lcu'},
        'data_source': 'WEO (World Economic Outlook), IMF — General Government',
        'extract_forecast': True,
    },
    {
        'flow': 'IMF.STA/GFS_SOO/12.0.0',
        'key_template': '{countries}.S1311B.*.{indicators}.XDC.*',
        'indicators': {'G1_T': 'revenue_current_lcu', 'G2M_T': 'expenditure_current_lcu'},
        'data_source': 'GFS_SOO (Statement of Operations), IMF — Budgetary Central Government',
    },
]

# COMMAND ----------

def _parse_payload(payload, extract_forecast):
    datasets = payload.get('data', {}).get('dataSets') or []
    if not datasets or not datasets[0].get('series'):
        return

    struct = payload['data']['structures'][0]
    dims = struct['dimensions']['series']
    pos = {d['id']: i for i, d in enumerate(dims)}
    countries = [v['id'] for v in dims[pos['COUNTRY']]['values']]
    indicators = [v['id'] for v in dims[pos['INDICATOR']]['values']]
    years = [int(v['value']) for v in struct['dimensions']['observation'][0]['values']]

    pub_idx = next(
        (i for i, a in enumerate(struct.get('attributes', {}).get('series', []))
         if a.get('id') == 'COUNTRY_UPDATE_DATE'),
        None,
    ) if extract_forecast else None

    for series_key, series in datasets[0]['series'].items():
        idx = [int(i) for i in series_key.split(':')]
        country = countries[idx[pos['COUNTRY']]]
        indicator = indicators[idx[pos['INDICATOR']]]

        pub_year = None
        if pub_idx is not None:
            attrs = series.get('attributes') or []
            raw = attrs[pub_idx] if pub_idx < len(attrs) else None
            if isinstance(raw, str):
                try:
                    pub_year = datetime.strptime(raw, '%m/%d/%Y').year
                except ValueError:
                    pass

        for time_idx, obs in series['observations'].items():
            year = years[int(time_idx)]
            yield {
                'country_code': country,
                'year': year,
                'indicator': indicator,
                'value': float(obs[0]) if obs[0] is not None else None,
                'forecast': bool(pub_year and year >= pub_year),
            }

def fetch_sdmx(country_codes, flow, key_template, indicators, data_source, extract_forecast=False):
    indicator_key = '+'.join(indicators.keys())
    records, n_chunks, n_404 = [], 0, 0

    for i in range(0, len(country_codes), CHUNK_SIZE):
        n_chunks += 1
        key = key_template.format(
            countries='+'.join(country_codes[i:i + CHUNK_SIZE]),
            indicators=indicator_key,
        )
        resp = session.get(
            f'{SDMX_DATA_API}/{flow}/{key}',
            params={'format': 'jsondata', 'attributes': 'dsd'},
            timeout=30,
        )
        # IMF returns 404 for both "no data" and "bad path" — count to detect the latter.
        if resp.status_code == 404:
            n_404 += 1
            continue
        resp.raise_for_status()
        records.extend(_parse_payload(resp.json(), extract_forecast))

    if n_chunks and n_404 == n_chunks:
        raise RuntimeError(f"{flow}: every chunk returned 404 — check version/key template")
    if not records:
        raise RuntimeError(f"{flow}: no records returned")

    idx_cols = ['country_code', 'year'] + (['forecast'] if extract_forecast else [])
    df = (pd.DataFrame(records)
            .pivot_table(index=idx_cols, columns='indicator', values='value', aggfunc='first')
            .reset_index()
            .rename_axis(columns=None)
            .rename(columns=indicators))
    df['data_source'] = data_source
    return df

# COMMAND ----------

country_codes = (spark.table('prd_mega.indicator.country')
    .filter("is_aggregate = false OR is_aggregate IS NULL")
    .select('country_code').toPandas()['country_code']
    .dropna().unique().tolist())

combined_df = pd.concat([fetch_sdmx(country_codes, **source) for source in SOURCES], ignore_index=True)
combined_df['forecast'] = combined_df['forecast'].fillna(False)

# COMMAND ----------

country_df = spark.table('prd_mega.indicator.country').select('country_name', 'country_code', 'region').toPandas()
merged_df = (pd.merge(combined_df, country_df, on='country_code', how='inner')
    [['country_name', 'country_code', 'region', 'year', 'forecast',
      'revenue_current_lcu', 'expenditure_current_lcu', 'data_source']]
    .sort_values(['country_name', 'year', 'data_source']))

# COMMAND ----------

merged_df.sample(5)

# COMMAND ----------

sdf = spark.createDataFrame(merged_df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("prd_mega.indicator.government_budget")
