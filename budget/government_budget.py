# Databricks notebook source
import pandas as pd
import requests

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sources (IMF SDMX v3 API, both in national currency / XDC)
# MAGIC
# MAGIC ### WEO (World Economic Outlook) — General Government
# MAGIC - `GGR`: Revenue
# MAGIC - `GGX`: Expenditure
# MAGIC
# MAGIC ### GFS_SOO (GFS Statement of Operations) — Budgetary Central Government
# MAGIC - `G1_T`: Revenue, Transactions
# MAGIC - `G2M_T`: Expense, Transactions

# COMMAND ----------

SDMX_API = 'https://api.imf.org/external/sdmx/3.0/data/dataflow'
CHUNK_SIZE = 50

# Each source declares its dataflow path, a key template with {countries}/{indicators}
# placeholders, the indicator code → output column map, and a data_source label.
SOURCES = [
    {
        'flow': 'IMF.RES/WEO/9.0.0',
        'key_template': '{countries}.{indicators}.A',
        'indicators': {
            'GGR': 'revenue_current_lcu',
            'GGX': 'expenditure_current_lcu',
        },
        'data_source': 'WEO (World Economic Outlook), IMF — General Government',
    },
    {
        'flow': 'IMF.STA/GFS_SOO/12.0.0',
        'key_template': '{countries}.S1311B.*.{indicators}.XDC.*',
        'indicators': {
            'G1_T': 'revenue_current_lcu',
            'G2M_T': 'expenditure_current_lcu',
        },
        'data_source': 'GFS_SOO (Statement of Operations), IMF — Budgetary Central Government',
    },
]

# COMMAND ----------

def _parse_sdmx_payload(payload):
    """Parse one SDMX-JSON v3 response into long-format records: (country_code, year, indicator, value)."""
    datasets = payload.get('data', {}).get('dataSets') or []
    if not datasets or not datasets[0].get('series'):
        return []

    series_dims = payload['data']['structures'][0]['dimensions']['series']
    obs_dim = payload['data']['structures'][0]['dimensions']['observation'][0]
    pos = {d['id']: i for i, d in enumerate(series_dims)}
    countries = [v['id'] for v in series_dims[pos['COUNTRY']]['values']]
    indicators = [v['id'] for v in series_dims[pos['INDICATOR']]['values']]
    years = [int(v['value']) for v in obs_dim['values']]

    records = []
    for series_key, series in datasets[0]['series'].items():
        idx = [int(i) for i in series_key.split(':')]
        country_code = countries[idx[pos['COUNTRY']]]
        indicator = indicators[idx[pos['INDICATOR']]]
        for time_idx, obs in series['observations'].items():
            records.append({
                'country_code': country_code,
                'year': years[int(time_idx)],
                'indicator': indicator,
                'value': float(obs[0]) if obs[0] is not None else None,
            })
    return records

def fetch_sdmx(country_codes, flow, key_template, indicators, data_source):
    """Chunked fetch over `country_codes`, pivot to wide, rename indicator codes to nice columns."""
    indicator_key = '+'.join(indicators.keys())
    all_records = []
    for i in range(0, len(country_codes), CHUNK_SIZE):
        chunk = '+'.join(country_codes[i:i + CHUNK_SIZE])
        key = key_template.format(countries=chunk, indicators=indicator_key)
        resp = requests.get(f'{SDMX_API}/{flow}/{key}?format=jsondata')
        if resp.status_code == 404:
            # No data for any country in this chunk — skip.
            continue
        resp.raise_for_status()
        all_records.extend(_parse_sdmx_payload(resp.json()))

    df = (
        pd.DataFrame(all_records)
            .pivot_table(index=['country_code', 'year'], columns='indicator', values='value', aggfunc='first')
            .reset_index()
            .rename_axis(columns=None)
            .rename(columns=indicators)
    )
    df['data_source'] = data_source
    return df

# COMMAND ----------

country_codes = (
    spark.table('prd_mega.indicator.country')
        .filter("is_aggregate = false OR is_aggregate IS NULL")
        .select('country_code')
        .toPandas()['country_code']
        .dropna()
        .unique()
        .tolist()
)

combined_df = pd.concat([fetch_sdmx(country_codes, **source) for source in SOURCES], ignore_index=True)

# COMMAND ----------

country_df = spark.table('prd_mega.indicator.country').select('country_name', 'country_code', 'region').toPandas()
merged_df = pd.merge(combined_df, country_df, on='country_code', how='inner')
merged_df = merged_df[['country_name', 'country_code', 'region', 'year', 'revenue_current_lcu', 'expenditure_current_lcu', 'data_source']]
merged_df.sort_values(['country_name', 'year', 'data_source'], inplace=True)

# COMMAND ----------

merged_df.sample(5)

# COMMAND ----------

sdf = spark.createDataFrame(merged_df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("prd_mega.indicator.government_budget")
