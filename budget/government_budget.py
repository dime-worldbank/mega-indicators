# Databricks notebook source
import pandas as pd
import requests

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source
# MAGIC
# MAGIC ### GFS_SOO (GFS Statement of Operations) — Budgetary Central Government, national currency (XDC)
# MAGIC - `G1_T`: Revenue, Transactions
# MAGIC - `G2M_T`: Expense, Transactions

# COMMAND ----------

GFS_INDICATORS = {
    'G1_T': 'revenue_current_lcu',
    'G2M_T': 'expenditure_current_lcu',
}
GFS_SECTOR = 'S1311B'                # Budgetary Central Government
GFS_UNIT = 'XDC'                     # Domestic currency
GFS_CHUNK_SIZE = 50
GFS_DATA_SOURCE = 'GFS_SOO (Statement of Operations), IMF — Budgetary Central Government'

def _parse_gfs_payload(payload):
    """Parse one SDMX-JSON GFS response into a list of {country_code, year, col_name: value} dicts."""
    datasets = payload.get('data', {}).get('dataSets') or []
    if not datasets or not datasets[0].get('series'):
        return []

    structures = payload['data']['structures'][0]['dimensions']
    series_dims = structures['series']
    dim_index = {d['id']: i for i, d in enumerate(series_dims)}
    dim_values = {d['id']: d['values'] for d in series_dims}

    obs_dim = structures['observation'][0]
    assert obs_dim['id'] == 'TIME_PERIOD', f"Expected TIME_PERIOD, got {obs_dim['id']}"
    time_values = obs_dim['values']

    records = []
    for series_key, series in datasets[0]['series'].items():
        idx = [int(i) for i in series_key.split(':')]
        country_code = dim_values['COUNTRY'][idx[dim_index['COUNTRY']]]['id']
        indicator = dim_values['INDICATOR'][idx[dim_index['INDICATOR']]]['id']
        col_name = GFS_INDICATORS[indicator]
        for time_idx, obs in series['observations'].items():
            year = int(time_values[int(time_idx)]['id'])
            records.append({'country_code': country_code, 'year': year, col_name: obs[0]})
    return records

def fetch_gfs():
    # Pull non-aggregate country codes from the country table so we can chunk requests.
    country_codes = (
        spark.table('prd_mega.indicator.country')
            .filter("is_aggregate = false OR is_aggregate IS NULL")
            .select('country_code')
            .toPandas()['country_code']
            .dropna()
            .unique()
            .tolist()
    )

    indicator_key = '+'.join(GFS_INDICATORS.keys())
    all_records = []
    for i in range(0, len(country_codes), GFS_CHUNK_SIZE):
        chunk = country_codes[i:i + GFS_CHUNK_SIZE]
        country_filter = '+'.join(chunk)
        url = (
            'https://api.imf.org/external/sdmx/3.0/data/dataflow/'
            f'IMF.STA/GFS_SOO/12.0.0/{country_filter}.{GFS_SECTOR}.*.{indicator_key}.{GFS_UNIT}.*'
            '?format=jsondata'
        )
        resp = requests.get(url)
        if resp.status_code == 404:
            # No data for any country in this chunk — skip.
            continue
        resp.raise_for_status()
        all_records.extend(_parse_gfs_payload(resp.json()))

    df = pd.DataFrame(all_records)
    # Collapse rows so each (country, year) has one row with both revenue and expenditure columns
    df = df.groupby(['country_code', 'year'], as_index=False).first()
    df['data_source'] = GFS_DATA_SOURCE
    return df

gfs_df = fetch_gfs()

# COMMAND ----------

country_df = spark.table('prd_mega.indicator.country').select('country_name', 'country_code', 'region').toPandas()
merged_df = pd.merge(gfs_df, country_df, on='country_code', how='inner')
merged_df = merged_df[['country_name', 'country_code', 'region', 'year', 'revenue_current_lcu', 'expenditure_current_lcu', 'data_source']]
merged_df.sort_values(['country_name', 'year'], inplace=True)

# COMMAND ----------

merged_df.sample(5)

# COMMAND ----------

sdf = spark.createDataFrame(merged_df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("prd_mega.indicator.government_budget")
