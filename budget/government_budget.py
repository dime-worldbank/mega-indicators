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
    """Parse one SDMX-JSON GFS response into long-format records: one per (country, year, indicator)."""
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

    df = (
        pd.DataFrame(all_records)
            .pivot_table(index=['country_code', 'year'], columns='indicator', values='value', aggfunc='first')
            .reset_index()
            .rename_axis(columns=None)
            .rename(columns=GFS_INDICATORS)
    )
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
