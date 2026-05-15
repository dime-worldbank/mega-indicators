# Databricks notebook source
%pip install sdmx1
dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import sdmx
from datetime import datetime

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

CHUNK_SIZE = 50

# IMF_DATA3 = sdmx1's source ID for api.imf.org (SDMX-REST 2.x / SDMX-ML 3.0)
client = sdmx.Client('IMF_DATA3')

# Each source declares the IMF dataflow (agency_id, resource_id, version), a key
# template with {countries}/{indicators} placeholders, the indicator code →
# output column map, and a data_source label. extract_forecast flags whether to
# use COUNTRY_UPDATE_DATE to distinguish historical vs forecast observations.
SOURCES = [
    {
        'agency_id': 'IMF.RES',
        'resource_id': 'WEO',
        'version': '9.0.0',
        'key_template': '{countries}.{indicators}.A',
        'indicators': {
            'GGR': 'revenue_current_lcu',
            'GGX': 'expenditure_current_lcu',
        },
        'data_source': 'WEO (World Economic Outlook), IMF — General Government',
        'extract_forecast': True,
    },
    {
        'agency_id': 'IMF.STA',
        'resource_id': 'GFS_SOO',
        'version': '12.0.0',
        'key_template': '{countries}.S1311B.*.{indicators}.XDC.*',
        'indicators': {
            'G1_T': 'revenue_current_lcu',
            'G2M_T': 'expenditure_current_lcu',
        },
        'data_source': 'GFS_SOO (Statement of Operations), IMF — Budgetary Central Government',
    },
]

# COMMAND ----------

def _publication_year(series):
    """Read COUNTRY_UPDATE_DATE from a series's attached attributes (mm/dd/yyyy)."""
    attrs = getattr(series, 'attrib', None) or getattr(series, 'attached_attribute', None)
    if not attrs:
        return None
    attr = attrs.get('COUNTRY_UPDATE_DATE')
    if attr is None:
        return None
    value = getattr(attr, 'value', attr)
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value, '%m/%d/%Y').year
    except ValueError:
        return None

def _dim_value(series_key, name):
    kv = getattr(series_key, name)
    return getattr(kv, 'value', kv)

def fetch_sdmx(country_codes, agency_id, resource_id, version, key_template, indicators, data_source, extract_forecast=False):
    """Chunked fetch via sdmx1, pivot to wide, rename indicator codes to nice columns."""
    indicator_key = '+'.join(indicators.keys())
    all_records = []

    for i in range(0, len(country_codes), CHUNK_SIZE):
        chunk = '+'.join(country_codes[i:i + CHUNK_SIZE])
        key = key_template.format(countries=chunk, indicators=indicator_key)
        try:
            msg = client.get(
                resource_type='data',
                resource_id=resource_id,
                provider=agency_id,
                version=version,
                key=key,
                params={'attributes': 'dsd'},
            )
        except Exception as e:
            # IMF returns 404 for chunks where no country has data — skip.
            if '404' in str(e):
                continue
            raise

        for dataset in msg.data:
            for series_key, series in dataset.series.items():
                country_code = _dim_value(series_key, 'COUNTRY')
                indicator = _dim_value(series_key, 'INDICATOR')
                publication_year = _publication_year(series) if extract_forecast else None

                for obs in series.obs:
                    period = obs.dim if hasattr(obs, 'dim') else obs.key[0]
                    period = getattr(period, 'value', period)
                    try:
                        year = int(str(period))
                    except (TypeError, ValueError):
                        continue
                    value = float(obs.value) if obs.value is not None else None
                    forecast = bool(publication_year and year >= publication_year)
                    all_records.append({
                        'country_code': country_code,
                        'year': year,
                        'indicator': indicator,
                        'value': value,
                        'forecast': forecast,
                    })

    df = pd.DataFrame(all_records)
    index_cols = ['country_code', 'year', 'forecast'] if extract_forecast else ['country_code', 'year']
    df = df.pivot_table(index=index_cols, columns='indicator', values='value', aggfunc='first').reset_index()
    df = df.rename_axis(columns=None).rename(columns=indicators)
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
combined_df['forecast'] = combined_df['forecast'].fillna(False)

# COMMAND ----------

country_df = spark.table('prd_mega.indicator.country').select('country_name', 'country_code', 'region').toPandas()
merged_df = pd.merge(combined_df, country_df, on='country_code', how='inner')
merged_df = merged_df[['country_name', 'country_code', 'region', 'year', 'forecast', 'revenue_current_lcu', 'expenditure_current_lcu', 'data_source']]
merged_df.sort_values(['country_name', 'year', 'data_source'], inplace=True)

# COMMAND ----------

merged_df.sample(5)

# COMMAND ----------

sdf = spark.createDataFrame(merged_df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("prd_mega.indicator.government_budget")
