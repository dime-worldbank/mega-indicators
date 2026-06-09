# Databricks notebook source
import pandas as pd
import requests
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

# COMMAND ----------

def _parse_payload(payload):
    """Generic SDMX-JSON parser. Yields one dict per (country, indicator, year) observation."""
    datasets = payload.get('data', {}).get('dataSets') or []
    if not datasets or not datasets[0].get('series'):
        return

    struct = payload['data']['structures'][0]
    dims = struct['dimensions']['series']
    pos = {d['id']: i for i, d in enumerate(dims)}
    countries = [v['id'] for v in dims[pos['COUNTRY']]['values']]
    indicators = [v['id'] for v in dims[pos['INDICATOR']]['values']]
    years = [int(v['value']) for v in struct['dimensions']['observation'][0]['values']]

    for series_key, series in datasets[0]['series'].items():
        idx = [int(i) for i in series_key.split(':')]
        country = countries[idx[pos['COUNTRY']]]
        indicator = indicators[idx[pos['INDICATOR']]]
        for time_idx, obs in series['observations'].items():
            yield {
                'country_code': country,
                'year': years[int(time_idx)],
                'indicator': indicator,
                'value': float(obs[0]) if obs[0] is not None else None,
            }

def _weo_annotate_forecast(records, payload):
    """Set `forecast` on each record using WEO's LATEST_ACTUAL_ANNUAL_DATA attribute.

    LAAD is the most recent year of *actual* data for a given (country, indicator); any
    strictly-later year is a projection. WEO exposes it as a dimensionGroup attribute
    (relationship: COUNTRY + INDICATOR), with values under
    `dataSets[0].dimensionGroupAttributes`, keyed by positional strings like '0:0::'
    (COUNTRY_idx : INDICATOR_idx : FREQUENCY : TIME_PERIOD; blank positions mean
    "applies to all values in that dimension"). Values come wrapped in lists, e.g.
    `["2025"]`.
    """
    struct = payload['data']['structures'][0]
    dims = struct['dimensions']['series']
    pos = {d['id']: i for i, d in enumerate(dims)}
    countries = [v['id'] for v in dims[pos['COUNTRY']]['values']]
    indicators = [v['id'] for v in dims[pos['INDICATOR']]['values']]

    attrs_meta = struct.get('attributes', {}).get('dimensionGroup', [])
    laad_idx = next(
        (i for i, a in enumerate(attrs_meta) if a.get('id') == 'LATEST_ACTUAL_ANNUAL_DATA'),
        None,
    )
    if laad_idx is None:
        available = [a.get('id') for a in attrs_meta]
        raise RuntimeError(
            f"WEO payload missing dimensionGroup attribute 'LATEST_ACTUAL_ANNUAL_DATA' for "
            f"chunk with countries {countries}. Available dimensionGroup attributes: {available}. "
            f"WEO schema may have changed, or the request 'attributes' param isn't including MSD (use 'all' or 'msd')."
        )

    country_pos = pos['COUNTRY']
    indicator_pos = pos['INDICATOR']
    last_actual = {}
    for key_str, values in payload['data']['dataSets'][0].get('dimensionGroupAttributes', {}).items():
        parts = key_str.split(':')
        # COUNTRY and INDICATOR index strings from the positional key — e.g.
        # for "1:0::" with COUNTRY at pos 0, INDICATOR at pos 1: c_part="1", i_part="0".
        c_part = parts[country_pos] if country_pos < len(parts) else ''
        i_part = parts[indicator_pos] if indicator_pos < len(parts) else ''
        if not c_part or not i_part:
            # blank in either position means the value isn't (country, indicator)-specific
            continue
        raw = values[laad_idx] if laad_idx < len(values) else None
        if isinstance(raw, list) and raw:
            raw = raw[0]
        if raw is None:
            continue
        try:
            last_actual[(countries[int(c_part)], indicators[int(i_part)])] = int(raw)
        except (ValueError, TypeError):
            pass

    for r in records:
        laad = last_actual.get((r['country_code'], r['indicator']))
        r['is_forecast'] = bool(laad is not None and r['year'] > laad)

def fetch_sdmx(country_codes, flow, key_template, indicators, data_source, post_process=None):
    """Generic SDMX flow fetcher.

    Chunks countries, fetches each chunk, parses, and pivots indicators into columns.
    `post_process(chunk_records, payload)` is called per chunk if provided; any extra
    keys it adds to records (e.g., 'forecast' for WEO) become additional pivot index
    columns automatically.
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
    df['data_source'] = data_source
    return df

# COMMAND ----------

SOURCES = [
    {
        'flow': 'IMF.RES/WEO/9.0.0',
        'key_template': '{countries}.{indicators}.A',
        'indicators': {'GGR': 'revenue_current_lcu', 'GGX': 'expenditure_current_lcu'},
        'data_source': 'WEO (World Economic Outlook), IMF — General Government',
        'post_process': _weo_annotate_forecast,
    },
    {
        'flow': 'IMF.STA/GFS_SOO/12.0.0',
        'key_template': '{countries}.S1311B.*.{indicators}.XDC.*',
        'indicators': {'G1_T': 'revenue_current_lcu', 'G2M_T': 'expenditure_current_lcu'},
        'data_source': 'GFS_SOO (Statement of Operations), IMF — Budgetary Central Government',
    },
]

# COMMAND ----------

country_df = (spark.table('prd_mega.indicator.country')
    .filter("is_aggregate = false OR is_aggregate IS NULL")
    .select('country_name', 'country_code', 'region')
    .toPandas())
country_codes = country_df['country_code'].dropna().unique().tolist()

combined_df = pd.concat([fetch_sdmx(country_codes, **source) for source in SOURCES], ignore_index=True)
combined_df['is_forecast'] = combined_df['is_forecast'].fillna(False)

# COMMAND ----------

merged_df = (pd.merge(combined_df, country_df, on='country_code', how='inner')
    [['country_name', 'country_code', 'region', 'year', 'is_forecast',
      'revenue_current_lcu', 'expenditure_current_lcu', 'data_source']]
    .sort_values(['country_name', 'year', 'data_source']))

# COMMAND ----------

merged_df.sample(5)

# COMMAND ----------

sdf = spark.createDataFrame(merged_df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("prd_mega.indicator.government_budget")
