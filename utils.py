# Databricks notebook source
import os
import wbgapi as wb
import pandas as pd
from databricks.sdk.runtime import spark, dbutils

def wbgapi_fetch(indicators, col_names, data_source, extra_col_names_from_country_table=None, how: str = 'inner'):
    if extra_col_names_from_country_table is None:
        extra_col_names_from_country_table = []
    if how not in {'inner', 'outer', 'left', 'right'}:
        raise ValueError(f"Unsupported merge how='{how}'")
    long_dfs = []
    for series, col_name in zip(indicators, col_names):
        df = wb.data.DataFrame(series, skipBlanks=True).reset_index()
        long_df = df.melt(id_vars='economy', var_name='year', value_name=col_name)
        long_df = long_df.dropna(subset=col_name)
        long_df['year'] = long_df['year'].str.replace('YR', '')
        long_df = long_df.astype({'year': 'int'})
        long_dfs.append(long_df)

    merged_df = long_dfs[0]
    for df in long_dfs[1:]:
        merged_df = pd.merge(merged_df, df, on=['economy', 'year'], how=how)

    merged_df['data_source'] = data_source

    country_df = spark.table(f'{INDICATOR_SCHEMA}.country').select('country_name', 'country_code', 'region', *extra_col_names_from_country_table).toPandas()
    country_df
    df = pd.merge(merged_df, country_df, left_on='economy', right_on='country_code', how='left')[['country_name', 'country_code', 'region', *extra_col_names_from_country_table, 'year', *col_names, 'data_source']]

    return df

# COMMAND ----------

import requests

UIS_API_URL = 'https://api.uis.unesco.org/api/public/data/indicators'

def uis_fetch(series_to_col_name, data_source, extra_col_names_from_country_table=None, how: str = 'inner', start: int = None, stop: int = None):
    """Fetch indicators straight from the UNESCO Institute for Statistics (UIS) API.

    Mirrors wbgapi_fetch's output (one row per country-year, one column per
    indicator) so downstream scripts are interchangeable. The UIS geoUnit is an
    ISO3 code, so it joins directly onto country.country_code; the join is inner,
    which drops UIS regional aggregates (e.g. ECOWAS, World) and keeps only
    countries.
    """
    if extra_col_names_from_country_table is None:
        extra_col_names_from_country_table = []
    if how not in {'inner', 'outer', 'left', 'right'}:
        raise ValueError(f"Unsupported merge how='{how}'")
    if not series_to_col_name:
        raise ValueError("series_to_col_name must contain at least one indicator")
    col_names = list(series_to_col_name.values())

    params = [('indicator', ind) for ind in series_to_col_name]
    if start is not None:
        params.append(('start', start))
    if stop is not None:
        params.append(('stop', stop))
    resp = requests.get(UIS_API_URL, params=params, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    raw_df = pd.DataFrame.from_records(payload.get('records', []))
    if raw_df.empty:
        cols = ['country_name', 'country_code', 'region', *extra_col_names_from_country_table, 'year', *col_names, 'data_source']
        return pd.DataFrame(columns=cols)

    long_dfs = []
    for series, col_name in series_to_col_name.items():
        long_df = raw_df.loc[raw_df['indicatorId'] == series, ['geoUnit', 'year', 'value']]
        long_df = long_df.rename(columns={'value': col_name}).dropna(subset=col_name)
        long_dfs.append(long_df)

    merged_df = long_dfs[0]
    for df in long_dfs[1:]:
        merged_df = pd.merge(merged_df, df, on=['geoUnit', 'year'], how=how)

    merged_df = merged_df.astype({'year': 'int'})
    merged_df['data_source'] = data_source

    country_df = spark.table(f'{INDICATOR_SCHEMA}.country').select('country_name', 'country_code', 'region', *extra_col_names_from_country_table).toPandas()
    df = pd.merge(merged_df, country_df, left_on='geoUnit', right_on='country_code', how='inner')[['country_name', 'country_code', 'region', *extra_col_names_from_country_table, 'year', *col_names, 'data_source']]

    return df

# COMMAND ----------

from urllib.parse import urlparse, unquote

def ddh_volume_path(url):
    """Volume path mirroring a DDH download URL
    (.../ddh-published/{dataset}/{resource}/{filename})."""
    parts = [unquote(p) for p in urlparse(url).path.split('/') if p]
    i = parts.index('ddh-published')
    return "/Volumes/prd_development_data/files/ddh/" + "/".join(parts[i + 1:])

def ddh_bytes(url):
    """Bytes of a DDH file: the mounted volume copy if present, else download the URL."""
    vol = ddh_volume_path(url)
    if os.path.exists(vol):
        with open(vol, 'rb') as f:
            return f.read()
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.content

# COMMAND ----------

import io
from datetime import datetime, timezone

def fetch_raw(source_url, table_name, parse=pd.read_csv, **parse_kwargs):
    """Overwrite table_name with a freshly fetched, parsed snapshot (a bronze table).

    Delta's transaction log is the audit trail (DESCRIBE HISTORY / VERSION AS OF).
    """
    resp = requests.get(source_url, timeout=60)
    resp.raise_for_status()
    df = parse(io.BytesIO(resp.content), **parse_kwargs)
    df['fetched_at'] = datetime.now(timezone.utc)
    (spark.createDataFrame(df)
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.columnMapping.mode", "name")
        # Defaults (7d/30d) let VACUUM drop a snapshot before the next monthly refresh.
        .option("delta.deletedFileRetentionDuration", "interval 365 days")
        .option("delta.logRetentionDuration", "interval 365 days")
        .saveAsTable(f"{INDICATOR_SCHEMA}.{table_name}"))

def versioned_dataframe(source_url, table_name, update_version, parse=pd.read_csv, **parse_kwargs):
    """Read table_name's cached snapshot, refreshing first if update_version or unset.

    A temporarily unreachable source yields stale data instead of a failure.
    """
    full_table_name = f"{INDICATOR_SCHEMA}.{table_name}"
    if update_version or not spark.catalog.tableExists(full_table_name):
        fetch_raw(source_url, table_name, parse=parse, **parse_kwargs)
    return spark.table(full_table_name).drop('fetched_at').toPandas()

def update_version_flag(widget_name):
    return dbutils.widgets.getArgument(widget_name, 'false').strip().lower() == 'true'

