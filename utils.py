# Databricks notebook source
import os
import json
import hashlib
from datetime import datetime, timezone
import wbgapi as wb
import pandas as pd
from databricks.sdk.runtime import spark

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

def _latest_version(volume_dir):
    """Highest N among existing ``vN`` snapshot folders under volume_dir, or 0 if none."""
    if not os.path.exists(volume_dir):
        return 0
    versions = [int(name[1:]) for name in os.listdir(volume_dir)
                if name.startswith('v') and name[1:].isdigit()]
    return max(versions, default=0)

def versioned_csv(source_url, volume_dir, update_version, filename=None, **read_csv_kwargs):
    """Read a CSV through an auto-versioned volume cache, fetching from source only when needed.

    Snapshots live under volume_dir as numbered subfolders (v1, v2, ...), each holding the
    downloaded file (filename defaults to the last path segment of source_url) plus a
    metadata.json recording the source URL, filename, fetch time and size. When
    update_version is True — or nothing is cached yet — the source is downloaded into the
    next version folder; otherwise the latest existing snapshot is read. Callers never pick
    a version number: a refresh is an explicit, opt-in run (driven by a bundle parameter)
    that increments automatically, so downstream processing stays reproducible and
    decoupled from source-site availability, while past snapshots are kept for audit.
    """
    if filename is None:
        filename = os.path.basename(urlparse(source_url).path)
    latest = _latest_version(volume_dir)

    if update_version or latest == 0:
        latest += 1
        version_dir = f"{volume_dir.rstrip('/')}/v{latest}"
        resp = requests.get(source_url, timeout=60)
        resp.raise_for_status()
        os.makedirs(version_dir, exist_ok=True)
        with open(f"{version_dir}/{filename}", 'wb') as f:
            f.write(resp.content)
        metadata = {
            'version': latest,
            'source_url': source_url,
            'filename': filename,
            'fetched_at': datetime.now(timezone.utc).isoformat(),
            'size_bytes': len(resp.content),
            'sha256': hashlib.sha256(resp.content).hexdigest(),
        }
        with open(f"{version_dir}/metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)

    return pd.read_csv(f"{volume_dir.rstrip('/')}/v{latest}/{filename}", **read_csv_kwargs)

