"""Shared helpers for the indicator scripts: table IO on CSV files under DATA_ROOT, and
fetchers for the World Bank, UNESCO UIS and single-file sources. Copied from
mega-indicators' utils.py with the Databricks branches removed; keep the fetchers
identical so fixes can be ported in either direction."""
import io
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests
import wbgapi as wb

from config import *

DEFAULT_TIMEOUT_SECONDS = 60

# wbgapi has no timeout by default, set it so a stalled connection doesn't hang forever
wb.get_options = {'timeout': DEFAULT_TIMEOUT_SECONDS}


# --- Table IO: INDICATOR_DIR/<table>.csv --------------------------------------------------

def _table_path(table_name):
    return os.path.join(INDICATOR_DIR, f'{table_name}.csv')

def table_exists(table_name):
    return os.path.exists(_table_path(table_name))

def read_table(table_name, columns=None):
    """The table as a pandas DataFrame, optionally restricted to `columns`."""
    # Only blanks (what write_table emits for nulls) and "null" (what a Databricks CSV
    # export emits) are nulls; pandas' default list would also swallow real values such
    # as Namibia's ISO2 code "NA".
    df = pd.read_csv(_table_path(table_name), keep_default_na=False, na_values=['', 'null'])
    return df if columns is None else df[list(columns)]

def write_table(df, table_name):
    """Overwrite the table with `df`; COUNTRY_NAME (config.py) restricts rows to that country."""
    if COUNTRY_NAME and 'country_name' in df.columns:
        df = df[df['country_name'] == COUNTRY_NAME]
    path = _table_path(table_name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"wrote {len(df)} rows to {path}")

def update_version_flag(name):
    """True when the upper-cased environment variable of that name is "true" (a job widget on Databricks)."""
    return os.environ.get(name.upper(), 'false').strip().lower() == 'true'


# --- World Bank API ---------------------------------------------------------------------

def _wb_dataframe_with_retry(series, attempts=5, backoff=2.0):
    # World Bank's API intermittently 502s mid-pagination; retry the whole fetch.
    for i in range(attempts):
        try:
            return wb.data.DataFrame(series, skipBlanks=True)
        except Exception:
            if i == attempts - 1:
                raise
            time.sleep(backoff * (2 ** i))

def wbgapi_fetch(indicators, col_names, data_source, extra_col_names_from_country_table=None, how: str = 'inner'):
    if extra_col_names_from_country_table is None:
        extra_col_names_from_country_table = []
    if how not in {'inner', 'outer', 'left', 'right'}:
        raise ValueError(f"Unsupported merge how='{how}'")
    long_dfs = []
    for series, col_name in zip(indicators, col_names):
        df = _wb_dataframe_with_retry(series).reset_index()
        long_df = df.melt(id_vars='economy', var_name='year', value_name=col_name)
        long_df = long_df.dropna(subset=col_name)
        long_df['year'] = long_df['year'].str.replace('YR', '')
        long_df = long_df.astype({'year': 'int'})
        long_dfs.append(long_df)

    merged_df = long_dfs[0]
    for df in long_dfs[1:]:
        merged_df = pd.merge(merged_df, df, on=['economy', 'year'], how=how)

    merged_df['data_source'] = data_source

    country_df = read_table('country', columns=['country_name', 'country_code', 'region', *extra_col_names_from_country_table])
    df = pd.merge(merged_df, country_df, left_on='economy', right_on='country_code', how='left')[['country_name', 'country_code', 'region', *extra_col_names_from_country_table, 'year', *col_names, 'data_source']]

    return df


# --- UNESCO UIS API ---------------------------------------------------------------------

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
    resp = requests.get(UIS_API_URL, params=params, timeout=DEFAULT_TIMEOUT_SECONDS)
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

    country_df = read_table('country', columns=['country_name', 'country_code', 'region', *extra_col_names_from_country_table])
    df = pd.merge(merged_df, country_df, left_on='geoUnit', right_on='country_code', how='inner')[['country_name', 'country_code', 'region', *extra_col_names_from_country_table, 'year', *col_names, 'data_source']]

    return df


# --- Single-file sources -----------------------------------------------------------------

def ddh_bytes(url):
    """Bytes of a World Bank Data Catalog (DDH) file."""
    resp = requests.get(url, timeout=DEFAULT_TIMEOUT_SECONDS)
    resp.raise_for_status()
    return resp.content

def fetch_raw(source_url, table_name, parse=pd.read_csv, **parse_kwargs):
    """Overwrite table_name with a freshly fetched, parsed snapshot (a bronze table)."""
    resp = requests.get(source_url, timeout=DEFAULT_TIMEOUT_SECONDS)
    resp.raise_for_status()
    df = parse(io.BytesIO(resp.content), **parse_kwargs)
    df['fetched_at'] = datetime.now(timezone.utc)
    write_table(df, table_name)

def versioned_dataframe(source_url, table_name, update_version, parse=pd.read_csv, **parse_kwargs):
    """Read table_name's cached snapshot, refreshing first if update_version or unset.

    A temporarily unreachable source yields stale data instead of a failure.
    """
    if update_version or not table_exists(table_name):
        fetch_raw(source_url, table_name, parse=parse, **parse_kwargs)
    return read_table(table_name).drop(columns='fetched_at')
