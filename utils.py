# Databricks notebook source
# Shared helpers, %run from the indicator notebooks. They run in two places: on
# Databricks, where tables are Delta tables in INDICATOR_SCHEMA, and off Databricks,
# where a table is a CSV at DATA_ROOT/<catalog>/<schema>/<table>.csv (both from
# config.py), or with DB_BACKEND=postgres a PostgreSQL table (postgres_tables.py).
# Notebooks should go through read_table / write_table / versioned_dataframe and never
# call spark or dbutils directly, so the same notebook works in all of them.
import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import wbgapi as wb
import pandas as pd

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
if IS_DATABRICKS:
    from databricks.sdk.runtime import spark, dbutils
else:
    from config import *  # off Databricks this file is imported, not %run alongside config

# Off Databricks: "csv" (default) or "postgres"
DB_BACKEND = "databricks" if IS_DATABRICKS else os.environ.get("DB_BACKEND", "csv")
if DB_BACKEND == "postgres":
    import postgres_tables
elif not IS_DATABRICKS and DB_BACKEND != "csv":
    raise RuntimeError(f"Unknown DB_BACKEND {DB_BACKEND!r}; expected csv or postgres.")

DEFAULT_TIMEOUT_SECONDS = 60

def retrying_session(total=5, read=1, backoff_factor=1):
    """A requests session whose adapter retries GETs on connection errors and 429/5xx
    responses (`total` times, waiting 0, 2, 4, 8 and 16 s between attempts) and on a read
    timeout only `read` times: a timeout means the server is up but slow, and each attempt
    costs the full timeout, so a stalled source fails in minutes rather than hours. When
    the status retries run out the last response is returned, so raise_for_status() applies."""
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=Retry(
        total=total, read=read, backoff_factor=backoff_factor, status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(['GET']), raise_on_status=False,
    ))
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

# The sources now and then reset a connection, stall or answer 5xx, and one such failure
# fails the notebook: one retrying session serves the notebooks' own requests (http_get)
# and wbgapi, which calls `requests.get` through its module global. wbgapi has no timeout
# by default; set one so a stalled connection doesn't hang forever.
SESSION = retrying_session()
wb.requests = SESSION
wb.get_options = {'timeout': DEFAULT_TIMEOUT_SECONDS}

def http_get(url, retries=2, **kwargs):
    """SESSION.get with the body read. The adapter's retries end once the response headers
    have arrived, so a body cut short after them (a ChunkedEncodingError; the WHO API does
    this now and then) is the one failure the whole request must be repeated for, which
    this does `retries` times. Call raise_for_status() on the result as usual."""
    kwargs.setdefault('timeout', DEFAULT_TIMEOUT_SECONDS)
    for attempt in range(retries + 1):
        try:
            return SESSION.get(url, **kwargs)
        except requests.exceptions.ChunkedEncodingError:
            if attempt == retries:
                raise
            # the URL without its query string: a key may be in it
            print(f"{url.split('?', 1)[0]}: response cut short; retry {attempt + 1}/{retries}", flush=True)
            time.sleep(2 ** attempt)

# COMMAND ----------

# Table IO. INDICATOR_SCHEMA / DATA_ROOT come from config.py: on Databricks every
# notebook %runs it alongside this file, locally the import above brings it in.
# A table name is either bare (`gdp`, qualified with INDICATOR_SCHEMA) or already
# `catalog.schema.table`; locally both map to DATA_ROOT/catalog/schema/table.csv, or
# with DB_BACKEND=postgres to schema.table in the database named after the catalog.

def _qualified_name(table_name):
    return table_name if '.' in table_name else f"{INDICATOR_SCHEMA}.{table_name}"

def _pg_name(table_name):
    parts = _qualified_name(table_name).split('.')
    if len(parts) != 3:
        raise ValueError(f"{table_name!r} is neither a bare table name nor catalog.schema.table")
    return parts

def _table_path(table_name):
    return os.path.join(DATA_ROOT, *_qualified_name(table_name).split('.')) + '.csv'

def table_exists(table_name):
    if IS_DATABRICKS:
        return spark.catalog.tableExists(_qualified_name(table_name))
    if DB_BACKEND == "postgres":
        return postgres_tables.table_exists(*_pg_name(table_name))
    return os.path.exists(_table_path(table_name))

def read_table(table_name, columns=None):
    """The table as a pandas DataFrame, optionally restricted to `columns`."""
    if IS_DATABRICKS:
        sdf = spark.table(_qualified_name(table_name))
        if columns is not None:
            sdf = sdf.select(*columns)
        return sdf.toPandas()
    if DB_BACKEND == "postgres":
        return postgres_tables.read_table(*_pg_name(table_name), columns=columns)
    # Only blanks (what write_table emits for nulls) and "null" (what a Databricks CSV
    # export emits) are nulls; pandas' default list would also swallow real values such
    # as Namibia's ISO2 code "NA".
    df = pd.read_csv(_table_path(table_name), keep_default_na=False, na_values=['', 'null'])
    return df if columns is None else df[list(columns)]

def write_table(df, table_name, delta_options=None):
    """Overwrite the table with `df`.

    `delta_options` are Delta table options (e.g. retention), applied on Databricks only;
    the local CSV store has no equivalent.
    Locally, COUNTRY_NAME (config.py) restricts rows to that country.
    """
    if IS_DATABRICKS:
        writer = spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true")
        for key, value in (delta_options or {}).items():
            writer = writer.option(key, value)
        writer.saveAsTable(_qualified_name(table_name))
        return
    if COUNTRY_NAME and 'country_name' in df.columns:
        df = df[df['country_name'] == COUNTRY_NAME]
    if DB_BACKEND == "postgres":
        catalog, schema, table = _pg_name(table_name)
        postgres_tables.replace_table(df, catalog, schema, table)
        print(f"wrote {len(df)} rows to {catalog}.{schema}.{table.lower()}")
    else:
        path = _table_path(table_name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=False)
        print(f"wrote {len(df)} rows to {path}")

def update_version_flag(widget_name):
    """True when the job widget (Databricks) or the environment variable of the same name (local) is "true"."""
    if IS_DATABRICKS:
        raw = dbutils.widgets.getArgument(widget_name, 'false')
    else:
        raw = os.environ.get(widget_name, 'false')
    return raw.strip().lower() == 'true'

def get_secret(scope, key):
    """A secret from the Databricks secret scope, or locally from the environment variable of the same name."""
    if IS_DATABRICKS:
        return dbutils.secrets.get(scope=scope, key=key)
    try:
        return os.environ[key]
    except KeyError:
        raise RuntimeError(
            f"Secret {scope}/{key} is read from the {key} environment variable off Databricks; it is not set."
        ) from None

# COMMAND ----------

def _wb_dataframe_with_retry(series, attempts=3, backoff=2.0):
    """wb.data.DataFrame, with the whole fetch retried when a page fails in a way the session's
    Retry (above) does not cover: a body cut short after the headers arrived (the API does this
    mid-pagination now and then) or a response that does not parse."""
    for i in range(attempts):
        try:
            return wb.data.DataFrame(series, skipBlanks=True)
        except (requests.exceptions.ChunkedEncodingError, wb.APIResponseError):
            if i == attempts - 1:
                raise
            wait = backoff * 2 ** i
            print(f'wbgapi: {series} failed mid-fetch; retry {i + 1}/{attempts - 1} in {wait}s', flush=True)
            time.sleep(wait)

def wbgapi_fetch(indicators, col_names, data_source, extra_col_names_from_country_table=None, how: str = 'inner'):
    if extra_col_names_from_country_table is None:
        extra_col_names_from_country_table = []
    if how not in {'inner', 'outer', 'left', 'right'}:
        raise ValueError(f"Unsupported merge how='{how}'")
    long_dfs = []
    for series, col_name in zip(indicators, col_names):
        print(f'wbgapi: fetching {series}', flush=True)
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

# COMMAND ----------

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
    resp = http_get(UIS_API_URL, params=params, timeout=DEFAULT_TIMEOUT_SECONDS)
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
    resp = http_get(url, timeout=DEFAULT_TIMEOUT_SECONDS)
    resp.raise_for_status()
    return resp.content

# COMMAND ----------

import io
from datetime import datetime, timezone

# Delta defaults (7d/30d) would let VACUUM drop a snapshot before the next monthly refresh.
_RAW_TABLE_OPTIONS = {
    "delta.columnMapping.mode": "name",
    "delta.deletedFileRetentionDuration": "interval 365 days",
    "delta.logRetentionDuration": "interval 365 days",
}

def fetch_raw(source_url, table_name, parse=pd.read_csv, **parse_kwargs):
    """Overwrite table_name with a freshly fetched, parsed snapshot (a bronze table).

    On Databricks, Delta's transaction log is the audit trail (DESCRIBE HISTORY /
    VERSION AS OF). Locally the CSV is simply replaced.
    """
    resp = http_get(source_url, timeout=DEFAULT_TIMEOUT_SECONDS)
    resp.raise_for_status()
    df = parse(io.BytesIO(resp.content), **parse_kwargs)
    df['fetched_at'] = datetime.now(timezone.utc)
    write_table(df, table_name, delta_options=_RAW_TABLE_OPTIONS)

def versioned_dataframe(source_url, table_name, update_version, parse=pd.read_csv, **parse_kwargs):
    """Read table_name's cached snapshot, refreshing first if update_version or unset.

    A temporarily unreachable source yields stale data instead of a failure.
    """
    if update_version or not table_exists(table_name):
        fetch_raw(source_url, table_name, parse=parse, **parse_kwargs)
    return read_table(table_name).drop(columns='fetched_at')

# COMMAND ----------

# Subnational population from the US Census Bureau's international programs time series, for
# the population/<ISO3> notebooks that use it: the workbook is cached through versioned_dataframe
# (refreshed when the census_population_update_version widget / env var is true).

def _read_census_gov_excel(buf):
    xls = pd.ExcelFile(buf)
    target_sheet = next(sheet for sheet in xls.sheet_names if sheet.startswith('2'))
    df_raw = pd.read_excel(xls, sheet_name=target_sheet, skiprows=2, header=None)
    header = df_raw.iloc[1]
    df_raw.columns = header
    df_raw = df_raw.drop([0, 1, 2])
    # Some columns (e.g. ADM3_NAME/ADM4_NAME/NSO_CODE/NSO_NAME) are entirely blank for
    # countries without that breakdown — an all-null column round-trips through Spark as
    # NullType, which Delta can't persist. Keep only what get_pop_from_census_gov uses.
    keep_cols = [c for c in df_raw.columns if c in ('COUNTRY', 'CNTRY_NAME', 'ADM1_NAME', 'ADM_LEVEL') or 'BTOTL' in c]
    return df_raw[keep_cols]

def get_pop_from_census_gov(country_filename, timeseries='pepfar', update_version=False):
    url = f'https://www2.census.gov/programs-surveys/international-programs/tables/time-series/{timeseries}/{country_filename}.xlsx'
    table_name = f"{country_filename.replace('-', '_')}_census_raw"
    df_raw = versioned_dataframe(url, table_name, update_version, parse=_read_census_gov_excel)

    # Determine country name column
    country_col = None
    for col in ['COUNTRY', 'CNTRY_NAME']:
        if col in df_raw.columns:
            country_col = col
            break

    if country_col is None:
        raise ValueError(f"Neither 'COUNTRY' nor 'CNTRY_NAME' found in dataframe columns {df_raw.columns}")

    # Extract Total population columns
    df_pop_wide = df_raw[df_raw.ADM_LEVEL==1][[country_col, 'ADM1_NAME']+[x for x in df_raw.columns if 'BTOTL' in x]]
    df_pop = pd.melt(df_pop_wide, id_vars=[country_col, 'ADM1_NAME'], var_name='year', value_name='population')
    df_pop['year'] = df_pop['year'].str.extract(r'(\d+)').astype(int)
    df_pop.columns = ['country_name', 'adm1_name', 'year', 'population']

    # Modifications to the admin1 and county name and add data_source
    df_pop['country_name'] = df_pop['country_name'].str.title()
    df_pop['adm1_name'] = df_pop['adm1_name'].str.replace(r'[-/]+', ' ', regex=True).str.title()
    df_pop['data_source'] = url
    df_pop = df_pop.astype({'year': 'int', 'population': 'int'})
    df_pop = df_pop.sort_values(['adm1_name', 'year'], ignore_index=True)

    return df_pop

# COMMAND ----------

# Global Data Lab (globaldatalab.org), through the URL scheme the gdldata R package uses:
# <dataset>/download/[<year>/]<indicator+indicator>/?format=csv&token=... . The token is the
# GDL_API_TOKEN secret (DIMEBOOSTKEYVAULT) on Databricks and the environment variable of the
# same name off it: get_secret('DIMEBOOSTKEYVAULT', 'GDL_API_TOKEN').
GDL_BASEURL = 'https://globaldatalab.org'
# GDL's country names where they differ from the rest of the pipeline's (the World Bank API's)
GDL_COUNTRY_RENAMES = {'Congo Democratic Republic': 'Congo, Dem. Rep.', 'Chili': 'Chile'}

def gdl_download(token, dataset, indicators, year=None):
    """One download, every country: `indicators` of `dataset` for `year` (all years when None).
    Errors (a bad token, an exhausted quota) come back as an HTML page, so the body is checked.
    Parsed from the bytes as UTF-8: when the response declares no charset, resp.text would decode
    the accented region names as Latin-1."""
    years = f'{year}/' if year is not None else ''
    url = f"{GDL_BASEURL}/{dataset}/download/{years}{'+'.join(indicators)}/"
    resp = http_get(url, params={'format': 'csv', 'token': token, 'interpolation': 1}, headers={'Accept': 'text/csv'})
    resp.raise_for_status()
    if resp.content.lstrip().startswith(b'<'):
        raise RuntimeError(f'Global Data Lab returned an error page for {url}; check the token and the API quota')
    return pd.read_csv(io.BytesIO(resp.content))
