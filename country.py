# Databricks notebook source
# MAGIC %pip install wbgapi
# MAGIC %pip install shapely

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# The country table: World Bank API metadata for every economy, the map's initial view
# (the centroid of the admin-1 boundaries, and a hand-set zoom) and the currency. Plain
# pandas on both sides. The currency comes from the corporate reference table where it is
# reachable (Databricks, or a CSV export of it under DATA_ROOT) and otherwise from the
# FALLBACK_CURRENCIES dictionary below.
import json

import pandas as pd
from shapely.geometry import shape, MultiPolygon, Polygon

# COMMAND ----------

df = wb.economy.DataFrame()

# COMMAND ----------

COL_NAME_MAP = {
    "id": "country_code",
    "name": "country_name",
    "lendingType": "lending_type",
    "incomeLevel": "income_level",
    "capitalCity": "capital_city",
    "aggregate": "is_aggregate",
}
COL_NAMES = [
    "country_code",
    "country_name",
    "longitude",
    "latitude",
    "region",
    "lending_type",
    "income_level",
    "capital_city",
    "is_aggregate"
]
countries = df.reset_index().rename(columns=COL_NAME_MAP)[COL_NAMES]

# COMMAND ----------

# add display_lat, display_lon and zoom to the coutnries table
zoom = {
    "Albania": 5.7,
    "Bangladesh": 5,
    "Bhutan": 6,
    "Burkina Faso": 4.7,
    "Burundi": 5.7,
    "Colombia": 3.6,
    "Kenya": 4.35,
    "Mozambique": 3.35,
    "Nigeria": 4.2,
    "Pakistan": 3.7,
    "Paraguay": 4.4,
    "Tunisia": 4.5,
    "Chile" : 2.0,
    "Liberia": 5.5,
    "Togo": 5.0
}
def get_zoom(country):
    return float(zoom.get(country, 3.0))  # TODO: replace this dict by a function that can compute this from the boundaries

def compute_country_centroid(boundaries_list):
    polygons = []
    for boundary_str in boundaries_list:
        geom = shape(json.loads(boundary_str))

        if isinstance(geom, Polygon):
            polygons.append(geom)
        elif isinstance(geom, MultiPolygon):
            polygons.extend(geom.geoms)

    if len(polygons) > 1:
        multi_polygon = MultiPolygon(polygons)
    else:
        multi_polygon = polygons[0]

    centroid = multi_polygon.centroid
    return (centroid.x, centroid.y)

admin1_boundaries = read_table('admin1_boundaries_gold', columns=['country_name', 'country_code_iso2', 'boundary'])
if admin1_boundaries.empty:
    raise RuntimeError('admin1_boundaries_gold is empty: run geo/admin_boundaries_transform_load.py first'
                       + (f"; with COUNTRY_NAME={COUNTRY_NAME!r} set, that must be the country's spelling in the boundaries file too" if COUNTRY_NAME else ''))
centroid_df = (admin1_boundaries.groupby('country_name')
    .agg(country_code_iso2=('country_code_iso2', 'first'), all_boundaries=('boundary', list))
    .reset_index())
centroid_df[['display_lon', 'display_lat']] = centroid_df['all_boundaries'].apply(lambda b: pd.Series(compute_country_centroid(b)))
centroid_df = centroid_df.drop(columns='all_boundaries')

sdf = countries.merge(centroid_df, on='country_name', how='left')
sdf['zoom'] = sdf['country_name'].map(get_zoom)

# COMMAND ----------

# v_dim_country would be more suitable for currency/country data, but it currently lacks comprehensive data. May switch to this table in the future.
CURRENCY_TABLE = "prd_corpdata.dm_reference_gold.v_dim_country_currency_exchange_rate"

# Fallback off Databricks, when no CSV export of the corporate table is under DATA_ROOT: ISO 4217 currencies of the countries
# in the pipeline, keyed by ISO3 code (names vary by source, codes do not; the corporate
# spelling is used where known). Add a country here when it is added to the pipeline.
FALLBACK_CURRENCIES = {
    'ALB': ('Lek', 'ALL'),
    'BDI': ('Burundi Franc', 'BIF'),
    'BFA': ('C.F.A. Francs BCEAO', 'XOF'),
    'BGD': ('Taka', 'BDT'),
    'BTN': ('Ngultrum', 'BTN'),
    'CHL': ('Chilean Peso', 'CLP'),
    'COD': ('Congolese Franc', 'CDF'),
    'COL': ('Colombian Peso', 'COP'),
    'GHA': ('Ghana Cedi', 'GHS'),
    'KEN': ('Kenyan Shilling', 'KES'),
    'LBR': ('Liberian Dollar', 'LRD'),
    'MOZ': ('Mozambique Metical', 'MZN'),
    'NGA': ('Naira', 'NGN'),
    'PAK': ('Pakistan Rupee', 'PKR'),
    'PRY': ('Guarani', 'PYG'),
    'TGO': ('C.F.A. Francs BCEAO', 'XOF'),
    'TUN': ('Tunisian Dinar', 'TND'),
    'ZAF': ('Rand', 'ZAR'),
}

if IS_DATABRICKS or table_exists(CURRENCY_TABLE):  # on Databricks an unreadable table fails the run rather than degrading to the fallback
    base_df = read_table(CURRENCY_TABLE, columns=['cntry_code', 'ccy_src_name', 'ccy_src_code', 'ccy_exch_rate_ref_date'])
    # the latest row per country, joined on the ISO2 code
    currency_df = (base_df.sort_values('ccy_exch_rate_ref_date', ascending=False)
        .drop_duplicates('cntry_code')
        .rename(columns={'cntry_code': 'country_code', 'ccy_src_name': 'currency_name', 'ccy_src_code': 'currency_code'})
        [['country_code', 'currency_name', 'currency_code']])
    joined_df = (sdf.merge(currency_df, left_on='country_code_iso2', right_on='country_code', how='left', suffixes=('', '_currency'))
        .drop(columns='country_code_currency'))
else:
    print(f"{CURRENCY_TABLE} is not available; taking currencies from FALLBACK_CURRENCIES")
    currency_df = pd.DataFrame([{'country_code': code, 'currency_name': name, 'currency_code': ccy}
                                for code, (name, ccy) in FALLBACK_CURRENCIES.items()])
    joined_df = sdf.merge(currency_df, on='country_code', how='left')

# COMMAND ----------

joined_df['country_code_iso3'] = joined_df['country_code']
joined_df = joined_df[['country_name', 'country_code', 'longitude', 'latitude', 'region', 'lending_type', 'income_level',
                       'capital_city', 'is_aggregate', 'country_code_iso2', 'display_lon', 'display_lat', 'zoom',
                       'currency_name', 'currency_code', 'country_code_iso3']]
joined_df

# COMMAND ----------

write_table(joined_df, 'country')
