from utils import *

import pandas as pd
import requests

# The country table: one row, in the same columns as the Databricks table. The codes,
# name, capital and coordinates come from the World Bank API; the map's initial view
# and the currency are not published anywhere the pipeline can fetch, so they are set here.
COUNTRY_CODE = 'TGO'
DISPLAY = {'display_lon': 0.9772626998914765, 'display_lat': 8.532927773990984, 'zoom': 5.0}
CURRENCY = {'currency_name': 'C.F.A. Francs BCEAO', 'currency_code': 'XOF'}

resp = requests.get(f'https://api.worldbank.org/v2/country/{COUNTRY_CODE}?format=json', timeout=DEFAULT_TIMEOUT_SECONDS)
resp.raise_for_status()
record = resp.json()[1][0]

df = pd.DataFrame([{
    'country_name': record['name'],
    'country_code': record['id'],
    'longitude': float(record['longitude']),
    'latitude': float(record['latitude']),
    'region': record['region']['id'],
    'lending_type': record['lendingType']['id'],
    'income_level': record['incomeLevel']['id'],
    'capital_city': record['capitalCity'],
    'is_aggregate': record['region']['id'] == 'NA',  # how the API marks aggregates such as WLD
    'country_code_iso2': record['iso2Code'],
    **DISPLAY,
    **CURRENCY,
    'country_code_iso3': record['id'],
}])

write_table(df, 'country')
