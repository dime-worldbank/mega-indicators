from utils import *

# Reads the Admin 1 GeoJSON that admin_boundaries_extract.py downloaded and
# writes admin1_boundaries_gold: one row per admin-1 region, boundary as GeoJSON text.
# (On Databricks a DLT pipeline also applies region-name corrections and polygon unions
# for other countries; Togo needs none.) admin0_disputed_boundaries_gold is written empty: Togo has no disputed areas.
import json
import pandas as pd

with open(f'{VOLUME_ROOT_PATH}/auxiliary_data/admin1geoboundaries/World Bank Official Boundaries - Admin 1.geojson', encoding='utf-8') as f:
    features = json.load(f)['features']

df = pd.DataFrame([x['properties'] for x in features])
df['boundary'] = [json.dumps(x['geometry']) for x in features]
df = df.rename(columns={'NAM_0': 'country_name', 'ISO_A3': 'country_code', 'ISO_A2': 'country_code_iso2', 'NAM_1': 'admin1_region'})
df['country_name'] = df['country_name'].replace('Democratic Republic of Congo', 'Congo, Dem. Rep.')
write_table(df[['country_name', 'country_code', 'country_code_iso2', 'admin1_region', 'boundary']], 'admin1_boundaries_gold')

write_table(pd.DataFrame(columns=['country_name', 'region_name', 'boundary', 'country_code_iso2']), 'admin0_disputed_boundaries_gold')
