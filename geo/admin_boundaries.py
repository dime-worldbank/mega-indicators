# Databricks notebook source
!pip install shapely

# COMMAND ----------

import requests
import os
import json
from shapely.geometry import shape, mapping
import pandas as pd
DATA_DIR = '/Volumes/prd_mega/sboost4/vboost4/Workspace/auxiliary_data/admin1geoboundaries'
WB_ADM1_GEO_FILENAME = f'{DATA_DIR}/WB_admin1geoboundaries.geojson'
ALT_ADM1_GEO_FILENAME = f'{DATA_DIR}/ALT_admin1geoboundaries.geojson'

URL = 'https://services.arcgis.com/iQ1dY19aHwbSDYIF/arcgis/rest/services/ADM1_1km/FeatureServer/0/query'
X1 = 90
X2 = 180
Y = 66.51326044311188
SECTIONS = [
    (-X1, 0, 0, Y),
    (0, 0, X1, Y),
    (-X1, -Y, 0, 0),
    (0, -Y, X1, 0),
    (-X2, 0, -X1, Y),
    (X1, 0, X2, Y),
    (-X2, -Y, -X1, 0),
    (X1, -Y, X2, 0),
]

def get_params(xmin, ymin, xmax, ymax):
    params = {
        'where': '1=1',
        'outSR': 4326,
        'outFields': '*',
        'inSR': 4326,
        'geometry': f'{{"xmin":{xmin},"ymin":{ymin},"xmax":{xmax},"ymax":{ymax},"spatialReference":{{"wkid":4326}}}}',
        'geometryType': 'esriGeometryEnvelope',
        'spatialRel': 'esriSpatialRelIntersects',
        'geometryPrecision': 6,
        'resultType': 'tile',
        'f': 'geojson'
    }
    return params

feature_ids = set()
features = []
for section in SECTIONS:
    response = requests.get(URL, params=get_params(*section))
    more_features = response.json()['features']
    features += list(f for f in more_features if f['id'] not in feature_ids)
    ids = [f['id'] for f in more_features]
    feature_ids.update(ids)

if not os.path.exists(DATA_DIR):
    dbutils.fs.mkdirs(DATA_DIR)
combined = {
    'type': response.json()['type'],
    'features': features,
}
with open(WB_ADM1_GEO_FILENAME, 'w', encoding='utf-8') as f:
    json.dump(combined, f, ensure_ascii=False, indent=2)
    
print(f'Exported {len(features)} admin 1 geo boundary records to {WB_ADM1_GEO_FILENAME}')

# COMMAND ----------

# Obtaining suitable boundaries (not available within the WB boundaries) from other sources for select countries

def simplify_geometry(geometry, tolerance=0.01):
    geom_shape = shape(geometry)
    simplified_geom = geom_shape.simplify(tolerance, preserve_topology=True)
    return mapping(simplified_geom)

# Kenya boudnaries
KEN_URL = 'https://github.com/wmgeolab/geoBoundaries/raw/9469f09/releaseData/gbOpen/KEN/ADM1/geoBoundaries-KEN-ADM1.geojson'
boundaries_KEN = requests.get(KEN_URL).json()
features_KEN = [
    {
        'type': x['type'], 
        'properties': {
            'A1N': x['properties']['shapeName'],
            'R':'AFR',
            'C':'KE',
            'RN':'Africa',
            'CN':'Kenya',
            'CWB2': 'KE',
            'CWB3': 'KEN',
            }, 
        'geometry': {
            'type': x['geometry']['type'],
            'coordinates': [list(map(list, polygon)) for polygon in simplify_geometry(x['geometry'])['coordinates']]
        },
    }
    for x in boundaries_KEN['features']
]
assert len(boundaries_KEN['features']) == 47, f"Expected 47 subnational regions, got {len(boundaries_KEN['features'])}"


# COMMAND ----------

# Bangladesh boundaries
BGD_URL = 'https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/gbOpen/BGD/ADM1/geoBoundaries-BGD-ADM1.geojson'
boundaries_BGD = requests.get(BGD_URL).json()
features_BGD = [
    {
        'type': x['type'], 
        'properties': {
            'A1N': x['properties']['shapeName'],
            'R':'SAR',
            'C':'BD',
            'RN':'South Asia',
            'CN':'Bangladesh',
            'CWB2': 'BD',
            'CWB3': 'BGD',
            }, 
        'geometry': {
            'type': x['geometry']['type'],
            'coordinates': [list(map(list, polygon)) for polygon in simplify_geometry(x['geometry'])['coordinates']]
        },
    }
    for x in boundaries_BGD['features']
]
assert len(boundaries_BGD['features']) == 8, f"Expected 47 subnational regions, got {len(boundaries_BGD['features'])}"

boundaries_alt = {'type':'FeatureCollection', 'features':features_KEN+features_BGD}
with open(ALT_ADM1_GEO_FILENAME, 'w', encoding='utf-8') as f:
    json.dump(boundaries_alt, f, ensure_ascii=False, indent=2)
