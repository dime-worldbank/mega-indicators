# Databricks notebook source
import requests
import os
import json
import pandas as pd

DATA_DIR = '/dbfs/mnt/DAP/data/admin1geoboundaries'
ADM1_GEO_FILENAME = f'{DATA_DIR}/admin1geoboundaries.geojson'
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
with open(ADM1_GEO_FILENAME, 'w', encoding='utf-8') as f:
    json.dump(combined, f, ensure_ascii=False, indent=2)
    
print(f'Exported {len(features)} admin 1 geo boundary records to {ADM1_GEO_FILENAME}')

# COMMAND ----------

with open(ADM1_GEO_FILENAME, 'r', encoding='utf-8') as f:
    boundaries = json.load(f)
    
df = pd.DataFrame([x['properties'] for x in boundaries['features']])
df = df.rename(columns = {'R':'region_code', 'R':'region', 'CN':'country_name','A1N':'admin1_region_raw', 'CWB3':'country_code'})
# different country code for Democratic republic of Congo
df['country_code'] = df['country_code'].replace('ZAR', 'COD')
df['boundary'] = [json.dumps(x['geometry']) for x in boundaries['features']]
df.sample(3)

# COMMAND ----------

# admin1 name corrections
correct_admin1_names = {
        ('BFA', 'Hauts-Bassins'): 'Hauts Bassins',
        ('BFA', 'Centre-Ouest'): 'Centre Ouest',
        ('BFA', 'Boucle-du-Mouhoun'): 'Boucle Du Mouhoun',
        ('BFA', 'Centre-Est'): 'Centre Est',
        ('BFA', 'Centre-Sud'):'Centre Sud Region Burkina Faso',
        ('BFA', 'Sud-Ouest'):'Sud Ouest',
        ('BFA', 'Centre-Nord'):  'Centre Nord',
        ('BFA', 'Est'): 'Est Region Burkina Faso',
        # check the accuracy of FATA and Khyber Pakhtunkhwa
        ('PAK', 'Federally Administered Tribal Areas'): 'Khyber Pakhtunkhwa',
        ('NGA', 'FCT, Abuja'): 'Federal Capital Territory',
        ('NGA', 'Nassarawa'): 'Nasarawa',
        ('KEN', 'Nairobi'): 'Nairobi City County',
        ('BTN', 'Ha'):'Haa',
        ('BTN', 'Samdrup-Jonkha'): 'Samdrup Jongkhar',
        ('BTN', 'Trashi Yangtse'): 'Trashiyangtse',
        ('BTN', 'Wangdue-Phodrang'): 'Wangduephodrang',
        #('COL', 'Buenaventura'), should be mapped to  'Valle Del Cauca' but this entry exists
        ('COL', 'Guajira'):  'La Guajira',
        ('COD', 'Bas-Congo'): 'Bas-congo',
        ('COD', 'Kasaï-Occidental'): 'Kasai-occidental',
        ('COD', 'Kasaï-Oriental'): 'Kasai-oriental',
        ('COD', 'Nord-Kivu'): 'Nord-kivu',
        ('COD', 'Province Orientale'): 'Orientale',
        ('COD', 'Sud-Kivu'): 'Sud-kivu',
}
# harmonize the subnational names
df['admin1_region'] = df.apply(lambda x: correct_admin1_names.get((x['country_code'], x['admin1_region_raw']), x['admin1_region_raw']), axis=1)

# COMMAND ----------

database_name = "indicator"

if not spark.catalog.databaseExists(database_name):
    print(f"Database '{database_name}' does not exist. Creating the database.")
    spark.sql(f"CREATE DATABASE {database_name}")

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").saveAsTable(f"{database_name}.admin1_boundaries")
