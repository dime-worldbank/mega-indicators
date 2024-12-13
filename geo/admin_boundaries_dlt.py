# Databricks notebook source
!pip install shapely
dbutils.library.restartPython()

# COMMAND ----------

import requests
import os
import dlt
import pandas as pd
import json
from itertools import chain
from shapely.geometry import Polygon, shape, MultiPolygon
from pyspark.sql.functions import col, first, collect_list, StringType, udf, when, lit, create_map, coalesce
from pyspark.sql.types import StructType, StructField, DoubleType
from shapely.ops import unary_union

DATA_DIR = '/dbfs/mnt/DAP/data/admin1geoboundaries'

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
        ('PAK', 'Sind'): 'Sindh',
        ('BGD', 'Rajshani'): 'Rajshahi',
        ('CHL', 'Ocean Islands'): '',
        ('CHL', 'Metropolitana (xiii)'): 'Región Metropolitana de Santiago',
        ('CHL', 'Biobio (viii)'):'Biobío', 
        ('CHL', 'Valparaiso (v)'):'Valparaíso',
        ('CHL', 'Los Lagos (x)'):'Los Lagos',
        ('CHL', 'Magallanes (xii)'):'Magallanes y la Antártica Chilena',
        ('CHL', 'Atacama (iii)'):'Atacama',
        ('CHL', 'Coquimbo (iv)'):'Coquimbo',
        ('CHL', 'Aysen Del Gen.d.c. (xi)'):'Aysén',
        ('CHL', 'Libertador (vi)'):"Libertador General Bernardo O'Higgins",
        ('CHL', 'Antofagasta (ii)'):'Antofagasta',
        ('CHL', 'Maule (vii)'):'Maule',
        ('CHL', 'Araucania (ix)'):'Araucanía',
        ('CHL', 'Tarapaca (i)'):'Tarapacá',
}

albania_region_to_county = {
    'Gjirokaster': 'Gjirokaster',
    'Kolonje': 'Korce',
    'Berat': 'Berat',
    'Devoll': 'Korce',
    'Pogradec': 'Korce',
    'Gramsh': 'Elbasan',
    'Tirane': 'Tirane',
    'Tepelene': 'Gjirokaster',
    'Kukes': 'Kukes',
    'Shkoder': 'Shkoder',
    'Elbasan': 'Elbasan',
    'Kavaje': 'Tirane',
    'Mirdite': 'Lezhe',
    'Has': 'Kukes',
    'Peqin': 'Elbasan',
    'Librazhd': 'Elbasan',
    'Lezhe': 'Lezhe',
    'Skrapar': 'Berat',
    'Fier': 'Fier',
    'Bulqize': 'Diber',
    'Kucove': 'Berat',
    'Mallakaster': 'Fier',
    'Diber': 'Diber',
    'Puke': 'Shkoder',
    'Tropoje': 'Kukes',
    'Kurbin': 'Lezhe',
    'Vlore': 'Vlore',
    'Mat': 'Diber',
    'Durres': 'Durres',
    'Sarande': 'Vlore',
    'Malesi e Madhe': 'Shkoder',
    'Korce': 'Korce',
    'Permet': 'Gjirokaster',
    'Lushnje': 'Fier',
    'Kruje': 'Durres',
    'Delvine': 'Vlore'
}

def union_polygons(polygon_list):
    polygons = [shape(json.loads(p)) for p in polygon_list]
    union_polygon = unary_union(polygons)
    return json.dumps(union_polygon.__geo_interface__)


@dlt.table(name=f'admin1_boundaries_bronze1')
def admin1_boundaries_bronze1():
    with open(f'{DATA_DIR}/WB_admin1geoboundaries.geojson', 'r', encoding='utf-8') as f:
        boundaries = json.load(f) 

    df = pd.DataFrame([x['properties'] for x in boundaries['features']])
    df = df.rename(columns = {'R':'region_code', 'RN':'region', 'CN':'country_name','A1N':'admin1_region_raw', 'CWB3':'country_code'})
    # different country code and country name for Democratic republic of Congo
    df['country_code'] = df['country_code'].replace('ZAR', 'COD')
    df['country_name'] = df['country_name'].replace('Congo, Democratic Republic of', 'Congo, Dem. Rep.')

    df['boundary'] = [json.dumps(x['geometry']) for x in boundaries['features']]
    df['admin1_region'] = df.apply(lambda x: correct_admin1_names.
    get((x['country_code'], x['admin1_region_raw']), x['admin1_region_raw']), axis=1)
    
    # manual correction to some changes to WB boundaries data
    # correct the region Beja
    beja_PRT = df[(df['admin1_region_raw'] == 'Beja') & (df['country_name']=='Portugal')].copy()
    beja_TUN = beja_PRT.copy()
    polygons = json.loads(beja_PRT.iloc[0]['boundary'])['coordinates']
    df.loc[beja_PRT.index, 'boundary'] = json.dumps({"type":"Polygon", "coordinates": polygons[1]})
    beja_TUN['boundary'] = json.dumps({"type":"Polygon", "coordinates": polygons[0]})
    beja_TUN['country_name'] = "Tunisia"
    beja_TUN['country_code'] = "TUN"
    beja_TUN['region_code'] = "MNA"
    beja_TUN['region'] = "Middle East and North Africa"
    beja_TUN['C'] = "TN"
    beja_TUN['CWB2'] = "TN"

    df = pd.concat([df, beja_TUN], ignore_index=True)
    bronze1 = spark.createDataFrame(df)
    return bronze1

@dlt.table(name=f'admin1_boundaries_bronze2')
def admin1_boundaries_bronze2():
    with open(f'{DATA_DIR}/ALT_admin1geoboundaries.geojson', 'r', encoding='utf-8') as f:
        boundaries = json.load(f) 

    df = pd.DataFrame([x['properties'] for x in boundaries['features']])
    df = df.rename(columns = {'R':'region_code', 'RN':'region', 'CN':'country_name','A1N':'admin1_region_raw', 'CWB3':'country_code'})
    df['boundary'] = [json.dumps(x['geometry']) for x in boundaries['features']]
    df['admin1_region'] = df.apply(lambda x: correct_admin1_names.
    get((x['country_code'], x['admin1_region_raw']), x['admin1_region_raw']), axis=1)
    bronze2 = spark.createDataFrame(df)
    return bronze2

@dlt.table(name=f'admin1_boundaries_bronze')
def admin1_boundaries_bronze_combined():
    # Remove countries that need to be substituted by the alternate boundaries
    bronze1 = dlt.read(f'admin1_boundaries_bronze1').filter(~col('country_name').isin('Kenya', 'Bangladesh'))
    bronze2 = dlt.read(f'admin1_boundaries_bronze2')
    bronze1_columns, bronze2_columns = set(bronze1.columns), set(bronze2.columns)
    bronze2_missing_columns = bronze1_columns - bronze2_columns
    for column in bronze2_missing_columns:
        bronze2 = bronze2.withColumn(column, lit(None))
    bronze = bronze1.unionByName(bronze2)
    return bronze

@dlt.table(name='admin1_boundaries_silver')
def admin1_boundaries_silver():
    bronze = dlt.read('admin1_boundaries_bronze')
    print(f"Number of ENTRIES: {bronze.toPandas().shape[0]}")
    # Edit Albania boundaries
    alb_bronze = bronze.filter(col('country_name') == 'Albania')
    print(f"Number of rows in the ALBANIA dataframe: {alb_bronze.toPandas().shape[0]}")
    alb_map = create_map([lit(x) for x in chain(*albania_region_to_county.items())])
    
    alb_bronze = alb_bronze.withColumn(
        "admin1_region",
        coalesce(alb_map[col("admin1_region")], col("admin1_region"))
    )
    union_udf = udf(union_polygons, StringType())
    alb_bronze_mod = alb_bronze.groupBy("admin1_region").agg(
        first("country_name").alias("country_name"),
        first("country_code").alias("country_code"),
        first("region").alias("region"),
        first("C").alias("C"),
        first("region_code").alias("region_code"),
        first("CWB2").alias("CWB2"),
        union_udf(collect_list("boundary")).alias("boundary")
    )
    common_columns = list(set(bronze.columns).intersection(set(alb_bronze_mod.columns)))
    bronze_filtered = bronze.filter(col('country_name') != 'Albania').select(common_columns)
    silver = bronze_filtered.unionByName(alb_bronze_mod.select(common_columns))
    return silver

@dlt.table(name=f'admin1_boundaries_gold')
def admin1_boundaries_gold():
    return (dlt.read(f'admin1_boundaries_silver')
        .select('country_name',
                'country_code',
                'admin1_region',
                'boundary',
                )
    )
