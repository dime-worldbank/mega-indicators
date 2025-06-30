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

DATA_DIR = '/Volumes/prd_mega/sboost4/vboost4/Workspace/auxiliary_data/admin1geoboundaries'

# admin1 name corrections
correct_admin1_names = {
        ('BFA', 'Hauts-bassins'): 'Hauts Bassins',
        ('BFA', 'Centre-ouest'): 'Centre Ouest',	
        ('BFA', 'Centre-est'): 'Centre Est',
        ('BFA', 'Centre-sud'):'Centre Sud Region Burkina Faso',
        ('BFA', 'Sud-ouest'):'Sud Ouest',
        ('BFA', 'Centre-nord'):  'Centre Nord',
        ('BFA', 'Est'): 'Est Region Burkina Faso',
        ('NGA', 'Nassarawa'): 'Nasarawa',
        ('KEN', 'Nairobi'): 'Nairobi City County',
        ('BTN', 'Samdrupjongkhar'): 'Samdrup Jongkhar',
        #('COL', 'Buenaventura'), should be mapped to  'Valle Del Cauca' but this entry exists
        ('COL', 'La Guajira'):  'La Guajira',
        #TODO rename boost admin data for consistency
        # ('COD', 'Bas-Congo'): 'Bas-congo', // now represented as Kongo Central
        #TODO Kasai-occidental is obsolete name: adjust boost data
        ('COD', 'Kasaï'): 'Kasai-occidental',
        ('COD', 'Kasaï-Central'): 'Kasai-central',
        ('COD', 'Kasaï-Oriental'): 'Kasai-oriental',
        ('COD', 'Nord-Kivu'): 'Nord-kivu',
        #Orientale is now split into 4 smaller providences
        # ('COD', 'Province Orientale'): 'Orientale',
        ('COD', 'Sud-Kivu'): 'Sud-kivu',
        ('CHL', 'Metropolitana'): 'Región Metropolitana de Santiago',
        ('CHL', 'Valparaiso'):'Valparaíso',
        ('CHL', 'Magallanes y Antartica chilena'):'Magallanes y la Antártica Chilena',
        ('CHL', 'Aisen del Gral. Carlos Ibañez del Campo'):'Aysén',
        ('CHL', "Libertador Gral. Bernardo O'Higgins"):"Libertador General Bernardo O'Higgins",
        ('CHL', 'Araucania'):'Araucanía',
        ('CHL', 'Tarapaca'):'Tarapacá',
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


@dlt.table(name=f'admin1_boundaries_bronze')
def admin1_boundaries_bronze():
    with open(f'{DATA_DIR}/World Bank Official Boundaries - Admin 1.geojson', 'r', encoding='utf-8') as f:
    boundaries = json.load(f) 
    df = pd.DataFrame([x['properties'] for x in boundaries['features']])
    df = df.rename(columns = {"WB_REGION": "region_code", "ISO_A2": "C", "NAM_0": "country_name","NAM_1": "admin1_region_raw", "ISO_A3": "country_code"})
    # # different country code and country name for Democratic republic of Congo
    #TODO adjust the BOOST data to match the World Bank data instead of the other way around
    df['country_name'] = df['country_name'].replace('Democratic Republic of Congo', 'Congo, Dem. Rep.')
    df['boundary'] = [json.dumps(x['geometry']) for x in boundaries['features']]
    df['admin1_region'] = df.apply(lambda x: correct_admin1_names.
    get((x['country_code'], x['admin1_region_raw']), x['admin1_region_raw']), axis=1)
    bronze = spark.createDataFrame(df)
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
