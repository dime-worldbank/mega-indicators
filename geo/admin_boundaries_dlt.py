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
from functools import reduce
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from shapely.ops import unary_union
ADMIN1_DATA_DIR = '/Volumes/prd_mega/sboost4/vboost4/Workspace/auxiliary_data/admin1geoboundaries'

# admin1 name corrections
correct_admin1_names = {
        ("BGD", "Barishal"): 'Barisal',
        ("BGD", "Chattogram"): 'Chittagong',
        ("BTN", "Monggar"): 'Mongar',
        ('BTN', 'Samdrupjongkhar'): 'Samdrup Jongkhar',
        ('BFA', 'Hauts-bassins'): 'Hauts Bassins',
        ('BFA', 'Centre-ouest'): 'Centre Ouest',	
        ('BFA', 'Centre-est'): 'Centre Est',
        ('BFA', 'Centre-sud'):'Centre Sud Region Burkina Faso',
        ('BFA', 'Sud-ouest'):'Sud Ouest',
        ('BFA', 'Centre-nord'):  'Centre Nord',
        ('BFA', 'Est'): 'Est Region Burkina Faso',
        ('CHL', 'Biobio'): 'Biobío',
        ('NGA', 'Nassarawa'): 'Nasarawa',
        ('KEN', 'Nairobi'): 'Nairobi City County',
        #('COL', 'Buenaventura'), should be mapped to  'Valle Del Cauca' but this entry exists
        ('COL', 'La Guajira'):  'La Guajira',
        ('COL', 'Atlántico'):  'Atlantico',
        ('COL', 'Bolívar'):  'Bolivar',
        ('COL', 'Boyacá'):  'Boyaca',
        ('COL', 'Caquetá'):  'Caqueta',
        ('COL', 'Chocó'):  'Choco',
        ('COL', 'Córdoba'):  'Cordoba',
        ('COL', 'Guainía'):  'Guainia',
        ('COL', 'Nariño'):  'Narino',
        ('COL', 'Archipiélago de San Andrés, Providencia y Santa Catalina'):  'San Andres Y Providencia',
        ('COL', 'Vaupés'):  'Vaupes',
        ('COL', 'Bogotá, D.C.'):  'Bogota',
        ('COL', 'Valle del Cauca'):  'Valle Del Cauca',
        #TODO rename boost admin data for consistency
        ('COD', 'Bas-Uélé'): 'Bas Uele',
        ('COD', 'Haut-Katanga'): 'Haut Katanga',
        ('COD', 'Haut-Lomami'): 'Haut Lomami',
        ('COD', 'Haut-Uélé'): 'Haut Uele',
        ('COD', 'Kasai-central'): 'Kasai Central',
        # Note: This mapping specifically uses 'Kasai-occidental' as the input key
        # and maps it to 'Kasai' as per your provided data.
        # In the DRC 2015 re-division, Kasai-Occidental was an old province that split into new ones.
        ('COD', 'Kasai-occidental'): 'Kasai',
        ('COD', 'Kasai-oriental'): 'Kasai Oriental',
        ('COD', 'Kinshasa'): 'Ville Province De Kinshasa',
        ('COD', 'Mai-Ndombe'): 'Mai Ndombe',
        # Note: The provided data for Nord-Ubangi was incomplete,
        # but based on general DRC province re-division, it maps to Équateur (old province).
        ('COD', 'Nord-Ubangi'): 'Équateur',
        ('COD', 'Nord-kivu'): 'Nord Kivu',
        ('COD', 'Sud-Ubangi'): 'Sud Ubangi',
        ('COD', 'Sud-kivu'): 'Sud Kivu',
        ('COD', 'Équateur'): 'Equateur', # Kept due to casing difference 'Équateur' vs 'Equateur'
        ('CHL', 'Metropolitana'): 'Región Metropolitana de Santiago',
        ('CHL', 'Valparaiso'):'Valparaíso',
        ('CHL', 'Magallanes y Antartica chilena'):'Magallanes y la Antártica Chilena',
        ('CHL', 'Aisen del Gral. Carlos Ibañez del Campo'):'Aysén',
        ('CHL', "Libertador Gral. Bernardo O'Higgins"):"Libertador General Bernardo O'Higgins",
        ('CHL', 'Araucania'):'Araucanía',
        ('CHL', 'Tarapaca'):'Tarapacá',
        ('CHL', 'Los Rios'):'Los Ríos',

        ('MOZ', 'Maputo (city)'): 'Cidade de Maputo',
        ('MOZ', 'Zambézia'): 'Zambezia',
        ('PAK', 'Islamabad Capital Territory'): 'Federal Capital Territory',
        ('ZAF', 'Kwazulu-natal'): 'Kwa-Zulu Natal',
        ('ZAF', 'North West'): 'North-west',
        ('ZAF', 'Limpopo'): 'Northern Province',
        ('TUN', 'Mednine'): 'Medenine',
        ('TUN', 'Sidi bouzid'): 'Sidi Bouz',
        ('TUN', 'Le kef'): 'Le Kef',
}

ghana_regions_new_to_old_map = {
    # Ghana (GHA) province/region name corrections/mappings
    # This maps the new (post-2019) regions to their old (pre-2019) counterparts.
    # The dictionary has been simplified to {new_region_name: old_region_name}.

    # Regions that were split from 'Brong Ahafo'
    'Ahafo': 'Brong Ahafo',
    'Bono': 'Brong Ahafo',
    'Bono East': 'Brong Ahafo',

    # Regions that were split from 'Northern'
    'Northern East': 'Northern', # Often referred to as North East Region
    'Savannah': 'Northern',
    'Oti': 'Volta', # Oti was created from the Volta Region

    # Regions that were split from 'Western'
    'Western North': 'Western',

    # Regions that largely retained their names and boundaries
    'Ashanti': 'Ashanti',
    'Central': 'Central',
    'Eastern': 'Eastern',
    'Greater Accra': 'Greater Accra',
    'Northern': 'Northern', # Remnant of the old Northern Region
    'Upper East': 'Upper East',
    'Upper West': 'Upper West',
    'Volta': 'Volta', # Remnant of the old Volta Region
    'Western': 'Western', # Remnant of the old Western Region
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
    'Malesi E Madhe': 'Shkoder',
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

def harmonize_admin1_regions(bronze_df, country_name, region_to_county_dict):
    """
    Harmonizes admin1 regions for a given country using a mapping dictionary.
    Groups by the harmonized region and unions polygons.
    """

    # Filter for the specified country
    country_df = bronze_df.filter(col('country_name') == country_name)
    # Create the mapping as a Spark map
    region_map = create_map([lit(x) for x in chain(*region_to_county_dict.items())])
    # Harmonize the region names
    country_df = country_df.withColumn(
        "admin1_region",
        coalesce(region_map[col("admin1_region")], col("admin1_region"))
    )
    # UDF for unioning polygons
    union_udf = udf(union_polygons, StringType())
    # Group by harmonized region and union polygons
    country_mod = country_df.groupBy("admin1_region").agg(
        first("country_name").alias("country_name"),
        first("country_code").alias("country_code"),
        first("C").alias("C"),
        first("region_code").alias("region_code"),
        union_udf(collect_list("boundary")).alias("boundary")
    )
    return country_mod

# COMMAND ----------

@dlt.table(name=f'admin1_boundaries_bronze')
def admin1_boundaries_bronze():
    with open(f'{ADMIN1_DATA_DIR}/World Bank Official Boundaries - Admin 1.geojson', 'r', encoding='utf-8') as f:
        boundaries = json.load(f)
    df = pd.DataFrame([x['properties'] for x in boundaries['features']])
    df = df.rename(columns = {"WB_REGION": "region_code", "ISO_A2": "C", "NAM_0": "country_name","NAM_1": "admin1_region_raw", "ISO_A3": "country_code"})
    # different country name for Democratic republic of Congo
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
    # Harmonize for Albania (and you can call for other countries as needed)
    alb_bronze_mod = harmonize_admin1_regions(bronze, 'Albania', albania_region_to_county)
    print(f"Number of rows in the ALBANIA dataframe: {alb_bronze_mod.toPandas().shape[0]}")
    gha_bronze_mod = harmonize_admin1_regions(bronze, 'Ghana', ghana_regions_new_to_old_map)
    print(f"Number of rows in the Ghana dataframe: {gha_bronze_mod.toPandas().shape[0]}")
    common_columns = list(set(bronze.columns).intersection(set(alb_bronze_mod.columns)))
    bronze_filtered = bronze.filter(~col('country_name').isin(['Albania', "Ghana"])).select(common_columns)
    dfs = [bronze_filtered] + [alb_bronze_mod.select(common_columns), gha_bronze_mod.select(common_columns)]
    silver = reduce(lambda df1, df2: df1.unionByName(df2), dfs)
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

# COMMAND ----------

import json
import pandas as pd
import dlt

ADMIN0_DATA_DIR = '/Volumes/prd_mega/sboost4/vboost4/Workspace/auxiliary_data/admin0geoboundaries'
disputed_area_country_map = {
    'Ilemi Triangle': ['Kenya', 'South Sudan'],
    #TODO add more countries: refer to map department's notes
}

@dlt.table(name=f'admin0_disputed_boundaries_bronze')
def admin1_boundaries_bronze():
    with open(f'{ADMIN0_DATA_DIR}/World Bank Official Boundaries - Admin 0_all_layers.geojson', 'r', encoding='utf-8') as f:
        boundaries = json.load(f)
    df = pd.DataFrame([x['properties'] for x in boundaries['features']])
    df['boundary'] = [json.dumps(x['geometry']) for x in boundaries['features']]
    df = df.rename(columns = {"WB_REGION": "region_code", "ISO_A2": "C", "NAM_0": "region_name"})
    df = df[df.WB_STATUS == 'Non-determined legal status area']
    return spark.createDataFrame(df.astype(str))

@dlt.table(name=f'admin0_disputed_boundaries_silver')
def admin0_disputed_boundaries_silver():
    bronze = dlt.read('admin0_disputed_boundaries_bronze')
    flattened_data = []

    for region, countries in disputed_area_country_map.items():
        for country in countries:
            flattened_data.append({'region_name': region, 'country': country})
    disputed_region_country = spark.createDataFrame(flattened_data)
    silver = bronze.join(disputed_region_country, on='region_name', how='inner')
    return silver

@dlt.table(name=f'admin0_disputed_boundaries_gold')
def admin0_disputed_boundaries_gold():
    return (dlt.read(f'admin0_disputed_boundaries_silver')
        .select(col('country').alias("country_name"),
                'region_name',
                'boundary',
                )
    )   
   


