# Databricks notebook source
# MAGIC %pip install shapely

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

# Admin-1 boundaries, and the admin-0 disputed areas, from the World Bank Official
# Boundaries GeoJSON files that admin_boundaries_extract.py downloads: one row per region
# with the boundary as GeoJSON text, region names corrected to match the BOOST data, and
# the Albania and Ghana regions merged into the units BOOST reports on. Plain pandas with
# shapely on both sides (it replaced a DLT pipeline).
import json

import pandas as pd
from shapely.geometry import shape
from shapely.ops import unary_union

ADMIN1_GEOJSON = f'{VOLUME_ROOT_PATH}/auxiliary_data/admin1geoboundaries/World Bank Official Boundaries - Admin 1.geojson'
ADMIN0_GEOJSON = f'{VOLUME_ROOT_PATH}/auxiliary_data/admin0geoboundaries/World Bank Official Boundaries - Admin 0_all_layers.geojson'

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
        ('NGA', 'Akwa lbom'): 'Akwa Ibom',  # WB geojson misspells with lowercase 'l'
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
    return json.dumps(unary_union(polygons).__geo_interface__)


def harmonize_admin1_regions(bronze_df, country_name, region_to_county_dict):
    """One country's rows with admin1_region mapped through the dict and the polygons of
    each resulting region unioned: one row per harmonized region."""
    country_df = bronze_df[bronze_df['country_name'] == country_name].copy()
    country_df['admin1_region'] = country_df['admin1_region'].map(region_to_county_dict).fillna(country_df['admin1_region'])
    return (country_df.groupby('admin1_region', as_index=False)
            .agg(country_name=('country_name', 'first'), country_code=('country_code', 'first'),
                 C=('C', 'first'), region_code=('region_code', 'first'), boundary=('boundary', union_polygons)))


def geojson_frame(path):
    """The features' properties, plus the geometry as GeoJSON text in `boundary`."""
    with open(path, encoding='utf-8') as f:
        boundaries = json.load(f)
    df = pd.DataFrame([x['properties'] for x in boundaries['features']])
    df['boundary'] = [json.dumps(x['geometry']) for x in boundaries['features']]
    return df

# COMMAND ----------

bronze = geojson_frame(ADMIN1_GEOJSON)
bronze = bronze.rename(columns={"WB_REGION": "region_code", "ISO_A2": "C", "NAM_0": "country_name", "NAM_1": "admin1_region_raw", "ISO_A3": "country_code"})
# different country name for Democratic republic of Congo
#TODO adjust the BOOST data to match the World Bank data instead of the other way around
bronze['country_name'] = bronze['country_name'].replace('Democratic Republic of Congo', 'Congo, Dem. Rep.')
bronze['admin1_region'] = [correct_admin1_names.get((code, raw), raw) for code, raw in zip(bronze['country_code'], bronze['admin1_region_raw'])]
print(f"Number of ENTRIES: {len(bronze)}")
write_table(bronze, 'admin1_boundaries_bronze')
# COMMAND ----------

# Harmonize for Albania (and you can call for other countries as needed)
alb_bronze_mod = harmonize_admin1_regions(bronze, 'Albania', albania_region_to_county)
print(f"Number of rows in the ALBANIA dataframe: {len(alb_bronze_mod)}")
gha_bronze_mod = harmonize_admin1_regions(bronze, 'Ghana', ghana_regions_new_to_old_map)
print(f"Number of rows in the Ghana dataframe: {len(gha_bronze_mod)}")
SILVER_COLUMNS = ['country_name', 'country_code', 'C', 'region_code', 'admin1_region', 'boundary']
silver = pd.concat([
    bronze[~bronze['country_name'].isin(['Albania', 'Ghana'])][SILVER_COLUMNS],
    alb_bronze_mod[SILVER_COLUMNS],
    gha_bronze_mod[SILVER_COLUMNS],
], ignore_index=True)
write_table(silver, 'admin1_boundaries_silver')
gold = silver.rename(columns={'C': 'country_code_iso2'})[['country_name', 'country_code', 'country_code_iso2', 'admin1_region', 'boundary']]
write_table(gold, 'admin1_boundaries_gold')
# COMMAND ----------

# Disputed areas: the 'Non-determined legal status area' features of the Admin 0 file,
# attributed to each country that claims them.
disputed_area_country_map = {
    'Ilemi Triangle': ['Kenya', 'South Sudan'],
    #TODO add more countries: refer to map department's notes
}

admin0 = geojson_frame(ADMIN0_GEOJSON)
admin0 = admin0.rename(columns={"WB_REGION": "region_code", "ISO_A2": "country_code_iso2", "NAM_0": "region_name"}).fillna('')
disputed_bronze = admin0[admin0['WB_STATUS'] == 'Non-determined legal status area']
write_table(disputed_bronze, 'admin0_disputed_boundaries_bronze')
disputed_region_country = pd.DataFrame(
    [{'region_name': region, 'country': country} for region, countries in disputed_area_country_map.items() for country in countries])
disputed_silver = disputed_bronze.merge(disputed_region_country, on='region_name', how='inner')
write_table(disputed_silver, 'admin0_disputed_boundaries_silver')
disputed_gold = disputed_silver.rename(columns={'country': 'country_name'})[['country_name', 'region_name', 'boundary', 'country_code_iso2']]
write_table(disputed_gold, 'admin0_disputed_boundaries_gold')