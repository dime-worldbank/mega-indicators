# Databricks notebook source
# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../utils

# COMMAND ----------

# global_data_lab_hd_index: the silver table joined to country, regions named as in
# admin1_boundaries_gold, attendance also as a 0-1 share. Plain pandas on both sides (it
# replaced a DLT pipeline).
import pandas as pd

# (country_name, GDL region) -> admin1 name, where the two spellings differ
REGION_NAME_FIXES = {
    ('Burkina Faso', 'Boucle de Mouhoun'): 'Boucle Du Mouhoun',
    ('Bhutan', 'Chukha'): 'Chhukha',
    ('Bhutan', 'Lhuntse'): 'Lhuentse',
    ('Bhutan', 'Samdrup jongkhar'): 'Samdrup Jongkhar',
    ('Bhutan', 'Wangdi'): 'Wangduephodrang',
    ('Nigeria', 'Abuja FCT'): 'Federal Capital Territory',
    ('Nigeria', 'Nassarawa'): 'Nasarawa',
    ('Nigeria', 'Zamfora'): 'Zamfara',
    ('Burundi', 'Bujumbura Mairie'): 'Mairie de Bujumbura',
    ('Burundi', 'Bujumbura Rural'): 'Bujumbura',
    ('Colombia', 'Norte de Santander'): 'Norte De Santander',
    ('Colombia', 'Guainja'): 'Guainia',
    ('Colombia', 'Guajira'): 'La Guajira',
    ('Colombia', 'San Andres'): 'San Andres Y Providencia',
    ('Colombia', 'Vaupis'): 'Vaupes',
    ('Mozambique', 'Maputo Cidade'): 'Cidade de Maputo',
    ('Mozambique', 'Maputo Provincia'): 'Maputo',
    ('Mozambique', 'Cabo delgado'): 'Cabo Delgado',
}


def adm1_name(country_name, region):
    if not isinstance(region, str):
        return region
    fixed = REGION_NAME_FIXES.get((country_name, region))
    if fixed is not None:
        return fixed
    if country_name == 'Burkina Faso':
        return region.replace('-', ' ')
    if country_name == 'Colombia':
        if 'Valle' in region:
            return 'Valle Del Cauca'
        if 'Bogota' in region:
            return 'Bogota'
    return region

# COMMAND ----------

silver = read_table('global_data_lab_hd_index_silver').rename(columns={'ISO_Code': 'country_code'})
countries = read_table('country', columns=['country_name', 'country_code'])
df = silver.merge(countries, on='country_code', how='inner')

df['Region'] = df['Region'].str.replace(r'\(.*\)', '', regex=True).str.strip()
df['adm1_name'] = [adm1_name(country, region) for country, region in zip(df['country_name'], df['Region'])]
df['attendance_6to17yo'] = df['attendance'] / 100
df = df.rename(columns={'edindex': 'education_index', 'healthindex': 'health_index', 'incindex': 'income_index'})
df = df[['country_name', 'adm1_name', 'year', 'education_index', 'health_index', 'income_index', 'attendance', 'attendance_6to17yo']]
df

# COMMAND ----------

write_table(df, 'global_data_lab_hd_index')
