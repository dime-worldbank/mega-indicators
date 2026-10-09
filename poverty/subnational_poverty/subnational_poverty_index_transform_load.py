# Databricks notebook source
# MAGIC %run ../../config

# COMMAND ----------

# MAGIC %run ../../utils

# COMMAND ----------

# Subnational poverty rate per region and year from the SPID/GSAP silver table. Region
# names are aligned to admin1_boundaries_gold with the fixes below, and the poverty line
# follows the country's income group, as for the national poverty_rate. Plain pandas on
# both sides (it replaced a DLT pipeline). The pipeline's intermediate
# subnational_poverty_rate_silver table is no longer written; nothing read it.
import numpy as np
import pandas as pd

# Instead of chained when clauses, use a mapping table to improve readability and make it easier to add new cases.
REGION_NAME_FIXES = [
    ('ALB', 'Durrës', 'Durres'),
    ('ALB', 'Durrës (AL012)', 'Durres'),
    ('ALB', 'Kukës', 'Kukes'),
    ('ALB', 'Kukës (AL013)', 'Kukes'),
    ('ALB', 'Lezhë', 'Lezhe'),
    ('ALB', 'Lezhë (AL014)', 'Lezhe'),
    ('ALB', 'Shkodër', 'Shkoder'),
    ('ALB', 'Shkodër (AL015)', 'Shkoder'),
    ('ALB', 'Dibër', 'Diber'),
    ('ALB', 'Dibër (AL011)', 'Diber'),
    ('ALB', 'Tiranë', 'Tirane'),
    ('ALB', 'Tiranë (AL022)', 'Tirane'),
    ('ALB', 'Korcë', 'Korce'),
    ('ALB', 'Korcë (AL034)', 'Korce'),
    ('ALB', 'Vlorë', 'Vlore'),
    ('ALB', 'Vlorë (AL035)', 'Vlore'),
    ('ALB', 'Gjirokastër', 'Gjirokaster'),
    ('ALB', 'Gjirokastër (AL033)', 'Gjirokaster'),
    ('ALB', 'Berat (AL031)', 'Berat'),
    ('ALB', 'Elbasan (AL021)', 'Elbasan'),
    ('ALB', 'Fier (AL032)', 'Fier'),
    ('BFA', 'Est', 'Est Region Burkina Faso'),
    ('BFA', 'Centre Sud', 'Centre Sud Region Burkina Faso'),
    ('BFA', 'Centre-sud', 'Centre Sud Region Burkina Faso'),
    ('BFA', 'Centre-nord', 'Centre Nord'),
    ('BFA', 'Centre-est', 'Centre Est'),
    ('BFA', 'Centre-ouest', 'Centre Ouest'),
    ('BFA', 'Sud-ouest', 'Sud Ouest'),
    ('BFA', 'Hauts-bassins', 'Hauts Bassins'),
    ('BFA', 'Boucle du Mouhoun', 'Boucle Du Mouhoun'),
    ('BTN', 'Ha', 'Haa'),
    ('BTN', 'Wangdi Phodrang', 'Wangduephodrang'),
    ('BTN', 'Chukha', 'Chhukha'),
    ('BTN', 'Lhuntshi', 'Lhuentse'),
    ('BTN', 'Monggar', 'Mongar'),
    ('BTN', 'Samdrupjongkhar', 'Samdrup Jongkhar'),
    ('BTN', 'Tashi Yangtse', 'Trashiyangtse'),
    ('BDI', 'Bujumbura Mairie', 'Mairie de Bujumbura'),
    ('BDI', 'Bujumbura Rural', 'Bujumbura'),
    ('BDI', 'Buyengero & Burambi & Rumonge & Bugarama & Muhuta', 'Rumonge'),
    # SPID_GSAP uses plain region names, not the Roman-numeral form; Antofagasta,
    # Atacama, Coquimbo, Los Lagos, Maule and 'Arica y Painacota' already match gold.
    # 'Ñuble' has no WB official admin1 boundary (region created 2018), so it stays unmapped.
    ('CHL', 'Aisen del Gral. Carlos Ibañez del Campo', 'Aysén'),
    ('CHL', 'Araucania', 'Araucanía'),
    ('CHL', 'Biobio', 'Biobío'),
    ('CHL', "Libertador Gral. Bernardo O'Higgins", "Libertador General Bernardo O'Higgins"),
    ('CHL', 'Los Rios', 'Los Ríos'),
    ('CHL', 'Magallanes y Antartica chilena', 'Magallanes y la Antártica Chilena'),
    ('CHL', 'Metropolitana', 'Región Metropolitana de Santiago'),
    ('CHL', 'Tarapaca', 'Tarapacá'),
    ('CHL', 'Valparaiso', 'Valparaíso'),
    ('COL', 'Guajira', 'La Guajira'),
    ('COL', 'Norte De Santander', 'Norte de Santander'),  # initcap over-capitalizes 'de'
    ('COL', 'Santafe De Bogota D.c.', 'Bogota'),
    ('KEN', 'Elgeyo/Marakwet', 'Elgeyo Marakwet'),
    ('KEN', 'Taita/Taveta', 'Taita Taveta'),
    ('KEN', 'Nairobi', 'Nairobi City County'),
    ('MOZ', 'Maputo City', 'Cidade de Maputo'),
    ('MOZ', 'Maputo Cidade', 'Cidade de Maputo'),
    ('MOZ', 'Maputo Province', 'Maputo'),
    ('NGA', 'Abuja', 'Federal Capital Territory'),
    ('NGA', 'Nassarawa', 'Nasarawa'),
    ('TUN', 'CenterE', 'Centre Est'),
    ('TUN', 'CenterW', 'Centre Ouest'),
    ('TUN', 'NE', 'Nord Est'),
    ('TUN', 'NW', 'Nord Ouest'),
    ('TUN', 'SE', 'Sud Est'),
    ('TUN', 'SW', 'Sud Ouest'),
    ('ZAF', 'KwaZulu-Natal', 'Kwa-Zulu Natal'),
    ('ZAF', 'North West', 'North-west'),
    ('ZAF', 'Limpopo', 'Northern Province'),
]


def initcap(name):
    """Spark's initcap: first letter of each space-separated word upper, the rest lower."""
    if not isinstance(name, str):
        return name
    return ' '.join(word[:1].upper() + word[1:].lower() for word in name.split(' '))

# COMMAND ----------

silver = read_table('poverty_rate_SPID_GSAP_silver')
countries = read_table('country', columns=['country_name', 'country_code', 'income_level'])
fixes = pd.DataFrame(REGION_NAME_FIXES, columns=['country_code', 'region_name', 'country_fixed_region_name'])

df = silver.merge(fixes, on=['country_code', 'region_name'], how='left')
# explicit fixes win over the COL initcap default
default_name = np.where(df['country_code'] == 'COL', df['region_name'].map(initcap), df['region_name'])
df['region_name'] = df['country_fixed_region_name'].fillna(pd.Series(default_name, index=df.index))
df = df.drop(columns='country_fixed_region_name')
df = df.merge(countries, on='country_code', how='inner')  # TODO: change to left & investigate dropped

df['poverty_rate'] = np.select(
    [df['income_level'].isin(['LIC', 'INX']),  # INX: income classification is not assigned or not applicable
     df['income_level'] == 'LMC',
     df['income_level'].isin(['UMC', 'HIC'])],
    [df['poor300'], df['poor420'], df['poor830']],
    np.nan,
)
assert df['poverty_rate'].notna().all(), 'poverty rates for country income level should be present'

by_region = df.groupby(['country_name', 'region_name'])['year']
df['earliest_year'] = by_region.transform('min')
df['latest_year'] = by_region.transform('max')
df

# COMMAND ----------

write_table(df, 'subnational_poverty_rate')