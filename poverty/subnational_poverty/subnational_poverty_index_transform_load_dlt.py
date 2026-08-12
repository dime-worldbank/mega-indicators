# Databricks notebook source
import dlt
import pyspark.sql.functions as F
from pyspark.sql import Window


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


def _poverty_rate(intermediate):
    # Transform one source's already single-source intermediate (no data_source).
    countries = spark.table('country').select('country_name', 'country_code', 'income_level')
    region_name_fixes = spark.createDataFrame(
        REGION_NAME_FIXES,
        ['country_code', 'region_name', 'country_fixed_region_name']
    )

    return (
        spark.table(intermediate)
        .join(region_name_fixes, ['country_code', 'region_name'], 'left')
        .withColumn(
            'region_name',
            F.coalesce(
                F.col('country_fixed_region_name'),  # explicit fixes win over the COL initcap default below
                F.when(F.col('country_code') == 'COL', F.initcap(F.col('region_name'))),
                F.col('region_name')
            )
        )
        .drop('country_fixed_region_name')
        .join(countries, ["country_code"], "inner") # TODO: change to left & investigate dropped
        .withColumn(
            'poverty_rate',
            F.when(
                F.col('income_level').isin('LIC', 'INX'), F.col('poor300') # INX: income classification is not assigned or not applicable
            ).when(
                F.col('income_level') == 'LMC', F.col('poor420')
            ).when(
                F.col('income_level').isin('UMC', 'HIC'), F.col('poor830')
            )
        )
    )


@dlt.expect_or_fail(
    'poverty rates for country income level should be present',
    'poverty_rate IS NOT NULL'
)
@dlt.table(name='subnational_poverty_rate_spid')
def subnational_poverty_rate_spid():
    return _poverty_rate('poverty_rate_spid_silver')


@dlt.expect_or_fail(
    'poverty rates for country income level should be present',
    'poverty_rate IS NOT NULL'
)
@dlt.table(name='subnational_poverty_rate_gsap')
def subnational_poverty_rate_gsap():
    return _poverty_rate('poverty_rate_gsap_silver')


@dlt.table(name='subnational_poverty_rate')
def subnational_poverty_rate():
    # Combined view over the split tables, adding a clean source_id FK (replaces the
    # free-text data_source). Year span is per (country, region) across both sources.
    w = Window.partitionBy('country_name', 'region_name')
    spid = dlt.read('subnational_poverty_rate_spid').withColumn('source_id', F.lit('pip_spid'))
    gsap = dlt.read('subnational_poverty_rate_gsap').withColumn('source_id', F.lit('pip_gsap'))
    return (
        spid.unionByName(gsap)
        .withColumn('earliest_year', F.min('year').over(w))
        .withColumn('latest_year', F.max('year').over(w))
    )
