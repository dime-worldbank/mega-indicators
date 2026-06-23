# Databricks notebook source
import dlt
import pyspark.sql.functions as F
from pyspark.sql import Window


# Instead of chained when clauses, use a mapping table to improve readability and make it easier to add new cases.
REGION_NAME_FIXES = [
    (None, 'Maputo City', 'Cidade de Maputo'),
    (None, 'Maputo Cidade', 'Cidade de Maputo'),
    (None, 'Maputo Province', 'Maputo'),
    (None, "Murang'a", "Murang'a County"),
    (None, 'Tana River', 'Tana River County'),
    (None, 'Ha', 'Haa'),
    (None, 'Wangdi Phodrang', 'Wangduephodrang'),
    (None, 'Chukha', 'Chhukha'),
    (None, 'Lhuntshi', 'Lhuentse'),
    (None, 'Tashi Yangtse', 'Trashiyangtse'),
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
    ('BFA', 'Boucle du Mouhoun', 'Boucle Du Mouhoun'),
    ('NGA', 'FCT', 'Federal Capital Territory'),
    ('TUN', 'CenterE', 'Centre Est'),
    ('TUN', 'CenterW', 'Centre Ouest'),
    ('TUN', 'NE', 'Nord Est'),
    ('TUN', 'NW', 'Nord Ouest'),
    ('TUN', 'SE', 'Sud Est'),
    ('TUN', 'SW', 'Sud Ouest'),
    ('KEN', 'Elgeyo/Marakwet', 'Elgeyo Marakwet'),
    ('KEN', 'Taita/Taveta', 'Taita Taveta'),
    ('KEN', 'Muranga', "Murang'a County"),
    ('KEN', 'Nairobi', 'Nairobi City'),
    ('CHL', 'I Región de Tarapacá', 'Tarapacá'),
    ('CHL', 'X Región de Los Lagos', 'Los Lagos'),
    ('CHL', 'XI Región de Aysén del Gral Carlos Ibáñez', 'Aysén'),
    ('CHL', 'XII Región de Magallanes y de la Antártica', 'Magallanes y la Antártica Chilena'),
    ('CHL', 'XIII Región Metropolitana de Santiago', 'Región Metropolitana de Santiago'),
    ('CHL', 'XIV Región de Los Ríos', 'Los Ríos'),
    ('CHL', 'XV Región de Arica y Parinacota', 'Arica y Parinacota'),
    ('CHL', 'XVI Región del Ñuble', 'Ñuble'),
    ('CHL', 'II Región de Antofagasta', 'Antofagasta'),
    ('CHL', 'III Región de Atacama', 'Atacama'),
    ('CHL', 'IV Región de Coquimbo', 'Coquimbo'),
    ('CHL', 'V Región de Valparaíso', 'Valparaíso'),
    ('CHL', "VI Región del Libertador Gral B O'Higgins", "Libertador General Bernardo O'Higgins"),
    ('CHL', 'VII Región del Maule', 'Maule'),
    ('CHL', 'VIII Región del BioBío', 'Biobío'),
    ('CHL', 'IX Región de la Araucanía', 'Araucanía'),
]


@dlt.table(name='subnational_poverty_rate_silver')
def subnational_poverty_rate_silver():
    countries = spark.table('prd_mega.indicator.country').select('country_name', 'country_code', 'income_level')
    region_name_fixes = spark.createDataFrame(
        REGION_NAME_FIXES,
        ['country_code', 'region_name', 'fixed_region_name']
    )
    generic_region_name_fixes = (
        region_name_fixes
        .where(F.col('country_code').isNull())
        .drop('country_code')
        .withColumnRenamed('fixed_region_name', 'generic_fixed_region_name')
    )
    country_region_name_fixes = (
        region_name_fixes
        .where(F.col('country_code').isNotNull())
        .withColumnRenamed('fixed_region_name', 'country_fixed_region_name')
    )

    return (
        spark.table('prd_mega.indicator_intermediate.poverty_rate_spid_gsap')
        .join(generic_region_name_fixes, ['region_name'], 'left')
        .join(country_region_name_fixes, ['country_code', 'region_name'], 'left')
        .withColumn(
            'region_name',
            F.coalesce(
                F.col('generic_fixed_region_name'),
                F.when(F.col('country_code') == 'COL', F.initcap(F.col('region_name'))),
                F.col('country_fixed_region_name'),
                F.col('region_name')
            )
        )
        .drop('country_fixed_region_name', 'generic_fixed_region_name')
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
@dlt.table(name='subnational_poverty_rate')
def subnational_poverty_rate():
    w = Window.partitionBy('country_name', 'region_name')
    return (
        dlt.read('subnational_poverty_rate_silver')
        .withColumn('earliest_year', F.min('year').over(w))
        .withColumn('latest_year', F.max('year').over(w))
    )
