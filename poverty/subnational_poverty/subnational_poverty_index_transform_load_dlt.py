# Databricks notebook source
import dlt
import pyspark.sql.functions as F
from pyspark.sql import Window

@dlt.table(name='subnational_poverty_rate_silver')
def subnational_poverty_rate_silver():
    countries = spark.table('prd_mega.indicator.country').select('country_name', 'country_code', 'income_level')

    return (
        spark.table('prd_mega.indicator_intermediate.poverty_rate_spid_gsap')
        .withColumn(
            'region_name',
            F.when(
                F.col("region_name").isin(['Maputo City', 'Maputo Cidade']),
                'Cidade de Maputo'
            ).when(
                F.col("region_name") == 'Maputo Province',
                'Maputo'
            ).when(
                F.col("region_name") == "Murang'a",
                "Murang'a County"
            ).when(
                F.col("region_name") == "Tana River",
                "Tana River County"
            ).when(
                F.col("region_name") == 'Ha',
                "Haa"
            ).when(
                F.col("region_name") == 'Wangdi Phodrang',
                "Wangduephodrang"
            ).when(
                F.col("region_name") == 'Chukha',
                "Chhukha"
            ).when(
                F.col("region_name") == 'Lhuntshi',
                "Lhuentse"
            ).when(
                F.col("region_name") == 'Tashi Yangtse',
                "Trashiyangtse"
            ).when(
                (F.col("region_name") == 'Est') & (F.col('country_code') == 'BFA'),
                "Est Region Burkina Faso"
            ).when(
                (F.col("region_name") == 'Centre Sud') & (F.col('country_code') == 'BFA'),
                "Centre Sud Region Burkina Faso"
            ).when(
                (F.col("region_name") == 'Boucle du Mouhoun') & (F.col('country_code') == 'BFA'),
                "Boucle Du Mouhoun"
            ).when(
                F.col('country_code') == 'COL',
                F.initcap(F.col('region_name'))
            ).when(
                (F.col("region_name") == 'FCT') & (F.col('country_code') == 'NGA'),
                "Federal Capital Territory"
            ).when(
                (F.col("region_name") == 'CenterE') & (F.col('country_code') == 'TUN'),
                "Centre Est"
            ).when(
                (F.col("region_name") == 'CenterW') & (F.col('country_code') == 'TUN'),
                "Centre Ouest"
            ).when(
                (F.col("region_name") == 'NE') & (F.col('country_code') == 'TUN'),
                "Nord Est"
            ).when(
                (F.col("region_name") == 'NW') & (F.col('country_code') == 'TUN'),
                "Nord Ouest"
            ).when(
                (F.col("region_name") == 'SE') & (F.col('country_code') == 'TUN'),
                "Sud Est"
            ).when(
                (F.col("region_name") == 'SW') & (F.col('country_code') == 'TUN'),
                "Sud Ouest"
            ).when(
                (F.col("region_name") == 'Elgeyo/Marakwet') & (F.col('country_code') == 'KEN'),
                "Elgeyo Marakwet"
            ).when(
                (F.col("region_name") == 'Taita/Taveta') & (F.col('country_code') == 'KEN'),
                "Taita Taveta"
            ).when(
                (F.col("region_name") == 'Muranga') & (F.col('country_code') == 'KEN'),
                "Murang'a County"
            ).when(
                (F.col("region_name") == 'Nairobi') & (F.col('country_code') == 'KEN'),
                "Nairobi City"
            ).when(
                (F.col("region_name") == 'I Región de Tarapacá') & (F.col('country_code') == 'CHL'),
                "Tarapacá"
            ).when(
                (F.col("region_name") == 'X Región de Los Lagos') & (F.col('country_code') == 'CHL'),
                "Los Lagos"
            ).when(
                (F.col("region_name") == 'XI Región de Aysén del Gral Carlos Ibáñez') & (F.col('country_code') == 'CHL'),
                "Aysén"
            ).when(
                (F.col("region_name") == 'XII Región de Magallanes y de la Antártica') & (F.col('country_code') == 'CHL'),
                "Magallanes y la Antártica Chilena"
            ).when(
                (F.col("region_name") == 'XIII Región Metropolitana de Santiago') & (F.col('country_code') == 'CHL'),
                "Región Metropolitana de Santiago"
            ).when(
                (F.col("region_name") == 'XIV Región de Los Ríos') & (F.col('country_code') == 'CHL'),
                "Los Ríos"
            ).when(
                (F.col("region_name") == 'XV Región de Arica y Parinacota') & (F.col('country_code') == 'CHL'),
                "Arica y Parinacota"
            ).when(
                (F.col("region_name") == 'XVI Región del Ñuble') & (F.col('country_code') == 'CHL'),
                "Ñuble"
            ).when(
                (F.col("region_name") == 'II Región de Antofagasta') & (F.col('country_code') == 'CHL'),
                "Antofagasta"
            ).when(
                (F.col("region_name") == 'III Región de Atacama') & (F.col('country_code') == 'CHL'),
                "Atacama"
            ).when(
                (F.col("region_name") == 'IV Región de Coquimbo') & (F.col('country_code') == 'CHL'),
                "Coquimbo"
            ).when(
                (F.col("region_name") == 'V Región de Valparaíso') & (F.col('country_code') == 'CHL'),
                "Valparaíso"
            ).when(
                (F.col("region_name") == "VI Región del Libertador Gral B O'Higgins") & (F.col('country_code') == 'CHL'),
                "Libertador General Bernardo O'Higgins"
            ).when(
                (F.col("region_name") == 'VII Región del Maule') & (F.col('country_code') == 'CHL'),
                "Maule"
            ).when(
                (F.col("region_name") == 'VIII Región del BioBío') & (F.col('country_code') == 'CHL'),
                "Biobío"
            ).when(
                (F.col("region_name") == 'IX Región de la Araucanía') & (F.col('country_code') == 'CHL'),
                "Araucanía"
            ).otherwise(F.col("region_name"))
        )
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
