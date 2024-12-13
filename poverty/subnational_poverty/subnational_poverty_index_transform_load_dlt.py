# Databricks notebook source
import dlt
import pyspark.sql.functions as F

@dlt.table(name=f'subnational_poverty_index_silver')
def subnational_poverty_index_silver():
    countries = spark.table(f'indicator.country').select('country_name', 'country_code')

    return (spark.table(f'indicator_intermediate.poverty_index_spid_gsap')
        .withColumn('region_name_tmp', 
                    F.trim(F.regexp_replace(
                        F.regexp_replace(
                            F.col("region_name"),
                            "[-\[\]–]+", " "),
                        "[\\.\\d]+", ""
                    )))
        .withColumn('region_name',
                    F.when(
                        F.col("region_name_tmp").isin(['Maputo City', 'Maputo Cidade']),
                        'Cidade de Maputo'
                     ).when(
                        F.col("region_name_tmp") == 'Maputo Province',
                        'Maputo'
                     ).when(
                        F.col("region_name_tmp") == "Murang'a",
                        "Murang'a County"
                     ).when(
                        F.col("region_name_tmp") == "Tana River",
                        "Tana River County"                         
                    ).when(
                        F.col("region_name_tmp") == 'Ha',
                        "Haa"
                    ).when(
                        F.col("region_name_tmp") == 'Wangdi Phodrang',
                        "Wangduephodrang"
                    ).when(
                        F.col("region_name_tmp") == 'Chukha',
                        "Chhukha"
                    ).when(
                        F.col("region_name_tmp") == 'Lhuntshi',
                        "Lhuentse"
                    ).when(
                        F.col("region_name_tmp") == 'Tashi Yangtse',
                        "Trashiyangtse"
                    ).when(
                        (F.col("region_name_tmp") == 'Est') & (F.col('country_code') == 'BFA'),
                        "Est Region Burkina Faso"
                    ).when(
                        (F.col("region_name_tmp") == 'Centre Sud') & (F.col('country_code') == 'BFA'),
                        "Centre Sud Region Burkina Faso"
                    ).when(
                        (F.col("region_name_tmp") == 'Boucle du Mouhoun') & (F.col('country_code') == 'BFA'),
                        "Boucle Du Mouhoun"
                    ).when(
                        F.col('country_code') == 'COL', F.initcap(F.col('region_name_tmp'))
                    ).when(
                        (F.col("region_name_tmp") == 'FCT') & (F.col('country_code') == 'NGA'),
                        "Federal Capital Territory"
                    ).when(
                        (F.col("region_name_tmp") == 'CenterE') & (F.col('country_code') == 'TUN'),
                        "Centre Est"
                    ).when(
                        (F.col("region_name_tmp") == 'CenterW') & (F.col('country_code') == 'TUN'),
                        "Centre Ouest"
                    ).when(
                        (F.col("region_name_tmp") == 'NE') & (F.col('country_code') == 'TUN'),
                        "Nord Est"
                    ).when(
                        (F.col("region_name_tmp") == 'NW') & (F.col('country_code') == 'TUN'),
                        "Nord Ouest"
                    ).when(
                        (F.col("region_name_tmp") == 'SE') & (F.col('country_code') == 'TUN'),
                        "Sud Est"
                    ).when(
                        (F.col("region_name_tmp") == 'SW') & (F.col('country_code') == 'TUN'),
                        "Sud Ouest"
                    ).when(
                        (F.col("region_name_tmp") == 'Elgeyo/Marakwet') & (F.col('country_code') == 'KEN'),
                        "Elgeyo Marakwet"
                    ).when(
                        (F.col("region_name_tmp") == 'Taita/Taveta') & (F.col('country_code') == 'KEN'),
                        "Taita Taveta"
                    ).when(
                        (F.col("region_name_tmp") == 'Muranga') & (F.col('country_code') == 'KEN'),
                        "Murang'a County"
                    ).when(
                        (F.col("region_name_tmp") == 'Nairobi') & (F.col('country_code') == 'KEN'),
                        "Nairobi City"
                    ).when(
                        (F.col("region_name_tmp") == 'I Región de Tarapacá') & (F.col('country_code') == 'CHL'),
                        "Tarapacá"
                    ).when(
                        (F.col("region_name_tmp") == 'X Región de Los Lagos') & (F.col('country_code') == 'CHL'),
                        "Los Lagos"
                    ).when(
                        (F.col("region_name_tmp") == 'XI Región de Aysén del Gral Carlos Ibáñez') & (F.col('country_code') == 'CHL'),
                        "Aysén"
                    ).when(
                        (F.col("region_name_tmp") == 'XII Región de Magallanes y de la Antártica') & (F.col('country_code') == 'CHL'),
                        "Magallanes y la Antártica Chilena"
                    ).when(
                        (F.col("region_name_tmp") == 'XIII Región Metropolitana de Santiago') & (F.col('country_code') == 'CHL'),
                        "Región Metropolitana de Santiago"
                    ).when(
                        (F.col("region_name_tmp") == 'XIV Región de Los Ríos') & (F.col('country_code') == 'CHL'),
                        "Los Ríos"
                    ).when(
                        (F.col("region_name_tmp") == 'XV Región de Arica y Parinacota') & (F.col('country_code') == 'CHL'),
                        "Arica y Parinacota"
                    ).when(
                        (F.col("region_name_tmp") == 'XVI Región del Ñuble') & (F.col('country_code') == 'CHL'),
                        "Ñuble"
                    ).when(
                        (F.col("region_name_tmp") == 'II Región de Antofagasta') & (F.col('country_code') == 'CHL'),
                        "Antofagasta"
                    ).when(
                        (F.col("region_name_tmp") == 'III Región de Atacama') & (F.col('country_code') == 'CHL'),
                        "Atacama"
                    ).when(
                        (F.col("region_name_tmp") == 'IV Región de Coquimbo') & (F.col('country_code') == 'CHL'),
                        "Coquimbo"
                    ).when(
                        (F.col("region_name_tmp") == 'V Región de Valparaíso') & (F.col('country_code') == 'CHL'),
                        "Valparaíso"
                    ).when(
                        (F.col("region_name_tmp") == 'VI Región del Libertador Gral B O\'Higgins') & (F.col('country_code') == 'CHL'),
                        "Libertador General Bernardo O'Higgins"
                    ).when(
                        (F.col("region_name_tmp") == 'VII Región del Maule') & (F.col('country_code') == 'CHL'),
                        "Maule"
                    ).when(
                        (F.col("region_name_tmp") == 'VIII Región del BioBío') & (F.col('country_code') == 'CHL'),
                        "Biobío"
                    ).when(
                        (F.col("region_name_tmp") == 'IX Región de la Araucanía') & (F.col('country_code') == 'CHL'),
                        "Araucanía"
                    ).otherwise(F.col("region_name_tmp")))
                            .drop('region_name_tmp')
                            .join(countries, ["country_code"], "inner") # TODO: change to left & investigate dropped
                        )

@dlt.table(name=f'subnational_poverty_index')
def subnational_poverty_index():
    year_ranges = (dlt.read('subnational_poverty_index_silver')
        .groupBy("country_name", "region_name")
        .agg(F.min("year").alias("earliest_year"), F.max("year").alias("latest_year"))
    )
    return (dlt.read('subnational_poverty_index_silver')
            .join(year_ranges, on=['country_name', "region_name"], how='left')
            .withColumn("region_name_for_map", 
                F.when(
                    ((F.col("country_name") == 'Pakistan') & (F.col("region_name") == 'Punjab')),
                    F.lit("PK-PB")
                ).when(
                    ((F.col("country_name") == 'Colombia') & (F.col("region_name") == 'Valle')),
                    F.concat(F.lit("Valle del Cauca"))
                ).when(
                    ((F.col("country_name") == 'Colombia')),
                    F.concat(F.col("region_name"), F.lit(" Department Colombia"))
                ).when(
                    ((F.col("country_name") == 'Burkina Faso') & (F.col("region_name") != 'Centre')),
                    F.concat(F.col("region_name"), F.lit(" Region Burkina Faso"))                    
                ).when(
                    ((F.col("country_name") == 'Burkina Faso') & (F.col("region_name") == 'Centre')),
                    'BFA Centre region'
                ).otherwise(
                    F.col("region_name")
                ))
    )
