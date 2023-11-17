# Databricks notebook source
import dlt
from pyspark.sql.functions import col, regexp_replace, trim, when

@dlt.table(name=f'subnational_poverty_index')
def subnational_poverty_index():
    countries = spark.table(f'indicator.country').select('country_name', 'country_code')
    return (spark.table(f'indicator_intermediate.poverty_index_spid_gsap')
        .withColumn('region_name_tmp', 
                    regexp_replace(
                        trim(
                            regexp_replace(
                                col("region_name"),
                                 "[-\[\]–]+", " ")
                        ),
                        "[\\d]+", ""
                    ))
        .withColumn('region_name_alt',
                     when(
                         col("region_name_tmp").contains('Maputo City') | col("region_name_tmp").contains('Maputo Cidade'),
                        'Cidade de Maputo'
                     ).otherwise(
                         when(
                            col("region_name_tmp").contains('Maputo Province'),
                            'Maputo'
                         ).otherwise(col("region_name_tmp"))
                     ))
        .drop('region_name_tmp')
        .join(countries, ["country_code"], "inner") # TODO: change to left & investigate dropped
    )
