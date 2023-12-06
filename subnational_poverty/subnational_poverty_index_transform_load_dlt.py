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
                            "[-\[\]–]+", " ")
                        ,
                        "[\\d]+", ""
                    )))
        .withColumn('region_name',
                     F.when(
                        F.col("region_name_tmp").isin(['Maputo City', 'Maputo Cidade']),
                        'Cidade de Maputo'
                     ).when(
                        F.col("region_name_tmp") == 'Maputo Province',
                        'Maputo'
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
                ).otherwise(
                    F.col("region_name")
                ))
    )
