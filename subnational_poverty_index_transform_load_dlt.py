# Databricks notebook source
import dlt
from pyspark.sql.functions import col, regexp_replace, trim

@dlt.table(name=f'subnational_poverty_index')
def subnational_poverty_index():
    countries = spark.table(f'indicator.country').select('country_name', 'country_code')
    return (spark.table(f'indicator_intermediate.poverty_index_spid_gsap')
        .withColumn('region_name_tmp', trim(regexp_replace(col("region_name"), "[-\[\]–]+", " ")))
        .withColumn('region_name_alt', regexp_replace(col("region_name_tmp"), "[\\d]+", ""))
        .drop('region_name_tmp')
        .join(countries, ["country_code"], "inner") # TODO: change to left & investigate dropped
    )
