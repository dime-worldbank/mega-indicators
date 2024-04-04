# Databricks notebook source
# TODO: Add PEFA score extraction step. Currently the data is imported manually for prototyping
# Data source: https://www.pefa.org/assessments/batch-downloads 
# Download once for 2016 framework then uploaded to indicator_intermediate.pefa_2016_bronze, 
#      and once for 2011 framework then uploaded to indicator_intermediate.pefa_2011_bronze

import dlt
from pyspark.sql.functions import when, col, lit

@dlt.table(name=f'pefa_2011_silver')
def pefa_2011_silver():
    return (
        spark.table('indicator_intermediate.pefa_2011_bronze')
        .withColumn('country_name',
            when(col('Country') == 'The Gambia', lit('Gambia, The'))
            .when(col('Country') == 'Macedonia', lit('North Macedonia'))
            .when(col('Country') == 'Democratic Republic of Congo', lit('Congo, Dem. Rep'))
            .when(col('Country') == 'The Bahamas', lit('Bahamas, The'))
            .otherwise(col('Country'))
        )
    )

@dlt.table(name=f'pefa_2016_silver')
def pefa_2016_silver():
    return (
        spark.table('indicator_intermediate.pefa_2016_bronze')
        .withColumn('country_name',
            when(col('Country') == 'The Gambia', lit('Gambia, The'))
            .when(col('Country') == 'Macedonia', lit('North Macedonia'))
            .when(col('Country') == 'Democratic Republic of Congo', lit('Congo, Dem. Rep'))
            .when(col('Country') == 'The Bahamas', lit('Bahamas, The'))
            .otherwise(col('Country'))
        )
    )
