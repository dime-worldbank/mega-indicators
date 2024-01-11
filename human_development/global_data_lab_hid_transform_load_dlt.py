# Databricks notebook source
import dlt
import pyspark.sql.functions as F

@dlt.table(name=f'global_data_lab_ed_index')
def hid_ed_index():
    countries = spark.table(f'indicator.country').select('country_name', 'country_code')

    ed = (spark.table(f'indicator_intermediate.global_data_lab_ed_index')
        .withColumnRenamed("ISO_Code", 'country_code')
        .join(countries, on=["country_code"], how="inner")
        .withColumn("adm1_name", 
            F.when(F.col("country_name") == 'Colombia',
                F.when(F.col("Region").contains('Valle'),
                    F.lit("Valle Del Cauca")
                ).when(F.col("Region").contains('Bogota'),
                    F.lit("Bogota")
                ).when(F.col("Region") == 'Norte de Santander',
                    F.lit("Norte De Santander")
                ).when(F.col("Region") == 'Guainja',
                    F.lit("Guainia")
                ).when(F.col("Region") == 'Guajira',
                    F.lit("La Guajira")
                ).when(F.col("Region") == 'San Andres',
                    F.lit("San Andres Y Providencia")
                ).when(F.col("Region") == 'Vaupis',
                    F.lit("Vaupes")
                ).otherwise(
                    F.trim(F.regexp_replace(F.col("Region"), "\\(.*\\)", ""))
                )
            ).otherwise(
                F.col("Region")
            ))
        .select(
            'country_name',
            'adm1_name',
            F.col('Level').alias('level'),
            'year',
            F.col('value').alias('education_index'),
        )
    )

    return ed
