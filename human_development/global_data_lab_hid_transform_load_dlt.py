# Databricks notebook source
import dlt
import pyspark.sql.functions as F

@dlt.table(name=f'global_data_lab_hd_index')
def global_data_lab_hd_index():
    countries = spark.table(f'indicator.country').select('country_name', 'country_code')

    return (spark.table(f'indicator_intermediate.global_data_lab_hd_index')
        .withColumnRenamed("ISO_Code", 'country_code')
        .join(countries, on=["country_code"], how="inner")
        .withColumn("Region", F.trim(F.regexp_replace(F.col("Region"), "\\(.*\\)", "")))
        .withColumn("adm1_name", 
            F.when(F.col("country_name") == 'Burkina Faso',
                F.when(F.col("Region") == 'Boucle de Mouhoun',
                    F.lit("Boucle Du Mouhoun")
                ).when(F.col("Region") == 'Centre (incl Ouagadougou)',
                    F.lit("Centre Region Burkina Faso")
                ).when(F.col("Region") == 'Centre-Sud',
                    F.lit("Centre Sud Region Burkina Faso")
                ).when(F.col("Region") == 'Est',
                    F.lit("Est Region Burkina Faso")    
                ).otherwise(
                    F.regexp_replace(F.col("Region"), "-", " ")
                )
            ).when(F.col("country_name") == 'Bhutan',
                F.when(F.col("Region") == 'Chukha',
                    F.lit("Chhukha")
                ).when(F.col("Region") == 'Lhuntse',
                    F.lit("Lhuentse")
                ).when(F.col("Region") == 'Samdrup jongkhar',
                    F.lit("Samdrup Jongkhar")
                ).when(F.col("Region") == 'Wangdi',
                    F.lit("Wangduephodrang")    
                ).otherwise(
                    F.col("Region")
                )
            ).when(F.col("country_name") == 'Nigeria',
                F.when(F.col("Region") == 'Abuja FCT',
                    F.lit("Federal Capital Territory")
                ).when(F.col("Region") == 'Nassarawa',
                    F.lit("Nasarawa")
                ).when(F.col("Region") == 'Zamfora',
                    F.lit("Zamfara")    
                ).otherwise(
                    F.col("Region")
                )
            ).when(F.col("country_name") == 'Colombia',
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
                    F.col("Region")
                )
            ).when(F.col("country_name") == 'Mozambique',
                F.when(F.col("Region") == 'Maputo Cidade',
                    F.lit("Cidade de Maputo")
                ).when(F.col("Region") == 'Maputo Provincia',
                    F.lit("Maputo")
                ).when(F.col("Region") == 'Cabo delgado',
                    F.lit("Cabo Delgado")
                ).otherwise(
                    F.col("Region")
                )
            ).when(F.col("country_name") == 'Congo Democratic Republic',
                F.initcap(F.regexp_replace(F.col("Region"), "-", " "))   
            ).otherwise(
                F.col("Region")
            )
        )
        .select(
            'country_name',
            'adm1_name',
            'year',
            F.col('ed').alias('education_index'),
            F.col('health').alias('health_index'),
            F.col('inc').alias('income_index'),
        )
    )
