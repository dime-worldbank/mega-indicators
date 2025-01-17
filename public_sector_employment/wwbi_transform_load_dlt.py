# Databricks notebook source
import dlt
import pyspark.sql.functions as F

@dlt.table(name=f'public_sector_employment')
def public_sector_employment():
    countries = spark.table(f'prd_mega.indicator.country').select('country_name', 'country_code', 'region')

    employment = (spark.table(f'prd_mega.indicator_intermediate.public_sector_employment')
        .withColumnRenamed("economy", 'country_code')
        .join(countries, on=["country_code"], how="inner"))

    regional_means = (
        employment.groupBy("region", "year")
            .agg(F.mean("wage_percent_gdp").alias("wage_percent_gdp"),
                 F.mean("wage_percent_expenditure").alias("wage_percent_expenditure"),
                 F.mean("wage_premium").alias("wage_premium"),
                 F.first("data_source").alias("data_source"))
            .withColumnRenamed("region", "country_code")
            .join(countries, on=['country_code'], how="inner")
    )

    return (employment.union(regional_means))
