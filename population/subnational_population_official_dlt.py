# Databricks notebook source
import dlt
from pyspark.sql import functions as F

# Adding a new country requires adding the country here
@dlt.table(name=f'subnational_population')
def subnational_population():
    moz = spark.table('indicator_intermediate.moz_subnational_population')
    pry = spark.table('indicator_intermediate.pry_subnational_population')
    return moz.union(pry)
