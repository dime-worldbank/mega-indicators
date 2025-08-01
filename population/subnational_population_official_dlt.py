# Databricks notebook source
import dlt
from pyspark.sql import functions as F

# Adding a new country requires adding the country here
country_codes = ['moz', 'pry', 'ken', 'pak', 'bfa', 'col', 'cod', 'tun', 'btn', 'chl', 'nga', 'bgd', 'alb', "zaf", 'chl', 'gha', 'lbr', 'tgo']

@dlt.table(name=f'subnational_population')
def subnational_population():
    # Consolidating all the country specific dataframes
    dfs = [spark.table(f'prd_mega.indicator_intermediate.{code}_subnational_population') for code in country_codes]
    result_df = dfs[0]
    for df in dfs[1:]:
        result_df = result_df.unionByName(df)
    return result_df
