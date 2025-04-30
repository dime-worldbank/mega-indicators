# Databricks notebook source
# MAGIC %run /Users/elysenko@worldbank.org/mega-indicators/population/read_subnational_population_gdl

# COMMAND ----------

country_code = 'LBR'
country_name = 'Liberia'
pop = read_subnational_population_gdl(country_name,country_code)
write_to_database(pop,country_code)
