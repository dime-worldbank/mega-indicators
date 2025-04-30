# Databricks notebook source
# MAGIC %run /Users/elysenko@worldbank.org/mega-indicators/population/process_to_indicator_intermediate

# COMMAND ----------

country_code = 'LBR'
country_name = 'Liberia'
pop = process_to_indicator_intermediate(country_name,country_code)
write_to_indicator_intermediate(pop,country_code)
