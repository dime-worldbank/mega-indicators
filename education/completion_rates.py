# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# Completion rate by education level (UIS CR.MOD modeled series). Pulled directly
# from the UNESCO Institute for Statistics (UIS) API. Values are percentages (0-100).
# Levels: .1 = primary, .2 = lower secondary, .3 = upper secondary.
series_to_col_name = {
    'CR.MOD.1': 'completion_rate_primary',
    'CR.MOD.2': 'completion_rate_lower_secondary',
    'CR.MOD.3': 'completion_rate_upper_secondary',
}

# outer join so a country-year is kept even if it reports only some levels
df = uis_fetch(series_to_col_name, how='outer')
df

# COMMAND ----------

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{INDICATOR_SCHEMA}.completion_rates")
