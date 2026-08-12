# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# Proportion of schools with access to basic services / infrastructure, by
# education level (UIS "school basic services", SDG 4.a.1). Pulled directly from
# the UNESCO Institute for Statistics (UIS) API. Values are percentages (0-100).
# Levels: .1 = primary, .2 = lower secondary, .3 = upper secondary.
series_to_col_name = {
    'SCHBSP.1.WELEC': 'schools_with_electricity_primary',
    'SCHBSP.2.WELEC': 'schools_with_electricity_lower_secondary',
    'SCHBSP.3.WELEC': 'schools_with_electricity_upper_secondary',
    'SCHBSP.1.WINTERN': 'schools_with_internet_primary',
    'SCHBSP.2.WINTERN': 'schools_with_internet_lower_secondary',
    'SCHBSP.3.WINTERN': 'schools_with_internet_upper_secondary',
    'SCHBSP.1.WCOMPUT': 'schools_with_computers_primary',
    'SCHBSP.2.WCOMPUT': 'schools_with_computers_lower_secondary',
    'SCHBSP.3.WCOMPUT': 'schools_with_computers_upper_secondary',
    'SCHBSP.1.WWATA': 'schools_with_basic_water_primary',
    'SCHBSP.2.WWATA': 'schools_with_basic_water_lower_secondary',
    'SCHBSP.3.WWATA': 'schools_with_basic_water_upper_secondary',
}

# outer join so a country-year is kept even if it reports only some indicators
df = uis_fetch(series_to_col_name, how='outer')
df

# COMMAND ----------

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{INDICATOR_SCHEMA}.school_basic_services")
