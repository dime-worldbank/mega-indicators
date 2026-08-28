# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# Teachers' salaries by education level (UIS TSALARY series). Pulled directly
# from the UNESCO Institute for Statistics (UIS) API.
# Levels: .0 = pre-primary, .1 = primary, .2 = lower secondary, .3 = upper secondary.
series_to_col_name = {
    'TSALARY.0': 'teacher_salary_pre_primary',
    'TSALARY.1': 'teacher_salary_primary',
    'TSALARY.2': 'teacher_salary_lower_secondary',
    'TSALARY.3': 'teacher_salary_upper_secondary',
}

# outer join so a country-year is kept even if it reports only some levels
df = uis_fetch(series_to_col_name, how='outer')
df

# COMMAND ----------

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{INDICATOR_SCHEMA}.teacher_salaries")
