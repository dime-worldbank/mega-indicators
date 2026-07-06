# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# Pupil-teacher ratio by education level (World Bank / UNESCO Institute for Statistics)
indicators = [
    'SE.PRE.ENRL.TC.ZS',
    'SE.PRM.ENRL.TC.ZS',
    'SE.SEC.ENRL.TC.ZS',
    'SE.SEC.ENRL.LO.TC.ZS',
    'SE.SEC.ENRL.UP.TC.ZS',
    'SE.TER.ENRL.TC.ZS',
]
col_names = [
    'pupil_teacher_ratio_pre_primary',
    'pupil_teacher_ratio_primary',
    'pupil_teacher_ratio_secondary',
    'pupil_teacher_ratio_lower_secondary',
    'pupil_teacher_ratio_upper_secondary',
    'pupil_teacher_ratio_tertiary',
]

data_source = 'UNESCO Institute for Statistics (UIS)'

# outer join so a country-year is kept even if it reports only some education levels
df = wbgapi_fetch(indicators, col_names, data_source, how='outer')
df

# COMMAND ----------

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("prd_mega.indicator.pupil_teacher_ratio")
