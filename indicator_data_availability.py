# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

# Earliest and latest year with data, per indicator and country, read by the dashboard's
# source notes. Plain pandas, run as a notebook task (it replaced a DLT SQL view). A row counts for
# an indicator when every column in `all_of` is present, or any column in `any_of` is.
# The producers are sequenced ahead of this notebook by depends_on in
# resources/indicators_weekly.job.yml; a new indicator here needs its producer added there.
import pandas as pd

INDICATORS = {
    # key: (table, all_of, any_of, source_url)
    'global_data_lab_hd_index': ('global_data_lab_hd_index', ['health_index', 'education_index'], [],
        'https://globaldatalab.org/shdi/about/'),
    'learning_poverty_rate': ('learning_poverty_rate', [], [],
        'https://data360.worldbank.org/en/indicator/WB_LPGD_SE_LPV_PRIM_SD'),
    'subnational_poverty_rate': ('subnational_poverty_rate', ['poverty_rate'], [],
        'https://pipmaps.worldbank.org/en/data/datatopics/poverty-portal/home'),
    'universal_health_coverage_index_gho': ('universal_health_coverage_index_GHO', ['universal_health_coverage_index'], [],
        'https://www.who.int/data/gho/data/indicators/indicator-details/GHO/uhc-index-of-service-coverage'),
    'pefa_by_pillar': ('pefa_by_pillar', [], [],
        'https://www.pefa.org/assessments/batch-downloads'),
    'health_private_expenditure': ('health_expenditure', ['oop_per_capita_usd'], [],
        'https://www.who.int/data/gho/data/indicators/indicator-details/GHO/out-of-pocket-expenditure-(oop)-per-capita-in-us'),
    'poverty_rate': ('poverty_rate', ['poverty_rate'], [],
        'https://data360.worldbank.org/en/dataset/WB_PIP'),
    'global_data_lab_attendance': ('global_data_lab_hd_index', ['attendance_6to17yo'], [],
        'https://globaldatalab.org/education/about/'),
    'pupil_teacher_ratio': ('pupil_teacher_ratio', [],
        ['pupil_teacher_ratio_pre_primary', 'pupil_teacher_ratio_primary', 'pupil_teacher_ratio_secondary',
         'pupil_teacher_ratio_lower_secondary', 'pupil_teacher_ratio_upper_secondary', 'pupil_teacher_ratio_tertiary'],
        'https://databrowser.uis.unesco.org/resources/glossary/3189'),
    'school_basic_services': ('school_basic_services', [],
        ['schools_with_electricity_primary', 'schools_with_electricity_lower_secondary', 'schools_with_electricity_upper_secondary',
         'schools_with_internet_primary', 'schools_with_internet_lower_secondary', 'schools_with_internet_upper_secondary',
         'schools_with_computers_primary', 'schools_with_computers_lower_secondary', 'schools_with_computers_upper_secondary',
         'schools_with_basic_water_primary', 'schools_with_basic_water_lower_secondary', 'schools_with_basic_water_upper_secondary'],
        'https://databrowser.uis.unesco.org/resources/glossary/3145'),
    'teacher_salaries': ('teacher_salaries', [],
        ['teacher_salary_pre_primary', 'teacher_salary_primary', 'teacher_salary_lower_secondary', 'teacher_salary_upper_secondary'],
        'https://databrowser.uis.unesco.org/resources/glossary/3218'),
    'completion_rates': ('completion_rates', [],
        ['completion_rate_primary', 'completion_rate_lower_secondary', 'completion_rate_upper_secondary'],
        'https://databrowser.uis.unesco.org/resources/glossary/3201'),
}

# COMMAND ----------

rows = []
for key, (table, all_of, any_of, source_url) in INDICATORS.items():
    df = spark.table(f'{INDICATOR_SCHEMA}.{table}').toPandas()
    if all_of:
        df = df.dropna(subset=all_of)
    if any_of:
        df = df.dropna(subset=any_of, how='all')
    years = (df.groupby('country_name', dropna=False)['year']
               .agg(earliest_year='min', latest_year='max').reset_index())
    years['indicator_key'] = key
    years['source_url'] = source_url
    rows.append(years)

availability = (pd.concat(rows, ignore_index=True)
                  [['country_name', 'indicator_key', 'earliest_year', 'latest_year', 'source_url']]
                  .astype({'earliest_year': int, 'latest_year': int}))
availability

# COMMAND ----------

spark.createDataFrame(availability).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{INDICATOR_SCHEMA}.indicator_data_availability")