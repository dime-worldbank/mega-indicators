# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# Earliest and latest year with data, and every year with data, per indicator and country,
# read by the dashboard's source notes. Source attribution lives in source_registry /
# indicator_source. Plain pandas on both sides. A row counts for
# an indicator when every column in `all_of` is present, or any column in `any_of` is.
# The producers are sequenced ahead of this notebook by depends_on in
# resources/indicators_weekly.job.yml; a new indicator here needs its producer added there.
import pandas as pd

INDICATORS = {
    # key: (table, all_of, any_of)
    'global_data_lab_hd_index': ('global_data_lab_hd_index', ['health_index', 'education_index'], []),
    'learning_poverty_rate': ('learning_poverty_rate', [], []),
    'subnational_poverty_rate': ('subnational_poverty_rate', ['poverty_rate'], []),
    'universal_health_coverage_index_gho': ('universal_health_coverage_index_GHO', ['universal_health_coverage_index'], []),
    'pefa_by_pillar': ('pefa_by_pillar', [], []),
    'health_private_expenditure': ('health_expenditure', ['oop_per_capita_usd'], []),
    'poverty_rate': ('poverty_rate', ['poverty_rate'], []),
    'global_data_lab_attendance': ('global_data_lab_hd_index', ['attendance_6to17yo'], []),
    'pupil_teacher_ratio': ('pupil_teacher_ratio', [],
        ['pupil_teacher_ratio_pre_primary', 'pupil_teacher_ratio_primary', 'pupil_teacher_ratio_secondary',
         'pupil_teacher_ratio_lower_secondary', 'pupil_teacher_ratio_upper_secondary', 'pupil_teacher_ratio_tertiary']),
    'school_basic_services': ('school_basic_services', [],
        ['schools_with_electricity_primary', 'schools_with_electricity_lower_secondary', 'schools_with_electricity_upper_secondary',
         'schools_with_internet_primary', 'schools_with_internet_lower_secondary', 'schools_with_internet_upper_secondary',
         'schools_with_computers_primary', 'schools_with_computers_lower_secondary', 'schools_with_computers_upper_secondary',
         'schools_with_basic_water_primary', 'schools_with_basic_water_lower_secondary', 'schools_with_basic_water_upper_secondary']),
    'teacher_salaries': ('teacher_salaries', [],
        ['teacher_salary_pre_primary', 'teacher_salary_primary', 'teacher_salary_lower_secondary', 'teacher_salary_upper_secondary']),
    'completion_rates': ('completion_rates', [],
        ['completion_rate_primary', 'completion_rate_lower_secondary', 'completion_rate_upper_secondary']),
}

# COMMAND ----------

rows = []
for key, (table, all_of, any_of) in INDICATORS.items():
    df = read_table(table)
    if all_of:
        df = df.dropna(subset=all_of)
    if any_of:
        df = df.dropna(subset=any_of, how='all')
    years = (df.groupby('country_name', dropna=False)['year']
               .agg(earliest_year='min', latest_year='max',
                    years=lambda y: sorted({int(v) for v in y.dropna()}))
               .reset_index())
    years['indicator_key'] = key
    rows.append(years)

availability = (pd.concat(rows, ignore_index=True)
                  [['country_name', 'indicator_key', 'earliest_year', 'latest_year', 'years']]
                  .astype({'earliest_year': int, 'latest_year': int}))
availability

# COMMAND ----------

write_table(availability, 'indicator_data_availability')