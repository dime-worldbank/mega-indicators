# Databricks notebook source
# MAGIC %run ../utils

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# Number of students enrolled and number of teachers by education level (both sexes).
# Pulled directly from the UNESCO Institute for Statistics (UIS) API. Values are head counts.
# Secondary is reported both as a whole and split into lower / upper secondary, but few
# countries report the split, so the secondary total is kept alongside it.
series_to_col_name = {
    '20060': 'students_pre_primary',
    '20062': 'students_primary',
    '20082': 'students_secondary',
    '20064': 'students_lower_secondary',
    '20070': 'students_upper_secondary',
    '25053': 'students_tertiary',
    '20160': 'teachers_pre_primary',
    '20162': 'teachers_primary',
    '20182': 'teachers_secondary',
    '20164': 'teachers_lower_secondary',
    '20170': 'teachers_upper_secondary',
    '25003': 'teachers_tertiary',
}

data_source = 'UNESCO Institute for Statistics (UIS)'

# outer join so a country-year is kept even if it reports only some levels
df = uis_fetch(series_to_col_name, data_source, how='outer')
df

# COMMAND ----------

write_table(df, 'students_and_teachers')

# COMMAND ----------

# Pupil-teacher ratio by education level (head count basis), derived from the counts above:
# the UIS API publishes only the qualified / trained teacher variants of this ratio.
levels = ['pre_primary', 'primary', 'secondary', 'lower_secondary', 'upper_secondary', 'tertiary']
id_cols = ['country_name', 'country_code', 'region', 'year']

ratio_df = df[id_cols].copy()
for level in levels:
    teachers = df[f'teachers_{level}'].where(df[f'teachers_{level}'] > 0)
    ratio_df[f'pupil_teacher_ratio_{level}'] = df[f'students_{level}'] / teachers
ratio_df['data_source'] = data_source
# keep only country-years where at least one level has a ratio
ratio_df = ratio_df.dropna(subset=[f'pupil_teacher_ratio_{l}' for l in levels], how='all')
ratio_df

# COMMAND ----------

write_table(ratio_df, 'pupil_teacher_ratio')
