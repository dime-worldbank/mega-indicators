from utils import *

# global_data_lab_hd_index: the silver table joined to country, regions named as in
# admin1_boundaries_gold, attendance also as a 0-1 share. (On Databricks a DLT pipeline
# does this and also applies per-country region-name fixes; Togo needs none.)
silver = read_table('global_data_lab_hd_index_silver').rename(columns={'ISO_Code': 'country_code'})
countries = read_table('country', columns=['country_name', 'country_code'])
df = silver.merge(countries, on='country_code', how='inner')

df['adm1_name'] = df['Region'].str.replace(r'\(.*\)', '', regex=True).str.strip()
df['attendance_6to17yo'] = df['attendance'] / 100

df = df.rename(columns={'edindex': 'education_index', 'healthindex': 'health_index', 'incindex': 'income_index'})
write_table(df[['country_name', 'adm1_name', 'year', 'education_index', 'health_index', 'income_index',
                'attendance', 'attendance_6to17yo']], 'global_data_lab_hd_index')
