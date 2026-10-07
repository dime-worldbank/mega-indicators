from utils import *

import pandas as pd

# Earliest and latest year with data, per indicator and country, read by the dashboard's
# source notes. A pandas port of indicator_data_availability_dlt.sql in mega-indicators,
# limited to the indicators whose tables this package produces.
INDICATORS = {
    # key: (table, columns that must all be non-null for a row to count, source url)
    'global_data_lab_hd_index': ('global_data_lab_hd_index', ['health_index', 'education_index'], 'https://globaldatalab.org/shdi/about/'),
    'global_data_lab_attendance': ('global_data_lab_hd_index', ['attendance_6to17yo'], 'https://globaldatalab.org/education/about/'),
    'learning_poverty_rate': ('learning_poverty_rate', [], 'https://data360.worldbank.org/en/indicator/WB_LPGD_SE_LPV_PRIM_SD'),
    'subnational_poverty_rate': ('subnational_poverty_rate', ['poverty_rate'], 'https://pipmaps.worldbank.org/en/data/datatopics/poverty-portal/home'),
    'universal_health_coverage_index_gho': ('universal_health_coverage_index_GHO', ['universal_health_coverage_index'], 'https://www.who.int/data/gho/data/indicators/indicator-details/GHO/uhc-index-of-service-coverage'),
    'pefa_by_pillar': ('pefa_by_pillar', [], 'https://www.pefa.org/assessments/batch-downloads'),
    'health_private_expenditure': ('health_expenditure', ['oop_per_capita_usd'], 'https://www.who.int/data/gho/data/indicators/indicator-details/GHO/out-of-pocket-expenditure-(oop)-per-capita-in-us'),
    'poverty_rate': ('poverty_rate', ['poverty_rate'], 'https://data360.worldbank.org/en/dataset/WB_PIP'),
}

rows = []
for key, (table, required, source_url) in INDICATORS.items():
    df = read_table(table)
    if required:
        df = df.dropna(subset=required)
    years = df.groupby('country_name')['year'].agg(earliest_year='min', latest_year='max').reset_index()
    years['indicator_key'] = key
    years['source_url'] = source_url
    rows.append(years)

availability = pd.concat(rows, ignore_index=True)[['country_name', 'indicator_key', 'earliest_year', 'latest_year', 'source_url']]
availability = availability.astype({'earliest_year': int, 'latest_year': int})
write_table(availability, 'indicator_data_availability')
