# Databricks notebook source
import pandas as pd

def get_pop_from_census_gov(country_filename, timeseries='pepfar'):
    url = f'https://www2.census.gov/programs-surveys/international-programs/tables/time-series/{timeseries}/{country_filename}.xlsx'

    # Find the first sheet name starting with '2' & read it
    xls = pd.ExcelFile(url)
    target_sheet = next(sheet for sheet in xls.sheet_names if sheet.startswith('2'))
    df_raw = pd.read_excel(xls, sheet_name=target_sheet, skiprows=2, header=None)

    # Read correct header
    header = df_raw.iloc[1]
    df_raw.columns = header
    df_raw = df_raw.drop([0,1,2])

    # Determine country name column
    country_col = None
    for col in ['COUNTRY', 'CNTRY_NAME']:
        if col in df_raw.columns:
            country_col = col
            break
    
    if country_col is None:
        raise ValueError(f"Neither 'COUNTRY' nor 'CNTRY_NAME' found in dataframe columns {df_raw.columns}")
    
    # Extract Total population columns 
    df_pop_wide = df_raw[df_raw.ADM_LEVEL==1][[country_col, 'ADM1_NAME']+[x for x in header if 'BTOTL' in x]]
    df_pop = pd.melt(df_pop_wide, id_vars=[country_col, 'ADM1_NAME'], var_name='year', value_name='population')
    df_pop['year'] = df_pop['year'].str.extract(r'(\d+)').astype(int)
    df_pop.columns = ['country_name', 'adm1_name', 'year', 'population']

    # Modifications to the admin1 and county name and add data_source
    df_pop['country_name'] = df_pop['country_name'].str.title()
    df_pop['adm1_name'] = df_pop['adm1_name'].str.replace(r'[-/]+', ' ', regex=True).str.title()
    df_pop['data_source'] = url
    df_pop = df_pop.astype({'year': 'int', 'population': 'int'})
    df_pop = df_pop.sort_values(['adm1_name', 'year'], ignore_index=True)
    
    return df_pop
