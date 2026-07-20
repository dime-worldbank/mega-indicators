-- Databricks notebook source
-- Canonical registry of data sources — the single source of truth for source
-- identity (id, display name, publisher, url). Consumed by indicator_source and,
-- in later work, by indicator_data_availability and the rpf-country-dash popover.
-- Presentation/i18n labels live dashboard-side, keyed by source_id.
CREATE
OR REFRESH LIVE TABLE source_registry USING DELTA AS (
  SELECT * FROM (
    VALUES
      ('boost',           'BOOST',                                                                       'World Bank',       'https://www.worldbank.org/en/programs/boost-portal/country-data'),
      ('imf_weo',         'World Economic Outlook — General Government',                                 'IMF',              'https://www.imf.org/en/Publications/WEO'),
      ('imf_gfs',         'Government Finance Statistics (Statement of Operations) — Budgetary Central Government', 'IMF',    'https://data.imf.org/en/datasets/IMF.STA:GFS_SOO'),
      ('togo_dgb',        'Budget Execution Report',                                                     'Togo DGB',         CAST(NULL AS STRING)),
      ('world_bank_pip',  'Poverty and Inequality Platform',                                             'World Bank',       'https://data360.worldbank.org/en/dataset/WB_PIP'),
      ('pip_spid',        'PIP — Subnational Poverty (SPID)',                                             'World Bank',       'https://pipmaps.worldbank.org/en/data/datatopics/poverty-portal/home'),
      ('pip_gsap',        'PIP — Global Subnational Atlas of Poverty (GSAP)',                             'World Bank',       'https://pipmaps.worldbank.org/en/data/datatopics/poverty-portal/home'),
      ('world_bank_icp',  'International Comparison Program',                                             'World Bank',       'https://www.worldbank.org/en/programs/icp/data'),
      ('world_bank_lpgd', 'Global Learning Poverty Database',                                            'World Bank',       'https://data360.worldbank.org/en/int/dataset/WB_LPGD'),
      ('who_gho',         'Global Health Observatory',                                                   'WHO',              'https://www.who.int/data/gho'),
      ('who_nha',         'Global Health Expenditure Database',                                          'WHO',              'https://apps.who.int/nha/database/'),
      ('pefa',            'Public Expenditure & Financial Accountability',                               'PEFA Secretariat', 'https://www.pefa.org/assessments/batch-downloads'),
      ('global_data_lab', 'Subnational HDI / Area Database',                                             'Global Data Lab',  'https://globaldatalab.org/shdi/about/'),
      ('uis',                       'Institute for Statistics database',                                 'UNESCO',           'https://uis.unesco.org/'),
      ('wdi_population',            'World Development Indicators — Total population',                    'World Bank',       'https://data.worldbank.org/indicator/SP.POP.TOTL'),
      ('wdi_gdp',                   'World Development Indicators — GDP, national accounts (NY.GDP.*)',   'World Bank',       'https://datatopics.worldbank.org/world-development-indicators/themes/economy.html#production'),
      ('oecd_eag',                  'Education at a Glance (UOE finance)',                               'OECD',             'https://www.oecd.org/en/about/programmes/education-at-a-glance.html'),
      ('wwbi',                      'Worldwide Bureaucracy Indicators',                                  'World Bank',       'https://www.worldbank.org/en/data/interactive/2019/05/21/worldwide-bureaucracy-indicators-dashboard'),
      ('who_mmr',                   'Maternal Mortality Estimates (MMEIG)',                              'WHO',              'https://www.who.int/data/gho/data/indicators/indicator-details/GHO/maternal-mortality-ratio'),
      ('census_gov',                'International Database',                                             'US Census Bureau', 'https://www.census.gov/programs-surveys/international-programs/about/idb.html'),
      ('wb_subnational_population', 'Subnational Population database',                                    'World Bank',       'https://databank.worldbank.org/source/subnational-population'),
      ('alb_instat',                'Population by prefecture',                                           'INSTAT',           'https://www.instat.gov.al/'),
      ('moz_ine',                   'Population projections',                                             'INE Mozambique',   'https://mozambique.opendataforafrica.org/'),
      ('pry_ine',                   'Population by department',                                           'INE Paraguay',     'https://www.ine.gov.py/')
  ) AS t(source_id, name, publisher, url)
)
