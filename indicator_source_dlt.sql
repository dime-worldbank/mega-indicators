-- Databricks notebook source
-- Many-to-many bridge: which source(s) feed each indicator. This is source
-- attribution as table-level metadata (replacing the per-row `data_source`
-- column). One indicator may have several sources (e.g. government_revenue_
-- expenditure ← imf_weo + imf_gfs); one source may feed several indicators
-- (e.g. global_data_lab). source_id references source_registry. `boost` is a
-- pseudo-indicator for the core expenditure data (coverage from boost_source_urls),
-- so charts can resolve their source through this bridge uniformly.
--
-- country_name scopes a row to one country; NULL means the source applies to every
-- country. Only subnational_population needs this — each country draws its admin1
-- population from one or more country-specific upstream sources (e.g. Albania
-- blends wb_subnational_population, alb_instat and imputed; Burkina Faso blends
-- census_gov and wb_subnational_population). country_name (not code) matches the
-- key the dashboard popover already uses everywhere; values are the canonical names
-- from the country table. Consumers resolve a source for (indicator_key, country)
-- with: country_name IS NULL OR country_name = <country>.
CREATE
OR REFRESH LIVE TABLE indicator_source USING DELTA AS (
  -- Sources that apply to every country (country_name NULL, cast once here).
  SELECT indicator_key, source_id, CAST(NULL AS STRING) AS country_name FROM (
    VALUES
      ('boost',                               'boost'),
      ('government_revenue_expenditure',      'imf_weo'),
      ('government_revenue_expenditure',      'imf_gfs'),
      ('togo_revenue_budget',                 'togo_dgb'),
      ('subnational_poverty_rate',            'pip_spid'),
      ('subnational_poverty_rate',            'pip_gsap'),
      ('poverty_rate',                        'world_bank_pip'),
      ('learning_poverty_rate',               'world_bank_lpgd'),
      ('pefa_by_pillar',                      'pefa'),
      ('universal_health_coverage_index_gho', 'who_gho'),
      ('health_private_expenditure',          'who_nha'),
      ('edu_private_expenditure',             'world_bank_icp'),
      ('global_data_lab_hd_index',            'global_data_lab'),
      ('global_data_lab_attendance',          'global_data_lab'),
      -- Onboarded indicators not yet charted; registered + bridged so a future chart
      -- resolves its source with no further pipeline work.
      ('completion_rates',                    'uis'),
      ('edu_gov_spending',                    'uis'),
      ('youth_literacy_rate_unesco',          'uis'),
      ('pupil_teacher_ratio',                 'uis'),
      ('school_basic_services',               'uis'),
      ('teacher_salaries',                    'uis'),
      ('edu_private_spending',                'oecd_eag'),
      ('edu_spending',                        'world_bank_icp'),
      ('gdp',                                 'wdi_gdp'),
      ('population',                          'wdi_population'),
      ('health_expenditure',                  'who_nha'),
      ('maternal_mortality_ratio',            'who_mmr'),
      ('public_sector_employment',            'wwbi')
  ) AS t(indicator_key, source_id)
  UNION ALL
  -- Subnational population: one or more sources per country (see country folders).
  SELECT indicator_key, source_id, country_name FROM (
    VALUES
      ('subnational_population', 'census_gov',                'Togo'),
      ('subnational_population', 'census_gov',                'Pakistan'),
      ('subnational_population', 'census_gov',                'Nigeria'),
      ('subnational_population', 'census_gov',                'Bangladesh'),
      ('subnational_population', 'census_gov',                'Kenya'),
      ('subnational_population', 'census_gov',                'Ghana'),
      ('subnational_population', 'census_gov',                'Burkina Faso'),
      ('subnational_population', 'census_gov',                'Colombia'),
      ('subnational_population', 'wb_subnational_population',  'Albania'),
      ('subnational_population', 'wb_subnational_population',  'Bhutan'),
      ('subnational_population', 'wb_subnational_population',  'Chile'),
      ('subnational_population', 'wb_subnational_population',  'Tunisia'),
      ('subnational_population', 'wb_subnational_population',  'South Africa'),
      ('subnational_population', 'wb_subnational_population',  'Burkina Faso'),
      ('subnational_population', 'global_data_lab',           'Congo, Dem. Rep.'),
      ('subnational_population', 'global_data_lab',           'Liberia'),
      ('subnational_population', 'alb_instat',                'Albania'),
      ('subnational_population', 'imputed',                   'Albania'),
      ('subnational_population', 'moz_ine',                   'Mozambique'),
      ('subnational_population', 'pry_ine',                   'Paraguay')
  ) AS t(indicator_key, source_id, country_name)
)
