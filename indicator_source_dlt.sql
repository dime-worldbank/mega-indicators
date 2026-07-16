-- Databricks notebook source
-- Many-to-many bridge: which source(s) feed each indicator. This is source
-- attribution as table-level metadata (replacing the per-row `data_source`
-- column). One indicator may have several sources (e.g. government_revenue_
-- expenditure ← imf_weo + imf_gfs); one source may feed several indicators
-- (e.g. global_data_lab). source_id references source_registry.
CREATE
OR REFRESH LIVE TABLE indicator_source USING DELTA AS (
  SELECT * FROM (
    VALUES
      ('government_revenue_expenditure',      'imf_weo'),
      ('government_revenue_expenditure',      'imf_gfs'),
      ('togo_revenue_budget',                 'togo_dgb'),
      ('subnational_poverty_rate',            'pip_spid'),
      ('subnational_poverty_rate',            'pip_gsap'),
      ('poverty_rate',                        'world_bank_pip'),
      ('learning_poverty_rate',               'world_bank_pip'),
      ('learning_poverty_rate',               'unesco_uis'),
      ('pefa_by_pillar',                      'pefa'),
      ('universal_health_coverage_index_gho', 'who_gho'),
      ('health_private_expenditure',          'who_nha'),
      ('edu_private_expenditure',             'world_bank_icp'),
      ('global_data_lab_hd_index',            'global_data_lab'),
      ('global_data_lab_attendance',          'global_data_lab')
  ) AS t(indicator_key, source_id)
)
