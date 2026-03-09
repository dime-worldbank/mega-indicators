-- Databricks notebook source
CREATE
OR REFRESH LIVE TABLE indicator_data_availability USING DELTA AS (
  WITH hd_index AS (
    SELECT
      country_name,
      'global_data_lab_hd_index' AS indicator_key,
      CAST(min(year) AS INT) AS start_year,
      CAST(max(year) AS INT) AS end_year
    FROM
      prd_mega.indicator.global_data_lab_hd_index
    WHERE
      health_index IS NOT NULL
      AND education_index IS NOT NULL
    GROUP BY
      1
  ),
  learning_poverty AS (
    SELECT
      country_name,
      'learning_poverty_rate' AS indicator_key,
      CAST(min(year) AS INT) AS start_year,
      CAST(max(year) AS INT) AS end_year
    FROM
      prd_mega.indicator.learning_poverty_rate
    GROUP BY
      1
  ),
  subnat_poverty AS (
    SELECT
      country_name,
      'subnational_poverty_rate' AS indicator_key,
      CAST(min(year) AS INT) AS start_year,
      CAST(max(year) AS INT) AS end_year
    FROM
      prd_mega.indicator.subnational_poverty_rate
    WHERE
      poverty_rate IS NOT NULL
    GROUP BY
      1
  ),
  health_coverage AS (
    SELECT
      country_name,
      'universal_health_coverage_index_gho' AS indicator_key,
      CAST(min(year) AS INT) AS start_year,
      CAST(max(year) AS INT) AS end_year
    FROM
      prd_mega.indicator.universal_health_coverage_index_gho
    WHERE
      universal_health_coverage_index IS NOT NULL
    GROUP BY
      1
  ),
  pefa AS (
    SELECT
      country_name,
      'pefa_by_pillar' AS indicator_key,
      CAST(min(year) AS INT) AS start_year,
      CAST(max(year) AS INT) AS end_year
    FROM
      prd_mega.indicator.pefa_by_pillar
    GROUP BY
      1
  ),
  all_indicators AS (
    SELECT * FROM hd_index
    UNION ALL
    SELECT * FROM learning_poverty
    UNION ALL
    SELECT * FROM subnat_poverty
    UNION ALL
    SELECT * FROM health_coverage
    UNION ALL
    SELECT * FROM pefa
  ),
  source_urls AS (
    SELECT
      indicator_key,
      source_url
    FROM
      LIVE.indicator_source_urls_bronze
  )
  SELECT
    a.country_name,
    a.indicator_key,
    a.start_year,
    a.end_year,
    su.source_url
  FROM
    all_indicators a
    LEFT JOIN source_urls su
      ON a.indicator_key = su.indicator_key
  ORDER BY
    a.country_name, a.indicator_key
)
