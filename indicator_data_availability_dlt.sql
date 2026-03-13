-- Databricks notebook source
CREATE
OR REFRESH LIVE TABLE indicator_data_availability USING DELTA AS (
  WITH hd_index AS (
    SELECT
      country_name,
      'global_data_lab_hd_index' AS indicator_key,
      CAST(min(year) AS INT) AS earliest_year,
      CAST(max(year) AS INT) AS latest_year
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
      CAST(min(year) AS INT) AS earliest_year,
      CAST(max(year) AS INT) AS latest_year
    FROM
      prd_mega.indicator.learning_poverty_rate
    GROUP BY
      1
  ),
  subnat_poverty AS (
    SELECT
      country_name,
      'subnational_poverty_rate' AS indicator_key,
      CAST(min(year) AS INT) AS earliest_year,
      CAST(max(year) AS INT) AS latest_year
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
      CAST(min(year) AS INT) AS earliest_year,
      CAST(max(year) AS INT) AS latest_year
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
      CAST(min(year) AS INT) AS earliest_year,
      CAST(max(year) AS INT) AS latest_year
    FROM
      prd_mega.indicator.pefa_by_pillar
    GROUP BY
      1
  ),
  health_private AS (
    SELECT
      country_name,
      'health_private_expenditure' AS indicator_key,
      CAST(min(year) AS INT) AS earliest_year,
      CAST(max(year) AS INT) AS latest_year
    FROM
      prd_mega.indicator.health_expenditure
    WHERE
      oop_per_capita_usd IS NOT NULL
    GROUP BY
      1
  ),
  national_poverty AS (
    SELECT
      country_name,
      'poverty_rate' AS indicator_key,
      CAST(min(year) AS INT) AS earliest_year,
      CAST(max(year) AS INT) AS latest_year
    FROM
      prd_mega.indicator.poverty_rate
    WHERE
      poverty_rate IS NOT NULL
    GROUP BY
      1
  ),
  edu_attendance AS (
    SELECT
      country_name,
      'global_data_lab_attendance' AS indicator_key,
      CAST(min(year) AS INT) AS earliest_year,
      CAST(max(year) AS INT) AS latest_year
    FROM
      prd_mega.indicator.global_data_lab_hd_index
    WHERE
      attendance_6to17yo IS NOT NULL
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
    UNION ALL
    SELECT * FROM health_private
    UNION ALL
    SELECT * FROM national_poverty
    UNION ALL
    SELECT * FROM edu_attendance
  ),
  source_urls AS (
    SELECT * FROM (
      VALUES
        ('global_data_lab_hd_index', 'https://globaldatalab.org/shdi/about/'),
        ('learning_poverty_rate', 'https://data360.worldbank.org/en/indicator/WB_LPGD_SE_LPV_PRIM_SD'),
        ('subnational_poverty_rate', 'https://pipmaps.worldbank.org/en/data/datatopics/poverty-portal/home'),
        ('universal_health_coverage_index_gho', 'https://www.who.int/data/gho/data/indicators/indicator-details/GHO/uhc-index-of-service-coverage'),
        ('pefa_by_pillar', 'https://www.pefa.org/assessments/batch-downloads'),
        ('health_private_expenditure', 'https://www.who.int/data/gho/data/indicators/indicator-details/GHO/out-of-pocket-expenditure-(oop)-per-capita-in-us'),
        ('poverty_rate', 'https://data360.worldbank.org/en/dataset/WB_PIP'),
        ('global_data_lab_attendance', 'https://globaldatalab.org/education/about/')
    ) AS t(indicator_key, source_url)
  )
  SELECT
    a.country_name,
    a.indicator_key,
    a.earliest_year,
    a.latest_year,
    s.source_url
  FROM
    all_indicators a
    LEFT JOIN source_urls s
      ON a.indicator_key = s.indicator_key
)
