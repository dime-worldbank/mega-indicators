-- Databricks notebook source
CREATE
OR REFRESH LIVE TABLE indicator_data_availability USING DELTA AS (
  WITH hd_index AS (
    SELECT
      country_name,
      'global_data_lab_hd_index' AS indicator_key,
      CAST(min(year) AS INT) AS earliest_year,
      CAST(max(year) AS INT) AS latest_year,
      array_sort(collect_set(CAST(year AS INT))) AS years
    FROM
      global_data_lab_hd_index
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
      CAST(max(year) AS INT) AS latest_year,
      array_sort(collect_set(CAST(year AS INT))) AS years
    FROM
      learning_poverty_rate
    GROUP BY
      1
  ),
  subnat_poverty AS (
    SELECT
      country_name,
      'subnational_poverty_rate' AS indicator_key,
      CAST(min(year) AS INT) AS earliest_year,
      CAST(max(year) AS INT) AS latest_year,
      array_sort(collect_set(CAST(year AS INT))) AS years
    FROM
      subnational_poverty_rate
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
      CAST(max(year) AS INT) AS latest_year,
      array_sort(collect_set(CAST(year AS INT))) AS years
    FROM
      universal_health_coverage_index_gho
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
      CAST(max(year) AS INT) AS latest_year,
      array_sort(collect_set(CAST(year AS INT))) AS years
    FROM
      pefa_by_pillar
    GROUP BY
      1
  ),
  health_private AS (
    SELECT
      country_name,
      'health_private_expenditure' AS indicator_key,
      CAST(min(year) AS INT) AS earliest_year,
      CAST(max(year) AS INT) AS latest_year,
      array_sort(collect_set(CAST(year AS INT))) AS years
    FROM
      health_expenditure
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
      CAST(max(year) AS INT) AS latest_year,
      array_sort(collect_set(CAST(year AS INT))) AS years
    FROM
      poverty_rate
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
      CAST(max(year) AS INT) AS latest_year,
      array_sort(collect_set(CAST(year AS INT))) AS years
    FROM
      global_data_lab_hd_index
    WHERE
      attendance_6to17yo IS NOT NULL
    GROUP BY
      1
  ),
  pupil_teacher_ratio AS (
    SELECT
      country_name,
      'pupil_teacher_ratio' AS indicator_key,
      CAST(min(year) AS INT) AS earliest_year,
      CAST(max(year) AS INT) AS latest_year,
      array_sort(collect_set(CAST(year AS INT))) AS years
    FROM
      pupil_teacher_ratio
    WHERE
      COALESCE(pupil_teacher_ratio_pre_primary, pupil_teacher_ratio_primary, pupil_teacher_ratio_secondary, pupil_teacher_ratio_lower_secondary, pupil_teacher_ratio_upper_secondary, pupil_teacher_ratio_tertiary) IS NOT NULL
    GROUP BY
      1
  ),
  school_basic_services AS (
    SELECT
      country_name,
      'school_basic_services' AS indicator_key,
      CAST(min(year) AS INT) AS earliest_year,
      CAST(max(year) AS INT) AS latest_year,
      array_sort(collect_set(CAST(year AS INT))) AS years
    FROM
      school_basic_services
    WHERE
      COALESCE(
        schools_with_electricity_primary, schools_with_electricity_lower_secondary, schools_with_electricity_upper_secondary,
        schools_with_internet_primary, schools_with_internet_lower_secondary, schools_with_internet_upper_secondary,
        schools_with_computers_primary, schools_with_computers_lower_secondary, schools_with_computers_upper_secondary,
        schools_with_basic_water_primary, schools_with_basic_water_lower_secondary, schools_with_basic_water_upper_secondary
      ) IS NOT NULL
    GROUP BY
      1
  ),
  teacher_salaries AS (
    SELECT
      country_name,
      'teacher_salaries' AS indicator_key,
      CAST(min(year) AS INT) AS earliest_year,
      CAST(max(year) AS INT) AS latest_year,
      array_sort(collect_set(CAST(year AS INT))) AS years
    FROM
      teacher_salaries
    WHERE
      COALESCE(
        teacher_salary_pre_primary, teacher_salary_primary,
        teacher_salary_lower_secondary, teacher_salary_upper_secondary
      ) IS NOT NULL
    GROUP BY
      1
  ),
  completion_rates AS (
    SELECT
      country_name,
      'completion_rates' AS indicator_key,
      CAST(min(year) AS INT) AS earliest_year,
      CAST(max(year) AS INT) AS latest_year,
      array_sort(collect_set(CAST(year AS INT))) AS years
    FROM
      completion_rates
    WHERE
      COALESCE(
        completion_rate_primary, completion_rate_lower_secondary, completion_rate_upper_secondary
      ) IS NOT NULL
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
    UNION ALL
    SELECT * FROM pupil_teacher_ratio
    UNION ALL
    SELECT * FROM school_basic_services
    UNION ALL
    SELECT * FROM teacher_salaries
    UNION ALL
    SELECT * FROM completion_rates
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
        ('global_data_lab_attendance', 'https://globaldatalab.org/education/about/'),
        ('pupil_teacher_ratio', 'https://databrowser.uis.unesco.org/resources/glossary/3189'),
        ('school_basic_services', 'https://databrowser.uis.unesco.org/resources/glossary/3145'),
        ('teacher_salaries', 'https://databrowser.uis.unesco.org/resources/glossary/3218'),
        ('completion_rates', 'https://databrowser.uis.unesco.org/resources/glossary/3201')
    ) AS t(indicator_key, source_url)
  )
  SELECT
    a.country_name,
    a.indicator_key,
    a.earliest_year,
    a.latest_year,
    a.years,
    s.source_url
  FROM
    all_indicators a
    LEFT JOIN source_urls s
      ON a.indicator_key = s.indicator_key
)
