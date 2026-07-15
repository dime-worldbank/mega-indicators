# Source Registry (Pipeline Foundation) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create two new declarative metadata tables — `source_registry` (canonical source identity) and `indicator_source` (the indicator→source bridge) — as the single source of truth for data-source metadata.

**Architecture:** Follow the existing `indicator_data_availability_dlt.sql` pattern: each table is a DLT `LIVE TABLE` seeded from an inline `VALUES` list, added to the same DLT pipeline so it materializes into `prd_mega.indicator`. This plan is **purely additive** — nothing reads these tables yet, so it cannot break the running dashboard. Rewiring `indicator_data_availability` and the dashboard are deliberately deferred to follow-on plans (see Out of Scope).

**Tech Stack:** Databricks Delta Live Tables (DLT), Spark SQL. No local Spark — validation runs as SQL cells in a Databricks notebook. There is no local test harness for pipeline SQL (only the pure-Python `imf_sdmx` helpers are pytest-covered), so acceptance checks in this plan are Databricks/SQL queries with expected output.

## Global Constraints

- Target schema is `prd_mega.indicator`, set by the DLT pipeline; SQL uses bare `LIVE TABLE <name>` (no catalog/schema prefix), exactly as `indicator_data_availability_dlt.sql` does.
- `source_id` values are stable snake_case slugs and are the primary key of the whole scheme.
- `indicator_key` values MUST match the keys already used in `indicator_data_availability_dlt.sql` where the indicator already exists there (`global_data_lab_hd_index`, `learning_poverty_rate`, `subnational_poverty_rate`, `universal_health_coverage_index_gho`, `pefa_by_pillar`, `health_private_expenditure`, `poverty_rate`, `global_data_lab_attendance`).
- Every `indicator_source.source_id` MUST exist in `source_registry.source_id` (referential integrity).
- New DLT SQL files live at repo root, alongside `indicator_data_availability_dlt.sql`.
- Spec: `docs/superpowers/specs/2026-07-15-source-registry-design.md`.

---

## File Structure

- Create: `source_registry_dlt.sql` — one `LIVE TABLE source_registry` seeded from `VALUES`. Columns: `source_id, name, publisher, url`. ~13 rows. Sole responsibility: canonical source identity.
- Create: `indicator_source_dlt.sql` — one `LIVE TABLE indicator_source` seeded from `VALUES`. Columns: `indicator_key, source_id`. ~14 rows. Sole responsibility: many-to-many indicator↔source bridge.
- Workspace-only (not in git): add both files to the existing DLT pipeline that already builds `indicator_data_availability`.

---

### Task 1: `source_registry` table

**Files:**
- Create: `source_registry_dlt.sql`

**Interfaces:**
- Consumes: nothing.
- Produces: table `prd_mega.indicator.source_registry(source_id STRING, name STRING, publisher STRING, url STRING)`. `source_id` unique. Consumed by Task 2's validation and by later plans (availability rewire, dashboard).

- [ ] **Step 1: Write the table definition**

Create `source_registry_dlt.sql` with exactly this content:

```sql
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
      ('unesco_uis',      'Institute for Statistics',                                                    'UNESCO',           'https://uis.unesco.org/'),
      ('who_gho',         'Global Health Observatory',                                                   'WHO',              'https://www.who.int/data/gho'),
      ('who_nha',         'Global Health Expenditure Database',                                          'WHO',              'https://apps.who.int/nha/database/'),
      ('pefa',            'Public Expenditure & Financial Accountability',                               'PEFA Secretariat', 'https://www.pefa.org/assessments/batch-downloads'),
      ('global_data_lab', 'Subnational HDI / Area Database',                                             'Global Data Lab',  'https://globaldatalab.org/shdi/about/')
  ) AS t(source_id, name, publisher, url)
)
```

`togo_dgb.url` is `NULL` deliberately — the national portal URL is not yet known; fill it when confirmed. `NULL` is `CAST(NULL AS STRING)` so the column types unify across rows.

- [ ] **Step 2: Pre-deploy validation — no duplicate source_id**

Paste the `VALUES` block from Step 1 into a Databricks SQL cell (or any Spark SQL cell) and run:

```sql
SELECT source_id, COUNT(*) AS n
FROM (
  VALUES
    ('boost'),('imf_weo'),('imf_gfs'),('togo_dgb'),('world_bank_pip'),
    ('pip_spid'),('pip_gsap'),('world_bank_icp'),('unesco_uis'),('who_gho'),
    ('who_nha'),('pefa'),('global_data_lab')
) AS t(source_id)
GROUP BY source_id
HAVING COUNT(*) > 1;
```

Expected: **0 rows** (no duplicate ids).

- [ ] **Step 3: Commit**

```bash
git add source_registry_dlt.sql
git commit -m "feat: add source_registry canonical source metadata table"
```

---

### Task 2: `indicator_source` bridge table

**Files:**
- Create: `indicator_source_dlt.sql`

**Interfaces:**
- Consumes: `source_registry.source_id` (Task 1) — every `source_id` here must exist there.
- Produces: table `prd_mega.indicator.indicator_source(indicator_key STRING, source_id STRING)`, many-to-many. Consumed by later plans (availability rewire, dashboard popover).

- [ ] **Step 1: Write the table definition**

Create `indicator_source_dlt.sql` with exactly this content:

```sql
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
```

- [ ] **Step 2: Pre-deploy validation — referential integrity + no duplicate pairs**

In a Spark SQL cell, run both `VALUES` sets against each other:

```sql
WITH registry AS (
  SELECT * FROM (VALUES
    ('boost'),('imf_weo'),('imf_gfs'),('togo_dgb'),('world_bank_pip'),
    ('pip_spid'),('pip_gsap'),('world_bank_icp'),('unesco_uis'),('who_gho'),
    ('who_nha'),('pefa'),('global_data_lab')
  ) AS t(source_id)),
bridge AS (
  SELECT * FROM (VALUES
    ('government_revenue_expenditure','imf_weo'),('government_revenue_expenditure','imf_gfs'),
    ('togo_revenue_budget','togo_dgb'),('subnational_poverty_rate','pip_spid'),
    ('subnational_poverty_rate','pip_gsap'),('poverty_rate','world_bank_pip'),
    ('learning_poverty_rate','world_bank_pip'),('learning_poverty_rate','unesco_uis'),
    ('pefa_by_pillar','pefa'),('universal_health_coverage_index_gho','who_gho'),
    ('health_private_expenditure','who_nha'),('edu_private_expenditure','world_bank_icp'),
    ('global_data_lab_hd_index','global_data_lab'),('global_data_lab_attendance','global_data_lab')
  ) AS t(indicator_key, source_id))
SELECT 'orphan_source_id' AS problem, b.indicator_key, b.source_id
FROM bridge b LEFT ANTI JOIN registry r ON b.source_id = r.source_id
UNION ALL
SELECT 'duplicate_pair', indicator_key, source_id
FROM bridge GROUP BY indicator_key, source_id HAVING COUNT(*) > 1;
```

Expected: **0 rows** (every `source_id` resolves; no duplicate `(indicator_key, source_id)` pairs).

- [ ] **Step 3: Commit**

```bash
git add indicator_source_dlt.sql
git commit -m "feat: add indicator_source indicator-to-source bridge table"
```

---

### Task 3: Deploy to the DLT pipeline and validate materialized tables

This task has no repo file changes — it wires the two SQL files into the existing DLT pipeline (a Databricks **workspace** action) and confirms the tables materialize correctly.

**Files:**
- None in git. Workspace: the DLT pipeline that already builds `indicator_data_availability`.

**Interfaces:**
- Consumes: `source_registry_dlt.sql`, `indicator_source_dlt.sql` (Tasks 1–2).
- Produces: materialized `prd_mega.indicator.source_registry` and `prd_mega.indicator.indicator_source`.

- [ ] **Step 1: Add both files to the DLT pipeline**

In the Databricks workspace, open the DLT pipeline that includes `indicator_data_availability_dlt.sql`, add `source_registry_dlt.sql` and `indicator_source_dlt.sql` to its source paths (Git folder sync should already surface them), and trigger a run. If the two tables belong in a different pipeline than availability, add them there instead — the only requirement is the pipeline's target is `prd_mega.indicator`.

- [ ] **Step 2: Validate row counts**

```sql
SELECT 'source_registry' AS tbl, COUNT(*) AS n FROM prd_mega.indicator.source_registry
UNION ALL
SELECT 'indicator_source', COUNT(*) FROM prd_mega.indicator.indicator_source;
```

Expected: `source_registry` = **13**, `indicator_source` = **14**.

- [ ] **Step 3: Validate referential integrity on the materialized tables**

```sql
SELECT b.indicator_key, b.source_id
FROM prd_mega.indicator.indicator_source b
LEFT ANTI JOIN prd_mega.indicator.source_registry r
  ON b.source_id = r.source_id;
```

Expected: **0 rows**.

- [ ] **Step 4: Spot-check the multi-source and shared-source cases**

```sql
-- government_revenue_expenditure has exactly two sources
SELECT source_id FROM prd_mega.indicator.indicator_source
WHERE indicator_key = 'government_revenue_expenditure' ORDER BY source_id;
-- Expected two rows: imf_gfs, imf_weo

-- global_data_lab feeds two indicators
SELECT indicator_key FROM prd_mega.indicator.indicator_source
WHERE source_id = 'global_data_lab' ORDER BY indicator_key;
-- Expected two rows: global_data_lab_attendance, global_data_lab_hd_index
```

Expected: as annotated above.

- [ ] **Step 5: Record deployment**

No code commit. Note in the PR description that the DLT pipeline was updated to include the two new tables and that Steps 2–4 passed (paste the query outputs).

---

## Out of Scope (follow-on plans)

- **Rewire `indicator_data_availability`** to drop its hardcoded `source_url` `VALUES` block and source URLs from `source_registry`. Deferred because `learning_poverty_rate` and `subnational_poverty_rate` are multi-source: joining the bridge multiplies the table's `(country, indicator_key)` grain, which the current dashboard depends on. This changes together with the dashboard's consumption model.
- **Data-table source-column rule** — drop constant `data_source` columns; split `government_revenue_expenditure` and `subnational_poverty_rate` into single-source tables; add an FK `source_id` column only for ALB imputed rows.
- **Dashboard (`rpf-country-dash`)** — loaders for `source_registry` / `indicator_source`, popover reads the registry, `split_imf_sources` reads split tables, delete the `WEO_SOURCE` / `GFS_SOO_SOURCE` constants.

## Self-Review

- **Spec coverage:** This plan implements spec rollout step 1 (the `source_registry` and `indicator_source` tables, with the SPID/GSAP-separate and government_revenue_expenditure→{weo,gfs} modeling). Steps 2–5 are explicitly deferred with rationale under Out of Scope.
- **Placeholder scan:** No TBD/TODO. `togo_dgb.url = NULL` is an intentional, documented data value, not a plan placeholder.
- **Type consistency:** `source_id` / `indicator_key` / `name` / `publisher` / `url` column names are identical across Tasks 1–3 and the validation queries. Bridge `source_id` values are a subset of registry `source_id` values (verified by the Task 2 and Task 3 anti-joins).
