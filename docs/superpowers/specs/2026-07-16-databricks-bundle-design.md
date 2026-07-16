# Databricks Asset Bundle for mega-indicators — design

**Date:** 2026-07-16
**Status:** Proposed
**Repo:** `mega-indicators`

## Problem

The three production jobs and four DLT pipelines are configured by hand in the
Databricks UI. There is no code-defined, version-controlled definition, so they
can't be validated, diffed, tested, or deployed programmatically. We want a
Databricks Asset Bundle (`databricks.yml` + `resources/`) so the whole thing is
deployable and runnable via the Databricks CLI.

## Current state (discovered)

Workspace `adb-6102124407836814` (Azure), profile `adb-6102124407836814`.

**Jobs** (all `MULTI_TASK`, all `git_source` = `github.com/weilu/mega-indicators`@`main`,
tasks `source: GIT` with repo-relative notebook paths, all on shared all-purpose
cluster `1111-165457-9ia3jd1w`):

| Job | id | tasks | schedule |
|---|---|---|---|
| Indicators.Weekly | 819639412975742 | 8 notebook | `28 17 8 ? * Mon` UTC, unpaused |
| Indicators.Monthly | 1106222747412815 | 4 notebook + 1 pipeline | `28 17 13 3 * ?` UTC, unpaused |
| Indicators on Demand | 21627673817458 | 19 notebook + 3 pipeline | `28 17 8 ? * Mon` UTC, **paused** |

**Pipelines** (all `catalog=prd_mega`, single `*_dlt` library each):

| Pipeline | id | target schema | library |
|---|---|---|---|
| Subnational Human Development Index UC | f0ef00a0-… | indicator | human_development/global_data_lab_hid_transform_load_dlt |
| Subnational Population agg UC | 5d895ef1-… | indicator | population/subnational_population_official_dlt |
| admin boundaries TL UC | 9b7c710c-… | (none) | geo/admin_boundaries_dlt |
| Subnational Poverty Index UC | f46c403f-… | (none) | poverty/subnational_poverty/subnational_poverty_index_transform_load_dlt |

**Issues found in current config:**
- **Plaintext secret:** Monthly job task `gdl_subnat_human_development_index_E` has
  `base_parameters.GDL_API_TOKEN` hardcoded. Must not be committed → move to a
  Databricks secret scope. **Rotate the token** (already exposed).
- **Stale path:** Monthly references `budget/government_budget` (renamed to
  `public_finance/government_revenue_expenditure`).
- **Malformed path:** On Demand references `population/BFA /bfa_subnational_population`
  (trailing space in `BFA `).
- **Personal fork:** `git_source` points at `weilu/mega-indicators`, not the
  canonical `dime-worldbank/mega-indicators`.

## Decisions (confirmed)

1. **Full dev/prod DAB**, notebooks synced from the repo by the bundle.
2. Canonical git remote: **`dime-worldbank/mega-indicators`**.
3. **Fix** the stale `government_budget` path (and the `BFA ` typo).
4. **Dev = orchestration only.** Notebooks/pipelines still write `prd_mega` (they
   hardcode the catalog), so dev job/pipeline *runs are not write-isolated*. Dev
   deploys prefixed, paused copies for wiring/validation; true write isolation
   (parameterized catalog) is a documented follow-up.
5. **DLT pipelines are bundle resources**, bound to the existing pipeline IDs in
   prod (so prod is not duplicated).

## Architecture

Standard DAB layout at repo root:

```
databricks.yml                      # bundle name, sync, variables, targets
resources/
  jobs/
    indicators_weekly.job.yml
    indicators_monthly.job.yml
    indicators_on_demand.job.yml
  pipelines/
    subnational_hdi.pipeline.yml
    subnational_population.pipeline.yml
    admin_boundaries.pipeline.yml
    subnational_poverty.pipeline.yml
```

### Notebook source model (and the prod ops change it implies)

Tasks switch from `source: GIT` (run from GitHub main) to **bundle-synced
`source: WORKSPACE`**: `bundle deploy` uploads the repo to a workspace path and
tasks reference the synced notebooks. Pipeline `libraries` likewise point at the
synced `*_dlt` files.

- **dev:** `bundle deploy -t dev` uploads your **current working tree** → dev jobs
  run exactly your local code (this is the testability win).
- **prod:** notebooks no longer auto-run "latest `main`" the instant it lands.
  Prod runs whatever was last `bundle deploy -t prod`'d. **This needs a
  deploy-on-merge step** (CI, or a manual `bundle deploy -t prod`). Called out
  explicitly — it changes prod ops. (Alternative if undesired: keep `git_source`
  and parameterize `git_branch` per target; note in review if you prefer that.)

### `databricks.yml` (shape)

```yaml
bundle:
  name: mega_indicators

variables:
  shared_cluster_id:
    description: Existing all-purpose cluster the tasks run on
    default: "1111-165457-9ia3jd1w"
  gdl_secret_scope:
    description: Databricks secret scope holding gdl_api_token
    default: mega_indicators

targets:
  dev:
    mode: development          # prefixes [dev you], pauses schedules, user-scoped
    default: true
    workspace:
      host: https://adb-6102124407836814.14.azuredatabricks.net
  prod:
    mode: production           # real names + schedules
    workspace:
      host: https://adb-6102124407836814.14.azuredatabricks.net
      root_path: /Workspace/Shared/.bundle/${bundle.name}/${bundle.target}
    # existing jobs/pipelines adopted via `databricks bundle deployment bind`
```

### Jobs

Each job resource reproduces its task DAG (task_key, depends_on, notebook/pipeline
tasks) faithfully, with:
- `existing_cluster_id: ${var.shared_cluster_id}` on notebook tasks;
- pipeline tasks referencing bundle pipelines by ref, e.g.
  `pipeline_id: ${resources.pipelines.subnational_hdi.id}`;
- schedules preserved verbatim (dev mode auto-pauses them);
- the GDL token via `base_parameters: {GDL_API_TOKEN: "{{secrets/${var.gdl_secret_scope}/gdl_api_token}}"}`;
- corrected notebook paths (`public_finance/government_revenue_expenditure`, `population/BFA/…`).

### Pipelines

Each pipeline resource: `catalog: prd_mega`, `target` schema as today (`indicator`
or none), `libraries` → the synced `*_dlt` file, `channel: CURRENT`, default +
maintenance clusters as today.

### Secret handling

Create a secret scope and store the token (one-time, outside git):

```
databricks secrets create-scope mega_indicators -p adb-6102124407836814
databricks secrets put-secret mega_indicators gdl_api_token --string-value <ROTATED_TOKEN> -p adb-6102124407836814
```

The bundle references it; the value never enters the repo.

## Build approach

1. Minimal `databricks.yml` (bundle + targets).
2. `databricks bundle generate job --existing-job-id <id>` and
   `databricks bundle generate pipeline --existing-pipeline-id <id>` to capture
   the 7 resources accurately, then **delete** the notebooks it downloads (the
   repo already has them) and repoint paths at the repo files.
3. Refine: git remote, corrected paths, secret ref, pipeline refs, variables,
   dev/prod targets.
4. `databricks bundle validate` (both targets) — the acceptance check.
5. Adopt prod: `databricks bundle deployment bind <resource> <existing_id> -t prod`
   for each of the 3 jobs + 4 pipelines (documented runbook; run by a maintainer).

## Acceptance

- `databricks bundle validate -t dev` and `-t prod` succeed.
- Generated resource set matches the discovered jobs/pipelines (task counts, deps,
  schedules, pipeline libraries) — verified against the JSON dumps.
- No secret in the committed tree (`git grep` for the token prefix returns nothing).

## Out of scope / follow-ups

- **Write isolation for dev** — parameterize catalog/schema across ~35 notebook
  tasks + pipelines so dev writes a dev catalog. Large; deferred.
- **A pipeline for the new `source_registry` + `indicator_source` tables** — these
  aren't in any pipeline yet (the registry work's pending Task 3). Natural to add
  as a bundle pipeline resource here or in that follow-up.
- **Migrating prod off the `weilu` fork** and wiring a deploy-on-merge CI.
- **Binding** is an operational step run once by a maintainer, not part of the
  committed bundle.
