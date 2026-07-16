# Databricks Asset Bundle — usage & one-time setup

This repo ships a Databricks Asset Bundle (`databricks.yml` + `resources/`) defining
the 3 jobs and 4 DLT pipelines, so they can be validated, deployed, and run via the
Databricks CLI.

- **Design:** `docs/superpowers/specs/2026-07-16-databricks-bundle-design.md`
- **Workspace:** `adb-6102124407836814` (Azure), CLI profile `adb-6102124407836814`
- **Targets:** `dev` (default; `mode: development` — prefixes `[dev <you>]`, pauses
  schedules, deploys under your user) and `prod` (`mode: production` — real names +
  schedules, bound to the existing resources).

## Everyday use

```bash
P=adb-6102124407836814

# Lint / preview
databricks bundle validate -t dev -p $P

# Deploy YOUR current working tree as [dev <you>] copies (safe — schedules paused)
databricks bundle deploy -t dev -p $P

# Run a job or pipeline in dev
databricks bundle run indicators_weekly -t dev -p $P
databricks bundle run subnational_poverty_index_uc -t dev -p $P
```

> ⚠️ **Dev is NOT write-isolated.** The notebooks and pipelines hardcode
> `catalog = prd_mega`, so a dev run still writes prod tables. Dev is for
> validating orchestration/wiring, not for safe data experiments. True isolation
> (parameterized catalog/schema) is a documented follow-up in the design spec.

## One-time setup (a maintainer runs these once)

### 1. Create the secret scope for the GDL token

The Monthly job needs `GDL_API_TOKEN`. It is **not** in git — it is read from a
secret scope. Create it and store a **freshly rotated** token (the previous value
was exposed in plaintext in the job config and should be rotated):

```bash
P=adb-6102124407836814
databricks secrets create-scope mega_indicators -p $P
databricks secrets put-secret mega_indicators gdl_api_token --string-value '<ROTATED_TOKEN>' -p $P
```

(The scope name is the `gdl_secret_scope` bundle variable, default `mega_indicators`.)

### 2. Adopt the existing prod jobs & pipelines (bind)

So `prod` manages the EXISTING resources instead of creating duplicates, bind each
bundle resource to its live id once:

```bash
P=adb-6102124407836814
# jobs
databricks bundle deployment bind indicators_weekly    819639412975742  -t prod -p $P
databricks bundle deployment bind indicators_monthly   1106222747412815 -t prod -p $P
databricks bundle deployment bind indicators_on_demand 21627673817458   -t prod -p $P
# pipelines
databricks bundle deployment bind subnational_human_development_index_uc f0ef00a0-9a62-456d-8cdf-1fb6af2fbe8c -t prod -p $P
databricks bundle deployment bind subnational_population_agg_uc          5d895ef1-3752-49f3-ad20-22724994679a -t prod -p $P
databricks bundle deployment bind admin_boundaries_tl_uc                 9b7c710c-e7ad-4320-aa61-abd18b0fcac4 -t prod -p $P
databricks bundle deployment bind subnational_poverty_index_uc           f46c403f-de3e-44bc-bb65-a84270509a18 -t prod -p $P
```

Then deploy prod (this is now the deploy-on-merge step — prod no longer auto-runs
GitHub `main`; it runs what was last deployed):

```bash
databricks bundle deploy -t prod -p $P
```

### 3. prod root-path warning

`bundle validate -t prod` warns that `/Workspace/Shared/.bundle/...` is writable by
all workspace users. Options: deploy prod via a **service principal** into a
restricted folder, or add an explicit `permissions:` block. Left as-is here because
the deploying identity/governance is a maintainer decision.

## What changed vs the hand-configured jobs

- Source model: tasks moved from `source: GIT` (auto-run GitHub `main`) to
  bundle-synced notebooks. **Deploying is now explicit** (see deploy-on-merge above).
- Git remote of record: `dime-worldbank/mega-indicators` (was `weilu/mega-indicators`).
- Fixed the stale `budget/government_budget` → `public_finance/government_revenue_expenditure`
  path and the `population/BFA ` trailing-space typo.
- `GDL_API_TOKEN` moved from plaintext to the secret scope.
- Shared cluster id and secret scope are bundle variables.

## Follow-ups (not in this bundle)

- Parameterize catalog/schema for real dev write-isolation.
- Add a pipeline resource for the new `source_registry` + `indicator_source` tables.
- CI to `bundle deploy -t prod` on merge to `main`.
