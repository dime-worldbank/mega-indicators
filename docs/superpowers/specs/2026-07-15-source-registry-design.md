# Source metadata single source of truth — design

**Date:** 2026-07-15
**Status:** Proposed
**Repos affected:** `mega-indicators` (pipeline), `rpf-country-dash` (dashboard)

## Problem

A data source (e.g. IMF WEO) is currently described in three disconnected places:

1. **Pipeline `data_source` column** — a free-text string baked into every row of each
   indicator table (`df['data_source'] = 'WEO (World Economic Outlook), IMF — General
   Government'`). For all but three tables this value is a single constant repeated on
   every row — pure redundancy. The strings are inconsistent free text: prose
   (`'UNESCO Institute for Statistics (UIS)'`), bare URLs
   (`'https://ghoapi.azureedge.net/api/'`), and compound labels.

2. **Dashboard `constants.py`** — `WEO_SOURCE` / `GFS_SOO_SOURCE` string literals that must
   be **byte-identical** (em-dash included) to the pipeline strings, because
   `components/fiscal_balance.py::split_imf_sources` splits the government table by matching
   `data_source` against them.

3. **Dashboard popover catalog** — `components/source_metadata_popover.py` hand-maintains a
   parallel, richer catalog (stable key, i18n label/name/description, `source_url`,
   per-country scoping) in `CHART_METADATA`, keyed by chart. It never reads `data_source`.

There is no single source of truth. Adding or editing a source means touching all three,
and the byte-identical coupling in (2) is silently fragile.

A partial registry already exists: `indicator_data_availability` (built by
`indicator_data_availability_dlt.sql`) carries a `source_url` per `indicator_key` via a
hardcoded `VALUES` block, repeated across every country row — the same redundancy one level
up.

## Goals

- One canonical definition per source that **both** the pipeline and the dashboard derive from.
- Source attribution modeled as **table-level metadata**, not a redundant per-row column.
- Remove the byte-identical string coupling between pipeline and dashboard.
- Reuse the dashboard's existing metadata read path and once-per-process cache; no new
  per-request overhead.

## Non-goals

- Renaming existing `indicator_key`s or physical tables for cosmetic consistency (call out as
  a separate cleanup).
- Reworking the i18n/translation system. i18n labels stay dashboard-side, keyed by `source_id`.
- Per-country source coverage beyond what `indicator_data_availability` already computes.

## Design overview

Three small, static, country-agnostic (except availability) reference tables in the
`prd_mega.indicator` schema:

| Table | Grain | Holds | Change |
|---|---|---|---|
| `source_registry` | `source_id` | canonical source identity — **the truth** | new |
| `indicator_source` | `indicator_key × source_id` | which sources feed each indicator (many-to-many bridge; authoritative, in git) | new |
| `indicator_data_availability` | `country × indicator_key` | per-country coverage years; embedded `source_url` dropped (now resolved via registry) | edit |

`source_id` is a stable slug and the primary key of the whole scheme. `indicator_key` is a
**logical measure** — finer than a physical table (one table can expose several keys) and
sometimes named differently — so it cannot be the source's key, and source attribution cannot
be implemented as Unity Catalog table tags (a tag is per-physical-table, cannot go sub-table,
and represents a source *set* only awkwardly). The bridge stays a declarative table.

### `source_registry` — example rows

Factual/canonical fields only. i18n label & description live dashboard-side, keyed by `source_id`.

| source_id | name | publisher | url |
|---|---|---|---|
| `boost` | BOOST | World Bank | https://www.worldbank.org/en/programs/boost-portal/country-data |
| `imf_weo` | World Economic Outlook — General Government | IMF | https://www.imf.org/en/Publications/WEO |
| `imf_gfs` | Government Finance Statistics (Statement of Operations) — Budgetary Central Gov | IMF | https://data.imf.org/en/datasets/IMF.STA:GFS_SOO |
| `togo_dgb` | Budget Execution Report | Togo DGB | *(national portal)* |
| `world_bank_pip` | Poverty and Inequality Platform | World Bank | https://data360.worldbank.org/en/dataset/WB_PIP |
| `pip_spid` | PIP — Subnational Poverty (SPID) | World Bank | https://pipmaps.worldbank.org/en/data/datatopics/poverty-portal/home |
| `pip_gsap` | PIP — Global Subnational Atlas of Poverty (GSAP) | World Bank | https://pipmaps.worldbank.org/en/data/datatopics/poverty-portal/home |
| `world_bank_icp` | International Comparison Program | World Bank | https://www.worldbank.org/en/programs/icp/data |
| `unesco_uis` | Institute for Statistics | UNESCO | https://uis.unesco.org/ |
| `who_gho` | Global Health Observatory | WHO | https://www.who.int/data/gho |
| `who_nha` | Global Health Expenditure Database | WHO | https://apps.who.int/nha/database/ |
| `pefa` | Public Expenditure & Financial Accountability | PEFA Secretariat | https://www.pefa.org/assessments/batch-downloads |
| `global_data_lab` | Subnational HDI / Area Database | Global Data Lab | https://globaldatalab.org/shdi/about/ |

### `indicator_source` — example rows (the bridge)

Many-to-many. Both multi-source-per-indicator and one-source-many-indicators fall out naturally.

| indicator_key | source_id | note |
|---|---|---|
| `government_revenue_expenditure` | `imf_weo` | two sources, one indicator |
| `government_revenue_expenditure` | `imf_gfs` | ← its pair |
| `togo_revenue_budget` | `togo_dgb` | |
| `subnational_poverty_rate` | `pip_spid` | SPID/GSAP kept as distinct sources |
| `subnational_poverty_rate` | `pip_gsap` | ← its pair |
| `poverty_rate` | `world_bank_pip` | same publisher, different source id from SPID/GSAP |
| `learning_poverty_rate` | `world_bank_pip` | multi-source; replaces compound free-text string |
| `learning_poverty_rate` | `unesco_uis` | ← its pair |
| `pefa_by_pillar` | `pefa` | |
| `universal_health_coverage_index_gho` | `who_gho` | |
| `health_private_expenditure` | `who_nha` | sub-table: private slice of `health_expenditure` table |
| `edu_private_expenditure` | `world_bank_icp` | key name ≠ table name (`edu_private_spending`) |
| `global_data_lab_hd_index` | `global_data_lab` | one source, two indicators |
| `global_data_lab_attendance` | `global_data_lab` | ← shares `global_data_lab` |

### `indicator_data_availability` — URL removed, mapping not duplicated

- Grain **stays** `(country, indicator_key)`.
- The hardcoded `source_url` `VALUES` block is **removed**; a source's URL comes from
  `source_registry`, reached via `indicator_source`.
- The `(indicator_key → source_id)` mapping lives **only** in `indicator_source`. It is
  deliberately not copied onto availability rows — doing so would duplicate the pair once per
  country. Availability references sources by joining to `indicator_source` when needed.
- Per-source coverage (distinct year spans for, e.g., WEO vs GFS within one indicator) would
  require source in the availability grain; that is out of scope here (see Open Questions).

## Data-table changes — the source-column rule

For the indicator tables themselves (not the metadata tables above):

- **Single-source tables (the majority):** drop the redundant per-row `data_source` column.
  Source is attached at table level via `indicator_source`.
- **Multi-source tables that are clean unions:** split into single-source physical tables and
  expose a union view where a combined read is needed. The bridge lists all sources. This is
  the "don't handle row-level source" path.
  - `government_revenue_expenditure` → `..._weo` + `..._gfs`. The dashboard already splits
    these for display (`split_imf_sources`), so this removes code rather than adding it.
  - `subnational_poverty_rate` (SPID + GSAP stacked) → same treatment.
- **Genuinely per-row provenance (the documented exception):** keep a `source_id` column that
  is a clean FK into `source_registry` (not free text). Applies to ALB subnational population,
  where the imputed rows are derived per-row and cannot be traced to a single upstream table.

## Dashboard changes (`rpf-country-dash`)

- Add `data_mapping` loaders for `source_registry` and `indicator_source` (two new small
  queries; `indicator_data_availability` is already loaded). All are static and cached
  once-per-process by `server_store` (see Overhead), so the extra reads are one-time and
  negligible — no folding/denormalization needed to avoid them.
- `source_metadata_popover.py`: replace hand-maintained factual fields (`source_url`,
  publisher name) with lookups into `source_registry` keyed by `source_id`. i18n `label_key`
  / `description_key` remain, re-keyed by `source_id`. `CHART_METADATA` references `source_id`s.
- `components/fiscal_balance.py::split_imf_sources`: read the two split tables (or filter by
  `source_id`) instead of matching `data_source` against string constants.
- `constants.py`: delete `WEO_SOURCE`, `GFS_SOO_SOURCE`,
  `IMF_GOVERNMENT_REVENUE_EXPENDITURE_SOURCES`. The byte-identical coupling is gone.
- `queries.py`: the gov queries select from the split tables / view rather than filtering
  `data_source IN (...)`.

## Overhead / caching

`server_store` (`server_store.py`) is a process-wide, thread-safe, load-once cache: the first
`.get(key)` runs the loader and stores the result; later accesses are in-memory dict lookup +
defensive copy. Loaders fire once per process, not per request. `source_registry` (~15–30
rows) and `indicator_source` (~30–50 rows) are the smallest tables in the system and
country-agnostic. Adding both as separate reads stays well within the existing metadata
budget — which is why normalized, separate tables are preferred over folding the mapping into
`indicator_data_availability` to save a query.

## Migration / rollout order

1. Pipeline: create `source_registry` and `indicator_source` tables (declarative SQL/DLT in git).
2. Pipeline: drop the `VALUES` URL block from `indicator_data_availability`; source URLs now
   resolve through `indicator_source` → `source_registry`.
3. Pipeline: apply the source-column rule to data tables (drop constant `data_source`; split
   `government_revenue_expenditure` and `subnational_poverty_rate`; FK `source_id` on ALB).
4. Dashboard: add loaders, re-point popover + `split_imf_sources` + `queries.py`, delete the
   `constants.py` source strings.
5. Remove the old free-text `data_source` columns once no reader depends on them.

Steps 1–2 are additive and safe to ship first; the dashboard (4) can migrate before the old
columns are dropped (5).

## Open questions / out of scope

- **Per-source coverage years.** Today availability is `(country, indicator_key)`, so a
  multi-source indicator shows one coverage span, not one per source (e.g. WEO vs GFS). If
  per-source coverage is wanted, availability grain must gain `source_id` — reopening the
  fold-vs-normalize trade-off, but then `source_id` carries genuine per-source coverage data,
  not just a copy of the mapping. Deferred until the display need is confirmed.
- Whether to additionally stamp Unity Catalog table tags *derived from* `indicator_source` for
  catalog-browsing discoverability (downstream convenience; the table stays authoritative).
- `indicator_key` naming cleanup is deferred; keep existing keys verbatim and add new ones.
