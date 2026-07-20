# Source Registry Completion — Design

**Date:** 2026-07-20
**Repos:** `mega-indicators` (pipeline) · `rpf-country-dash` (dashboard)
**Predecessor:** [2026-07-15-source-registry-design.md](./2026-07-15-source-registry-design.md)

## Goal

Phase C dropped the per-row `data_source` column from every single-source table.
This spec makes `source_registry` + `indicator_source` the **complete** catalogue of
those dropped sources, so attribution is a property of the data (every onboarded
indicator's source is registered and bridged), not merely of the current dashboard
UI. It also surfaces **subnational population** sources in the dashboard popover,
picking the right source per selected country.

## Background

An audit matched every removed `data_source` value against the current registry
(13 sources) and bridge (14 rows). Findings:

- Every **dashboard-shown** source was already captured (gov revenue WEO/GFS, togo
  DGB, poverty PIP, subnational poverty SPID/GSAP, learning poverty, edu private ICP,
  UHC GHO, health private NHA, Global Data Lab HDI).
- Many **onboarded but not-yet-charted** indicators had their `data_source` dropped
  with no registry/bridge entry: `gdp`, national `population`, the UIS education
  feeder tables, `public_sector_employment`, `maternal_mortality_ratio`, etc.
- **Subnational population** feeds the home *subnational-spending* per-capita
  choropleth (via BOOST's `per_capita_expenditure`, computed upstream in mega-boost).
  Each country's subnational population comes from a **different** upstream source,
  and none were registered.

## Decisions

1. **Register all, bridge all.** Every dropped source is added to `source_registry`;
   every onboarded indicator gets an `indicator_source` bridge row — even indicators
   no chart shows yet. A future chart for `gdp`/`completion_rates`/etc. then resolves
   its source with zero further pipeline work.

2. **Per-country attribution via a `country_name` dimension on the bridge (C2).**
   `subnational_population` stays a single indicator key. The bridge gains a
   `country_name` column: `NULL` means "applies to all countries" (every existing
   row), and subnational-population rows are scoped per country. The dashboard
   resolver returns sources where `indicator_key = X AND (country_name IS NULL OR
   country_name = <selected country>)`. It's keyed by `country_name` (not ISO3)
   because the popover subsystem is already uniformly `country_name`-keyed
   (SOURCE_DISPLAY whitelists, coverage, boost URLs) and no name→code map exists there
   — using the name avoids a conversion layer for the bridge's only consumer. Values
   are the canonical names from the country table. This keeps the indicator vocabulary
   honest, puts the country→source mapping in the pipeline metadata (its correct home),
   and composes with `indicator_data_availability` (already keyed by indicator+country).

3. **The bridge is the attribution source of truth; no *new* per-row `source_id`.**
   The dashboard never queries subnational population directly (it's baked into BOOST
   upstream), so the popover resolves sources through the bridge, not row data. We do
   not add per-row `source_id` to the ~16 population tables that lack it. ALB **keeps**
   its Phase-C per-row `source_id` — its codes (`wb_subnational_population`,
   `alb_instat`, `imputed`) already match the registry and carry finer within-country
   provenance (WB pre-2016 / INSTAT 2018+ / imputed 2017) that the bridge can't express.

## Source registry additions

11 new rows in `source_registry_dlt.sql`. Sources are attributed to the **most
specific dataset/series we can cite** — the WDI series pages, not a dataset landing
page — so provenance is trackable to the exact indicator. National `population` and
`gdp` are fetched from the World Bank WDI API and map to the series-specific
`wdi_population` / `wdi_gdp` (the series page carries the UN/Eurostat/OECD upstream
credit for free). `edu_private_spending` is fetched from the OECD SDMX API
(`DF_UOE_FIN`) → `oecd_eag`. `maternal_mortality_ratio` (`SH.STA.MMRT`) is credited to
its substantive originator, the WHO-led MMEIG (`who_mmr`), not the WDI API endpoint.
Names/URLs below are proposed values — **please correct any during review**.

| `source_id`                 | `name`                                        | `publisher`            | `url` |
|-----------------------------|-----------------------------------------------|------------------------|-------|
| `uis`                       | Institute for Statistics database             | UNESCO                 | https://uis.unesco.org/ |
| `wdi_population`            | World Development Indicators — Total population | World Bank | https://data.worldbank.org/indicator/SP.POP.TOTL |
| `wdi_gdp`                   | World Development Indicators — GDP, national accounts (`NY.GDP.*`) | World Bank | https://datatopics.worldbank.org/world-development-indicators/themes/economy.html#production |
| `oecd_eag`                  | Education at a Glance (UOE finance)             | OECD                   | https://www.oecd.org/en/about/programmes/education-at-a-glance.html |
| `wwbi`                      | Worldwide Bureaucracy Indicators               | World Bank             | https://www.worldbank.org/en/data/interactive/2019/05/21/worldwide-bureaucracy-indicators-dashboard |
| `who_mmr`                   | Maternal Mortality Estimates (MMEIG)           | WHO                    | https://www.who.int/data/gho/data/indicators/indicator-details/GHO/maternal-mortality-ratio |
| `census_gov`                | International Database                          | US Census Bureau       | https://www.census.gov/programs-surveys/international-programs/about/idb.html |
| `wb_subnational_population` | Subnational Population database                 | World Bank             | https://databank.worldbank.org/source/subnational-population |
| `alb_instat`                | Population by prefecture                        | INSTAT (Albania)       | https://www.instat.gov.al/ |
| `moz_ine`                   | Population projections                          | INE (Mozambique)       | https://mozambique.opendataforafrica.org/ |
| `pry_ine`                   | Population by department                        | INE (Paraguay)         | https://www.ine.gov.py/ |

`global_data_lab` is already registered (reused for subnational population in COD/LBR).

## Bridge additions

`indicator_source_dlt.sql` gains a `country_code` column (existing 14 rows →
`country_code = NULL`), plus these new rows.

### Non-subnational (country_code = NULL)

| `indicator_key`                | `source_id`        |
|--------------------------------|--------------------|
| `completion_rates`             | `uis`              |
| `edu_gov_spending`             | `uis`              |
| `youth_literacy_rate_unesco`   | `uis`              |
| `pupil_teacher_ratio`          | `uis`              |
| `school_basic_services`        | `uis`              |
| `teacher_salaries`             | `uis`              |
| `edu_private_spending`         | `oecd_eag`         |
| `edu_spending`                 | `world_bank_icp`   |
| `gdp`                          | `wdi_gdp`          |
| `population`                   | `wdi_population`    |
| `health_expenditure`           | `who_nha`          |
| `maternal_mortality_ratio`     | `who_mmr`          |
| `public_sector_employment`     | `wwbi`             |

`learning_poverty_rate` stays `world_bank_lpgd` only (the WB+UNESCO Global Learning
Poverty Database is that single joint product). `poverty_rate` /
`universal_health_coverage_index_gho` are already bridged.

### Subnational population (per country)

`indicator_key = subnational_population`, one row per (source, country):

| `source_id`                 | `country_name` |
|-----------------------------|----------------|
| `census_gov`                | Togo, Pakistan, Nigeria, Bangladesh, Kenya, Ghana, Burkina Faso, Colombia |
| `wb_subnational_population` | Albania, Bhutan, Chile, Tunisia, South Africa, Burkina Faso |
| `global_data_lab`           | Congo, Dem. Rep.; Liberia |
| `alb_instat`                | Albania |
| `moz_ine`                   | Mozambique |
| `pry_ine`                   | Paraguay |

Notes: **BFA** has two sources (census.gov post-2015 + WB DB #50 pre-2015) → two rows.
**ALB** has two (WB pre-2016 + INSTAT 2018+) → two rows; the imputed 2017 rows are a
derived interpolation, not a citable source, so they get **no** bridge row.

## Pipeline changes (`mega-indicators`)

1. `source_registry_dlt.sql` — add the 11 `VALUES` rows above.
2. `indicator_source_dlt.sql` — add `country_code` to the `SELECT ... AS t(...)`
   column list; set `NULL` on all existing rows; add the new rows above.
No population `.py` files change. C2 needs no per-row source column, and ALB keeps
the per-row `source_id` it already has from Phase C.

## Dashboard changes (`rpf-country-dash`)

Only `subnational_population` is newly *rendered* (the other bridged indicators have
no chart yet), so presentation work is limited to the subnational population sources.

1. `queries.py` — `get_indicator_source` selects `country_name` alongside
   `indicator_key, source_id`.
2. `components/source_metadata_popover.py`:
   - `_sources_for_indicator(indicator_key, source_meta, country=None)` — filter
     bridge rows by `country_name IS NULL OR country_name == country`. `build_modal_info`
     already has the selected `country` name; thread it in.
   - `CHART_METADATA["subnational-spending"]` — `["boost"]` → `["boost",
     "subnational_population"]`.
   - `SOURCE_DISPLAY` — add entries for `census_gov`, `wb_subnational_population`,
     `alb_instat`, `moz_ine`, `pry_ine` (`global_data_lab` exists). `coverage_key`:
     `subnational_population` (no availability rows yet → no year span shown, per
     decision). `countries: None` — country scoping is handled by the bridge now, not
     the whitelist.
3. `translations/{en,fr,pt}.py` — i18n keys (`label`/`publisher`/`name`) for the five
   new subnational population sources.

## Out of scope

- Coverage year spans for the new sources (no `indicator_data_availability` rows).
- Surfacing any of the register-only indicators (gdp/completion_rates/etc.) in a
  chart — they're bridged and ready, but no UI is built here.
- Flagging ALB's imputed 2017 rows in the UI (data-quality note, separate feature).
- Adding per-row `source_id` to the population tables that lack it (no consumer; ALB
  keeps the one it already has).

## Testing

- Pipeline: `python3 -m py_compile` on the three edited files; the two DLT SQL files
  are validated on deploy to the staging schema (next branch of work).
- Dashboard: the 157-test unittest suite. Add tests for `_sources_for_indicator`
  country filtering (NULL applies everywhere; ALB returns WB+INSTAT; KEN returns
  census_gov; COD returns global_data_lab) and that the subnational-spending popover
  lists boost + the country's population source.

## Rollout

Bundled with Phase C. Registry/bridge changes are additive and safe to deploy ahead
of the dashboard (the new bridge rows are inert until the dashboard reads them). The
`country_code` column is additive. Deploy to staging, validate the popover per
country, then prod cutover.
```
