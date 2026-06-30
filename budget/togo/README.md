# Togo Budget Data

Annual fiscal metrics for Togo, derived from the DGB/DGBFTG budget execution
reports. Feeds `prd_mega.indicator.togo_revenue_budget`.

## Files

- `togo_finance_report_extract.py` — downloads each year's PDF from
  togoreformes.gouv.tg into the Unity Catalog volume. Run manually when a new
  year's report is published.
- `togo_finance_report_transform_load_dlt.py` — Databricks job. Reads the
  `BUDGET_DATA` dict (committed in this file) and writes the Delta table.

## Adding a new year

The DGB reports vary year-to-year (the `Tableau n°` number shifts, layout
changes), so we don't parse them at pipeline time. Values are extracted once,
reviewed, and committed as Python literals in `BUDGET_DATA`. The PR diff is
the audit trail.

When a new report is published:

1. Add the URL to `URLS` in `togo_finance_report_extract.py` and run it to
   archive the PDF in the volume.
2. Open the archived PDF and find the budget-execution summary table, titled
   *Situation résumée de l'exécution du budget de l'État*. **Identify it by
   that title, not by the `Tableau n°` number** — the number drifts year to
   year. Note its page number for `source_page`.

   Read the three values from the **`EXECUTION`** column (usually subtitled
   *base ordonnancement*):

   | PDF row | Dict key |
   |---|---|
   | `Recettes budgétaires` | `revenue_current_lcu` |
   | `Dépenses budgétaires` | `expenditure_current_lcu` |
   | `Dépenses en atténuation de recettes` | `tax_expenditure` (sometimes sits in the column just right of `EXECUTION`) |

   Values are in **billions of CFA** with French formatting (space thousands
   separator, comma decimal). Convert to whole CFA by multiplying by 1e9 —
   e.g. `1 234,5` billions → `1_234_500_000_000`. Add a new entry to
   `BUDGET_DATA` as underscored integer literals; use `None` for any metric
   genuinely absent from the table.
3. Review the PR — eyeball each value against the source page recorded in
   `source_page`. Magnitudes should be plausible (revenue is typically
   hundreds of billions to low trillions of CFA).
4. Merge, then manually trigger the `togo_finance_report_transform_load_dlt`
   job to overwrite the Delta table.

## Why this isn't automated

The DGB report layout drifts every year (the summary table's `Tableau n°`
number changes, columns shift), so the original deterministic pdfplumber
parser broke after the first format change. The obvious next step would be
to drop in an LLM call at pipeline time — but doing that *correctly* in
production means designing for fallback when the model returns a bad value,
validation rules that catch hallucinations without rejecting legitimate
shifts in the data, and an audit trail that links each row in the Delta
table back to a specific model response. That's a real amount of engineering
to harden one annual extract.

Instead, a human extracts the values once and commits them as Python
literals. The reviewer sanity-checks the diff, the PR is the audit trail, and
the pipeline stays a deterministic "read the dict, write Delta." It's one
small annual extract — manual extraction is cheap and keeps the data
trustworthy without any runtime model dependency.
