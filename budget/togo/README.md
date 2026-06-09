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
2. Open Claude Code in this repo and ask it to update the Togo budget for the
   new year. The `update-togo-budget` skill
   (`.claude/skills/update-togo-budget/SKILL.md`) walks through finding the
   summary table, extracting the three metrics, and updating `BUDGET_DATA`.
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

Instead, we use the LLM where it pays off without the production overhead:
as a **developer-time skill** that helps a human extract the values once,
which then get committed as Python literals. The reviewer sanity-checks the
diff, the PR is the audit trail, and the pipeline stays a deterministic
"read the dict, write Delta." LLM as productivity tool, not as runtime
dependency.
