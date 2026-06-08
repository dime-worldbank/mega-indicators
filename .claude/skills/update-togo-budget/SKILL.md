---
name: update-togo-budget
description: Use this skill when the user wants to parse a Togo budget execution PDF and add the extracted values to BUDGET_DATA in budget/togo/togo_finance_report_transform_load_dlt.py. Assumes the PDF is already archived in the project's Unity Catalog volume.
---

# Parse Togo Budget PDF → BUDGET_DATA

Read an already-archived Togo budget execution PDF from the UC volume,
extract three metrics, and add (or correct) the year's entry in
`BUDGET_DATA` inside `budget/togo/togo_finance_report_transform_load_dlt.py`.
The pipeline reads `BUDGET_DATA` and writes
`prd_mega.indicator.togo_revenue_budget`. The PR diff is the audit trail.

PDFs live in this Unity Catalog volume:

```
Catalog: prd_mega
Schema:  sboost4
Volume:  vboost4
Path:    /Volumes/prd_mega/sboost4/vboost4/Workspace/auxiliary_data/official_finance_reports/togo/
```

with filenames `togo_budget_YYYY.pdf` (one per year). Source URLs for each
year are already listed in `budget/togo/togo_finance_report_extract.py`.

## Prerequisites (one-time)

The skill reads PDFs directly from the UC volume, which requires the
Databricks CLI authenticated against the prd_mega workspace.

1. Install the CLI if missing: `brew install databricks` (mac) or
   `pip install databricks-cli` (≥0.205 — needed for UC volume support).
2. Log in: `databricks auth login --host <prd_mega workspace URL>`. This
   opens a browser for OAuth and stores a profile in `~/.databrickscfg`.
3. Smoke test: `databricks fs ls dbfs:/Volumes/prd_mega/sboost4/vboost4/Workspace/auxiliary_data/official_finance_reports/togo/` — should list the existing PDFs. (The `dbfs:` prefix is just the CLI's URI scheme for any remote path; the file itself is a UC volume object, not DBFS storage.)

## Steps

### 1. Pull the PDF down locally

```bash
YEAR=2025
VOLUME=/Volumes/prd_mega/sboost4/vboost4/Workspace/auxiliary_data/official_finance_reports/togo
databricks fs cp "dbfs:$VOLUME/togo_budget_$YEAR.pdf" "/tmp/togo_budget_$YEAR.pdf"
```

### 2. Find the page number of the summary table

Don't read the whole PDF — it's hundreds of pages. Use `pdftotext` to scan
for the table's title (or distinctive row label) and return the page number:

```bash
pdftotext -layout "/tmp/togo_budget_$YEAR.pdf" - \
  | awk 'BEGIN{p=1} /\f/{p++} /Situation résumée/{print p; exit}'
```

If "Situation résumée" isn't matched (wording sometimes drifts), try the
distinctive combination of row labels:

```bash
pdftotext -layout "/tmp/togo_budget_$YEAR.pdf" - \
  | awk 'BEGIN{p=1} /\f/{p++} /Dépenses en atténuation/{print p; exit}'
```

(`pdftotext` is from poppler-utils: `brew install poppler` on mac.)

**Record this page number** — it goes into the dict as `source_page` so
reviewers can jump straight to the source page when auditing.

### 3. Read only that page (and the next, in case the table spans two)

Use Claude Code's Read tool with `pages: "<page>-<page+1>"`, or:

```bash
pdftotext -layout -f $PAGE -l $((PAGE + 1)) "/tmp/togo_budget_$YEAR.pdf" -
```

Confirm the table is what you expected:

- Title resembles *Situation résumée de l'exécution du budget de l'État*.
  The `Tableau n°` number drifts year to year — **identify by content, not
  by number**.
- Rows include **`Recettes budgétaires`**, **`Dépenses budgétaires`**, and
  **`Dépenses en atténuation de recettes`**.
- Columns include an **`EXECUTION`** column, usually subtitled
  *`base ordonnancement`*.

### 4. Read three values from the EXECUTION (base ordonnancement) column

Values are in **billions of CFA francs** with French formatting (space
thousands separator, comma decimal). Convert to **whole CFA** by multiplying
by 1,000,000,000 before adding to the dict.

| PDF row | Dict key | Notes |
|---|---|---|
| `Recettes budgétaires` | `revenue_current_lcu` | |
| `Dépenses budgétaires` | `expenditure_current_lcu` | |
| `Dépenses en atténuation de recettes` | `tax_expenditure` | Sometimes lives in the column immediately to the right of EXECUTION. |

Example conversion: `1 234,5` billions → `1_234_500_000_000`.

### 5. Update BUDGET_DATA

In `budget/togo/togo_finance_report_transform_load_dlt.py`:

```python
BUDGET_DATA = {
    2025: {
        'revenue_current_lcu': 1_234_500_000_000,
        'expenditure_current_lcu': 1_500_000_000_000,
        'tax_expenditure': 50_000_000_000,
        'source_url': 'https://togoreformes.gouv.tg/documents/download/XXX',
        'source_page': 47,  # page number from step 2
    },
    # ...existing years
}
```

**`source_url`**: First check if `togo_finance_report_extract.py` already
has a URL for this year — if so, copy it. Otherwise, **ask the user** for
the download URL (e.g., "What is the source URL for the {year} report?").
If the user doesn't have one available, set it to `None`; don't invent or
guess a URL.

Use underscored integer literals for the numeric values. If a metric is
genuinely absent in the PDF, set it to `None`.

### 6. Sanity check before committing

- Magnitudes plausible for Togo (revenue is typically hundreds of billions
  to low trillions of CFA).
- Year-over-year delta reasonable (not 10× off — that usually means a
  unit/decimal mistake).
- All three metric keys present (`None` only if truly missing).

