# Databricks notebook source
!pwd

# COMMAND ----------

!pip install pdfplumber

# COMMAND ----------

# MAGIC %run ../../utils

# COMMAND ----------

VOLUME_ROOT_PATH = get_volume_root_path()
VOLUME_PATH = f'{VOLUME_ROOT_PATH}/auxiliary_data/official_finance_reports/togo/'

# COMMAND ----------

import os
import re
import pandas as pd
import pdfplumber

BILLION = 1_000_000_000

# French DGB budget-execution row label → output column.
LABELS = {
    'recettes budgétaires': 'revenue_current_lcu',
    'dépenses budgétaires': 'expenditure_current_lcu',
    'dépenses en atténuation': 'tax_expenditure',
}

def parse_amount(val):
    """Parse French-formatted PDF amount in billions (e.g. '123 123, 00') to float, or None."""
    if not val:
        return None
    try:
        s = re.sub(r'\s+', '', str(val)).replace(',', '.')
        return float(s) * BILLION if s else None
    except (ValueError, TypeError):
        return None

def extract_year(filename):
    # Matches the togo_budget_YYYY.pdf convention emitted by togo_budget_documents_download.py.
    m = re.fullmatch(r'togo_budget_(\d{4})\.pdf', filename)
    return int(m.group(1)) if m else None

def extract_table_summary_metrics(pdf):
    """Return {column: amount} from EXECUTION column, or {} if unparsable."""
    for page in pdf.pages:
        if 'tableau n° 22' not in (page.extract_text().lower()):
            continue
        print(page.page_number)

        tables = page.extract_tables()
        if not tables:
            continue
        df = pd.DataFrame(tables[0])
        exec_col = next(
            (i for i in range(len(df.columns))
             if 'execution' in str(df.iloc[1, i]).lower() and 'base' in str(df.iloc[1, i]).lower()),
            None,
        )
        if exec_col is None:
            return {}
        metrics = {}
        for _, row in df.iterrows():
            label = str(row[0]).strip().lower() if row[0] else ''
            col = next((c for n, c in LABELS.items() if n in label), None)
            if col is None:
                continue
            val = row[exec_col]
            # Tax-expenditure value occasionally lives in the next column.
            if col == 'tax_expenditure' and not (val and str(val).strip()):
                val = row.get(exec_col + 1)
            metrics[col] = parse_amount(val)
        return metrics
    return {}

# COMMAND ----------

rows_by_year = {}
for filename in sorted(os.listdir(VOLUME_PATH)):
    year = extract_year(filename)
    if year is None:
        continue
    with pdfplumber.open(VOLUME_PATH + filename) as pdf:
        metrics = extract_table_summary_metrics(pdf)

    if not metrics:
        raise ValueError(f"Summary table not found / unparsable in {filename}")
                         
    rows_by_year[year] = {
        'country_name': 'Togo',
        'country_code': 'TGO',
        'year': year,
        'revenue_current_lcu': metrics.get('revenue_current_lcu'),
        'expenditure_current_lcu': metrics.get('expenditure_current_lcu'),
        'tax_expenditure': metrics.get('tax_expenditure'),
        'data_source': 'Togo DGB Budget Execution Report',
    }
    print(f"✓ {filename}: {metrics}")

df = pd.DataFrame(rows_by_year.values())
print(df.to_string(index=False))

# COMMAND ----------

metrics

# COMMAND ----------

filename = 'togo_budget_2022.pdf'

# COMMAND ----------

pdf = pdfplumber.open(VOLUME_PATH + filename)
for page in pdf.pages:
    if "dépenses en atténuation de recettes " not in (page.extract_text().lower() or ''):
        continue
    tables = page.extract_tables()
    break

# COMMAND ----------

for page in pdf.pages:
    if 'tableau n° 22' not in (page.extract_text().lower()):
        continue
    print(page.page_number)

    tables = page.extract_tables()
    if not tables:
        continue
    df = pd.DataFrame(tables[0])
    exec_col = next(
        (i for i in range(len(df.columns))
            if 'EXECUTION' in str(df.iloc[1, i]).lower() and 'base' in str(df.iloc[1, i]).lower()),
        None,
    )
    if exec_col is None:
        print('return')
    metrics = {}
    for _, row in df.iterrows():
        label = str(row[0]).strip().lower() if row[0] else ''
        col = next((c for n, c in LABELS.items() if n in label), None)
        if col is None:
            continue
        val = row[exec_col]
        # Tax-expenditure value occasionally lives in the next column.
        if col == 'tax_expenditure' and not (val and str(val).strip()):
            val = row.get(exec_col + 1)
        metrics[col] = parse_amount(val)


# COMMAND ----------

df

# COMMAND ----------

page.extract_text().lower()

# COMMAND ----------

sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("prd_mega.indicator.togo_revenue_budget")
