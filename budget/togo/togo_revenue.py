# Databricks notebook source
!pip install pdfplumber


# COMMAND ----------

import pdfplumber
import pandas as pd
import re

BILLION = 1000000000

def parse_amount(val):
    """Parse French-formatted PDF amount (e.g. '123 123, 00') to float, returning None on failure."""
    if not val:
        return None
    try:
        s = re.sub(r'\s+', '', str(val)).replace(',', '.')
        return float(s) * BILLION if s else None
    except (ValueError, TypeError):
        return None

def extract_year(filename):
    """Extract year from filename (e.g. '2022', '2023', '2024')."""
    match = re.search(r'(202[0-9])', filename)
    return int(match.group(1)) if match else None

def extract_table_23_data(pdf, year, extracted_data):
    """Search for Table 23 in PDF and extract budget data. Updates extracted_data dict in place."""
    for page in pdf.pages:
        if 'Tableau n° 23' not in page.extract_text():
            continue

        tables = page.extract_tables()
        if not tables:
            continue

        df = pd.DataFrame(tables[0])
        execution_col = next(
            (i for i in range(len(df.columns))
             if 'execution' in str(df.iloc[1, i]).lower() and 'base' in str(df.iloc[1, i]).lower()),
            None
        )

        if execution_col is None:
            print(f"  Warning: Could not find EXECUTION column")
            return

        for idx, row in df.iterrows():
            row_label = str(row[0]).strip().lower() if row[0] else ""
            val_cell = row[execution_col]

            if 'recettes budgétaires' in row_label:
                val = parse_amount(val_cell)
                extracted_data[year]['revenue_current_lcu'] = val
                print(f"  ✓ Revenue: {val}")
            elif 'dépenses budgétaires' in row_label:
                val = parse_amount(val_cell)
                extracted_data[year]['expenditure_current_lcu'] = val
                print(f"  ✓ Expenditure: {val}")
            elif 'dépenses en atténuation' in row_label:
                val = val_cell if val_cell and str(val_cell).strip() else row.get(execution_col + 1)
                val = parse_amount(val)
                extracted_data[year]['tax_expenditure'] = val
                print(f"  ✓ Tax expenditure: {val}")
        return

# Volume path where PDFs are stored
volume_path = '/Volumes/prd_mega/sboost4/vboost4/Workspace/auxiliary_data/buget/togo/'

# List all PDF files in the volume
pdf_files = [f for f in dbutils.fs.ls(volume_path) if f.name.endswith('.pdf')]

print(f"Found {len(pdf_files)} PDF files in {volume_path}")

extracted_data = {}

for file_info in pdf_files:
    pdf_path = file_info.path.replace('dbfs:', '')
    year = extract_year(file_info.name)

    if year is None:
        print(f"Warning: Could not extract year from {file_info.name}")
        continue

    print(f"\nProcessing: {file_info.name} (Year: {year})")

    if year not in extracted_data:
        extracted_data[year] = {
            'country_name': 'Togo',
            'country_code': 'TGO',
            'year': year,
            'revenue_current_lcu': None,
            'expenditure_current_lcu': None,
            'tax_expenditure': None,
            'data_source': 'Togo DGB Budget Execution Report'
        }

    try:
        with pdfplumber.open(pdf_path) as pdf:
            extract_table_23_data(pdf, year, extracted_data)
    except Exception as e:
        print(f"  Error processing {file_info.name}: {str(e)}")

# Convert dictionary to list
extracted_data = list(extracted_data.values())

# Create DataFrame from merged data
if extracted_data:
    df = pd.DataFrame(extracted_data)

    print(f"\n{'='*60}")
    print("Extracted Data:")
    print(f"{'='*60}")
    print(df.to_string(index=False))

    # Convert to Spark DataFrame and save
    sdf = spark.createDataFrame(df)
    sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("prd_mega.indicator.togo_revenue_budget")
    print(f"\nData saved to: prd_mega.indicator.togo_revenue_budget")
else:
    print("No data extracted!")
