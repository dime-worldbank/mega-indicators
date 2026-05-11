# Databricks notebook source
import pdfplumber
import pandas as pd
import os
from datetime import datetime

# Volume path where PDFs are stored
volume_path = '/Volumes/prd_mega/sboost4/vboost4/Workspace/auxiliary_data/buget/togo/'

# List all PDF files in the volume
pdf_files = [f for f in dbutils.fs.ls(volume_path) if f.name.endswith('.pdf')]

print(f"Found {len(pdf_files)} PDF files in {volume_path}")

extracted_data = []

for file_info in pdf_files:
    pdf_path = file_info.path.replace('dbfs:', '')

    try:
        with pdfplumber.open(pdf_path) as pdf:
            # Determine year from filename or PDF content
            year = None
            if '2022' in file_info.name:
                year = 2022
            elif '2023' in file_info.name:
                year = 2023
            elif '2024' in file_info.name or '24' in file_info.name:
                year = 2024

            print(f"\nProcessing: {file_info.name} (Year: {year})")

            # Search for Table 23
            for page in pdf.pages:
                text = page.extract_text()
                if 'Tableau n° 23' in text:
                    tables = page.extract_tables()
                    if tables:
                        df = pd.DataFrame(tables[0])

                        # Find EXECUTION column
                        execution_col = None
                        for col_idx in range(len(df.columns)):
                            header = str(df.iloc[1, col_idx]).lower() if col_idx < len(df.columns) else ""
                            if 'execution' in header and 'base' in header:
                                execution_col = col_idx
                                break

                        if execution_col is None:
                            print(f"  Warning: Could not find EXECUTION column")
                            continue

                        # Extract rows
                        for idx, row in df.iterrows():
                            row_label = str(row[0]).strip().lower() if row[0] else ""

                            if 'recettes budgétaires' in row_label:
                                val = row[execution_col]
                                extracted_data.append({
                                    'country': 'Togo',
                                    'country_code': 'TGO',
                                    'year': year,
                                    'revenue': val,
                                    'source': 'Togo DGB Budget Execution Report'
                                })
                                print(f"  ✓ Revenue: {val}")

                            elif 'dépenses budgétaires' in row_label:
                                val = row[execution_col]
                                extracted_data.append({
                                    'country': 'Togo',
                                    'country_code': 'TGO',
                                    'year': year,
                                    'expenditure': val,
                                    'source': 'Togo DGB Budget Execution Report'
                                })
                                print(f"  ✓ Expenditure: {val}")

                            elif 'dépenses en atténuation' in row_label:
                                val = row[execution_col]
                                if not val or str(val).strip() == '':
                                    val = row[execution_col + 1] if execution_col + 1 < len(row) else None
                                extracted_data.append({
                                    'country': 'Togo',
                                    'country_code': 'TGO',
                                    'year': year,
                                    'tax_expenditure': val,
                                    'source': 'Togo DGB Budget Execution Report'
                                })
                                print(f"  ✓ Tax expenditure: {val}")
                        break

    except Exception as e:
        print(f"  Error processing {file_info.name}: {str(e)}")

# Create DataFrame and pivot to wide format
if extracted_data:
    df = pd.DataFrame(extracted_data)

    # Pivot to wide format with metrics as columns
    df_wide = df.pivot_table(
        index=['country', 'country_code', 'year'],
        columns='metric',
        values='value',
        aggfunc='first'
    ).reset_index()

    # Rename columns to clean up the metric names
    df_wide.columns.name = None

    print(f"\n{'='*60}")
    print("Extracted Data (Wide Format):")
    print(f"{'='*60}")
    print(df_wide.to_string(index=False))

    # Convert to Spark DataFrame and save
    sdf = spark.createDataFrame(df_wide)
    sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("prd_mega.indicator.togo_revenue_budget")
    print(f"\nData saved to: prd_mega.indicator.togo_revenue_budget")
else:
    print("No data extracted!")
