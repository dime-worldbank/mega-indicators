# Databricks notebook source
import requests
import os
from datetime import datetime
import pandas as pd

# URLs for Togo budget documents
urls = [
    'https://dgbftg.org/index.php/documentation/category-layout/category-style2?task=download.send&id=138&catid=12&m=0',
    'https://dgbftg.org/index.php/documentation/category-layout/category-style2?task=download.send&id=118&catid=12&m=0',
    'https://dgbftg.org/index.php/documentation/category-layout/category-style2?task=download.send&id=99&catid=12&m=0'
]

# Volume path
volume_path = '/Volumes/prd_mega/sboost4/vboost4/Workspace/auxiliary_data/buget/togo/'

# Create directory if it doesn't exist
dbutils.fs.mkdirs(volume_path)

# Download and store files
download_metadata = []

for idx, url in enumerate(urls, 1):
    try:
        print(f"Downloading file {idx}/3 from {url[:50]}...")

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Get filename from content-disposition header or use default
        content_disposition = response.headers.get('content-disposition', '')
        if 'filename=' in content_disposition:
            filename = content_disposition.split('filename=')[1].strip('"')
        else:
            # Fallback filename
            filename = f'togo_budget_document_{idx}.pdf'

        # Full file path in volume
        file_path = volume_path + filename

        # Write file to volume
        dbutils.fs.put(file_path, response.content, overwrite=True)

        file_size = len(response.content)
        download_metadata.append({
            'filename': filename,
            'url': url,
            'file_path': file_path,
            'file_size_bytes': file_size,
            'download_timestamp': datetime.now().isoformat(),
            'status': 'SUCCESS'
        })

        print(f"✓ Successfully downloaded {filename} ({file_size} bytes)")

    except Exception as e:
        download_metadata.append({
            'filename': f'file_{idx}',
            'url': url,
            'file_path': None,
            'file_size_bytes': None,
            'download_timestamp': datetime.now().isoformat(),
            'status': f'FAILED: {str(e)}'
        })
        print(f"✗ Failed to download file {idx}: {str(e)}")

# Create metadata DataFrame
metadata_df = pd.DataFrame(download_metadata)

# Save metadata
metadata_path = volume_path + 'download_metadata.csv'
metadata_df.to_csv(f'/dbfs{metadata_path}', index=False)

print("\n=== Download Summary ===")
print(metadata_df.to_string())
print(f"\nMetadata saved to: {metadata_path}")
