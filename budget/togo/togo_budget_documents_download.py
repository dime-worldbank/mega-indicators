# Databricks notebook source
import requests

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
for idx, url in enumerate(urls, 1):
    try:
        print(f"Downloading file {idx}/3...")

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Get filename from content-disposition header or use default
        content_disposition = response.headers.get('content-disposition', '')
        if 'filename=' in content_disposition:
            filename = content_disposition.split('filename=')[1].strip('"')
        else:
            filename = f'togo_budget_document_{idx}.pdf'

        # Full file path in volume
        file_path = volume_path + filename

        # Write file to volume
        dbutils.fs.put(file_path, response.content, overwrite=True)

        file_size = len(response.content)
        print(f"✓ Successfully downloaded {filename} ({file_size} bytes)")

    except Exception as e:
        print(f"✗ Failed to download file {idx}: {str(e)}")

print("\n=== Download Complete ===")
