import os
import requests
from bs4 import BeautifulSoup
import gzip
import json
import csv
import pandas as pd
from google.cloud import storage
import logging
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from urllib.parse import urljoin

# Disable SSL certificate verification warning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'mlops-421203-829082ef9915.json'

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the GCS client
storage_client = storage.Client()
bucket_name = 'mlops_pro'
bucket = storage_client.bucket(bucket_name)


def download_amazon_dataset():
    download_directory = os.getcwd()  # or set a specific download directory

    # Set up Chrome options to set the download path

    # URL and download setup
    url = 'https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/'
    criteria = '5-core'
    # Ensure the directory exists
    os.makedirs(download_directory, exist_ok=True)

    # Fetch the content from the URL
    try:
        response = requests.get(url, verify=False)  # Should handle SSL properly in production
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch the URL: {e}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    five_core_links = [a['href'] for a in soup.find_all('a', href=True) if criteria in a.text]

    downloaded_files = []

    # Download the first two "5-core" files
    for link in five_core_links[2:4]:  # Adjust the range as needed
        full_link = urljoin(url, link)
        local_path = os.path.join(download_directory, os.path.basename(link))  # Save to the specified directory

        try:
            with requests.get(full_link, stream=True, verify=False) as r:
                r.raise_for_status()
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            downloaded_files.append(local_path)  # Store the downloaded file path
            logging.info(f"Downloaded {os.path.basename(link)} to {local_path}")
        except requests.RequestException as e:
            logging.error(f"Failed to download {link}: {e}")

    all_data_frames = []
    for file_path in downloaded_files:  # Iterate over the downloaded file paths
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as file:
                data = [json.loads(line) for line in file]
                df = pd.DataFrame(data)
                all_data_frames.append(df)
            os.remove(file_path)  # Clean up downloaded gz file
        except Exception as e:
            logging.error(f"Failed to process file {file_path}: {e}")
            continue

    if not all_data_frames:
        logging.error("No data frames were created due to earlier errors.")
        return

    combined_df = pd.concat(all_data_frames, ignore_index=True)
    logging.info('Converted gz to CSV and merged them.')

    existing_csv_blob = bucket.blob('Clean_1.csv')
    existing_csv_path = '/tmp/existing_Clean_1.csv'
    try:
        existing_csv_blob.download_to_filename(existing_csv_path)
        existing_df = pd.read_csv(existing_csv_path)
        logging.info('Downloaded existing Clean_1.csv for merging.')
    except Exception as e:
        logging.error(f"Failed to download existing CSV from GCS: {e}")
        return

    existing_df.drop_duplicates(inplace=True)
    existing_df.fillna(method='ffill', inplace=True)

    clean_csv_path = '/tmp/Clean_1.csv'
    try:
        existing_csv_blob.delete()
        logging.info('Deleted existing Clean_1.csv from GCS.')

        existing_df.to_csv(clean_csv_path, index=False)
        clean_blob = bucket.blob('Clean_1.csv')
        clean_blob.upload_from_filename(clean_csv_path)
        logging.info('Uploaded cleaned CSV to GCS.')
    except Exception as e:
        logging.error(f"Failed to update CSV in GCS: {e}")
    finally:
        os.remove(clean_csv_path)
        logging.info('Cleaned up temporary files.')

if __name__ == "__main__":
    download_amazon_dataset()
