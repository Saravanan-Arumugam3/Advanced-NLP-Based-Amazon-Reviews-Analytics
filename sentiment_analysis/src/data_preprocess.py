import os
import requests
from bs4 import BeautifulSoup
import gzip
import json
import pandas as pd
from google.cloud import storage
import logging
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from urllib.parse import urljoin
import io

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
    url = 'https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/'
    criteria = '5-core'

    # Fetch the content from the URL
    try:
        response = requests.get(url, verify=False)  # Should handle SSL properly in production
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch the URL: {e}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    five_core_links = [a['href'] for a in soup.find_all('a', href=True) if criteria in a.text]

    all_data_frames = []

    # Download and process the first two "5-core" files in memory
    for link in five_core_links[2:4]:
        full_link = urljoin(url, link)

        try:
            with requests.get(full_link, stream=True, verify=False) as r:
                r.raise_for_status()
                bytes_stream = io.BytesIO(r.content)
                with gzip.open(bytes_stream, 'rt', encoding='utf-8') as file:
                    data = [json.loads(line) for line in file]
                    df = pd.DataFrame(data)
                    # Drop specified columns and add a column for filename
                    columns_to_remove = ['reviewerID', 'reviewerName', 'unixReviewTime']
                    df.drop(columns=columns_to_remove, inplace=True)
                    df['filename'] = 'filename.csv'  # Add filename column
                    all_data_frames.append(df)
            logging.info(f"Processed {link} in memory.")
        except requests.RequestException as e:
            logging.error(f"Failed to download and process {link}: {e}")

    if not all_data_frames:
        logging.error("No data frames were created due to earlier errors.")
        return

    combined_df = pd.concat(all_data_frames, ignore_index=True)

    existing_csv_blob = bucket.blob('Clean_1.csv')
    # Process existing CSV directly in memory
    existing_df = pd.read_csv(io.BytesIO(existing_csv_blob.download_as_bytes()))
    existing_df.drop_duplicates(inplace=True)
    existing_df.fillna(method='ffill', inplace=True)
    combined_df = pd.concat([existing_df, combined_df], ignore_index=True)

    # Upload the updated dataframe to GCS
    try:
        # Convert DataFrame to CSV in memory
        clean_csv_bytes = combined_df.to_csv(index=False).encode()
        clean_blob = bucket.blob('Clean_1.csv')
        clean_blob.upload_from_string(clean_csv_bytes)
        logging.info('Uploaded cleaned CSV to GCS.')
    except Exception as e:
        logging.error(f"Failed to update CSV in GCS: {e}")

if __name__ == "__main__":
    download_amazon_dataset()
