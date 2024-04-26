import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
from google.cloud import storage
import logging


def download_amazon_dataset_to_gcs():
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Google Cloud Storage setup
    bucket_name = 'all_beauty_5'  # Specify your GCS bucket name
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # URL and download setup
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

    # Download and upload the first two "5-core" files directly to GCS
    for link in five_core_links[1:3]:  # Adjust the range as needed
        full_link = urljoin(url, link)
        blob = bucket.blob(link.split('/')[-1])  # Create a blob for file

        try:
            with requests.get(full_link, stream=True, verify=False) as r:
                r.raise_for_status()
                stream = r.raw
                blob.upload_from_file(stream)
            #ogging.info(f"Uploaded {link.split('/')[-1]} to GCS bucket {bucket_name}")
        except requests.RequestException as e:
            logging.error(f"Failed to download {link}: {e}")
        except Exception as e:
            logging.error(f"Failed to upload {link.split('/')[-1]}: {e}")

if __name__ == "__main__":
    download_amazon_dataset_to_gcs()








