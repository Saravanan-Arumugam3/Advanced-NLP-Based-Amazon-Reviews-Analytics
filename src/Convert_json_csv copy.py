import os
import json
import csv
import gzip
import logging
from google.cloud import storage

def main():
    # Setup and configuration
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    gcs_credentials_path = os.environ.get('GCS_CREDENTIALS_PATH', 'regal-bonito-415801-017316284a67.json')
    bucket_name = os.environ.get('GCS_BUCKET_NAME', 'all_beauty_5')
    AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/Users/yasir/Desktop/MLops_Project/Advanced-NLP-Based-Amazon-Reviews-Analytics/data')
    DIRECTORY_PATH = AIRFLOW_HOME
    columns = ["overall", "verified", "reviewTime", "reviewerID", "asin", "reviewerName", "reviewText", "summary", "unixReviewTime"]
    
    # Initialize Google Cloud Storage client
    gcs_client = storage.Client.from_service_account_json(gcs_credentials_path)
    bucket = gcs_client.bucket(bucket_name)

    # Check if directory exists
    if not os.path.exists(DIRECTORY_PATH):
        logging.error(f"Directory does not exist: {DIRECTORY_PATH}")
        return

    # Process .gz files in the directory
    gz_files = [f for f in os.listdir(DIRECTORY_PATH) if f.endswith('.gz')]
    for gz_file in gz_files:
        gz_file_path = os.path.join(DIRECTORY_PATH, gz_file)
        json_file_path = gz_file_path.rsplit('.', 1)[0]  # Remove the '.gz' extension
        csv_file_name = os.path.basename(json_file_path).replace('.json', '.csv')
        csv_file_path = os.path.join(DIRECTORY_PATH, csv_file_name)
        json_destination_blob_name = f"extracted/{os.path.basename(json_file_path)}"
        csv_destination_blob_name = f"CSV/{csv_file_name}"

        try:
            # Unzip .gz to JSON
            with gzip.open(gz_file_path, 'rt', encoding='utf-8') as gfile, open(json_file_path, 'w', encoding='utf-8') as jfile:
                jfile.write(gfile.read())
            logging.info(f"Unzipped {gz_file}")

            # Upload JSON to GCS
            blob = bucket.blob(json_destination_blob_name)
            blob.upload_from_filename(json_file_path)
            logging.info(f"File {json_file_path} uploaded to {json_destination_blob_name}.")

            # Convert JSON to CSV
            with open(json_file_path, 'r', encoding='utf-8') as jfile, open(csv_file_path, 'w', newline='', encoding='utf-8') as cfile:
                writer = csv.DictWriter(cfile, fieldnames=columns)
                writer.writeheader()
                for line in jfile:
                    jdata = json.loads(line.strip())
                    writer.writerow({col: jdata.get(col, "") for col in columns})

            # Upload CSV to GCS
            blob = bucket.blob(csv_destination_blob_name)
            blob.upload_from_filename(csv_file_path)
            logging.info(f"File {csv_file_path} uploaded to {csv_destination_blob_name}.")
        except Exception as e:
            logging.error(f"Failed to process {gz_file}: {e}")
        finally:
            # Cleanup
            #if os.path.exists(json_file_path):
             #   os.remove(json_file_path)
            #if os.path.exists(gz_file_path):
             #   os.remove(gz_file_path)

if __name__ == "__main__":
    main()