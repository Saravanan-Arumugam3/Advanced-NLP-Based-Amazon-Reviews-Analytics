import os
import json
import csv
import gzip
import logging
from google.cloud import storage

# # Initialize Google Cloud Storage client and specify your bucket name
# AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/home/saravanan/Desktop/MLOps_Spring24/Advanced-NLP-Based-Amazon-Reviews-Analytics')
# json_file_path = os.path.join(AIRFLOW_HOME, 'src', 'mlops-project-417704-47dfa275f621.json')
# #json_file_path = os.path.join(AIRFLOW_HOME, 'dags','src', 'mlops-project-417704-47dfa275f621.json')


# gcs_client = storage.Client.from_service_account_json(json_file_path)
# bucket_name = 'amazon_reviews_project'
# bucket = gcs_client.bucket(bucket_name)

# # Set the directory where your .gz or .json files are located
# AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/home/saravanan/Desktop/MLOps_Spring24/Advanced-NLP-Based-Amazon-Reviews-Analytics')
# DIRECTORY_PATH = os.path.join(AIRFLOW_HOME, 'data')


# Use environment variables to get the path to the JSON file and bucket name
json_file_path = os.environ.get('GCP_SERVICE_ACCOUNT_JSON')
bucket_name = os.environ.get('GCS_BUCKET_NAME')

# Initialize Google Cloud Storage client
gcs_client = storage.Client.from_service_account_json(json_file_path)
bucket = gcs_client.bucket(bucket_name)

# Set the directory where your .gz or .json files are located
# Assuming your data files are inside the "data" directory under AIRFLOW_PROJ_DIR
data_directory_path = os.path.join(os.environ.get('AIRFLOW_PROJ_DIR', '.'), 'data')

def upload_to_gcs(source_file_path, destination_blob_name):
    """Uploads a file to the bucket."""
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    logging.info(f"File {source_file_path} uploaded to {destination_blob_name}.")

def json_to_csv_and_upload(json_file_path, csv_file_path, destination_blob_name):
    """Converts a JSON file to CSV and uploads it to GCS."""
    # Define your JSON keys here
    columns = ["overall", "verified", "reviewTime", "reviewerID", "asin", "reviewerName", "reviewText", "summary", "unixReviewTime"]
    with open(json_file_path, 'r', encoding='utf-8') as jfile, open(csv_file_path, 'w', newline='', encoding='utf-8') as cfile:
        writer = csv.DictWriter(cfile, fieldnames=columns)
        writer.writeheader()
        for line in jfile:
            jdata = json.loads(line.strip())
            writer.writerow({col: jdata.get(col, "") for col in columns})
    upload_to_gcs(csv_file_path, destination_blob_name)

def process_files():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f"Starting process in directory: {DIRECTORY_PATH}")

    if not os.path.exists(DIRECTORY_PATH):
        logging.error(f"Directory does not exist: {DIRECTORY_PATH}")
        return

    # Process .gz files
    gz_files = [f for f in os.listdir(DIRECTORY_PATH) if f.endswith('.gz')]
    for gz_file in gz_files:
        gz_file_path = os.path.join(DIRECTORY_PATH, gz_file)
        json_file_path = gz_file_path.rsplit('.', 1)[0]  # Remove the '.gz' extension

        try:
            # Unzip the .gz file to JSON
            with gzip.open(gz_file_path, 'rt', encoding='utf-8') as gfile, open(json_file_path, 'w', encoding='utf-8') as jfile:
                jfile.write(gfile.read())
            logging.info(f"Unzipped {gz_file}")

            # Upload the extracted JSON file to GCS
            json_destination_blob_name = f"extracted/{os.path.basename(json_file_path)}"
            upload_to_gcs(json_file_path, json_destination_blob_name)

            # Convert JSON to CSV and upload
            csv_file_name = os.path.basename(json_file_path).replace('.json', '.csv')
            csv_file_path = os.path.join(DIRECTORY_PATH, csv_file_name)
            csv_destination_blob_name = f"CSV/{csv_file_name}"
            json_to_csv_and_upload(json_file_path, csv_file_path, csv_destination_blob_name)

            os.remove(json_file_path)
            logging.info(f"Deleted {json_file_path} after conversion.")

            os.remove(csv_file_path)
            logging.info(f"Deleted {csv_file_path} after conversion.")

        except Exception as e:
            logging.error(f"Failed to process {gz_file}: {e}")
            continue
    
process_files()
