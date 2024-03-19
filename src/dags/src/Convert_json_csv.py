import os
import json
import csv
import gzip
import logging
from google.cloud import storage

# Initialize Google Cloud Storage client and specify your bucket name
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/Users/phanibhavanaatluri')
json_file_path = os.path.join(AIRFLOW_HOME, 'dags','src', 'regal-bonito-415801-017316284a67.json')


gcs_client = storage.Client.from_service_account_json(json_file_path)
bucket_name = 'all_beauty_5'
bucket = gcs_client.bucket(bucket_name)

# Set the directory where your .gz or .json files are located
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/Users/phanibhavanaatluri')
DIRECTORY_PATH = os.path.join(AIRFLOW_HOME, 'dags')

def upload_to_gcs(source_file_path, destination_blob_name):
    """
    Function to upload a file from a local path to a specified destination in Google Cloud Storage (GCS),
    logging the upload progress.

    Parameters:
        source_file_path (str): The local file path of the file to be uploaded.
            This should be a string representing the absolute or relative path to the file.
        destination_blob_name (str): The name to assign to the file in the GCS bucket.
            This should be a string representing the desired name for the file in the bucket.
            The file will be uploaded to the bucket with this name.
    """
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    logging.info(f"File {source_file_path} uploaded to {destination_blob_name}.")


def json_to_csv_and_upload(json_file_path, csv_file_path, destination_blob_name):
    """ 
    Function to convert a JSON file to CSV format, using specified JSON keys as columns, 
    then upload the resulting CSV file to Google Cloud Storage (GCS) with a given destination blob name.
    Converts a JSON file to CSV and uploads it to GCS.
    
    Parameters:
        json_file_path (str): The path to the JSON file that needs to be converted to CSV.
        csv_file_path (str): The path where the resulting CSV file will be saved.
        destination_blob_name (str): The name of the blob (file) in Google Cloud Storage where the CSV file will be uploaded.
    """
    columns = ["overall", "verified", "reviewTime", "reviewerID", "asin", "reviewerName", "reviewText", "summary", "unixReviewTime"]
    with open(json_file_path, 'r', encoding='utf-8') as jfile, open(csv_file_path, 'w', newline='', encoding='utf-8') as cfile:
        writer = csv.DictWriter(cfile, fieldnames=columns)
        writer.writeheader()
        for line in jfile:
            jdata = json.loads(line.strip())
            writer.writerow({col: jdata.get(col, "") for col in columns})
    upload_to_gcs(csv_file_path, destination_blob_name)

 
def process_files():
    """
    Function to process files:
  - Set up logging configuration to display INFO level messages with timestamp and format.
  - Log the start of the process with the directory path where files are processed.
  - If the specified directory does not exist, log an error and return. 
    """
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
