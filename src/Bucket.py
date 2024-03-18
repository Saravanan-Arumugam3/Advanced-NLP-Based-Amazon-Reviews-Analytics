import os
import logging
from google.cloud import storage

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the directory where Apache Airflow is installed, defaulting to a specific path if not set,
# then construct the path to the JSON file within the Airflow directory structure.
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/Users/phanibhavanaatluri')
json_file_path = os.path.join(AIRFLOW_HOME, 'dags', 'src','regal-bonito-415801-017316284a67.json')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = json_file_path
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "regal-bonito-415801-017316284a67.json"


# Define a function to list all buckets in a Google Cloud Storage project.
# If successful, logs the name of each bucket using the logging module.
# If an error occurs, logs the error message.
def list_buckets():
    """Lists all buckets."""
    try:
        storage_client = storage.Client()
        buckets = storage_client.list_buckets()
        for bucket in buckets:
            logging.info(bucket.name)
    except Exception as e:
        logging.error(f"Failed to list buckets: {e}")


# Define a function to upload all CSV files from a local directory to a specified Google Cloud Storage (GCS) bucket.
# If the directory doesn't exist, logs an error and returns.
# If no CSV files are found in the directory, logs a message and returns.
# For each CSV file found, uploads it to the specified GCS bucket with a chunk size of 20 MB,
# logging the upload progress and any errors that occur.
def upload_files_in_directory_to_gcs(bucket_name, directory):
    """Uploads all CSV files in the given directory to the specified GCS bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        if not os.path.exists(directory):
            logging.error(f"Directory does not exist: {directory}")
            return

        csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
        if not csv_files:
            logging.info("No .csv files found.")
            return

        for filename in csv_files:
            full_file_path = os.path.join(directory, filename)
            destination_blob_name = filename  # The filename in GCS will be the same as the local file
            blob = bucket.blob(destination_blob_name)
            blob.chunk_size = 20 * 1024 * 1024  # Set chunk size to 20 MB
            blob.upload_from_filename(full_file_path, timeout=300)
            logging.info(f"File {filename} uploaded to {bucket_name} as {destination_blob_name}")

    except Exception as e:
        logging.error(f"Failed to upload files to GCS: {e}")

#   - Set the directory to upload as the current directory.
#   - Set the GCS bucket name to 'all_beauty_5'. Modify it as needed.
#   - List all buckets in the GCS project.
#   - Upload all files from the specified directory to the specified GCS bucket.
if __name__ == "__main__":
    directory_to_upload = '.'
    bucket_name = 'all_beauty_5'  
    list_buckets()  
    upload_files_in_directory_to_gcs(bucket_name, directory_to_upload)  
