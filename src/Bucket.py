import os
import logging
from google.cloud import storage

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set your Google Cloud credentials file here

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/Users/phanibhavanaatluri')
json_file_path = os.path.join(AIRFLOW_HOME, 'dags', 'src','regal-bonito-415801-017316284a67.json')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = json_file_path
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "regal-bonito-415801-017316284a67.json"

def list_buckets():
    """Lists all buckets."""
    try:
        storage_client = storage.Client()
        buckets = storage_client.list_buckets()
        for bucket in buckets:
            logging.info(bucket.name)
    except Exception as e:
        logging.error(f"Failed to list buckets: {e}")

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

# Example usage
if __name__ == "__main__":
    directory_to_upload = '.'  # Current directory. Change '.' to your directory if needed.
    bucket_name = 'all_beauty_5'  # Replace with your bucket name.
    list_buckets()  # List all buckets
    upload_files_in_directory_to_gcs(bucket_name, directory_to_upload)  # Upload files
