import os
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] ="regal-bonito-415801-017316284a67.json"

def list_buckets():
    """Lists all buckets."""
    storage_client = storage.Client()
    buckets = storage_client.list_buckets()
    for bucket in buckets:
        print(bucket.name)

list_buckets()

def upload_files_in_directory_to_gcs(bucket_name, directory):
    """Uploads all CSV files in the given directory to the specified GCS bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for filename in os.listdir(directory):
        if filename.endswith('.csv'):  # Check if the file is a CSV
            full_file_path = os.path.join(directory, filename)
            if os.path.isfile(full_file_path):
                destination_blob_name = filename  # The filename in GCS will be the same as the local file
                blob = bucket.blob(destination_blob_name)
                blob.chunk_size = 20 * 1024 * 1024
                blob.upload_from_filename(os.path.join(directory, filename), timeout=300)
                print(f"File {filename} uploaded to {bucket_name} as {destination_blob_name}")

# Example usage:
directory_to_upload = '.'  # Current directory. Change '.' to your directory if needed.
bucket_name = 'all_beauty_5'  # Replace with your bucket name.

upload_files_in_directory_to_gcs(bucket_name, directory_to_upload)