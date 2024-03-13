import os
import glob
import pandas as pd
from google.cloud import storage

# Set the Google Cloud Storage credentials in the environment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "regal-bonito-415801-017316284a67.json"

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/Users/phanibhavanaatluri')
current_directory = os.path.join(AIRFLOW_HOME, 'dags')
bucket_name = 'all_beauty_5'

def merge_files():
    # Initialize the GCP Storage client
    storage_client = storage.Client()
    # Define the bucket object
    bucket = storage_client.bucket(bucket_name)
    # List all blobs in the bucket that have a .csv extension
    blobs = bucket.list_blobs()

    # Initialize a list to store DataFrames
    dfs = []

    for blob in blobs:
        if blob.name.endswith('.csv'):
            # Define the destination file path in a temporary directory
            destination_file_path = os.path.join("/tmp", blob.name)
            
            # Download the file
            blob.download_to_filename(destination_file_path)
            
            # Read the CSV file into a DataFrame
            df = pd.read_csv(destination_file_path)
            
            # Add the DataFrame to the list
            dfs.append(df)
            
            # Remove the file after processing if desired
            os.remove(destination_file_path)

    # Concatenate all DataFrames in the list
    df_merged = pd.concat(dfs, ignore_index=True)

    # Save the merged DataFrame to a new CSV file in a temporary directory (or any desired directory)
    merged_csv_file = os.path.join("/tmp", 'MergedData.csv')
    df_merged.to_csv(merged_csv_file, index=False)

    # Optionally upload the merged file back to GCS
    blob = bucket.blob('MergedData.csv')  # The path in GCS where you want to store the merged file
    blob.chunk_size = 5 * 1024 * 1024  # Set chunk size
    blob.upload_from_filename(merged_csv_file, timeout=300)

    print(f"Merged file saved as '{merged_csv_file}' and uploaded back to GCS.")
    print(df_merged.head(10))
    print(df_merged.tail(10))

# Call the function to perform the merging and saving process
merge_files()
