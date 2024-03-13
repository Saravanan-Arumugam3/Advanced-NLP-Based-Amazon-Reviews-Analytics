import os
import pandas as pd
from google.cloud import storage

# Set the Google Cloud Storage credentials in the environment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "regal-bonito-415801-017316284a67.json"

bucket_name = 'all_beauty_5'

def merge_files():
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='CSV/')  # Access files within the 'csv' folder

    dfs = []

    for blob in blobs:
        if blob.name.endswith('.csv'):
            tmp_dir = os.path.join(AIRFLOW_HOME, 'temp')
            os.makedirs(tmp_dir, exist_ok=True)  # This creates the directory if it doesn't exist
            destination_file_path = os.path.join(tmp_dir, os.path.basename(blob.name))
            
            blob.download_to_filename(destination_file_path)
            
            df = pd.read_csv(destination_file_path)
            # Add a new column populated with the filename
            df['filename'] = os.path.basename(blob.name)
            
            dfs.append(df)
            
            # Optionally remove the file after processing
            # os.remove(destination_file_path)

    # Concatenate all DataFrames in the list
    df_merged = pd.concat(dfs, ignore_index=True)

    # Specify the path for the merged CSV file
    merged_csv_file = os.path.join(tmp_dir, 'MergedData.csv')
    df_merged.to_csv(merged_csv_file, index=False)

    # Upload the merged file back to GCS
    blob = bucket.blob('CSV/MergedData.csv')  
    blob.chunk_size = 5 * 1024 * 1024
    blob.upload_from_filename(merged_csv_file, timeout=300)

    print(f"Merged file saved as '{merged_csv_file}' and uploaded back to GCS in 'CSV' folder.")
    print(df_merged.head(10))
    print(df_merged.tail(10))

merge_files()