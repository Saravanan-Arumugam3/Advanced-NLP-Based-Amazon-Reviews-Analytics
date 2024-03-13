import os
import glob
import pandas as pd
from google.cloud import storage

# Set the Google Cloud Storage credentials in the environment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "regal-bonito-415801-017316284a67.json"

#AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/Users/phanibhavanaatluri')
#current_directory = os.path.join(AIRFLOW_HOME, 'dags')
bucket_name = 'all_beauty_5'

def merge_files():
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs()

    dfs = []

    for blob in blobs:
        if blob.name.endswith('.csv'):
            destination_file_path = os.path.join("/tmp", blob.name)
            
            blob.download_to_filename(destination_file_path)
            
            df = pd.read_csv(destination_file_path)
            
            dfs.append(df)
            
            os.remove(destination_file_path)

    # Concatenate all DataFrames in the list
    df_merged = pd.concat(dfs, ignore_index=True)

    merged_csv_file = os.path.join("/tmp", 'MergedData.csv')
    df_merged.to_csv(merged_csv_file, index=False)

    blob = bucket.blob('MergedData.csv')  
    blob.chunk_size = 5 * 1024 * 1024
    blob.upload_from_filename(merged_csv_file, timeout=300)

    print(f"Merged file saved as '{merged_csv_file}' and uploaded back to GCS.")
    print(df_merged.head(10))
    print(df_merged.tail(10))

merge_files()
