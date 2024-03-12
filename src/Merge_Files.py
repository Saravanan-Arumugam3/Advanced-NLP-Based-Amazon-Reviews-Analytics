import os
from google.cloud import storage
import pandas as pd

# Set the Google Cloud Storage credentials in the environment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "regal-bonito-415801-017316284a67.json"

# Define your GCS bucket name
bucket_name = 'all_beauty_5'

# Initialize the GCP Storage client
storage_client = storage.Client()

# Define the bucket object
bucket = storage_client.bucket(bucket_name)

# List all blobs in the bucket that have a .csv extension
blobs = bucket.list_blobs()

# Initialize a list to store DataFrames
dfs = []

# Loop through each blob
for blob in blobs:
    if blob.name.endswith('.csv'):
        # Define the destination file path
        destination_file_path = f"/tmp/{blob.name}"
        
        # Download the file
        blob.download_to_filename(destination_file_path)
        
        # Read the CSV file
        temp_df = pd.read_csv(destination_file_path)
        
        # Create a new column with the name of the file (without the extension)
        temp_df['Product_Type'] = os.path.splitext(blob.name)[0]
        
        # Add the DataFrame to the list
        dfs.append(temp_df)
        
        # Remove the file after processing if desired
        os.remove(destination_file_path)

# Concatenate all DataFrames in the list
merged_df = pd.concat(dfs, ignore_index=True)

# Save the merged DataFrame to a new CSV file in the /tmp directory (or any desired directory)
merged_csv_file = '/tmp/Data.csv'
merged_df.to_csv(merged_csv_file, index=False)

# Optionally upload the merged file back to GCS
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob('MergedData.csv')  # The path in GCS where you want to store the merged file
blob.chunk_size = 5 * 1024 * 1024
blob.upload_from_filename(os.path.join('.', merged_csv_file), timeout=300)
print(f"Merged file saved as '{merged_csv_file}' and uploaded back to GCS.")
print(merged_df.head(10))
print(merged_df.tail(10))
print(merged_df['Product_Type'].unique())