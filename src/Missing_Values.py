import os
from google.cloud import storage
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "regal-bonito-415801-017316284a67.json"

# Define your GCS bucket name
bucket_name = 'all_beauty_5'

# Initialize the GCP Storage client
storage_client = storage.Client()

# Define the bucket object
bucket = storage_client.bucket(bucket_name)

# The name of the file in your GCS bucket
blob_name = 'MergedData.csv'

# The path to which the file should be downloaded
downloaded_blob_path = '/tmp/MergedData.csv'

# Define the blob object
blob = bucket.blob(blob_name)

# Download the file to a local directory
blob.download_to_filename(downloaded_blob_path)

# Read the merged CSV file into a DataFrame
df = pd.read_csv(downloaded_blob_path)

# Remove the specified columns
columns_to_remove = ['reviewerID', 'reviewerName', 'unixReviewTime']
df.drop(columns=columns_to_remove, inplace=True)

# Check for missing values in the DataFrame
missing_values_count = df.isnull().sum()

# Calculate the percentage of missing values for each column
total_rows = len(df)
missing_percentage = (missing_values_count / total_rows) * 100

# Print the percentage of missing values for each column
print("Percentage of missing values per column after removing specified columns:")
print(missing_percentage)

# Clean the DataFrame by dropping rows with missing values
df_clean = df.dropna().drop_duplicates()

# Save the clean DataFrame to a new CSV file
clean_csv_file = '/tmp/Clean_1.csv'
df_clean.to_csv(clean_csv_file, index=False)

# Upload the clean CSV file back to GCS
bucket = storage_client.bucket(bucket_name)
clean_blob = bucket.blob('Clean_1.csv')  # The path in GCS where you want to store the clean file
clean_blob.upload_from_filename(clean_csv_file)

# Remove the temporary files if needed
os.remove(downloaded_blob_path)
os.remove(clean_csv_file)


print(f"Cleaned file uploaded to GCS bucket {bucket_name} as Clean_1.csv.")
