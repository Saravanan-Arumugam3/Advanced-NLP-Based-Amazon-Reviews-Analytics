import os
import pandas as pd
from google.cloud import storage
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def download_and_clean_gcs_data():
    """
    Function to download a CSV file from a specified Google Cloud Storage (GCS) bucket,
    clean it by removing specified columns, handling missing values and duplicates,
    and upload the cleaned data back to the GCS bucket.
    Log various stages of the process such as downloading, cleaning, and uploading,
    as well as any encountered errors.
    """
    
    bucket_name = 'all_beauty_5'
    blob_name = 'MergedData.csv'
    columns_to_remove = ['reviewerID', 'reviewerName', 'unixReviewTime']

    try:
        # Initialize the Google Cloud Storage client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Download the blob to a temporary location
        downloaded_blob_path = f'/tmp/{blob_name}'
        blob.download_to_filename(downloaded_blob_path)
        logging.info(f"Downloaded {blob_name} to {downloaded_blob_path}")

        # Read the downloaded CSV file into a DataFrame, handling inconsistent rows
        df = pd.read_csv(downloaded_blob_path, error_bad_lines=False, warn_bad_lines=True)
        df.drop(columns=columns_to_remove, inplace=True)
        logging.info(f"Removed columns: {columns_to_remove}")

        # Calculate and log the percentage of missing values per column
        missing_values_count = df.isnull().sum()
        total_rows = len(df)
        missing_percentage = (missing_values_count / total_rows) * 100
        logging.info("Percentage of missing values per column after removing specified columns:\n" + str(missing_percentage))

        # Clean the DataFrame by dropping rows with missing values and duplicates
        df_clean = df.dropna().drop_duplicates()
        logging.info("Cleaned DataFrame by dropping rows with missing values and duplicates.")

        # Save the cleaned DataFrame to a temporary CSV file
        clean_csv_file = '/tmp/Clean_1.csv'
        df_clean.to_csv(clean_csv_file, index=False)
        logging.info(f"Saved cleaned DataFrame to {clean_csv_file}")

        # Upload the cleaned CSV file to the Google Cloud Storage bucket
        clean_blob = bucket.blob('Clean_1.csv')
        clean_blob.upload_from_filename(clean_csv_file)
        logging.info(f"Uploaded cleaned file to GCS bucket {bucket_name} as Clean_1.csv")

        # Remove the temporary files
        os.remove(downloaded_blob_path)
        os.remove(clean_csv_file)
        logging.info("Removed temporary files.")

    except Exception as e:
        logging.error(f"Failed to process data: {e}")

# Call the modified function
download_and_clean_gcs_data()


