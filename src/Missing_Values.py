import os
import pandas as pd
from google.cloud import storage
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "regal-bonito-415801-017316284a67.json"

bucket_name = 'all_beauty_5'

def download_and_clean_gcs_data(blob_name, columns_to_remove):
    try:
        storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)

        blob = bucket.blob(blob_name)

        downloaded_blob_path = f'/tmp/{blob_name}'

        blob.download_to_filename(downloaded_blob_path)
        logging.info(f"Downloaded {blob_name} to {downloaded_blob_path}")

        df = pd.read_csv(downloaded_blob_path)

        df.drop(columns=columns_to_remove, inplace=True)
        logging.info(f"Removed columns: {columns_to_remove}")

        missing_values_count = df.isnull().sum()
        total_rows = len(df)
        missing_percentage = (missing_values_count / total_rows) * 100
        logging.info("Percentage of missing values per column after removing specified columns:\n" + str(missing_percentage))

        df_clean = df.dropna().drop_duplicates()
        logging.info("Cleaned DataFrame by dropping rows with missing values and duplicates.")

        clean_csv_file = '/tmp/Clean_1.csv'
        df_clean.to_csv(clean_csv_file, index=False)
        logging.info(f"Saved cleaned DataFrame to {clean_csv_file}")

        # The blob name in GCS will also be 'Clean_1.csv', replacing the dynamic naming with a fixed one
        clean_blob = bucket.blob('Clean_1.csv')
        clean_blob.upload_from_filename(clean_csv_file)
        logging.info(f"Uploaded cleaned file to GCS bucket {bucket_name} as Clean_1.csv")

        os.remove(downloaded_blob_path)
        os.remove(clean_csv_file)
        logging.info("Removed temporary files.")

    except Exception as e:
        logging.error(f"Failed to process data: {e}")

# Call the function with your blob name and columns to remove
blob_name = 'MergedData.csv'
columns_to_remove = ['reviewerID', 'reviewerName', 'unixReviewTime']
download_and_clean_gcs_data(blob_name, columns_to_remove)
