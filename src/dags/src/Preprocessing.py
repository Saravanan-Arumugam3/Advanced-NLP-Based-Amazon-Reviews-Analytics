import pandas as pd
import numpy as np
from google.cloud import storage
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def replace_text(dataframe, column, replace, substitute):
  dataframe[column] = dataframe['reviewTime'].str.replace(replace, substitute)
  return dataframe

def delete_columns(dataframe, columns):
  dataframe = dataframe.drop(columns, axis=1)
  dataframe = dataframe.drop_duplicates()
  dataframe = dataframe.dropna()
  return dataframe

def Preprocessing():

    bucket_name = 'all_beauty_5'
    blob_name = 'Clean_1.csv'

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
        df_clean = pd.read_csv(downloaded_blob_path, error_bad_lines=False, warn_bad_lines=True)
        df_merged = df_clean
        df_merged['reviewText']=df_merged['reviewText'].fillna('Missing')
        df_merged = replace_text(df_merged, 'reviewTime', ',', '')
        df_merged = replace_text(df_merged, 'reviewTime', ' ', '-')
        df_merged['reviewTime'] = pd.to_datetime(df_merged['reviewTime'])
        df_merged['reviews'] = df_merged['reviewText']+ " " + df_merged['summary']
        df_merged = delete_columns(df_merged, ['reviewText', 'summary'])
        df_merged['sentiment'] = np.where(df_merged['overall'] > 3, 'Positive', np.where(df_merged['overall'] < 3, 'Negative', 'Neutral'))
        
        clean_csv_file = '/tmp/Preprocessed.csv'
        df_merged.to_csv(clean_csv_file, index=False)
        logging.info(f"Saved cleaned DataFrame to {clean_csv_file}")

        # Upload the cleaned CSV file to the Google Cloud Storage bucket
        clean_blob = bucket.blob('Preprocessed.csv')
        clean_blob.upload_from_filename(clean_csv_file)
        logging.info(f"Uploaded cleaned file to GCS bucket {bucket_name} as Preprocessed.csv")
    
    except Exception as e:
        logging.error(f"Failed to process data: {e}")

Preprocessing()

    

