
import os
import gzip
import pandas as pd
import argparse
from google.cloud import storage
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Constants from environment variables
BUCKET_NAME = os.getenv('BUCKET_NAME')
# The other constants are not used in the functions provided, so they're commented out
# PROJECT_ID = os.getenv('PROJECT_ID')
# CONTAINER_URI = os.getenv('CONTAINER_URI')
# MODEL_SERVING_CONTAINER_IMAGE_URI = os.getenv('MODEL_SERVING_CONTAINER_IMAGE_URI')
# AIP_MODEL_DIR = os.getenv('AIP_MODEL_DIR')

# Initialize Google Cloud Storage Client
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

def upload_file_to_gcs(file_path):
    if not file_path.endswith('.gz'):
        raise ValueError("Unsupported file type, expecting a '.gz' file")

    filename = secure_filename(os.path.basename(file_path))
    blob_path = f'uploads/{filename}'
    blob = bucket.blob(blob_path)
    
    with open(file_path, 'rb') as file:
        blob.upload_from_file(file)
    
    return blob_path

def download_file_from_gcs(blob_path):
    local_path = blob_path.split('/')[-1]
    blob = bucket.blob(blob_path)
    blob.download_to_filename(local_path)
    return local_path

def convert_gz_csv(gz_path):
    csv_path = gz_path.replace('.gz', '.csv')
    with gzip.open(gz_path, 'rt') as gz_file:
        df = pd.read_csv(gz_file)
        df.to_csv(csv_path, index=False)
    return csv_path

def predict(csv_path):
    df = pd.read_csv(csv_path)
    # Here, replace this with your actual preprocessing and model prediction logic
    predictions = [0.5] * len(df)  # Mock prediction
    
    results = [
        {"asin": asin, "sentiment": "Positive" if pred > 0.5 else "Negative", "probability": pred}
        for asin, pred in zip(df['asin'], predictions)
    ]
    
    return results

def main(upload_file_path):
    # Upload the file to Google Cloud Storage
    blob_path = upload_file_to_gcs(upload_file_path)
    
    # Download the file from GCS to local file system
    downloaded_file_path = download_file_from_gcs(blob_path)
    
    # Convert .gz file to .csv
    csv_path = convert_gz_csv(downloaded_file_path)
    
    # Predict using the CSV
    results = predict(csv_path)
    
    # For the sake of this example, we just print the results
    print(results)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Upload a .gz file to GCS and perform prediction.')
    parser.add_argument('file_path', type=str, help='Path to the .gz file to upload and predict on.')
    
    args = parser.parse_args()
    
    main(args.file_path)



    

