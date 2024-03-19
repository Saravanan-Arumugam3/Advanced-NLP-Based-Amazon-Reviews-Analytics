import tensorflow_data_validation as tfdv
from tensorflow_metadata.proto.v0 import schema_pb2
import pandas as pd
from sklearn.model_selection import train_test_split
import os
import logging
from google.cloud import storage
from util import add_extra_rows_to_df

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# # Set Google Cloud Storage credentials and bucket name
# Fetch the bucket name and JSON file path from environment variables
bucket_name = os.environ.get('GCS_BUCKET_NAME', 'default-bucket-name')
json_file_path = os.environ.get('GCP_SERVICE_ACCOUNT_JSON', '/path/to/your/service-account-file.json')

# Set Google Cloud Storage credentials in the environment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = json_file_path

# AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/home/saravanan/Desktop/MLOps_Spring24/Advanced-NLP-Based-Amazon-Reviews-Analytics')
# json_file_path = os.path.join(AIRFLOW_HOME, 'src', 'mlops-project-417704-47dfa275f621.json')
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = json_file_path
# bucket_name = 'amazon_reviews_project'

def download_cleaned_data_from_gcs(cleaned_data_blob_name, local_file_path):
    """Download the cleaned data from GCS to a local file."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(cleaned_data_blob_name)
    blob.download_to_filename(local_file_path)
    logging.info(f"Downloaded cleaned data to {local_file_path}")

def generate_statistics(df):
    """Generate statistics for the given DataFrame using TensorFlow Data Validation."""
    return tfdv.generate_statistics_from_dataframe(df)

def infer_schema(statistics):
    """Infer a schema for the given statistics using TensorFlow Data Validation."""
    return tfdv.infer_schema(statistics)

def validate_statistics(statistics, schema):
    """Validate the given statistics against the provided schema using TensorFlow Data Validation."""
    return tfdv.validate_statistics(statistics, schema)

def save_schema(schema, output_path):
    """Save the TFDV schema to a file."""
    schema_text = schema_pb2.Schema()
    schema_text.CopyFrom(schema)
    with open(output_path, 'w') as f:
        f.write(str(schema_text))

def modify_schema(schema):
    """Modify the schema based on specific requirements."""
    # Example modifications (customize as needed)
    product_type_domain = tfdv.get_domain(schema, 'filename')
    product_type_domain.value.append('AMAZON_FASHION_6')
    tfdv.set_domain(schema, 'overall', schema_pb2.FloatDomain(name='overall', min=1.0, max=5.0))

def run_tfdv_workflow(schema_output_path):
    """Runs the TFDV workflow using the cleaned data from GCS."""
    cleaned_data_blob_name = 'Clean_1.csv'
    local_cleaned_data_path = '/tmp/Clean_1.csv'

    # Download the cleaned data file from GCS
    download_cleaned_data_from_gcs(cleaned_data_blob_name, local_cleaned_data_path)

    # Load the cleaned data
    df = pd.read_csv(local_cleaned_data_path)

    # Split the data into training and evaluation datasets
    train_df, eval_df = train_test_split(df, test_size=0.2, shuffle=False)
    eval_df=add_extra_rows_to_df(eval_df)

    # Generate statistics
    train_stats = generate_statistics(train_df)
    eval_stats = generate_statistics(eval_df)

    # Infer schema
    schema = infer_schema(train_stats)

    # Validate statistics
    initial_anomalies = validate_statistics(eval_stats, schema)
    logging.info(f"Initial detected anomalies: {initial_anomalies}")

    # Modify the schema as needed
    modify_schema(schema)

    # Save the modified schema
    save_schema(schema, schema_output_path)

    # Re-validate statistics with the modified schema
    final_anomalies = validate_statistics(eval_stats, schema)
    logging.info(f"Anomalies after schema modification: {final_anomalies}")

if __name__ == '__main__':
    schema_output_path = 'path/to/your/schema.pbtxt'  # Update this path as needed
    run_tfdv_workflow(schema_output_path)
