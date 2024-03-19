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

# Set Google Cloud Storage credentials and bucket name
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/Users/phanibhavanaatluri')
json_file_path = os.path.join(AIRFLOW_HOME, 'dags', 'src', 'regal-bonito-415801-017316284a67.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = json_file_path
bucket_name = 'all_beauty_5'


def download_cleaned_data_from_gcs(cleaned_data_blob_name, local_file_path):
    """
    Function to download cleaned data from a specified blob in Google Cloud Storage (GCS) to a local file,
    logging the download progress.
    
    Parameters
    cleaned_data_blob_name (str): The name of the blob (object) in Google Cloud Storage (GCS) that contains the cleaned data.
                                     This parameter specifies the exact location of the data to be downloaded.
                                     
    local_file_path (str): The path to the local file where the downloaded data will be stored.
                               This parameter specifies where on the local filesystem the downloaded data will be saved.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(cleaned_data_blob_name)
    blob.download_to_filename(local_file_path)
    logging.info(f"Downloaded cleaned data to {local_file_path}")


def generate_statistics(df):
    """
    Function to generate statistics for the provided DataFrame using TensorFlow Data Validation (TFDV).

    Parameters
    df: Represents the DataFrame for which statistics are to be generated using TensorFlow Data Validation (TFDV).
    """
    return tfdv.generate_statistics_from_dataframe(df)


def infer_schema(statistics):
    """
    Function to infer a schema for given statistics using TensorFlow Data Validation.

    Parameters
    statistics: Holds the statistics generated from the DataFrame. It's used to infer a schema using TensorFlow Data Validation.
    """
    return tfdv.infer_schema(statistics)


def validate_statistics(statistics, schema):
    """
    Function to validate given statistics against provided schema using TensorFlow Data Validation.

    Parameters
    statistics: The statistics to be validated against the provided schema using TensorFlow Data Validation.
    schema: The schema against which the statistics are to be validated.

    """
    return tfdv.validate_statistics(statistics, schema)


def save_schema(schema, output_path):
    """
    Function to save the TFDV schema to a file.

    Parameters
    schema: The schema to be saved to a file.
    output_path: The path where the schema will be saved.
    """
    schema_text = schema_pb2.Schema()
    schema_text.CopyFrom(schema)
    with open(output_path, 'w') as f:
        f.write(str(schema_text))


def modify_schema(schema):
    """
    Function to modify the schema based on specific requirements.

    """
    product_type_domain = tfdv.get_domain(schema, 'filename')
    product_type_domain.value.append('AMAZON_FASHION_6')
    tfdv.set_domain(schema, 'overall', schema_pb2.FloatDomain(name='overall', min=1.0, max=5.0))

# Function to run the TFDV workflow using cleaned data from GCS.
def run_tfdv_workflow(schema_output_path):
    """
    Runs the TFDV workflow using the cleaned data from GCS.
    
    Parameters
    schema_output_path: The path where the schema file will be saved. 
    This is used to run the TFDV workflow using cleaned data from GCS.
    """
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
