# Import necessary libraries
import tensorflow_data_validation as tfdv
from tensorflow_metadata.proto.v0 import schema_pb2
import pandas as pd
import os

def generate_statistics(df):
    """
    Generate statistics for the given DataFrame using TensorFlow Data Validation.
    
    Args:
        df (pandas.DataFrame): The DataFrame for which to generate statistics.
    
    Returns:
        tfdv.Statistics: The generated statistics.
    """
    return tfdv.generate_statistics_from_dataframe(df)

def infer_schema(statistics):
    """
    Infer a schema for the given statistics using TensorFlow Data Validation.
    
    Args:
        statistics (tfdv.Statistics): The statistics for which to infer a schema.
    
    Returns:
        tfdv.Schema: The inferred schema.
    """
    return tfdv.infer_schema(statistics)

def validate_statistics(statistics, schema):
    """
    Validate the given statistics against the provided schema using TensorFlow Data Validation.
    
    Args:
        statistics (tfdv.Statistics): The statistics to validate.
        schema (tfdv.Schema): The schema against which to validate the statistics.
    
    Returns:
        tfdv.Anomalies: The detected anomalies.
    """
    return tfdv.validate_statistics(statistics, schema)

def save_schema(schema, output_path):
    """
    Save the TFDV schema to a file.
    
    Args:
        schema (tfdv.Schema): The TFDV schema to save.
        output_path (str): The file path where the schema should be saved.
    """
    schema_text = schema_pb2.Schema()
    schema_text.CopyFrom(schema)
    with open(output_path, 'w') as f:
        f.write(str(schema_text))

def run_tfdv_workflow(data_path, schema_output_path):
    """
    Runs the complete TFDV workflow including statistics generation, schema inference, anomaly detection, and schema saving.
    
    Args:
        data_path (str): Path to the CSV file containing the dataset.
        schema_output_path (str): Path where the inferred schema should be saved.
    """
    # Load the dataset
    df = pd.read_csv('clean1.csv')
    
    # Generate statistics
    statistics = generate_statistics(df)
    
    # Infer schema
    schema = infer_schema(statistics)
    
    # Save the schema
    save_schema(schema, schema_output_path)
    
    # Validate statistics
    anomalies = validate_statistics(statistics, schema)
    
    # Display anomalies
    print("Detected anomalies:", anomalies)

#Define the main execution logic
if __name__ == '__main__':
    # Specify the data path and schema output path
    data_path = '/clean_1.csv'
    schema_output_path = 'schema.pbtxt'
    # Run the TFDV workflow
    run_tfdv_workflow(data_path, schema_output_path)
