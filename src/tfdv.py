import tensorflow_data_validation as tfdv
from tensorflow_metadata.proto.v0 import schema_pb2
import pandas as pd
from sklearn.model_selection import train_test_split
import os
from util import add_extra_rows_to_df

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
    # Add new value to the domain of the feature 'Product_Type'
    product_type_domain = tfdv.get_domain(schema, 'Product_Type')
    product_type_domain.value.append('AMAZON_FASHION_6')
    
    # Restrict the range of the 'overall' feature
    tfdv.set_domain(schema, 'overall', schema_pb2.FloatDomain(name='overall', min=1.0, max=5.0))

def run_tfdv_workflow(data_path, schema_output_path):
    """Runs the complete TFDV workflow including statistics generation, schema inference, anomaly detection, and schema saving."""
    # Load the dataset
    df = pd.read_csv(data_path)
    
    # Split dataset into train and eval without shuffling
    train_df, eval_df = train_test_split(df, test_size=0.2, shuffle=False)
    
    # Add extra rows to eval_df
    eval_df = add_extra_rows_to_df(eval_df)
    
    # Generate statistics for train and eval datasets
    train_stats = generate_statistics(train_df)
    eval_stats = generate_statistics(eval_df)
    
    # Infer schema from the train dataset
    schema = infer_schema(train_stats)
    
    # First anomaly detection: eval dataset statistics against the original train dataset schema
    initial_anomalies = validate_statistics(eval_stats, schema)
    print("Initial detected anomalies:", initial_anomalies)
    
    # Modify the schema as required
    modify_schema(schema)
    
    # Save the modified schema
    save_schema(schema, schema_output_path)
    
    # Second anomaly detection: eval dataset statistics against the modified train dataset schema
    final_anomalies = validate_statistics(eval_stats, schema)
    print("Anomalies after schema modification:", final_anomalies)
    
    # Concatenate anomalies information for the email, with an empty line between final and initial anomalies
    anomalies_info = str(final_anomalies) + "\n\n" + str(initial_anomalies)
    return anomalies_info

if __name__ == '__main__':
    # Specify the data path and schema output path
    data_path = 'path/to/your/clean1.csv'
    schema_output_path = 'path/to/your/schema.pbtxt'
    # Run the TFDV workflow
    anomalies_info = run_tfdv_workflow(data_path, schema_output_path)
    print("Anomalies Information:", anomalies_info)
