import os
import datetime
import pandas as pd
import datetime
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Embedding, LSTM, Bidirectional
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
from sklearn.model_selection import train_test_split
import tensorflow as tf
import mlflow
import mlflow.tensorflow
from io import StringIO
from google.cloud import storage
from dotenv import load_dotenv
import gcsfs
import joblib
import pytz


# Load environment variables
load_dotenv()

# Initialize variables
fs = gcsfs.GCSFileSystem()
storage_client = storage.Client()
bucket_name = os.getenv("BUCKET_NAME")
MODEL_DIR = os.environ['AIP_STORAGE_URI']
source_blob_name='Clean_1.csv'


# Function to download data from GCS and load into DataFrame
def download_blob_to_dataframe(bucket_name, source_blob_name):
    """Downloads a blob from the bucket and loads it into a Pandas DataFrame."""

   
    source_blob_name='Clean_1.csv'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    data_string = blob.download_as_text()
    df = pd.read_csv(StringIO(data_string))
    return df


'''
# Set environment variables for GCS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "regal-bonito-415801-017316284a67.json"
bucket_name = os.getenv('BUCKET_NAME', 'cleancsv')
source_blob_name = os.getenv('DATA_BLOB_NAME', 'Clean_1.csv')
model_destination_blob_name = os.getenv('MODEL_BLOB_NAME', 'ModelOutput/model.h5')
'''
# Download and preprocess the dataset



# MLflow setup
mlflow.set_experiment("Sentiment Analysis with LSTM")

# TensorBoard setup
log_dir = os.path.join("logs", "fit", datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
tensorboard_callback = tf.keras.callbacks.TensorBoard(log_dir=log_dir, histogram_freq=1)

# Load your dataset
#df = pd.read_csv("Clean_1.csv")

# Preprocess data
def preprocess_data(texts, tokenizer, max_len=200):
    tokenizer.fit_on_texts(texts)
    sequences = tokenizer.texts_to_sequences(texts)
    padded = pad_sequences(sequences, maxlen=max_len, padding='post')
    return padded

# Build the model
def build_model():
    model = Sequential([
        Embedding(input_dim=5000, output_dim=64),
        Bidirectional(LSTM(32)),
        Dense(1, activation='sigmoid')
    ])
    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
    return model

# Function to upload the model to GCP
def save_and_upload_model(model, local_model_path,gcs_model_path):



    joblib.dump(model, local_model_path)

    # Upload the model to GCS
    with fs.open(gcs_model_path, 'wb') as f:
        joblib.dump(model, f)
    
    


    '''
    """Uploads the model to Google Cloud Storage."""
    # Define the model file path with the .keras extension
    temp_model_file = 'model-' + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + '.keras'
    
    # Save the model to the temporary file
    model.save(temp_model_file)  # The extension will determine the format
    
    # Initialize Google Cloud Storage client
    storage_client = storage.Client()
    
    # Upload the model file to GCP
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(temp_model_file)
    print(f"Model uploaded to {destination_blob_name} in bucket {bucket_name}.")
    
    # Remove the temporary file
    os.remove(temp_model_file)
    '''

# Main execution block
if __name__ == '__main__':
    df=download_blob_to_dataframe(bucket_name,source_blob_name)
    df['label'] = (df['overall'] > 3).astype(int)  # Assuming 'overall' is the rating column
    tokenizer = Tokenizer(num_words=5000)
    X = preprocess_data(df['reviewText'].fillna('Missing'), tokenizer)
    y = df['label'].values

    # Split data
    X_train, X_test, y_train, y_test, asins_train, asins_test = train_test_split(
        X, y, df['asin'], test_size=0.2, random_state=42
    )
    

    # Save the model locally and upload to GCS
    edt = pytz.timezone('US/Eastern')
    current_time_edt = datetime.datetime.now(edt)
    version = current_time_edt.strftime('%Y%m%d_%H%M%S')
    local_model_path = "model.pkl"
    gcs_model_path = f"{MODEL_DIR}/model_{version}.pkl"
    print(gcs_model_path)
    
    # MLFlow tracking
    with mlflow.start_run():
        mlflow.tensorflow.autolog()

        # Build and train the model
        model = build_model()
        history = model.fit(
            X_train, y_train,
            epochs=5,
            validation_data=(X_test, y_test),
            callbacks=[tensorboard_callback],
            batch_size=32
        )
        
        # Evaluate the model
        loss, accuracy = model.evaluate(X_test, y_test)
        print(f'Test Loss: {loss:.4f}, Test Accuracy: {accuracy:.4f}')
        

        '''
        # Predict and log predictions
        predictions = model.predict(X_test)
        print("ASIN, Sentiment, Probability")
        for i, (asin, pred) in enumerate(zip(asins_test, predictions)):
            sentiment = "Positive" if pred[0] > 0.5 else "Negative"
            print(f"{asin}, {sentiment}, {pred[0]:.4f}")
    
        '''

    save_and_upload_model(model, local_model_path,gcs_model_path)