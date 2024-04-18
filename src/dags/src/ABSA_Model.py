import pandas as pd
import numpy as np
import os
import glob
import json
import string
from nltk.corpus import stopwords
import nltk
import re
import string
from nltk.stem.porter import PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.stem import WordNetLemmatizer
nltk.download('omw-1.4')
nltk.download('punkt')
from nltk.tokenize import word_tokenize
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import accuracy_score, classification_report
! pip install pyabsa
from pyabsa import available_checkpoints
from pyabsa import ATEPCCheckpointManager
from pyabsa import AspectTermExtraction as ATEPC
from google.cloud import storage
import logging


def ABSA_Model():
    bucket_name = 'all_beauty_5'
    blob_name = 'Preprocessed.csv'

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
        df_merged = pd.read_csv(downloaded_blob_path, error_bad_lines=False, warn_bad_lines=True)

        checkpoint_map = available_checkpoints()
        aspect_extractor = ATEPCCheckpointManager.get_aspect_extractor(checkpoint='english', auto_device=True)
        aspects = []
        sentiments = []
        aspects_sentiments = {}
        user_input = input("Enter an asin to get aspect based results")
        model_data = list(df_merged[df_merged['asin']==user_input]['reviews'][:10])
        model_data = [[i] for i in model_data]
        for j in model_data:
            atepc_result = aspect_extractor.extract_aspect(inference_source=j, pred_sentiment=True)
            for aspect in atepc_result[0]['aspect']:
                aspects.append(aspect)
            for sentiment in atepc_result[0]['sentiment']:
                sentiments.append(sentiment)

        for key, value in zip(aspects, sentiments):
            # Check if the key already exists in the dictionary
            if key in aspects_sentiments:
                # If key exists, append the value to the list of values
                aspects_sentiments[key].append(value)
            else:
                # If key doesn't exist, create a new key-value pair
                aspects_sentiments[key] = [value]
        # Initialize dictionaries to store counts and percentages
        positive_counts = {}
        negative_counts = {}
        percentage_dict = {}

        # Iterate through the result_dict
        for key, values in aspects_sentiments.items():
            # Count positive and negative occurrences
            positive_count = values.count('Positive')
            negative_count = values.count('Negative')
            
            # Calculate total count for percentage calculation
            total_count = positive_count + negative_count
            
            # Calculate percentages
            positive_percentage = (positive_count / total_count) * 100 if total_count != 0 else 0
            negative_percentage = (negative_count / total_count) * 100 if total_count != 0 else 0
            
            # Store counts and percentages in respective dictionaries
            positive_counts[key] = positive_count
            negative_counts[key] = negative_count
            
            # Store percentages in percentage_dict
            percentage_dict[key] = {'Positive': positive_percentage, 'Negative': negative_percentage}

        df_result = pd.DataFrame(percentage_dict).T.reset_index()
        df_result.columns = ['Aspect', 'Positive', 'Negative']

        excel_file = 'output.xlsx'
        with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
            df_result.to_excel(writer, sheet_name= user_input, index=False, header=['Aspect', 'Positive (%)', 'Negative (%)'])

    except Exception as e:
        logging.error(f"Failed to process data: {e}")