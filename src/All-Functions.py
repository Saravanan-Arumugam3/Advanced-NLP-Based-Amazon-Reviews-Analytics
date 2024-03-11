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

current_directory = os.getcwd()


def read_csv(folder_path):
  csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
  dfs = []
  for csv_file in csv_files:
    df = pd.read_csv(csv_file)
    dfs.append(df)
  df_merged = pd.concat(dfs, ignore_index=True)
  return df_merged

def information(dataframe):
  print(dataframe.info())
  print(" \n")
  print("Null values")
  print(dataframe.isna().sum())

def delete_columns(dataframe, columns):
  dataframe = dataframe.drop(columns, axis=1)
  dataframe = dataframe.drop_duplicates()
  dataframe = dataframe.dropna()
  return dataframe

def replace_text(dataframe, column, replace, substitute):
  dataframe[column] = dataframe['reviewTime'].str.replace(replace, substitute)
  return dataframe

nltk.download('stopwords')
def remove_punctuation(text):
    return text.translate(str.maketrans('', '', string.punctuation))

stopwords_set = set(stopwords.words('english'))
def remove_stopwords(text):
    return ' '.join(word for word in text.split() if word.lower() not in stopwords_set)



df_merged = read_csv(current_directory)

df_merged['reviewText']=df_merged['reviewText'].fillna('Missing')

df_merged = replace_text(df_merged, 'reviewTime', ',', '')
df_merged = replace_text(df_merged, 'reviewTime', ' ', '-')
df_merged['reviewTime'] = pd.to_datetime(df_merged['reviewTime'])

df_merged['reviews'] = df_merged['reviewText']+ " " + df_merged['summary']
df_merged = delete_columns(df_merged, ['reviewText', 'summary'])
df_merged.head()

df_merged['sentiment'] = np.where(df_merged['overall'] > 3, 'Positive', np.where(df_merged['overall'] < 3, 'Negative', 'Neutral'))

nltk.download('stopwords')
stopwords_set = stopwords.words('english')

df_merged['reviews'] = df_merged['reviews'].str.replace('[^\w\s]','')
pat = r'\b(?:{})\b'.format('|'.join(stopwords_set))
df_merged['reviews']  = df_merged['reviews'] .str.replace(pat, '')
df_merged['reviews'] = df_merged['reviews'] .str.replace(r'\s+', ' ')

tfidf_vectorizer = TfidfVectorizer()
tfidf_matrix = tfidf_vectorizer.fit_transform(df_merged['reviews'])
tfidf_df = pd.DataFrame(tfidf_matrix.toarray(), columns=tfidf_vectorizer.get_feature_names_out())

print(tfidf_df)