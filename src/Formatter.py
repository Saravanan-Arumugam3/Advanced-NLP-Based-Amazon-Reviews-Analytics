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

file_path = os.path.join(current_directory, 'Clean_1.csv')

def replace_text(dataframe, column, replace, substitute):
  dataframe[column] = dataframe['reviewTime'].str.replace(replace, substitute)
  return dataframe

def delete_columns(dataframe, columns):
  dataframe = dataframe.drop(columns, axis=1)
  dataframe = dataframe.drop_duplicates()
  dataframe = dataframe.dropna()
  return dataframe

nltk.download('stopwords')
def remove_punctuation(text):
    return text.translate(str.maketrans('', '', string.punctuation))

stopwords_set = set(stopwords.words('english'))
def remove_stopwords(text):
    return ' '.join(word for word in text.split() if word.lower() not in stopwords_set)

df = pd.read_csv(file_path)

df = replace_text(df, 'reviewTime', ',', '')
df = replace_text(df, 'reviewTime', ' ', '-')
df['reviewTime'] = pd.to_datetime(df['reviewTime'])

df['reviews'] = df['reviewText']+ " " + df['summary']
df = delete_columns(df, ['reviewText', 'summary'])

df['sentiment'] = np.where(df['overall'] > 3, 'Positive', np.where(df['overall'] < 3, 'Negative', 'Neutral'))

nltk.download('stopwords')
stopwords_set = stopwords.words('english')

df['reviews'] = df['reviews'].str.replace('[^\w\s]','')
pat = r'\b(?:{})\b'.format('|'.join(stopwords_set))
df['reviews']  = df['reviews'] .str.replace(pat, '')
df['reviews'] = df['reviews'] .str.replace(r'\s+', ' ')

tfidf_vectorizer = TfidfVectorizer()
tfidf_matrix = tfidf_vectorizer.fit_transform(df['reviews'])
tfidf_df = pd.DataFrame(tfidf_matrix.toarray(), columns=tfidf_vectorizer.get_feature_names_out())

print(tfidf_df)

tfidf_df.to_csv("tfidf.csv")

os.remove("Clean_1.csv")
