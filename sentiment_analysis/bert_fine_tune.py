import mlflow
from mlflow import log_metric, log_param, log_artifacts

# Start an MLflow experiment
mlflow.set_experiment("BERT Fine-Tuning Experiment")

# Log a parameter (example)
log_param("learning_rate", 0.01)

from torch.utils.tensorboard import SummaryWriter

# Initialize the TensorBoard writer
writer = SummaryWriter()

# Example of logging a metric
# Inside your training loop:
# writer.add_scalar('Training Loss', loss, epoch)
# writer.add_scalar('Validation Loss', val_loss, epoch)

import pandas as pd

# Load the dataset to see its structure
file_path = 'Clean_1.csv'    #After preprocessing the final file is Clean_1.csv. give that here to run and save model locally
data = pd.read_csv(file_path)
data.head()

data.shape

#data = data.iloc[:1000]

data.shape

# Check for missing values in the reviewText column
missing_reviews = data['reviewText'].isnull().sum()

# Dropping rows where 'reviewText' is missing
data_clean = data.dropna(subset=['reviewText'])

# Display the number of missing values and the shape of the cleaned data
missing_reviews, data_clean.shape

import spacy
from spacy.matcher import Matcher
from collections import Counter

# Load the English tokenizer, tagger, parser, NER and word vectors
nlp = spacy.load("en_core_web_sm")

# Function to preprocess and extract aspects from text
def extract_aspects(text):
    # Preprocess the text
    doc = nlp(text.lower())

    # Pattern for matching noun phrases
    pattern = [{'POS': 'NOUN', 'OP': '+'}, {'POS': 'ADJ', 'OP': '*'}]

    # Matcher for finding matches
    matcher = Matcher(nlp.vocab)
    matcher.add("NOUN_PHRASE", [pattern])

    # Extract matches
    matches = matcher(doc)
    aspects = [doc[start:end].text for match_id, start, end in matches]

    return aspects

# Apply aspect extraction to the review texts
aspect_lists = data_clean['reviewText'].apply(extract_aspects)

# Count the most common aspects to see examples
aspect_counts = Counter([aspect for sublist in aspect_lists for aspect in sublist])
aspect_counts.most_common(10)

aspects = []

for i in aspect_counts.most_common(20):
    aspects.append(i[0])

aspects

aspect_lists

# Define common aspects

# Function to find and label aspects in reviews
def label_aspects(row):
    text = row['reviewText'].lower()
    labeled_aspects = {}
    for aspect in aspects:
        if aspect in text:
            # Assign sentiment based on the overall rating
            if row['overall'] > 3:
                sentiment = 'positive'
            elif row['overall'] == 3:
                sentiment = 'neutral'
            else:
                sentiment = 'negative'
            labeled_aspects[aspect] = sentiment
    return labeled_aspects

# Apply the function to each row in the dataset
data_clean['aspects'] = data_clean.apply(label_aspects, axis=1)
data_clean.head()

from transformers import BertTokenizer, BertForSequenceClassification
import torch

# Flatten the dataset
rows = []
for _, row in data_clean.iterrows():
    for aspect, sentiment in row['aspects'].items():
        rows.append({
            'text': row['reviewText'],
            'aspect': aspect,
            'sentiment': sentiment
        })

# Create a DataFrame for training
training_data = pd.DataFrame(rows)

# Check the distribution of aspect and sentiment
training_data.head(), training_data['sentiment'].value_counts()

pos = training_data[training_data['sentiment'] == 'positive']
neg = training_data[training_data['sentiment'] == 'negative']
neu = training_data[training_data['sentiment'] == 'neutral']

x = min(len(pos), len(neg), len(neu))

training_data = pd.concat([pos[:x], neg[:x], neu[:x]])
training_data.head()

training_data = training_data.sample(frac=1)

training_data

from transformers import BertTokenizer, BertForSequenceClassification, AdamW
from torch.utils.data import DataLoader, Dataset
import torch

class AspectDataset(Dataset):
    def __init__(self, texts, aspects, sentiments, tokenizer, max_len=128):
        self.texts = texts
        self.aspects = aspects
        self.sentiments = sentiments
        self.tokenizer = tokenizer
        self.max_len = max_len

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, item):
        text = str(self.texts[item]) + " [SEP] " + str(self.aspects[item])
        sentiment = int(self.sentiments[item])

        encoding = self.tokenizer.encode_plus(
            text,
            add_special_tokens=True,
            max_length=self.max_len,
            return_token_type_ids=False,
            padding='max_length',
            return_attention_mask=True,
            return_tensors='pt',
            truncation=True
        )

        return {
            'review_text': text,
            'input_ids': encoding['input_ids'].flatten(),
            'attention_mask': encoding['attention_mask'].flatten(),
            'labels': torch.tensor(sentiment, dtype=torch.long)
        }

# Initialize the tokenizer
tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')

# Prepare the dataset and data loader
dataset = AspectDataset(
    texts=training_data['text'].tolist(),
    aspects=training_data['aspect'].tolist(),
    sentiments=training_data['sentiment'].replace({'positive': 2, 'neutral': 1, 'negative': 0}).tolist(),
    tokenizer=tokenizer
)
data_loader = DataLoader(dataset, batch_size=64, shuffle=True)

# Load the model
model = BertForSequenceClassification.from_pretrained('bert-base-uncased', num_labels=3)
model.train()

# Training loop
optimizer = AdamW(model.parameters(), lr=2e-5)
total_batches = len(data_loader)
print(f"Starting training for {total_batches} batches per epoch.")
for epoch in range(4):  # For each epoch
    for i, batch in enumerate(data_loader):
        optimizer.zero_grad()
        input_ids = batch['input_ids']
        attention_mask = batch['attention_mask']
        labels = batch['labels']
        outputs = model(input_ids, attention_mask=attention_mask, labels=labels)
        loss = outputs.loss
        loss.backward()
        optimizer.step()
        if i % 5 == 0:  # Print loss every 10 batches
            print(f"Epoch {epoch}, Batch {i}/{total_batches}, Loss: {loss.item()}")
    print(f"Completed Epoch {epoch}")

import datetime

# Get the current timestamp
now = datetime.datetime.now()
timestamp = now.strftime("%Y%m%d_%H%M%S")

# Combine the timestamp with the model name
model_name = f"./bert_finetuned_aspect_sentiment_{timestamp}"
tokenizer_name = f"./bert_finetuned_aspect_sentiment_{timestamp}"
# Save the model
#model.save_pretrained(model_name)
#tokenizer.save_pretrained(tokenizer_name)

model.save_pretrained(model_name)  # Save the model
tokenizer.save_pretrained(tokenizer_name)  # Save the tokenizer


#torch.save(model, 'model.safetensors')
#torch.save(tokenizer, 'tokenizer.safetensors')

import os
print(os.listdir(model_name))
print(os.listdir(tokenizer_name))

from transformers import BertTokenizer, BertForSequenceClassification

model_path = model_name
tokenizer = BertTokenizer.from_pretrained(model_path)
model = BertForSequenceClassification.from_pretrained(model_path)

import os

model_path = model_name

if os.path.exists(model_path):
    print("Files in model directory:", os.listdir(model_path))
else:
    print("Model directory not found. Please check the path.")

from transformers import BertTokenizer, BertForSequenceClassification
import torch

# Function to perform prediction
def predict_aspect_sentiment(model, tokenizer, text, aspect, max_len=128):
    # Prepare the text input
    encoded_review = tokenizer.encode_plus(
        text + " [SEP] " + aspect,
        max_length=max_len,
        add_special_tokens=True,
        return_token_type_ids=False,
        padding='max_length',
        return_attention_mask=True,
        return_tensors='pt',
        truncation=True
    )

    input_ids = encoded_review['input_ids']
    attention_mask = encoded_review['attention_mask']

    # Move tensors to the same device as the model
    input_ids = input_ids.to(model.device)
    attention_mask = attention_mask.to(model.device)

    # Get model predictions
    with torch.no_grad():
        outputs = model(input_ids, attention_mask=attention_mask)

    # Convert logits to probabilities
    probs = torch.nn.functional.softmax(outputs.logits, dim=-1)

    # Convert probabilities to sentiment labels
    sentiment_labels = ['negative', 'neutral', 'positive']
    prediction = sentiment_labels[probs.argmax().item()]

    return prediction, probs[0].tolist()

# Load model and tokenizer
model_path = model_name
tokenizer = BertTokenizer.from_pretrained(model_path)
model = BertForSequenceClassification.from_pretrained(model_path)

# Example text and aspect
text = "The customer service is bad."
aspect = "customer service"

# Predict the sentiment
sentiment, probabilities = predict_aspect_sentiment(model, tokenizer, text, aspect)
print(f"Sentiment: {sentiment}, Probabilities: {probabilities}")

