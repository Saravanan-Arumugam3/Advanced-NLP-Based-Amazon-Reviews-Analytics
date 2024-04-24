import streamlit as st
from transformers import BertTokenizer, BertForSequenceClassification
import torch
# Assuming model and tokenizer are saved using the save_pretrained method
model_path = r'C:\Users\DELL\Desktop\streamlitapp'  # Directory where the model is saved
tokenizer_path = r'C:\Users\DELL\Desktop\streamlitapp'  # Directory where the tokenizer is saved

# Load the model and tokenizer
model = BertForSequenceClassification.from_pretrained(model_path)
tokenizer = BertTokenizer.from_pretrained(tokenizer_path)

def predict_aspect_sentiment(model, tokenizer, text, aspect, max_len=128):
    encoded_review = tokenizer.encode_plus(
        text + " [SEP] " + aspect,
        max_length=max_len,
        add_special_tokens=True,
        padding='max_length',
        return_attention_mask=True,
        return_tensors='pt',
        truncation=True
    )
    input_ids = encoded_review['input_ids'].to(model.device)
    attention_mask = encoded_review['attention_mask'].to(model.device)
    
    with torch.no_grad():
        outputs = model(input_ids, attention_mask=attention_mask)
    
    probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
    sentiment_labels = ['negative', 'neutral', 'positive']
    return sentiment_labels[probs.argmax().item()], probs[0].tolist()

# Streamlit interface
st.title('Aspect-based Sentiment Analysis')
text = st.text_input("Enter the text:")
aspect = st.text_input("Enter the aspect:")
if st.button('Analyze Sentiment'):
    sentiment, probabilities = predict_aspect_sentiment(model, tokenizer, text, aspect)
    st.write(f"Sentiment: {sentiment}, Probabilities: {probabilities}")
