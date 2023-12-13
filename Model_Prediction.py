from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
from transformers import pipeline
from pymongo import MongoClient



# Kafka Consumer
consumer = KafkaConsumer(
    'data_preprocessed',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


sent_pipeline = pipeline("sentiment-analysis")

# Function to perform sentiment analysis and store result in MongoDB
def analyze_sentiment_and_store(tweet_text):
    # Perform sentiment analysis
    result = sent_pipeline(tweet_text)

    # Connect to MongoDB
    # Set up MongoDB connection
    mongo_client = MongoClient('localhost', 27017)
    db = mongo_client['Sentiment_Analysis_Db']
    collection = db['sentiement_coll']

    # Create a document to store sentiment analysis result
    sentiment_data = {
        "tweet": tweet_text,
        "sentiment_label": result[0]['label'],
        "sentiment_score": result[0]['score']
    }

    #Insert the sentiment analysis result into MongoDB
    collection.insert_one(sentiment_data)
    print("Sentiment analysis sent To Mongo DB")

for message in consumer:
    text_content = message.value.get('processed_text', '')  # Assuming your text is in a 'text' field
    if text_content:
        analyze_sentiment_and_store(text_content)

print("DONE !!")