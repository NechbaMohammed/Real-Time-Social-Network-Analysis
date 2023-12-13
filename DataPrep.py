from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import nltk
from nltk.corpus import stopwords, wordnet
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer,PorterStemmer
import re, string

# Kafka Consumer
consumer = KafkaConsumer(
    'extract_data_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# for message in consumer:
#     print("Received message:", message.value)

# Download necessary NLTK data
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

# def preprocess_text(text):
#     # Normalization: convert to lower case and remove non-alphanumeric characters
#     text = text.lower()
#     text = re.sub(r'\W+', ' ', text)

#     # Tokenization
#     tokens = word_tokenize(text)

#     # Removing stopwords
#     tokens = [word for word in tokens if word not in stopwords.words('english')]

#     # Stemming
#     stemmer = PorterStemmer()
#     tokens = [stemmer.stem(word) for word in tokens]

#     # Lemmatization
#     lemmatizer = WordNetLemmatizer()
#     tokens = [lemmatizer.lemmatize(word) for word in tokens]

#     return tokens

######################
#convert to lowercase, strip and remove punctuations
def preprocess(text):
    text = text.lower() 
    text=text.strip()  
    text=re.compile('<.*?>').sub('', text) 
    text = re.compile('[%s]' % re.escape(string.punctuation)).sub(' ', text)  
    text = re.sub('\s+', ' ', text)  
    text = re.sub(r'\[[0-9]*\]',' ',text) 
    text = re.sub(r'[^\w\s]', '', str(text).lower().strip())
    text = re.sub(r'\d',' ',text) 
    text = re.sub(r'\s+',' ',text) 
    return text

# STOPWORD REMOVAL
def stopword(string):
    a= [i for i in string.split() if i not in stopwords.words('english')]
    return ' '.join(a)

#LEMMATIZATION
# Initialize the lemmatizer
wl = WordNetLemmatizer()

# This is a helper function to map NTLK position tags
def get_wordnet_pos(tag):
    if tag.startswith('J'):
        return wordnet.ADJ
    elif tag.startswith('V'):
        return wordnet.VERB
    elif tag.startswith('N'):
        return wordnet.NOUN
    elif tag.startswith('R'):
        return wordnet.ADV
    else:
        return wordnet.NOUN

# Tokenize the sentence
def lemmatizer(string):
    word_pos_tags = nltk.pos_tag(word_tokenize(string)) # Get position tags
    a=[wl.lemmatize(tag[0], get_wordnet_pos(tag[1])) for idx, tag in enumerate(word_pos_tags)] # Map the position tag and lemmatize the word/token
    return " ".join(a)

def finalpreprocess(string):
    if string is not None:
        return lemmatizer(stopword(preprocess(string)))
    else :
        return string



# Function to send data to Kafka
# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Change to your Kafka server
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializer for JSON data
)

def send_to_kafka(topic, data):
    producer.send(topic, value=data)
    producer.flush()

# Example usage
for message in consumer:
    text = message.value.get("text") 
    preprocessed_text = finalpreprocess(text)
    print("Preprocessed text:", preprocessed_text)

    # Sending the preprocessed text to Kafka
    send_to_kafka('data_preprocessed', {'original_text': text, 'processed_text': preprocessed_text})