from elasticsearch import Elasticsearch
from pymongo import MongoClient



# MongoDB configuration
mongo_client = MongoClient('localhost', 27017)
db = mongo_client['Sentiment_Analysis_Db']
collection = db['sentiement_coll']

# Elasticsearch configuration
es_host = "localhost"
es_port = 9200
es_scheme = "http"  # or "https" if you're using SSL/TLS
es_index_name = "sentiment_analysis"

# Connect to Elasticsearch
es = Elasticsearch([{'host': es_host, 'port': es_port, 'scheme': es_scheme}])

# Function to transfer data from MongoDB to Elasticsearch
def transfer_data():
    # Retrieve data from MongoDB
    mongo_data = collection.find()

    # Index data into Elasticsearch
    for document in mongo_data:
        # Remove the _id field
        document.pop('_id', None)

        # Index data into Elasticsearch
        es.index(index=es_index_name, body=document)

# Execute the data transfer
transfer_data()

print("DONE !!")

# Close connections
mongo_client.close()
