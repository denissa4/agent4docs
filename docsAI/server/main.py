from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
import os

import storageHandler
import embeddingHandler



app = Flask(__name__)
CORS(app) # Enable CORS for sending info to the client-side

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    handlers=[logging.StreamHandler()])

# Create sample query to get embedding dimensionality
dim = len(embeddingHandler.create_query_embeddings("sample"))
# Create the vector in-memory storage object
db = storageHandler.FAISSConnector(dim)

@app.route('/upload', methods=['POST'])
def handle_upload():
    logging.info("Uploading file...")
    if 'files[]' not in request.files:
        return 'Something went wrong. No file detected', 400
    
    try:
        files = request.files.getlist('files[]')  # Handle multiple files

        for file_storage in files:
            # TODO: Check flow for multi-file uploads - this is liekly errorsome
            # Process document - creates an object with document name and text chunks
            processed_doc = embeddingHandler.process_document(file_storage)
            # Create embeddings for text chunks
            embeddings = embeddingHandler.create_document_embeddings(processed_doc['chunks'])
            # Add embeddings to processed_doc object
            processed_doc['embeddings'] = embeddings

            # Insert document data & embeddings to Weaviate
            if not insert_doc_data(processed_doc):
                raise Exception

        # Return a JSON response
        return jsonify({"message": "Files uploaded successfully"}), 200
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({"message": f"Something went wrong... {e}"}), 500
    
@app.route('/query', methods=['POST'])
def handle_query():
    # Get the JSON data from the incoming request
    data = request.get_json()

    # Extract the channel_id and text from the request data
    channel_id = data.get('channel_id')
    text = data.get('text')
    logging.info(f"Received message from {channel_id}: {text}")

    # Embed user's query
    query_vector = embeddingHandler.create_query_embeddings(text)
    # Get indices of closest matches
    _, indices = db.search(query_vector, 2)
    # Use indices to obtain relevant text chunks
    if indices.tolist() == [[-1, -1]]:
        return {"status": "No documents to search"}
    
    res = db.data_dict['chunks'][indices[0][0]]
    return {"status": "Message received", "message": res}

def insert_doc_data(data):
    """ Function to connect to vector storage and insert document data. """
    try:
        logging.info(f"Number of Chunks: {len(data['chunks'])}")
        logging.info(f"Number of Embeddings: {len(data['embeddings'])}")

        # Insert document data into FAISS
        db.insert_data(data)
        logging.info("Embeddings inserted successfully.")

        # Retrieve all uploaded document names for client-side feedback
        # TODO client-side feedback for doc names
        uploaded_doc_names = set(db.data_dict['names'])
        return True
    except Exception as e:
        raise e


if __name__ == '__main__':
    # if os.getenv('TalkToDocs') == "True":
        app.run(port=8000)
    # else:
    #     logging.info("TalkToDocs is turned off.")