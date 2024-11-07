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
                raise Exception("Failed to insert document data into Weaviate")

        # Return a JSON response
        return jsonify({"message": "Files uploaded successfully"}), 200
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({"message": f"Something went wrong... {e}"}), 500


def insert_doc_data(data):
    """ Function to connect to in-memory database (Weaviate) and insert document data. """
    try:
        logging.info(f"Number of Chunks: {len(data['chunks'])}")
        logging.info(f"Number of Embeddings: {len(data['embeddings'])}")
        # Create a connection to the Weaviate in-memory db
        db = storageHandler.WeaveiateConnector()
        # Vailidate connection
        if not db.create_schema():
            logging.error("Failed to create/connect to schema, please make sure Weaviate is running.")
            return False
        # Insert document data into Weaviate db
        db.insert_data(data)

        # Retrieve all uploaded document names for client-side feedback
        # doc_names = db.fetch_doc_names()
        # print(doc_names)
        return True
    except:
        return False


if __name__ == '__main__':
    if os.getenv('TalkToDocs') == "True":
        app.run(port=8001)
    else:
        logging.info("TalkToDocs is turned off.")