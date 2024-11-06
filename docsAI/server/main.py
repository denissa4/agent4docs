from flask import Flask, request, jsonify
from flask_cors import CORS
import logging

import storageHandler
import embeddingHandler

# TODO: Integrate weaviate (storageHandler)

app = Flask(__name__)
CORS(app) # Enable CORS for sending info to the client-side

@app.route('/upload', methods=['POST'])
def handle_upload():
    if 'files[]' not in request.files:
        return 'Something went wrong. No file detected', 400
    
    try:
        files = request.files.getlist('files[]')  # Handle multiple files
        uploaded_files = []

        for file_storage in files:
            # Process document - creates an object with document name and text chunks
            processed_doc = embeddingHandler.process_document(file_storage)
            # Create embeddings for text chunks
            embeddings = embeddingHandler.create_document_embeddings(processed_doc['chunks'])
            # Add embeddings to processed_doc object
            processed_doc['embeddings'] = embeddings

        handle_storage(processed_doc)

        # Return a JSON response with the names of uploaded files
        return jsonify({"message": "Files uploaded successfully", "files": uploaded_files}), 200
    except Exception as e:
        return jsonify({"message": f"Something went wrong... {e}"}), 500


def handle_storage(data):
    db = storageHandler.WeaveiateConnector()
    if not db.create_schema():
        logging.error("Failed to create/connect to schema, please make sure Weaviate is running.")
        return
    
    res = db.insert_data(data)
    if res:
        print("Data inserted")
    else:
        print("error")


if __name__ == '__main__':
    app.run(port=8000)