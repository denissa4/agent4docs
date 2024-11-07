from tika import parser
from nomic import embed
import numpy as np
import logging
import os


def process_document(file_storage):
    """ Function to gather and process relevant data from document """
    # Parse the file, extract text and metadata
    logging.info(f"Parsing document...")
    try:
        data = parser.from_buffer(file_storage) # Using FileStorage object to process file from memory instead of storage
    finally:
        file_storage.close()

    name = file_storage.filename  # Document name
    text = data['content']  # Document text content

    # Separate text into list of words
    words = text.split()

    # Create text chunks
    text_chunks = []
    chunk = []
    counter = 0
    logging.info("Creating text chunks...")
    for i, word in enumerate(words):
        counter += 1
        chunk.append(word)
        if word[-1] in {'.', '!', '?'} and counter >= 200:
            text_chunks.append(' '.join(chunk))
            logging.info(f"Chunk #{len(text_chunks)}, chunk length: {counter}")
            # Add overlap by re-adding the last 20 words to the next chunk
            chunk = chunk[-20:] if len(chunk) > 20 else []
            counter = len(chunk)

    if chunk:
        text_chunks.append(' '.join(chunk))
        logging.info(f"Chunk #{len(text_chunks)}, chunk length: {len(chunk)}")

    processed_data = {
        "name": name,
        "chunks": text_chunks,
        "embeddings": []
    }

    logging.info(f"Created {len(text_chunks)} text chunks.")
    return processed_data


def create_document_embeddings(data):
    """ Function to create document embeddings. Returns: nested list of embeddings """
    embeddings = []
    logging.info("Creating embeddings for text chunks...")
    for i, chunk in enumerate(data):
        output = embed.text(
            texts=[chunk],
            inference_mode='local',
            model=os.getenv('EmbeddingModel', 'nomic-embed-text-v1.5'),
            task_type='search_document'
        )
        embedding = output['embeddings'][0]
        embeddings.append(embedding)
        logging.info(f"Created embedding for chunk {i+1}/{len(data)}")

    return embeddings


def create_query_embeddings(query):
    """ Function to create embedding for user's query """
    logging.info("Embedding query...")
    output = embed.text(
        texts=[query],
        inference_mode='local',
        model=os.getenv('EmbeddingModel', 'nomic-embed-text-v1.5'),
        task_type='search_query'
    )
    embeddings = np.array(output['embeddings'])
    return embeddings
