import faiss
import numpy as np
import logging

class FAISSConnector():
    def __init__(self, dimensionality):
        self.index = faiss.IndexFlatL2(dimensionality)
        self.next_index = 0

        self.data_dict = {"names": [],
                          "chunks": [],
                          "indices": []}

    def insert_data(self, data):
        logging.info("Inserting embeddings...")
        embeddings = np.array(data['embeddings']).astype('float32')
        self.index.add(embeddings)

        # Add indexes to data object
        for i in range(len(data['embeddings'])):
            self.data_dict['names'].append(data['name'])
            self.data_dict['chunks'].append(data['chunks'][i])
            self.data_dict['indices'].append(self.next_index)
            self.next_index += 1

    def search(self, query_embedding, n_results):
        logging.info(f"Performing vector search...")
        query_vector = np.array(query_embedding).reshape(1, -1).astype('float32')
        distances, indices = self.index.search(query_vector, n_results)
        return distances, indices

