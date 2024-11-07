import weaviate
import weaviate.classes as wvc
import logging

# TODO: Complete Weaviate methods

class WeaveiateConnector():
    def __init__(self):
        self.client = weaviate.connect_to_local()
        self.schema = None

    def create_schema(self):
        """ Function to create or connect to Document schema """
        try:
            self.schema = self.client.collections.create(
                    name="Document",
                    vectorizer_config=wvc.config.Configure.Vectorizer.none()
                )
        except:
            try:
                self.schema = self.client.collections.get("Document")
            except:
                self.client.close()
                return False
        return True


    def insert_data(self, data):
        try:
            name = data['name']

            compiled_data = list()
            for chunk, vector in zip(data['chunks'], data['embeddings']):
                compiled_data.append(wvc.data.DataObject(
                    properties={
                        "name": name,
                        "text": chunk,
                    },
                    vector=vector
                ))
            
            self.schema.data.insert_many(compiled_data)

            self.client.close()
            return True
        except:
            self.client.close()
            return False
        

    def remove_data(self, data):
        # TODO # Add function to remove items from Weaviate + client-side 'remove' button
        return

    def search(self, query):
        return

    def fetch_doc_names(self):
        document_names = set()
        for item in self.schema.iterator():
            document_names.add(item.properties['name'])
        return list(document_names)