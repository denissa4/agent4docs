import weaviate
import weaviate.classes as wvc

# TODO: Complete Weaviate functions 

def create_schema():
    """ Function to create or connect to Document schema """
    client = weaviate.connect_to_local()
    try:
        schema = client.collections.create(
                name="Document",
                vectorizer_config=wvc.config.Configure.Vectorizer.none()
            )
    except:
        try:
            schema = client.collections.get("Document")
        except:
            client.close()
            return False
    client.close()
    return schema


def insert_data(schema, data):
    try:
        client = weaviate.connect_to_local()
        
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
        
        schema.data.insert_many(compiled_data)

        client.close()
        return True
    except:
        client.close()
        return False
    

def remove_data(schema, data):
    # TODO # Add function to remove items from Weaviate + client-side 'remove' button
    return

def search(schema, query):
    return

def fetch_doc_names(schema):
    # TODO: function to fetch document names stored in Weaviate for client-side feedback
    return