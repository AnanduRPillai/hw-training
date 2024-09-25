from pymongo import MongoClient


mongo_uri = 'mongodb://localhost:27017/'
database_name = 'cat_db'
source_collection_name = 'parsed_product_data'
destination_collection_name = 'copied_product_data'


client = MongoClient(mongo_uri)
db = client[database_name]


source_collection = db[source_collection_name]
destination_collection = db[destination_collection_name]

def copy_data():
    """
    Copies all documents from the source collection to the destination collection.
    """
    try:

        documents = list(source_collection.find())

        if not documents:
            print("No documents found in the source collection.")
            return


        destination_collection.insert_many(documents)
        print(f"Successfully copied {len(documents)} documents from '{source_collection_name}' to '{destination_collection_name}'.")

    except Exception as e:
        print(f"Error occurred while copying data: {e}")


copy_data()
