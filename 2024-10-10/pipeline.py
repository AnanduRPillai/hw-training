import logging
from pymongo import MongoClient
from export import save_to_json_line_by_line

# Configure logging for the pipeline
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AgentPipeline:
    def __init__(self, mongo_uri='mongodb://localhost:27017/', database_name='agent_database', collection_name='details'):
        """
        Initializes the AgentPipeline with MongoDB details.

        Args:
            mongo_uri (str): URI for connecting to MongoDB.
            database_name (str): The name of the database to store data.
            collection_name (str): The name of the collection in the database.
        """
        self.mongo_uri = mongo_uri
        self.database_name = database_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None

    def open_spider(self):
        """Opens the connection to MongoDB when the spider starts."""
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]['agent']  # Using sub-collection 'agent'
            logging.info(f"Connected to MongoDB at {self.mongo_uri} - Database: {self.database_name}, Collection: {self.collection_name}.agent")
        except Exception as e:
            logging.error(f"Error connecting to MongoDB: {e}")

    def close_spider(self):
        """Closes the MongoDB connection when the spider finishes."""
        if self.client:
            self.client.close()
            logging.info("Closed MongoDB connection.")

    def process_item(self, item):
        """
        Processes and stores each item in MongoDB and writes it to a JSONL file.

        Args:
            item (dict): The data dictionary representing a single agent's details.
        """
        try:
            # Store the item in MongoDB
            if self.collection:
                self.collection.insert_one(item)
                logging.info(f"Stored item in MongoDB: {item['profile_url']}")

            # Write the item to a JSONL file using the function from export.py
            save_to_json_line_by_line(item)
        except Exception as e:
            logging.error(f"Error processing item: {e}")
        return item
