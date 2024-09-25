import os
import re
import json
import requests
import logging
from pymongo import MongoClient

mongo_uri = 'mongodb://localhost:27017/'
database_name = 'cat_db'
collection_name = 'copied_product_data'
client = MongoClient(mongo_uri)
db = client[database_name]
collection = db[collection_name]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_price(price):
    if isinstance(price, (float, int)):
        return float(price)
    elif isinstance(price, str):
        cleaned_price = re.sub(r'[^\d.,]', '', price)
        cleaned_price = cleaned_price.replace(',', '.') if ',' in cleaned_price else cleaned_price
        try:
            return float(cleaned_price)
        except ValueError:
            return 0.0
    return 0.0

def update_documents_with_new_fields():
    documents = collection.find()
    output_data = []

    for document in documents:
        unique_id = document.get('unique_id', '')
        regular_price = clean_price(document.get('regular_price', ""))
        selling_price = clean_price(document.get('selling_price', ""))
        currency = "EUR" if regular_price or selling_price else ""

        image_urls = document.get('image_urls', [])
        updated_entry = {'currency': currency}

        for idx, img_url in enumerate(image_urls):
            if img_url.startswith('http'):
                image_extension = os.path.splitext(img_url.split('/')[-1])[-1]
                file_name = f"{unique_id}_{idx + 1}{image_extension}"
                updated_entry[f'file_name_{idx + 1}'] = file_name
                updated_entry[f'image_url_{idx + 1}'] = img_url

        logging.info(f"Updating document ID: {document['_id']} with data: {updated_entry}")
        collection.update_one({'_id': document['_id']}, {'$set': updated_entry})
        output_data.append({**document, **updated_entry})

    try:
        with open('updated_product_data.jsonl', 'w') as jsonl_file:
            for entry in output_data:
                jsonl_file.write(json.dumps(entry) + '\n')
        logging.info("JSONL file 'updated_product_data.jsonl' has been created successfully.")
    except Exception as e:
        logging.error(f"Error writing JSONL file: {e}")

update_documents_with_new_fields()
