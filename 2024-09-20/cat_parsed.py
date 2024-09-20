import requests
import jsonlines
import logging
from parsel import Selector
from pymongo import MongoClient
import time
import re
import sys

mongo_uri = 'mongodb://localhost:27017/'
database_name = 'cat_db'
input_collection_name = 'cat_products'
output_collection_name = 'parsed_product_data'

client = MongoClient(mongo_uri)
db = client[database_name]
input_collection = db[input_collection_name]
output_collection = db[output_collection_name]

logging.basicConfig(filename='product_scraper.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.DEBUG)
logging.getLogger().addHandler(console)

def clean_text(text):
    return re.sub(r'\s+', ' ', text.strip()) if text else ''

def clean_price(price):
    if price:
        cleaned_price = re.sub(r'[^\d.,]', '', price)
        cleaned_price = cleaned_price.replace(',', '.') if ',' in cleaned_price else cleaned_price
        try:
            return float(cleaned_price)
        except ValueError:
            return 0.0
    return 0.0

def parse_product(url):
    try:
        logging.info(f"Fetching URL: {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        selector = Selector(response.text)

        product_name = clean_text(selector.xpath('//h1[@class="heading pos-title h4"]/text()').get())
        brand = clean_text(selector.xpath('//span[@class="link-text link-text--post"]/span/text()').get())
        regular_price = clean_price(selector.xpath('//span[@class="p-former-price-value p-recommended-price-value"]/text()').get())
        selling_price = clean_price(selector.xpath('//span[@class="p-regular-price-value"]/text()').get())
        price_per_unit = clean_price(selector.xpath('//div[@class="p-per-unit p-regular-price p-with-savings p-with-recommended"]/text()').get())
        breadcrumbs = selector.xpath('//nav[@class="breadcrumbs"]//li[@class="b-item"]//span[@class="link-text"]//span/text()').getall()
        cleaned_breadcrumbs = ' > '.join([clean_text(breadcrumb) for breadcrumb in breadcrumbs])
        description = selector.xpath('//div[@class="pos-selling-points"]//li/text()').getall()
        cleaned_description = ' '.join([clean_text(desc) for desc in description])

        product_data = {
            'url': url,
            'product_name': product_name,
            'brand': brand,
            'regular_price': regular_price,
            'selling_price': selling_price,
            'price_per_unit': price_per_unit,
            'breadcrumbs': cleaned_breadcrumbs,
            'description': cleaned_description
        }

        logging.info(f"Successfully parsed data for URL: {url}")
        return product_data

    except requests.Timeout:
        logging.error(f"Request timed out for {url}")
        return None
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return None

def process_product_urls(output_jsonl_file):
    start_time = time.time()
    total_urls = 0

    with jsonlines.open(output_jsonl_file, mode='w') as writer:
        urls = input_collection.find({}, {'url': 1})
        for obj in urls:
            total_urls += 1
            url = obj.get('url')
            if url:
                product_data = parse_product(url)
                if product_data:
                    output_collection.update_one({'url': url}, {'$set': product_data}, upsert=True)
                    logging.info(f"Data for {url} stored in MongoDB")
                    writer.write(product_data)
                    logging.info(f"Data for {url} written to JSONL file")

            if total_urls % 10 == 0:
                logging.info(f"Processed {total_urls} URLs so far...")

    end_time = time.time()
    logging.info(f"Completed parsing {total_urls} product URLs in {end_time - start_time:.2f} seconds.")

output_jsonl_file = 'cleaned_product_data.jsonl'

process_product_urls(output_jsonl_file)

logging.info("Completed parsing all product URLs.")
