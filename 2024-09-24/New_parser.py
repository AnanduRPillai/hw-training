import requests
import jsonlines
import logging
from parsel import Selector
from pymongo import MongoClient
import time
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    if text:
        text = re.sub(r'\s+', ' ', text.strip())
        return re.sub(r'[^\x00-\x7F]+', '', text)
    return ''

def clean_price(price):
    if price:
        cleaned_price = re.sub(r'[^\d.,]', '', price)
        cleaned_price = cleaned_price.replace(',', '.') if ',' in cleaned_price else cleaned_price
        try:
            return f"{float(cleaned_price):.2f} €"
        except ValueError:
            return "0.0 €"
    return "0.0 €"

def clean_price_per_unit(price_per_unit):
    if price_per_unit:
        cleaned_price_per_unit = price_per_unit.strip().replace('(', '').replace(')', '')
        match = re.match(r'(\d+[.,]?\d*)\s*(€/[a-zA-Z]+)', cleaned_price_per_unit)
        if match:
            price_value = match.group(1).replace(',', '.')
            unit = match.group(2)
            return f"{price_value} {unit}"
    return ''

def validate_image_url(url):
    try:
        response = requests.head(url, timeout=5)
        return response.status_code == 200
    except Exception:
        return False

def parse_product(url):
    try:
        logging.info(f"Fetching URL: {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        selector = Selector(response.text)

        unique_id = url.split('-')[-1].rstrip('/')

        product_name = clean_text(selector.xpath('//h1[@class="heading pos-title h4"]/text()').get())
        brand = clean_text(selector.xpath('//span[@class="link-text link-text--post"]/span/text()').get())
        regular_price = clean_price(selector.xpath('//span[@class="p-former-price-value p-recommended-price-value"]/text()').get())
        selling_price = clean_price(selector.xpath('//span[@class="p-regular-price-value"]/text()').get()) or regular_price

        price_per_unit = clean_price_per_unit(selector.xpath('//div[@class="p-per-unit p-regular-price"]//text()').get())

        breadcrumbs = selector.xpath('//nav[@class="breadcrumbs"]//li//text()').getall()
        cleaned_breadcrumbs = [clean_text(breadcrumb) for breadcrumb in breadcrumbs if clean_text(breadcrumb)]
        cleaned_breadcrumbs = cleaned_breadcrumbs[:4]

        product_hierarchy = {f'product_hierarchy_level_{i + 1}': cleaned_breadcrumbs[i] for i in range(len(cleaned_breadcrumbs))}

        description = selector.xpath('//div[@class="pos-selling-points"]//li/text()').getall()
        cleaned_description = ' '.join([clean_text(desc) for desc in description])

        image_urls = selector.xpath("//div[@class='zoom-image g-image']//img/@data-src").getall()
        logging.debug(f"Raw image URLs: {image_urls}")

        
        valid_image_urls = set()
        for img_url in image_urls:
            full_url = img_url if img_url.startswith('http') else f"https:{img_url}"
            if validate_image_url(full_url):
                valid_image_urls.add(full_url)

        
        valid_image_urls = list(valid_image_urls)[:5]
        logging.debug(f"Valid Image URLs: {valid_image_urls}")

        product_data = {
            'unique_id': unique_id,
            'product_name': product_name,
            'brand': brand,
            **product_hierarchy,
            'regular_price': regular_price,
            'selling_price': selling_price,
            'price_per_unit': price_per_unit,
            'breadcrumb': ' > '.join(cleaned_breadcrumbs),
            'product_url': url,
            'product_description': cleaned_description,
            'image_urls': valid_image_urls  
        }

        logging.info(f"Successfully parsed data for URL: {url}")
        return product_data

    except requests.Timeout:
        logging.error(f"Request timed out for {url}")
        return None
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return None

def process_product_urls(output_jsonl_file, max_workers=10):
    start_time = time.time()
    total_urls = 0

    
    batch_size = 10
    batch_data = []

    urls = input_collection.find({}, {'url': 1})
    url_list = [obj.get('url') for obj in urls]

    with jsonlines.open(output_jsonl_file, mode='w') as writer:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(parse_product, url): url for url in url_list}
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                total_urls += 1
                try:
                    product_data = future.result()
                    if product_data:
                        batch_data.append(product_data)
                        writer.write(product_data)
                        logging.info(f"Data for {url} written to JSONL file")

                        
                        if len(batch_data) >= batch_size:
                            output_collection.insert_many(batch_data)
                            logging.info(f"{len(batch_data)} records written to MongoDB")
                            batch_data = []  

                except Exception as e:
                    logging.error(f"Error processing URL {url}: {e}")

                if total_urls % 10 == 0:
                    logging.info(f"Processed {total_urls} URLs so far...")

    
    if batch_data:
        output_collection.insert_many(batch_data)
        logging.info(f"{len(batch_data)} remaining records written to MongoDB")

    end_time = time.time()
    logging.info(f"Completed parsing {total_urls} product URLs in {end_time - start_time:.2f} seconds.")

output_jsonl_file = 'product_data_output.jsonl'
process_product_urls(output_jsonl_file)

logging.info("Completed parsing all product URLs.")
