import requests
from parsel import Selector
import json
import logging
import pymongo

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['fressnapf']
collection = db['products']

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def scrape_page(url, seen_urls):
    response = requests.get(url)
    if response.status_code == 200:
        sel = Selector(response.text)
        product_urls = sel.xpath("//a[@class='pt-header' and contains(@href, '/p/')]/@href").extract()

        with open(file_name, 'a') as f:
            for product_url in product_urls:
                full_url = "https://www.fressnapf.at" + product_url
                if full_url not in seen_urls:
                    seen_urls.add(full_url)
                    collection.insert_one({"url": full_url})
                    f.write(json.dumps({"url": full_url}) + '\n')
                    logger.info(f"Inserted URL: {full_url}")

        next_page = sel.xpath('//a[@id="pagination-nextPage"]/@href').get()
        if next_page:
            next_page_url = "https://www.fressnapf.at" + next_page
            logger.info(f"Proceeding to the next page: {next_page_url}")
            scrape_page(next_page_url, seen_urls)
    else:
        logger.error(f"Failed to retrieve page: {url} with status code {response.status_code}")

def main():
    start_url = "https://www.fressnapf.at/c/hund/welpe/welpenfutter/"
    global file_name
    file_name = "crawled_urls.jsonl"
    seen_urls = set()
    logger.info(f"Starting crawl on {start_url}")
    scrape_page(start_url, seen_urls)

if __name__ == "__main__":
    main()
