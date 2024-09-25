import requests
from parsel import Selector
import json
import logging
import pymongo

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['cat_db']
collection = db['cat_products']
collection.drop()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


def scrape_page(url, file_name):
    response = requests.get(url)
    if response.status_code == 200:
        sel = Selector(response.text)
        product_urls = sel.xpath("//a[@class='pt-header' and contains(@href, '/p/')]/@href").extract()

        with open(file_name, 'a') as f:
            for product_url in product_urls:
                full_url = "https://www.fressnapf.de" + product_url
                if not collection.find_one({"url": full_url}):
                    collection.insert_one({"url": full_url})
                    f.write(json.dumps({"url": full_url}) + '\n')
                    logger.info(f"Inserted URL: {full_url}")
                else:
                    logger.info(f"Duplicate URL found and skipped: {full_url}")

        next_page = sel.xpath("//a[@id='pagination-nextPage']/@href").get()
        if next_page:
            next_page_url = "https://www.fressnapf.de" + next_page
            logger.info(f"Proceeding to the next page: {next_page_url}")
            scrape_page(next_page_url, file_name)
    else:
        logger.error(f"Failed to retrieve page: {url} with status code {response.status_code}")


def main():
    start_url = "https://www.fressnapf.de/c/katze/katzenfutter/"
    file_name = "cat_crawler.jsonl"
    logger.info(f"Starting crawl on {start_url}")
    scrape_page(start_url, file_name)


if __name__ == "__main__":
    main()

