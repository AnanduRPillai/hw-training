import requests
from scrapy import Selector
from pymongo import MongoClient, errors
import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EWMAgentAddressParser:
    def __init__(self, db_name, collection_name):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        logging.info(f"Connected to MongoDB database: {db_name}, collection: {collection_name}")

        self.headers = {
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ml;q=0.7',
            'referer': 'https://www.ewm.com/',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        }

    def fetch_urls_from_db(self):
        """Fetch the website URLs from MongoDB."""
        return self.collection.find({"website": {"$ne": ""}}, {"_id": 1, "website": 1})

    def scrape_agent_address(self, website_url):
        """Extract address, city, state, zipcode, and description from the agent's webpage."""
        try:
            logging.info(f"Fetching page content from {website_url}")
            response = requests.get(website_url, headers=self.headers)
            response.raise_for_status()
            selector = Selector(text=response.text)

            address = selector.xpath("//div[@class='footer-top-left']//address/text()[1]").extract_first(default="").strip()

            # Extract city, state, and zipcode
            address_line_2 = selector.xpath("//div[@class='footer-top-left']//address/text()[2]").extract_first(default="").strip()
            city = state = zipcode = ""
            if address_line_2:
                parts = address_line_2.split(',')
                if len(parts) == 2:
                    city = parts[0].strip()
                    state_zip = parts[1].strip().split(' ')
                    if len(state_zip) >= 2:
                        state = state_zip[0].strip()
                        zipcode = state_zip[-1].strip()

            description = selector.xpath("//div[@class='listing-box-content']/p/text()").extract_first(default="").strip()

            # Constructing agent data dictionary without '_id'
            agent_data = {
                'address': address,
                'city': city,
                'state': state,
                'zipcode': zipcode,
                'description': description,
                'website_url': website_url
            }

            return agent_data

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for {website_url}: {e}")
            return None

    def run(self):
        """Run the address parsing process."""
        agent_urls = self.fetch_urls_from_db()
        urls = [agent['website'] for agent in agent_urls]

        with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers for desired parallelism
            future_to_url = {executor.submit(self.scrape_agent_address, url): url for url in urls}

            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                    if data:
                        logging.info(f"Parsed data for {url}: {data}")
                        # Save the data into MongoDB or any other desired output
                    else:
                        logging.warning(f"No data returned for {url}")
                except Exception as e:
                    logging.error(f"Error while parsing {url}: {e}")

if __name__ == "__main__":
    parser = EWMAgentAddressParser(db_name='agents_db', collection_name='agents')
    parser.run()
