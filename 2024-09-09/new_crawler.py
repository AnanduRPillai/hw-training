import requests
import json
from parsel import Selector
import logging
import time
from pymongo import MongoClient, errors

class BHHSAMBCrawler:
    def __init__(self):
        self.start_url = 'https://www.bhhsamb.com/CMS/CmsRoster/RosterSection?layoutID=963&pageSize=10&pageNumber=1&sortBy=random'
        self.agent_count = 0
        self.max_agents = 1120

        self.headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Host': 'www.bhhsamb.com',
            'Referer': 'https://www.bhhsamb.com/roster/Agents',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }

        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        self.mongo_client = self.connect_to_mongo()
        self.db = self.mongo_client['mining']
        self.collection = self.db['crawler_data']

    def connect_to_mongo(self, retries=5, delay=2):
        for i in range(retries):
            try:
                client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
                client.server_info()
                self.logger.info('Connected to MongoDB')
                return client
            except errors.ServerSelectionTimeoutError as e:
                self.logger.error(f'MongoDB connection attempt {i+1} failed: {e}')
                time.sleep(delay)
        self.logger.error('Could not connect to MongoDB after several attempts')
        raise ConnectionError('Failed to connect to MongoDB')

    def start_requests(self):
        self.logger.info('Starting requests to %s', self.start_url)
        try:
            response = self.get_with_retries(self.start_url)
            self.parse(response.text, 1)
        except requests.exceptions.RequestException as e:
            self.logger.error('Request failed: %s', e)

    def get_with_retries(self, url, retries=5, timeout=20, delay=5):
        for i in range(retries):
            try:
                self.logger.debug(f'Attempt {i+1}: Fetching {url}')
                response = requests.get(url, headers=self.headers, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                self.logger.error(f'Error on attempt {i+1}: {e}')
                time.sleep(delay)  # Increased delay between retries
        self.logger.error(f'Failed to fetch {url} after {retries} attempts')
        raise requests.exceptions.RequestException(f"Failed to fetch {url}")

    def parse(self, response_text, page, retries=3):
        self.logger.debug('Parsing page %d', page)
        sel = Selector(response_text)
        agent_links = sel.xpath('//a[@class="cms-int-roster-card-image-container site-roster-card-image-link"]//@href').extract()

        if not agent_links and retries > 0:
            self.logger.warning(f'No agent links found on page {page}. Retrying...')
            time.sleep(2)
            try:
                response = self.get_with_retries(f'https://www.bhhsamb.com/CMS/CmsRoster/RosterSection?layoutID=963&pageSize=10&pageNumber={page}&sortBy=random')
                return self.parse(response.text, page, retries - 1)
            except requests.exceptions.RequestException as e:
                self.logger.error(f'Retry failed for page {page}: {e}')
        elif not agent_links:
            self.logger.warning(f'No bio links to insert for page {page} after retries')

        bio_links = [f"https://www.bhhsamb.com{link}" for link in agent_links]

        if bio_links:
            with open('new_crawler.json', 'a') as file:
                if self.agent_count == 0:
                    file.write('[\n')
                for link in bio_links:
                    file.write(f'    "{link}",\n')
            self.logger.info('Data written to new_crawler.json')

            self.agent_count += len(bio_links)

            try:
                self.collection.insert_many([{'link': link} for link in bio_links])
            except errors.PyMongoError as e:
                self.logger.error(f'Failed to insert data into MongoDB: {e}')

        if self.agent_count >= self.max_agents:
            self.logger.info('Reached the target number of agents')
            with open('new_crawker.json', 'a') as file:
                file.write(']\n')
            return

        page += 1
        next_page = f'https://www.bhhsamb.com/CMS/CmsRoster/RosterSection?layoutID=963&pageSize=10&pageNumber={page}&sortBy=random'
        if self.agent_count < self.max_agents:
            self.logger.debug('Moving to page %d', page)
            time.sleep(2)
            try:
                response = self.get_with_retries(next_page)
                self.parse(response.text, page)
            except requests.exceptions.RequestException as e:
                self.logger.error('Request failed: %s', e)

if __name__ == "__main__":
    spider = BHHSAMBCrawler()
    spider.start_requests()
