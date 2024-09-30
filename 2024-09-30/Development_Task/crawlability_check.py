import requests
from parsel import Selector
import logging
import time
from pymongo import MongoClient

class BHHSAMBCrawler:
    def __init__(self):
        self.start_url = 'https://www.iowarealty.com/CMS/CmsRoster/RosterSection?layoutID=1215&pageSize=10&pageNumber=1&sortBy=lastname-asc'
        self.agent_count = 0
        self.all_agent_links = []

        self.headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Host': 'www.iowarealty.com',
            'Referer': 'https://www.iowarealty.com/roster/Agents',
            'sec-ch-ua': '"Google Chrome";v="97", "Chromium";v="97", ";Not A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }

        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.db = self.mongo_client['iowa_realty_db']
        self.collection = self.db['agents']

    def start_requests(self):
        self.logger.info('Starting requests to %s', self.start_url)
        try:
            response = requests.get(self.start_url, headers=self.headers)
            response.raise_for_status()
            self.parse(response.text, 1)
        except requests.exceptions.RequestException as e:
            self.logger.error('Request failed: %s', e)

    def parse(self, response_text, page):
        self.logger.debug('Parsing page %d', page)
        sel = Selector(response_text)

        agent_links = sel.xpath('//a[@class="site-roster-card-image-link"]//@href').extract()

        if not agent_links:
            self.logger.debug('No agent links found on page %d', page)

        bio_links = [f"https://www.iowarealty.com{link}" for link in agent_links]
        self.all_agent_links.extend(bio_links)

        self.agent_count += len(bio_links)

        page += 1
        next_page = f'https://www.iowarealty.com/CMS/CmsRoster/RosterSection?layoutID=1215&pageSize=10&pageNumber={page}&sortBy=lastname-asc'
        self.logger.debug('Moving to page %d', page)

        time.sleep(1)
        try:
            response = requests.get(next_page, headers=self.headers)
            response.raise_for_status()
            self.parse(response.text, page)
        except requests.exceptions.RequestException as e:
            self.logger.error('Request failed: %s', e)

    def save_links(self):
        with open('crawler.jsonl', 'w') as file:
            for link in self.all_agent_links:
                file.write(f'{{"link": "{link}"}}\n')
                self.collection.insert_one({"link": link})
        self.logger.info('All agent URLs saved to crawler.jsonl and MongoDB')

if __name__ == "__main__":
    spider = BHHSAMBCrawler()
    spider.start_requests()
    spider.save_links()
