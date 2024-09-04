import requests
import json
from parsel import Selector
import logging
from pymongo import MongoClient

class BHHSAMBParser:
    def __init__(self):
        # Logger setup
        self.logger = logging.getLogger(__name__)
        # MongoDB setup
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['mining']
        self.collection = self.db['parsed_agents']

    def parse_agent(self, response_text):
        sel = Selector(response_text)

        # Extract and clean fields
        name = sel.xpath('//p[@class="rng-agent-profile-contact-name"]/text()').get()
        name = name.strip() if name else None

        image_url = sel.xpath('//article[@class="rng-agent-profile-main"]//img//@src').get(default=None)

        phone_number = sel.xpath('//ul//li[@class="rng-agent-profile-contact-phone"]//a//text()').get()
        phone_number = phone_number.strip() if phone_number else None

        # Format address by removing extra spaces and using commas instead of \r\n
        address_parts = sel.xpath('//ul//li[@class="rng-agent-profile-contact-address"]//text()').getall()
        address = ', '.join(part.strip() for part in address_parts if part.strip()).replace('\r\n', '').replace('\n', '').replace('\r', '')
        address = address if address else None

        # Extract social media links
        social_links = {
            'facebook': sel.xpath('//li[@class="social-facebook"]//a/@href').get(default=None),
            'twitter': sel.xpath('//li[@class="social-twitter"]//a/@href').get(default=None),
            'linkedin': sel.xpath('//li[@class="social-linkedin"]//a/@href').get(default=None),
            'youtube': sel.xpath('//li[@class="social-youtube"]//a/@href').get(default=None),
            'pinterest': sel.xpath('//li[@class="social-pinterest"]//a/@href').get(default=None),
            'instagram': sel.xpath('//li[@class="social-instagram"]//a/@href').get(default=None),
        }

        # Extract description
        description = sel.xpath('//article[@class="rng-agent-profile-content"]//span/text()').get()
        description = description.strip() if description else None

        # Prepare the agent data dictionary
        agent_data = {
            'name': name,
            'image_url': image_url,
            'phone_number': phone_number,
            'address': address,
            'description': description,
            'social_links': {key: value if value else None for key, value in social_links.items()}
        }

        return agent_data

    def fetch_agent_data(self, bio_link):
        try:
            self.logger.info('Fetching data from %s', bio_link)
            response = requests.get(bio_link)
            response.raise_for_status()
            agent_data = self.parse_agent(response.text)
            self.save_to_file(agent_data)
            self.save_to_mongodb(agent_data)
        except requests.exceptions.RequestException as e:
            self.logger.error('Request failed for %s: %s', bio_link, e)

    def save_to_file(self, agent_data):
        # Save in JSON Lines format (JSONL), each agent data on a new line
        with open('agents_data.jsonl', 'a') as file:
            json.dump(agent_data, file)
            file.write('\n')  # New line for JSONL format

    def save_to_mongodb(self, agent_data):
        # Insert the parsed agent data into MongoDB
        self.collection.insert_one(agent_data)

    def load_bio_links_from_file(self, filename='crawler.json'):
        bio_links = []
        try:
            with open(filename, 'r') as file:
                bio_links = json.load(file)
                self.logger.info("Loaded %d bio links from %s", len(bio_links), filename)
        except Exception as e:
            self.logger.error("Failed to load bio links from %s: %s", filename, e)
        return bio_links

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    parser = BHHSAMBParser()

    # Load bio links from the 'crawler.json' file
    bio_links = parser.load_bio_links_from_file('crawler.json')

    for link in bio_links:
        parser.fetch_agent_data(link)
