import requests
import logging
from parsel import Selector
import json
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)

class AgentScraper:
    def __init__(self):
        self.headers = {
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ml;q=0.7',
            'referer': 'https://www.ewm.com/',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        }
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db_name = 'agents_db'
        self.collection_name = 'agents'
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]
        self.collection.delete_many({})

    def fetch_listing_page(self, url):
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return Selector(text=response.text)
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            return None

    def scrape_agents(self, url):
        selector = self.fetch_listing_page(url)
        if selector is None:
            return []

        try:
            agents = selector.xpath("//div[contains(@class,'listing-box')]")
            if agents:
                logging.info(f"Found {len(agents)} agents on page {url}")
                return [self.extract_agent_data(agent) for agent in agents]
            else:
                logging.warning(f"No agents found on page {url}")
                return []
        except Exception as e:
            logging.error(f"Error while scraping agents: {e}")
            return []

    def extract_agent_data(self, agent):
        try:
            name = agent.xpath(".//div[contains(@class,'listing-box-title')]/h2/a/text()").extract_first()
            title = agent.xpath(".//div[contains(@class,'listing-box-title')]/h3/text()").extract_first()

            if name:
                name = name.strip()
                name_parts = name.split()
                if len(name_parts) > 3:
                    first_name = name
                    middle_name = ''
                    last_name = ''
                else:
                    first_name = name_parts[0] if len(name_parts) > 0 else ''
                    middle_name = name_parts[1] if len(name_parts) > 2 else ''
                    last_name = name_parts[-1] if len(name_parts) > 1 else ''
            else:
                first_name = middle_name = last_name = ''

            title = title.strip() if title else ''

            image_url = agent.xpath(".//div[contains(@class,'listing-box-image')]/img/@src").extract_first()

            office_phone_numbers = [phone.strip() for phone in agent.xpath(".//div[contains(@class,'listing-box-content')]/p/a[3]/text()").extract() if phone.strip()]
            agent_phone_numbers = [phone.strip() for phone in agent.xpath(".//div[contains(@class,'listing-box-content')]/p/a[5]/text()").extract() if phone.strip()]

            profile_url = agent.xpath(".//div[contains(@class,'listing-box-image')]/a/@href").extract_first()

            email = agent.xpath(".//div[contains(@class,'listing-box-content')]//a[contains(@href, 'emailme')]/@href").extract_first()
            email = email.split(':')[-1] if email and '@' in email else ''

            social_links = agent.xpath(".//ul[@class='listing-box-social']/li/a/@href").extract()

            social = {
                'linkedin': '',
                'facebook': '',
                'twitter': '',
                'other_urls': [],
            }

            for link in social_links:
                if 'linkedin.com' in link:
                    social['linkedin'] = link
                elif 'facebook.com' in link:
                    social['facebook'] = link
                elif 'twitter.com' in link:
                    social['twitter'] = link
                else:
                    social['other_urls'].append(link)

            website = agent.xpath(".//p/a[contains(@href, 'ewm.com')]/@href").extract_first()
            website = website.strip() if website else ''

            office_name = agent.xpath(".//div[contains(@class,'listing-box-title')]/h6/text()").extract_first()
            office_name = office_name.strip() if office_name else ''

            languages = agent.xpath(".//div[contains(@class,'listing-box-content')]/p//a[contains(@href, '#')]/i[contains(@class, 'fa-comments-o')]/following-sibling::text()").extract()
            languages = [lang.replace('Speaks:', '').strip() for lang in languages]

            if languages:
                languages = [lang.strip() for lang in ', '.join(languages).split(',')]
            else:
                languages = []

            if not profile_url:
                logging.warning("No profile URL found, skipping agent.")
                return None

            agent_data = {
                'first_name': first_name,
                'middle_name': middle_name,
                'last_name': last_name,
                'title': title,
                'image_url': image_url,
                'office_phone_numbers': office_phone_numbers,
                'agent_phone_numbers': agent_phone_numbers,
                'profile_url': profile_url,
                'email': email,
                'social': social,
                'website': website,
                'office_name': office_name,
                'languages': languages,
                'country': 'United States'
            }

            self.collection.insert_one(agent_data)

            agent_data.pop('_id', None)

            with open('agents_data.jsonl', 'a') as jsonl_file:
                jsonl_file.write(json.dumps(agent_data) + '\n')

            return agent_data
        except Exception as e:
            logging.error(f"Error extracting agent data: {e}")
            return None

    def run(self):
        base_url = 'https://www.ewm.com/agents.php'
        for page_number in range(1, 15):  # Changed the range to 1, 15
            url = f"{base_url}?page={page_number}"
            self.scrape_agents(url)

if __name__ == "__main__":
    scraper = AgentScraper()
    scraper.run()
