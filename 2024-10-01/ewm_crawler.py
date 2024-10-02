import requests
from parsel import Selector
import json
import logging
from pymongo import MongoClient
from urllib.parse import urljoin
import validators

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

client = MongoClient('mongodb://localhost:27017/')
db = client['agents_db']
collection = db['ewm_agents']

headers = {
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ml;q=0.7',
    'referer': 'https://www.ewm.com/',
    'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
}

def clean_url(url, base_url):
    full_url = urljoin(base_url, url.strip())
    if validators.url(full_url):
        return full_url
    return None

def get_agent_urls():
    base_url = "https://www.ewm.com/agents.php"
    current_page_url = base_url
    agent_urls_by_name = {}

    while current_page_url:
        try:
            response = requests.get(current_page_url, headers=headers)
            response.raise_for_status()

            selector = Selector(response.text)

            agent_links = selector.xpath("//div[@class='listing-box-image']/a/@href").getall()
            cleaned_agent_links = [clean_url(link, base_url) for link in agent_links if link]

            valid_agent_links = [link for link in cleaned_agent_links if link]

            for link in valid_agent_links:
                agent_name = link.split('/')[-1]
                if agent_name not in agent_urls_by_name:
                    agent_urls_by_name[agent_name] = link

            next_page_link = selector.xpath("//a[@class='page-link' and @aria-label='Next']/@href").get()

            current_page_url = clean_url(next_page_link, base_url) if next_page_link else None

        except requests.exceptions.RequestException:
            break

    return list(agent_urls_by_name.values())

def save_to_jsonl(data, filename='ewm_agent_urls.jsonl'):
    with open(filename, 'w') as jsonl_file:
        for item in data:
            jsonl_file.write(json.dumps(item) + '\n')

def insert_into_mongodb(data):
    if data:
        for url in data:
            if not collection.find_one({"url": url}):
                collection.insert_one({"url": url})

if __name__ == "__main__":
    agent_urls = get_agent_urls()
    if agent_urls:
        save_to_jsonl(agent_urls)
        insert_into_mongodb(agent_urls)
