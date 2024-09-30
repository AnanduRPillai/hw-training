import requests
from parsel import Selector
import logging
from pymongo import MongoClient
import json
import re

class AgentParser:
    def __init__(self):
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.db = self.mongo_client['iowa_realty_db']
        self.collection = self.db['agents']
        self.parsed_collection = self.db['parsed_agents']
        self.session = requests.Session()
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def parse_agent_data(self, agent_url):
        response = self.session.get(agent_url)
        sel = Selector(response.text)
        try:
            name = sel.xpath('//section[@class="rng-bio-account-content-office"]/h1/text()').get(default="").strip()
            title = sel.xpath('//section[@class="rng-bio-account-content-office"]/div/span/text()').get(default="").strip()
            address_raw = sel.xpath('//section[@class="rng-bio-account-content-office"]/div[2]/text()[normalize-space()]').get(default="").strip()
            city_state_zip = address_raw.split(' ')
            city = self.extract_city(address_raw)
            state = self.extract_state(city_state_zip)
            zipcode = self.extract_zipcode(city_state_zip)
            country = "United States"
            email = self.extract_email(agent_url)
            description = sel.xpath('//section[contains(@class, "rng-bio-account-content-description")]//div[@id="bioAccountContentDesc"]//p/text()').get(default="").strip()
            languages = sel.xpath('//section[@class="rng-bio-account-languages"]/div[2]/text()').get(default="").strip()
            office_name = sel.xpath('//section[@class="rng-bio-account-content-office"]/div[1]/strong/text()').get(default="").strip()
            image_url = sel.xpath('//div[@class="site-account-image"]/@style').get(default="").strip().split('url(')[-1].split(')')[0].strip().replace("'", "")
            social_links = self.extract_socials(sel)
            agent_phone_numbers = self.extract_phone_number(sel)
            website = sel.xpath('//li/a[contains(text(), "Visit my site")]/@href').get(default="").strip()
            address = ', '.join(filter(None, [address_raw.strip(), city, state, zipcode, country]))
            office_name = office_name.replace('  ', ', ').strip()

            parsed_data = {
                "first_name": name.split()[0] if len(name.split()) > 0 else "",
                "middle_name": name.split()[1] if len(name.split()) > 2 else "",
                "last_name": name.split()[-1] if len(name.split()) > 0 else "",
                "title": title,
                "address": address,
                "city": city,
                "state": state,
                "zipcode": str(zipcode),
                "country": country,
                "description": description,
                "email": email,
                "agent_phone_numbers": agent_phone_numbers,
                "languages": languages,
                "office_name": office_name,
                "profile_url": agent_url,
                "social": social_links,
                "image_url": image_url,
                "website": website
            }

            self.save_to_db(parsed_data)

        except Exception as e:
            logging.error(f"Error parsing agent data from {agent_url}: {e}")

    def extract_city(self, address):
        match = re.search(r'\s([A-Z][a-z]+)\s', address)
        return match.group(1) if match else ""

    def extract_state(self, address_parts):
        return address_parts[-2] if len(address_parts) >= 2 else ""

    def extract_zipcode(self, address_parts):
        return address_parts[-1] if len(address_parts) > 0 else ""

    def extract_email(self, agent_url):
        email_response = self.session.get(agent_url)
        email_sel = Selector(email_response.text)
        email_link = email_sel.xpath('//a[contains(@href, "Contact/") and contains(text(), "Email")]/@href').get()
        return email_link.split('/')[-1] if email_link else ""

    def extract_socials(self, sel):
        social_links = {}
        social_media = {
            "facebook": sel.xpath('//ul[@class="rng-agent-bio-content-contact-social"]/li/a[contains(@href, "facebook")]/@href').get(),
            "twitter": sel.xpath('//ul[@class="rng-agent-bio-content-contact-social"]/li/a[contains(@href, "twitter")]/@href').get(),
            "linkedin": sel.xpath('//ul[@class="rng-agent-bio-content-contact-social"]/li/a[contains(@href, "linkedin")]/@href').get(),
            "instagram": sel.xpath('//ul[@class="rng-agent-bio-content-contact-social"]/li/a[contains(@href, "instagram")]/@href').get(),
            "pinterest": sel.xpath('//ul[@class="rng-agent-bio-content-contact-social"]/li/a[contains(@href, "pinterest")]/@href').get()
        }

        for key, value in social_media.items():
            if value:
                social_links[key] = value

        return social_links

    def extract_phone_number(self, sel):
        phone_number = sel.xpath('//section[@class="rng-bio-account-details"]/ul/li/a/@href').get(default="").strip()
        return [phone_number.replace("tel:", "").strip()] if phone_number else []

    def save_to_db(self, parsed_data):
        try:
            inserted_data = self.parsed_collection.insert_one(parsed_data)
            logging.info(f"Saved data to database: {parsed_data['first_name']} {parsed_data['last_name']}")
            parsed_data['_id'] = str(inserted_data.inserted_id)
            self.save_to_jsonl(parsed_data)
        except Exception as e:
            logging.error(f"Error saving to database: {e}")

    def save_to_jsonl(self, parsed_data):
        if '_id' in parsed_data:
            del parsed_data['_id']
        with open('parsed_agents.jsonl', 'a') as file:
            json.dump(parsed_data, file)
            file.write('\n')

if __name__ == "__main__":
    parser = AgentParser()
    agent_urls = parser.collection.find({}, {'link': 1})
    for agent in agent_urls:
        parser.parse_agent_data(agent['link'])
