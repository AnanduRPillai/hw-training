import requests
import json
from parsel import Selector

class BHHSAMBParser:
    def __init__(self):
        self.headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Host': 'www.bhhsamb.com',
            'Referer': 'https://www.bhhsamb.com/roster/Agents',
            'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }
        self.agent_data_list = []

    def parse_bio_links(self):
        with open('crawler.json', 'r') as file:
            bio_links = file.readlines()

        for link in bio_links:
            link = link.strip()
            self.parse_agent(link)

        self.save_to_file()

    def parse_agent(self, agent_url):
        response = requests.get(agent_url, headers=self.headers)
        sel = Selector(response.text)

        name = sel.xpath('//p[@class="rng-agent-profile-contact-name"]/text()').get(default='').strip()
        image_url = sel.xpath('//article[@class="rng-agent-profile-main"]//img//@src').get(default='')
        phone_number = sel.xpath('//ul//li[@class="rng-agent-profile-contact-phone"]//a//text()').get(default='').strip()

        address = ''.join(sel.xpath('//ul//li[@class="rng-agent-profile-contact-address"]//text()').getall()).strip()

        social_links = {
            'facebook': sel.xpath('//li[@class="social-facebook"]//a/@href').get(),
            'twitter': sel.xpath('//li[@class="social-twitter"]//a/@href').get(),
            'linkedin': sel.xpath('//li[@class="social-linkedin"]//a/@href').get(),
            'youtube': sel.xpath('//li[@class="social-youtube"]//a/@href').get(),
            'pinterest': sel.xpath('//li[@class="social-pinterest"]//a/@href').get(),
            'instagram': sel.xpath('//li[@class="social-instagram"]//a/@href').get(),
        }

        agent_data = {
            'name': name,
            'image_url': image_url,
            'phone_number': phone_number,
            'address': address,
            'description': sel.xpath('//article[@class="rng-agent-profile-content"]//span/text()').get(default='').strip(),
            'social_links': {key: value for key, value in social_links.items() if value}
        }

        self.agent_data_list.append(agent_data)

    def save_to_file(self):
        with open('parser.json', 'w') as file:
            json.dump(self.agent_data_list, file, indent=4)

if __name__ == "__main__":
    parser = BHHSAMBParser()
    parser.parse_bio_links()