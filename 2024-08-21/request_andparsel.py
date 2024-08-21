import requests
import json
from parsel import Selector

class BHHSAMBSpider:
    def __init__(self):
        self.start_url = 'https://www.bhhsamb.com/CMS/CmsRoster/RosterSection?layoutID=963&pageSize=10&pageNumber=1&sortBy=random'
        self.agent_count = 0
        self.max_agents = 1120

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

    def start_requests(self):
        response = requests.get(self.start_url, headers=self.headers)
        self.parse(response.text, 1)

    def parse(self, response_text, page):
        sel = Selector(response_text)
        agent_links = sel.xpath('//a[@class="site-roster-card-image-link"]//@href').extract()

        for link in agent_links:
            if self.agent_count < self.max_agents:
                agent_url = f"https://www.bhhsamb.com{link}"
                self.parse_agent(agent_url)
            else:
                break

        page += 1
        next_page = f'https://www.bhhsamb.com/CMS/CmsRoster/RosterSection?layoutID=963&pageSize=10&pageNumber={page}&sortBy=random'
        if self.agent_count < self.max_agents:
            response = requests.get(next_page, headers=self.headers)
            self.parse(response.text, page)

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

        with open('agents_data.json', 'a') as file:
            file.write(json.dumps(agent_data, separators=(',', ':')) + '\n')

        self.agent_count += 1
        if self.agent_count >= self.max_agents:
            print('Reached the target number of agents')

if __name__ == "__main__":
    spider = BHHSAMBSpider()
    spider.start_requests()

