import requests
import json
from parsel import Selector

class BHHSAMBCrawler:
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

        bio_links = [f"https://www.bhhsamb.com{link}" for link in agent_links]

        with open('crawler.json', 'a') as file:
            json.dump(bio_links, file, indent=2)

        self.agent_count += len(bio_links)
        if self.agent_count >= self.max_agents:
            print('Reached the target number of agents')
            return

        page += 1
        next_page = f'https://www.bhhsamb.com/CMS/CmsRoster/RosterSection?layoutID=963&pageSize=10&pageNumber={page}&sortBy=random'
        if self.agent_count < self.max_agents:
            response = requests.get(next_page, headers=self.headers)
            self.parse(response.text, page)

if __name__ == "__main__":
    spider = BHHSAMBCrawler()
    spider.start_requests()
