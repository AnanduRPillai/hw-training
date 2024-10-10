import requests
import json
import concurrent.futures

# Path to the file containing city URLs
city_urls_file = "city_urls.jsonl"
# Output file for storing all agent URLs
output_file = "agent_urls.jsonl"

# Define headers to be used in the requests
headers = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.bhgre.com/sitemap/agents/florida-real-estate-agents",
    "Sec-CH-UA": '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "X-Nextjs-Data": "1"
}

# Function to fetch agent data for a given city URL
def fetch_agents(city_data):
    city_url = city_data['city_url']
    city_slug = city_url.split('/')[-2]  # Extract the city name (e.g., "aiea")
    base_url = f"https://www.bhgre.com/_next/data/X0myMN1mB5iArq7OQ1x_n/city/nj/{city_slug}/agents.json"
    page = 1
    more_pages = True
    collected_urls = []

    while more_pages:
        url = f"{base_url}?page={page}"
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"Failed to fetch page {page} for {city_url}, status code: {response.status_code}")
            break

        data = response.json()
        agents = data.get('pageProps', {}).get('results', {}).get('agents', [])

        if not agents:
            more_pages = False
            print(f"No more agents found for {city_url}.")
        else:
            for agent in agents:
                agent_url = agent.get('canonicalUrl')
                if agent_url:
                    collected_urls.append(json.dumps({"url": agent_url}))

            page += 1

    return collected_urls

# Load city URLs from the 'city_urls.jsonl' file
with open(city_urls_file, 'r') as city_file:
    city_data_list = [json.loads(line) for line in city_file]

# Use ThreadPoolExecutor to parallelize requests
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    # Submit the fetch_agents function for each city
    futures = [executor.submit(fetch_agents, city_data) for city_data in city_data_list]

    with open(output_file, 'w') as out_file:
        for future in concurrent.futures.as_completed(futures):
            agent_urls = future.result()
            if agent_urls:
                out_file.write("\n".join(agent_urls) + "\n")
                print(f"Wrote {len(agent_urls)} agent URLs.")

print(f"\nAgent URLs have been saved to {output_file}")
