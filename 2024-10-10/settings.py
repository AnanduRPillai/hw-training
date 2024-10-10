import logging

# MongoDB settings
MONGO_URI = 'mongodb://localhost:27017/'  # MongoDB connection URI
DATABASE_NAME = 'agent_database'  # Database name where agent data is stored
COLLECTION_NAME = 'details'  # Main collection name

# JSONL output file path for agent details
JSONL_OUTPUT_FILE = 'agent_details.jsonl'

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Retry settings for web requests
RETRY_COUNT = 5  # Number of retries for a failed request
RETRY_DELAY = (5, 15)  # Delay range between retries in seconds (min, max)

# Thread pool settings
MAX_WORKERS = 10  # Maximum number of concurrent threads for parallel processing

# Timeout setting for HTTP requests
REQUEST_TIMEOUT = 10  # Timeout in seconds for HTTP requests

# Default values for some settings in the main code
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0"

# Input file containing agent URLs
INPUT_FILE = 'cleaned_agent_urls.json'
