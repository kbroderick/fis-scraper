import os
from dotenv import load_dotenv

load_dotenv()

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/fis_data')

# FIS website configuration
FIS_BASE_URL = os.getenv('FIS_BASE_URL', 'https://www.fis-ski.com')
FIS_POINTS_LIST_URL = os.getenv('FIS_POINTS_LIST_URL', '/DB/alpine-skiing/fis-points-lists.html')
FIS_ATHLETE_URL = os.getenv('FIS_ATHLETE_URL', '/DB/alpine-skiing/biographies.html')

# Scraping configuration
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '30'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5')) 