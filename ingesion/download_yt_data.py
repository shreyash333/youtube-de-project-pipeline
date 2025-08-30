"""
YouTube Trending Data Fetcher
-----------------------------

This script fetches trending YouTube videos for multiple countries/regions 
using the YouTube Data API v3 and stores the raw API responses as JSON files.

Workflow:
1. Define a list of region codes (ISO 3166-1 alpha-2 country codes).
2. For each region:
   - Call the YouTube Data API `videos.list` endpoint with `chart=mostPopular`.
   - Request video details including `snippet`, `statistics`, and `contentDetails`.
   - Save the response as a JSON file inside `data/raw/YYYY_MM_DD/`.
3. The output files are named as `<REGION>_trending_<YYYY_MM_DD>.json`.

Usage Notes:
- Requires a valid YouTube Data API key, stored in `config.cfg` under `[API]`.
- Creates a new dated folder each day inside `data/raw/`.
- Useful as a data ingestion layer for building a YouTube trending analysis pipeline.

Author: Shreyash Singh
"""

import requests
import json
from datetime import datetime
import os
import configparser

# Load API key from config.cfg
config = configparser.ConfigParser()
config.read("config.cfg")

# Read API_KEY from [API] section
API_KEY = config.get("API", "API_KEY")   

# List of region codes (ISO 3166-1 alpha-2 country codes)
# Each code represents a country/region from which trending YouTube videos will be fetched
REGIONS = [
    'AR', 'AU', 'AT', 'AZ', 'BH', 'BD', 'BY', 'BE', 'BO', 'BA', 'BR', 'BG',
    'CA', 'CL', 'CO', 'CR', 'HR', 'CY', 'CZ', 'DK', 'DO', 'EC', 'EG', 'SV',
    'EE', 'FI', 'FR', 'GE', 'DE', 'GH', 'GR', 'GT', 'HN', 'HK', 'HU', 'IS',
    'IN', 'ID', 'IQ', 'IE', 'IL', 'IT', 'JM', 'JP', 'JO', 'KZ', 'KE', 'KW',
    'LV', 'LB', 'LT', 'LU', 'MK', 'MY', 'MX', 'ME', 'MA', 'NP', 'NL', 'NZ',
    'NI', 'NG', 'NO', 'OM', 'PK', 'PA', 'PY', 'PE', 'PH', 'PL', 'PT', 'PR',
    'QA', 'RO', 'RU', 'SA', 'RS', 'SG', 'SK', 'SI', 'ZA', 'KR', 'ES', 'LK',
    'SE', 'CH', 'TW', 'TZ', 'TH', 'TN', 'TR', 'UG', 'UA', 'AE', 'GB', 'US',
    'UY', 'VE', 'VN', 'YE', 'ZW'
]

def fetch_trending(region_code):
    """
    Fetch trending YouTube videos for a given region.
    
    Args:
        region_code (str): Country/region code (e.g., 'US', 'IN').
    
    Returns:
        dict: JSON response from YouTube Data API containing trending videos.
    """
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        'part': 'snippet,statistics,contentDetails',  # Include video details
        'chart': 'mostPopular',                      # Fetch trending/most popular videos
        'maxResults': 200,                           # Max number of videos per API call
        'regionCode': region_code,                   # Region to fetch data for
        'key': API_KEY                               # API key for authentication
    }
    response = requests.get(url, params=params)  # Send GET request to API
    return response.json()  # Return JSON response

def save_json(data, region):
    """
    Save API response data as a JSON file in a date-based folder.
    
    Args:
        data (dict): API response data to save.
        region (str): Region code used for naming the file.
    """
    today = datetime.today().strftime('%Y_%m_%d')  # Get today's date (YYYY_MM_DD format)
    
    # Create folder for today's date if it doesn't exist (e.g., data/raw/2025_08_21/)
    os.makedirs(f'data/raw/{today}', exist_ok=True)
    
    # Save response JSON into region-specific file
    with open(f'data/raw/{today}/{region}_trending_{today}.json', 'w') as f:
        json.dump(data, f, indent=2)  # Pretty-print with indentation

if __name__ == '__main__':
    # Main execution block
    # Loop through all regions and fetch trending videos
    for region in REGIONS:
        print(f"Fetching for region: {region}")  # Log current region
        data = fetch_trending(region)            # Fetch trending videos
        save_json(data, region)                  # Save results to JSON
