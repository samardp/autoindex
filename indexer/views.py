import os
import asyncio
import aiohttp
import json
import requests
from tqdm import tqdm
from django.conf import settings  # Import settings
from django.http import JsonResponse
from django.shortcuts import render
from oauth2client.service_account import ServiceAccountCredentials

# Constants
ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"
URLS_PER_ACCOUNT = 200
SHEET_ID = '1PDEvylCllwVrTGo5s_864kIYeGhm37OP90qhXBF-7n0'  # Replace with your Google Sheet ID
RANGE_NAME = 'A2:A2001'  # Replace with your desired range
API_KEY = 'AIzaSyBcD2jEabo77Vgg-wEYNLUmQlgxVkNqEi8'  # Replace with your actual API key

async def send_url(session, http, url):
    content = {
        'url': url.strip(),
        'type': "URL_UPDATED"
    }
    for _ in range(3):  # Retry up to 3 times
        try:
            async with session.post(ENDPOINT, json=content, headers={"Authorization": f"Bearer {http}"}, ssl=False) as response:
                return await response.text()
        except aiohttp.ServerDisconnectedError:
            await asyncio.sleep(2)  # Wait for 2 seconds before retrying
            continue
    return '{"error": {"code": 500, "message": "Server Disconnected after multiple retries"}}'  # Return a custom error message after all retries fail

async def index_urls(http, urls):
    successful_urls = 0
    error_429_count = 0
    other_errors_count = 0
    tasks = []

    async with aiohttp.ClientSession() as session:
        # Using tqdm for progress bar
        for url in tqdm(urls, desc="Processing URLs", unit="url"):
            tasks.append(send_url(session, http, url))

        results = await asyncio.gather(*tasks)

        for result in results:
            data = json.loads(result)
            if "error" in data:
                if data["error"]["code"] == 429:
                    error_429_count += 1
                else:
                    other_errors_count += 1
            else:
                successful_urls += 1

    print(f"\nTotal URLs Tried: {len(urls)}")
    print(f"Successful URLs: {successful_urls}")
    print(f"URLs with Error 429: {error_429_count}")

def fetch_google_sheet_data():
    """Fetches URLs from a specified public Google Sheet."""
    url = f'https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/{RANGE_NAME}?alt=json&key={API_KEY}'

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        values = data.get('values', [])
        return [row[0] for row in values if row]
    else:
        print(f"Error fetching data: {response.status_code} - {response.text}")
        return []

def setup_http_client(json_key_file):
    scopes = ['https://www.googleapis.com/auth/indexing']
    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(json_key_file, scopes)
        token = credentials.get_access_token().access_token
        print(f"Access Token: {token}")
        return token
    except Exception as e:
        print(f"Error setting up HTTP client: {e}")
        raise e

async def start_indexing_task(request):
    all_urls = fetch_google_sheet_data()
    num_accounts = 14  # Assuming you have 15 accounts
    all_results = []
    accounts_data = []

    total_urls_tried = 0
    successful_urls = 0
    urls_with_error_429 = 0

    for i in range(num_accounts):
        print(f"\nProcessing URLs for Account {i+1}...")
        json_key_file = os.path.join(settings.JSON_FOLDER, f'account{i+1}.json')

        # Check if account JSON file exists
        if not os.path.exists(json_key_file):
            print(f"Error: {json_key_file} not found!")
            accounts_data.append({
                'account': i+1,
                'total_urls_tried': 0,
                'successful_urls': 0,
                'urls_with_error_429': 0,
                'results': [{'url': 'N/A', 'status': 'Error', 'response': f'{json_key_file} not found!'}]
            })
            continue

        start_index = i * URLS_PER_ACCOUNT
        end_index = start_index + URLS_PER_ACCOUNT
        urls_for_account = all_urls[start_index:end_index]
        total_urls_tried += len(urls_for_account)

        http_token = setup_http_client(json_key_file)
        account_results = await index_urls(http_token, urls_for_account)
        
        if not account_results:
            account_results = [{'url': 'N/A', 'status': 'Error', 'response': 'No results found'}]

        account_successful_urls = sum(1 for result in account_results if result['status'] == 'success')
        account_urls_with_error_429 = sum(1 for result in account_results if 'error 429' in result['status'])

        successful_urls += account_successful_urls
        urls_with_error_429 += account_urls_with_error_429

        accounts_data.append({
            'account': i+1,
            'total_urls_tried': len(urls_for_account),
            'successful_urls': account_successful_urls,
            'urls_with_error_429': account_urls_with_error_429,
            'results': account_results
        })

    response_data = {
        'total_urls_tried': total_urls_tried,
        'successful_urls': successful_urls,
        'urls_with_error_429': urls_with_error_429,
        'accounts_data': accounts_data
    }

    return JsonResponse(response_data)


    return render(request, 'index.html', context)

def index_view(request):
    return render(request, 'index.html')
