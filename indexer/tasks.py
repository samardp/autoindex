import os
import asyncio
import aiohttp
from tqdm import tqdm
import pandas as pd
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from django.conf import settings

# Google Sheets and Indexing API configurations
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly", "https://www.googleapis.com/auth/indexing"]
SHEET_ID = '1PDEvylCllwVrTGo5s_864kIYeGhm37OP90qhXBF-7n0'  # Google Sheet ID
RANGE_NAME = 'sample!A1:A200'  # Range of cells in the sheet to read
ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"

def get_urls_from_sheet():
    """Fetches URLs from a specified Google Sheet."""
    json_file_path = os.path.join(settings.BASE_DIR, 'json_folder', 'service_account.json')  # Adjust based on your setup
    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_file_path, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=credentials)

    # Call the Sheets API to fetch URLs
    result = service.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=RANGE_NAME).execute()
    values = result.get('values', [])
    return [row[0] for row in values if row]  # Assuming URLs are in the first column

async def send_url(session, http, url):
    """Sends URL to Google Indexing API."""
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


async def indexURL(http, urls):
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

def setup_http_client(account_number):
    """Sets up HTTP client for Google API using specified account JSON."""
    json_file_path = os.path.join(settings.BASE_DIR, 'json_folder', f'account{account_number}.json')
    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"JSON file {json_file_path} not found")
    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_file_path, scopes=SCOPES)
    return credentials.get_access_token().access_token

def main():
    urls = get_urls_from_sheet()  # Fetch URLs from Google Sheet
    num_accounts = 15  # Assuming you have 15 accounts

    for i in range(1, num_accounts + 1):
        http = setup_http_client(i)
        asyncio.run(indexURL(http, urls))  # Index URLs for each account

if __name__ == "__main__":
    main()
