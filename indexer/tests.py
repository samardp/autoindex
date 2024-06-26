
# Constants
import requests


ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"
URLS_PER_ACCOUNT = 200
SHEET_ID = '1PDEvylCllwVrTGo5s_864kIYeGhm37OP90qhXBF-7n0'  # Replace with your Google Sheet ID
RANGE_NAME = 'A2:A1578'  # Replace with your desired range
API_KEY = 'AIzaSyBcD2jEabo77Vgg-wEYNLUmQlgxVkNqEi8'  # Replace with your actual API key




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

# Example usage
urls = fetch_google_sheet_data()
for url in urls:
    print(url)
