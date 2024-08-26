import requests
import pandas as pd
from io import StringIO
import datetime

def get_todays_prices(date: str = None):
    if date is None:
        date = datetime.datetime.today().strftime('%Y-%m-%d')

    api = f'https://www.nepalstock.com.np/api/nots/market/export/todays-price/{date}'
    response = requests.get(api, headers={'User-Agent': 'Mozilla/5.0'}, verify=False)
    if response.status_code == 200:
        text = response.text
        if not text:
            return None
        text = text.replace("\",", "")
        df = pd.read_csv(StringIO(text), sep=",", thousands=',', engine='python')
        return df
    return None

# Example usage
df = get_todays_prices('2024-01-31')

if df is not None:
    # Save DataFrame to CSV
    df.to_csv(r'C:\Users\Asmit\Desktop\treeleaf_technology\NEPSE_project\data\todays_prices.csv', index=False)
    print("Data saved to 'todays_prices.csv'")
else:
    print("No data to save.")
