import pandas as pd
import requests
from io import StringIO
import datetime
import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import platform
from database import DatabaseManager
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from dotenv import load_dotenv
import traceback
load_dotenv()
import logging

logging.basicConfig(filename='nepse_data_fetcher.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class NepseDataFetcher:

    def __init__(self, driver_path=None, db_config=None):
        self.chrome_options = Options()
        self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument("--disable-gpu")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("start-maximized")
        self.chrome_options.add_argument("disable-infobars")
        self.chrome_options.add_argument("--disable-extensions")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")

        
        if not driver_path:
            if platform.system() == 'Linux':
                driver_path = '/usr/local/bin/chromedriver'
                # self.chrome_binary_path = '/usr/bin/google-chrome'
            else:
                driver_path = r'C:\Program Files\chromedriver-win64\chromedriver.exe'
                # self.chrome_binary_path = r'C:\Program Files\Google\Chrome\Application\chrome.exe'

        # self.chrome_options.binary_location = self.chrome_binary_path
        self.webdriver_service = Service(driver_path)
        self.db_manager = DatabaseManager(db_config)

    def _init_driver(self):
        return webdriver.Chrome(service=self.webdriver_service, options=self.chrome_options)

    def fetch_company_details(self):
        driver = self._init_driver()
        driver.get('https://nepalstock.com/company')
        all_data = []

        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            headers = [header.text.strip() for header in driver.find_elements(By.XPATH, '//table/thead/tr/th')]
            
            while True:
                rows = driver.find_elements(By.XPATH, '//table/tbody/tr')
                for row in rows:
                    columns = row.find_elements(By.TAG_NAME, 'td')
                    data = [col.text.strip() for col in columns]
                    all_data.append(data)
                
                try:
                    next_button_disabled = driver.find_elements(By.XPATH, '//li[@class="pagination-next disabled"]')
                    if next_button_disabled:
                        break
                    next_button = WebDriverWait(driver, 15).until(
                        EC.element_to_be_clickable((By.XPATH, '//li[@class="pagination-next" and not(contains(@class, "disabled"))]/a'))
                    )
                    next_button.click()
                    time.sleep(3)
                except Exception as e:
                    print(f"Error finding or clicking the Next page button: {e}")
                    break
        except Exception as e:
            print(f"An error occurred while extracting company details: {e}")
        finally:
            driver.quit()
        
        company_details = pd.DataFrame(all_data, columns=headers)
        self.db_manager.save_to_db('company_details', company_details)

    def fetch_todays_prices(self, date: str = None):
        if date is None:
            date = datetime.datetime.today().strftime('%Y-%m-%d')
        
        api = f'https://www.nepalstock.com.np/api/nots/market/export/todays-price/{date}'
        try:
            response = requests.get(api, headers={'User-Agent': 'Mozilla/5.0'}, verify=False)
            if response.status_code == 200:
                text = response.text.replace("\",", "")
                df = pd.read_csv(StringIO(text), sep=",", thousands=',', engine='python')
                self.db_manager.save_to_db('todays_prices', df)
            else:
                print(f"Failed to fetch data: {response.status_code}")
        except Exception as e:
            print(f"An error occurred while fetching today's prices: {e}")

    def fetch_live_market_data(self):
        """Fetch live market data from the NEPSE website and save to a CSV file and database."""
        driver = self._init_driver()
        driver.get('https://nepalstock.com/live-market')

        # Initialize variables
        nepse_index = None
        sensitive_index = None
        total_turnover = None
        total_traded_shares = None
        total_transactions = None
        total_scrips_traded = None

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, "/html/body/app-root/div/main/div/app-live-market/div/div/div[3]"))
            )

            # Extract NEPSE Index and Sensitive Index using the new XPath
            nepse_index = driver.find_element(By.XPATH, '//div[@class="marketdepth__item flex-fill live-market-index"][1]/div/span').text
            sensitive_index = driver.find_element(By.XPATH, '//div[@class="marketdepth__item flex-fill live-market-index"][2]/div/span').text

            # Find all summary details in the correct order
            summary_details = driver.find_elements(By.XPATH, '//div[@class="market-summary-detail"]')

            # Initialize market summary data
            total_turnover = summary_details[0].text.split('Rs:')[-1].strip() if len(summary_details) > 0 else None
            total_traded_shares = summary_details[1].text.split('Total Traded Shares')[-1].strip() if len(summary_details) > 1 else None
            total_transactions = summary_details[2].text.split('Total Transactions')[-1].strip() if len(summary_details) > 2 else None
            total_scrips_traded = summary_details[3].text.split('Total Scrips Traded')[-1].strip() if len(summary_details) > 3 else None

            # Print extracted data
            print("NEPSE Index:", nepse_index)
            print("Sensitive Index:", sensitive_index)
            print("Total Turnover:", total_turnover)
            print("Total Traded Shares:", total_traded_shares)
            print("Total Transactions:", total_transactions)
            print("Total Scrips Traded:", total_scrips_traded)

        except Exception as e:
            print(f"An error occurred while extracting market summary details: {e}")
        finally:
            driver.quit()

        # Save market summary data to a dictionary
        market_summary = {
            "NEPSE Index": nepse_index if nepse_index else "N/A",
            "Sensitive Index": sensitive_index if sensitive_index else "N/A",
            "Total Turnover": total_turnover if total_turnover else "N/A",
            "Total Traded Shares": total_traded_shares if total_traded_shares else "N/A",
            "Total Transactions": total_transactions if total_transactions else "N/A",
            "Total Scrips Traded": total_scrips_traded if total_scrips_traded else "N/A",
        }

        # Convert market summary to DataFrame and save to CSV
        summary_df = pd.DataFrame([market_summary])
        summary_df.to_csv('nepal_market_summary.csv', index=False)
        print("Market summary data has been saved to 'nepal_market_summary.csv'.")

        # Save to the database
        try:
            self.db_manager.save_to_db('live_market_summary', summary_df)
            print("Live market summary data has been saved to the database.")
        except Exception as e:
            print(f"An error occurred while saving to the database: {e}")

    def fetch_floorsheet_data(self):
        driver = self._init_driver()
        driver.get('https://nepalstock.com/floor-sheet')
        all_data = []

        data_file = 'nepal_floorsheet_data.csv'
        progress_file = 'nepal_floorsheet_progress.txt'
        
        start_page = 1
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                start_page = int(f.read().strip())
        
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            headers = [header.text.strip() for header in driver.find_elements(By.XPATH, '//table/thead/tr/th')]

            while True:
                rows = driver.find_elements(By.XPATH, '//table/tbody/tr')
                for row in rows:
                    columns = row.find_elements(By.TAG_NAME, 'td')
                    data = [col.text.strip() for col in columns]
                    all_data.append(data)
                
                try:
                    next_button_disabled = driver.find_elements(By.XPATH, '//li[@class="pagination-next disabled"]')
                    if next_button_disabled:
                        break
                    next_button = WebDriverWait(driver, 15).until(
                        EC.element_to_be_clickable((By.XPATH, '//li[@class="pagination-next" and not(contains(@class, "disabled"))]/a'))
                    )
                    next_button.click()
                    time.sleep(3)
                except Exception as e:
                    print(f"Error finding or clicking the Next page button: {e}")
                    break
        except Exception as e:
            print(f"An error occurred while extracting floor sheet data: {e}")
        finally:
            driver.quit()

        floorsheet_data = pd.DataFrame(all_data, columns=headers)
        floorsheet_data.to_csv(data_file, index=False)
        self.db_manager.save_to_db('floor_sheet_data', floorsheet_data)

        # Update progress
        with open(progress_file, 'w') as f:
            f.write(str(start_page + 1))
    
    def fetch_indices_data(self):
        """Fetch indices data from the NEPSE website and save to the database."""
        driver = self._init_driver()
        driver.get('https://nepalstock.com/indices')
        all_data = []
    
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            headers = [header.text.strip() for header in driver.find_elements(By.XPATH, '//table/thead/tr/th')]
            
            while True:
                rows = driver.find_elements(By.XPATH, '//table/tbody/tr')
                for row in rows:
                    columns = row.find_elements(By.TAG_NAME, 'td')
                    data = [col.text.strip() for col in columns]
                    all_data.append(data)
                
                # Handle pagination
                try:
                    next_button_disabled = driver.find_elements(By.XPATH, '//li[@class="pagination-next disabled"]')
                    if next_button_disabled:
                        print("Reached the last page. No more pages to click.")
                        break
                    
                    next_button = WebDriverWait(driver, 15).until(
                        EC.element_to_be_clickable((By.XPATH, '//li[@class="pagination-next" and not(contains(@class, "disabled"))]/a'))
                    )
                    next_button.click()
                    time.sleep(3)
                except Exception as e:
                    print(f"Error finding or clicking the Next page button: {e}")
                    break
        except Exception as e:
            print(f"An error occurred while extracting indices data: {e}")
        finally:
            driver.quit()
    
        indices_df = pd.DataFrame(all_data, columns=headers)
        indices_df.to_csv('nepal_stock_data.csv', index=False)
        print("Data has been saved to 'nepal_stock_data.csv'.")



    
        # Save to the database
        self.db_manager.save_to_db('indices_data', indices_df)
        print("Indices data has been saved to the database.")

if __name__ == "__main__":
    driver_path = None
    db_config = {
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'dbname': os.getenv('DB_NAME')
    }
    fetcher = NepseDataFetcher(driver_path=driver_path, db_config=db_config)
    fetcher.fetch_company_details()
    fetcher.fetch_todays_prices('2024-01-31')
    fetcher.fetch_live_market_data()
    fetcher.fetch_indices_data()
    fetcher.fetch_floorsheet_data()
