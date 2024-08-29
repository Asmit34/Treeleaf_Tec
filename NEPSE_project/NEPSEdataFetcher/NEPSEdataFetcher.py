from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
import requests
from io import StringIO
import datetime
import os
import platform


class NepseDataFetcher:
    def __init__(self, driver_path=None):
        self.chrome_options = Options()
        self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument("--disable-gpu")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("start-maximized")
        self.chrome_options.add_argument("disable-infobars")
        self.chrome_options.add_argument("--disable-extensions")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 "
            "Safari/537.36"
        )

        # Set driver path based on OS if not provided
        if not driver_path:
            if platform.system() == 'Linux':
                driver_path = './chromedriver'
            else:
                driver_path = (
                    r'C:\Program Files\chromedriver-win64\chromedriver.exe'
                )

        self.webdriver_service = Service(driver_path)

    def _init_driver(self):
        # Initialize the Chrome driver with the configured options
        return webdriver.Chrome(
            service=self.webdriver_service,
            options=self.chrome_options
        )

    def fetch_company_details(self):
        """Fetch company details from the NEPSE website
           Save to a CSV file."""
        driver = self._init_driver()
        driver.get('https://nepalstock.com/company')
        all_data = []

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            headers = [
                header.text.strip()
                for header in driver.find_elements(
                    By.XPATH, '//table/thead/tr/th'
                )
            ]

            while True:
                rows = driver.find_elements(By.XPATH, '//table/tbody/tr')
                for row in rows:
                    columns = row.find_elements(By.TAG_NAME, 'td')
                    data = [col.text.strip() for col in columns]
                    all_data.append(data)

                # Handle pagination
                try:
                    next_button_disabled = driver.find_elements(
                        By.XPATH,
                        '//li[@class="pagination-next disabled"]'
                    )
                    if next_button_disabled:
                        break
                    next_button = WebDriverWait(driver, 15).until(
                        EC.element_to_be_clickable(
                            (By.XPATH,
                             '//li[@class="pagination-next" '
                             'and not(contains(@class, "disabled"))]/a')
                        )
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

        # Save the data
        file_path = os.path.join(
            'C:', 'Users', 'Asmit', 'Desktop', 'treeleaf_technology',
            'NEPSE_project', 'NEPSEdataFetcher',
            'nepal_listed_securities_data.csv'
        )
        company_details = pd.DataFrame(all_data, columns=headers)
        company_details.to_csv(file_path, index=False)
        print("Company details have been saved to 'nepal_listed_securities_data.csv'.")

    def fetch_todays_prices(self, date: str = None):
        """Fetch today's prices from the NEPSE API and save to a CSV file."""
        if date is None:
            date = datetime.datetime.today().strftime('%Y-%m-%d')

        api = (
            f'https://www.nepalstock.com.np/api/nots/market/export/todays-price/{date}'
        )
        try:
            response = requests.get(
                api,
                headers={'User-Agent': 'Mozilla/5.0'},
                verify=False
            )
            if response.status_code == 200:
                text = response.text.replace("\",", "")
                df = pd.read_csv(
                    StringIO(text),
                    sep=",",
                    thousands=',',
                    engine='python'
                )
                df.to_csv(
                    r'C:\Users\Asmit\Desktop\treeleaf_technology\NEPSE_project\NEPSEdataFetcher\todays_prices.csv',
                    index=False
                )
                print("Today's prices have been saved to 'todays_prices.csv'.")
            else:
                print(f"Failed to fetch data: {response.status_code}")
        except Exception as e:
            print(f"An error occurred while fetching today's prices: {e}")

    def fetch_live_market_data(self):
        """Fetch live market data from the NEPSE website and save to a CSV file."""
        driver = self._init_driver()
        driver.get('https://nepalstock.com/live-market')

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located(
                    (By.XPATH, "/html/body/app-root/div/main/div/app-live-market/"
                                "div/div/div[3]")
                )
            )

            # Extract NEPSE Index and Sensitive Index using the new XPath
            nepse_index = driver.find_element(
                By.XPATH,
                '//div[@class="marketdepth__item flex-fill live-market-index"]'
                '[1]/div/span'
            ).text
            sensitive_index = driver.find_element(
                By.XPATH,
                '//div[@class="marketdepth__item flex-fill live-market-index"]'
                '[2]/div/span'
            ).text

            summary_details = driver.find_elements(
                By.XPATH, '//div[@class="market-summary-detail"]'
            )

            # Initialize market summary data
            total_turnover = (
                summary_details[0].text.split('Rs:')[-1].strip()
                if len(summary_details) > 0 else None
            )
            total_traded_shares = (
                summary_details[1].text.split('Total Traded Shares')[-1].strip()
                if len(summary_details) > 1 else None
            )
            total_transactions = (
                summary_details[2].text.split('Total Transactions')[-1].strip()
                if len(summary_details) > 2 else None
            )
            total_scrips_traded = (
                summary_details[3].text.split('Total Scrips Traded')[-1].strip()
                if len(summary_details) > 3 else None
            )

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

        summary_df = pd.DataFrame([market_summary])
        summary_df.to_csv(
            r'C:\Users\Asmit\Desktop\treeleaf_technology\NEPSE_project\NEPSEdataFetcher\nepal_live_market_summary.csv',
            index=False
        )
        print("Market summary data has been saved to 'nepal_live_market_summary.csv'.")

    def fetch_indices_data(self):
        """Fetch indices data from the NEPSE website and save to a CSV file."""
        driver = self._init_driver()
        driver.get('https://nepalstock.com/indices')
        all_data = []

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            headers = [
                header.text.strip()
                for header in driver.find_elements(
                    By.XPATH, '//table/thead/tr/th'
                )
            ]

            while True:
                rows = driver.find_elements(By.XPATH, '//table/tbody/tr')
                for row in rows:
                    columns = row.find_elements(By.TAG_NAME, 'td')
                    data = [col.text.strip() for col in columns]
                    all_data.append(data)

                # Handle pagination
                try:
                    next_button_disabled = driver.find_elements(
                        By.XPATH, 
                        '//li[@class="pagination-next disabled"]'
                    )
                    if next_button_disabled:
                        print("Reached the last page. No more pages to click.")
                        break

                    next_button = WebDriverWait(driver, 15).until(
                        EC.element_to_be_clickable(
                            (By.XPATH,
                             '//li[@class="pagination-next" '
                             'and not(contains(@class, "disabled"))]/a')
                        )
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

        # Save the data
        file_path = os.path.join(
            'C:', 'Users', 'Asmit', 'Desktop', 'treeleaf_technology',
            'NEPSE_project', 'NEPSEdataFetcher', 'nepal_indices_data.csv'
        )
        indices_df = pd.DataFrame(all_data, columns=headers)
        indices_df.to_csv(file_path, index=False)
        print("Data has been saved to 'nepal_indices_data.csv'.")

    def fetch_floorsheet_data(self):
        """Fetch floorsheet data from the NEPSE website and save to a CSV file."""
        driver = self._init_driver()
        driver.get('https://nepalstock.com/floor-sheet')
        all_data = []

        # Define file paths
        data_file = os.path.join(
            'C:', 'Users', 'Asmit', 'Desktop', 'treeleaf_technology',
            'NEPSE_project', 'NEPSEdataFetcher', 'nepal_floorsheet_data.csv'
        )
        progress_file = 'nepal_floorsheet_progress.txt'

        # Load progress if available
        start_page = 1
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                start_page = int(f.read().strip())

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            headers = [
                header.text.strip()
                for header in driver.find_elements(
                    By.XPATH, '//table/thead/tr/th'
                )
            ]

            current_page = start_page
            while True:
                if current_page > start_page:
                    print(f"Resuming from page {current_page}")

                # Extract rows of data
                rows = driver.find_elements(By.XPATH, '//table/tbody/tr')
                for row in rows:
                    columns = row.find_elements(By.TAG_NAME, 'td')
                    data = [col.text.strip() for col in columns]
                    all_data.append(data)

                # Save data incrementally
                pd.DataFrame(all_data, columns=headers).to_csv(data_file, index=False)
                print(f"Data saved up to page {current_page}.")

                # Save progress
                with open(progress_file, 'w') as f:
                    f.write(str(current_page))

                # Handle pagination
                try:
                    next_button_disabled = driver.find_elements(
                        By.XPATH, 
                        '//li[@class="pagination-next disabled"]'
                    )
                    if next_button_disabled:
                        print("Reached the last page. No more pages to click.")
                        break

                    next_button = WebDriverWait(driver, 15).until(
                        EC.element_to_be_clickable(
                            (By.XPATH,
                             '//li[@class="pagination-next" '
                             'and not(contains(@class, "disabled"))]/a')
                        )
                    )
                    next_button.click()
                    time.sleep(2)
                    current_page += 1
                except Exception as e:
                    print(f"Error finding or clicking the Next page button: {e}")
                    break
        except Exception as e:
            print(f"An error occurred while extracting floorsheet data: {e}")
        finally:
            driver.quit()

            # Final save to ensure all data is written
            pd.DataFrame(all_data, columns=headers).to_csv(data_file, index=False)
            print("Floorsheet data has been saved to 'nepal_floorsheet_data.csv'.")

            if os.path.exists(progress_file):
                os.remove(progress_file)


if __name__ == "__main__":
    fetcher = NepseDataFetcher()
    fetcher.fetch_company_details()
    fetcher.fetch_todays_prices('2024-01-31')
    fetcher.fetch_live_market_data()
    fetcher.fetch_indices_data()
    fetcher.fetch_floorsheet_data()
