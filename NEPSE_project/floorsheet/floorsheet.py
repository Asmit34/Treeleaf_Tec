from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time

# Setup Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("disable-infobars")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
chrome_options.add_argument("--blink-settings=imagesEnabled=false")


webdriver_service = Service(r'C:\Program Files\chromedriver-win64\chromedriver.exe')
driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)

#target URL
driver.get('https://nepalstock.com/floor-sheet')  # Replace with the correct URL for floorsheet

# Create a list to store all the data
all_data = []

headers = []
try:
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.TAG_NAME, "table"))
    )
    # Locate the table header
    headers = driver.find_elements(By.XPATH, '//table/thead/tr/th')
    headers = [header.text.strip() for header in headers]

except Exception as e:
    print(f"An error occurred while extracting headers: {e}")
    driver.quit()
    exit()

while True:
    try:
        # Wait until the table is present
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        
        # Extract rows of data
        rows = driver.find_elements(By.XPATH, '//table/tbody/tr')
        for row in rows:
            columns = row.find_elements(By.TAG_NAME, 'td')
            data = [col.text.strip() for col in columns]
            all_data.append(data)
        
        # Handle pagination
        try:
            # Check if the Next button is disabled
            next_button_disabled = driver.find_elements(By.XPATH, '//li[@class="pagination-next disabled"]')
            if next_button_disabled:
                print("Reached the last page. No more pages to click.")
                break
            
            # Wait for the Next button to be clickable
            next_button = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, '//li[@class="pagination-next" and not(contains(@class, "disabled"))]/a'))
            )
            print("Clicking Next page button...")
            next_button.click()
            time.sleep(2)
        except Exception as e:
            print(f"Error finding or clicking the Next page button: {e}")
            break

    except Exception as e:
        print(f"An error occurred: {e}")
        break

driver.quit()

floor_data = pd.DataFrame(all_data, columns=headers)
floor_data.to_csv('nepal_floorsheet_data.csv', index=False)

print("Data has been saved to 'nepal_floorsheet_data.csv'.")
