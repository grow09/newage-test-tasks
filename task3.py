import sys
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.common.by import By
import re

def get(url, sheet):
    """
    Scrapes data from a given URL and writes it to a Google Sheet.

    Parameters:
        url (str): The URL to scrape data from.
        sheet (gspread.Spreadsheet): The Google Sheet to write the data to.

    Returns:
        None
    """
    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    options.add_argument('--ignore-gpu-blacklist')
    options.add_argument('--use-gl')
    options.add_argument('--disable-web-security')
    driver = webdriver.Chrome(options=options)

    driver.get(url)
    initial_url = url  # Store the initial URL

    row_index = 2  # Start inserting data from row 2
    while True:
        while True:
            ads = driver.find_elements(By.CSS_SELECTOR, 'div[data-cy="l-card"]')
            ad_urls = [ad.find_element(By.TAG_NAME, 'a').get_attribute('href') for ad in ads]

            for url in ad_urls:
                try:
                    driver.get(url)
                    time.sleep(1)

                    title = driver.find_element(By.CSS_SELECTOR, 'div[data-cy="ad_title"]').text
                    price = driver.find_element(By.CSS_SELECTOR, 'div[data-testid="ad-price-container"]').text
                    description = driver.find_element(By.CSS_SELECTOR, 'div[data-cy="ad_description"]').text
                    
                    # Find the element containing "height"
                    height_text = None
                    height_elements = driver.find_elements(By.CSS_SELECTOR, 'p')
                    for element in height_elements:
                        if "поверховість:" in element.text.lower():
                            height_text = element.text
                            break
                    height = re.search(r'\d+', height_text).group() if height_text else ''
                    # Find the element containing "floor"
                    floor_text = None
                    floor_elements = driver.find_elements(By.CSS_SELECTOR, 'p')
                    for element in floor_elements:
                        if "поверх:" in element.text.lower():
                            floor_text = element.text
                            break
                    floor = re.search(r'\d+', floor_text).group() if floor_text else ''
                    # Find the element containing "area"
                    area_text = None
                    area_elements = driver.find_elements(By.CSS_SELECTOR, 'p')
                    for element in area_elements:
                        if "площа:" in element.text.lower():
                            area_text = element.text
                            break
                    area = re.search(r'\d+', area_text).group() if area_text else ''

                    city = driver.find_element(By.CSS_SELECTOR, 'p.css-1cju8pu').text.split(',')[0]

                    ad_data = {
                        "title": title,
                        "price": price,
                        "description": description,
                        "url": url,
                        "floor": floor,
                        "height": height,
                        "area": area,
                        "city": city
                    }
                    print(ad_data)
                    # Write the data to Google Sheet
                    write_to_google_sheet(sheet, ad_data, row_index)
                    row_index += 1  # Increment the row index for the next iteration

                except Exception as e:
                    print("[~] error: " + str(e) + ". line: " + str(sys.exc_info()[-1].tb_lineno))
                    print("Failed URL:", url)  # Print the URL where the failure occurred

            try:
                next_page_button = driver.find_element(By.CSS_SELECTOR, 'a[data-testid="pagination-forward"]')
                next_page_button.click()
                time.sleep(2)  # Adjust the sleep time as needed

                # Update initial_url
                initial_url = driver.current_url

            except Exception as e:
                print("[~] error: " + str(e) + ". line: " + str(sys.exc_info()[-1].tb_lineno))
                break  # Break the loop if there is an error or if there is no next page button

        driver.get(initial_url)  # Go back to the initial URL

    driver.quit()

def write_to_google_sheet(sheet, ad_data, row_index):
    """
    Writes the provided ad_data to the Google Sheet at the specified row_index.

    Parameters:
        sheet (gspread.Spreadsheet): The Google Sheet to write the data to.
        ad_data (dict): A dictionary containing the ad data with keys: 
        'title', 'price', 'description', 'url', 'floor', 'height', 'area', 'city'.
        row_index (int): The row index where the data should be inserted.

    Returns:
        None
    """
    sheet.insert_row([ad_data['title'], ad_data['price'], ad_data['description'], 
                      ad_data['url'], ad_data['floor'], ad_data['height'], ad_data['area'], 
                      ad_data['city']], row_index)

    print("Data has been written to Google Sheet successfully.")

if __name__ == '__main__':
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    creds = ServiceAccountCredentials.from_json_keyfile_name('', scope) ## json keyfile
    client = gspread.authorize(creds)

    sheet = client.open_by_key('1YYIDU_sVEFgT3cveq7Lp67NnIcJ4jjNpIY4FUvJJW2A').sheet1

    get("https://www.olx.ua/uk/nedvizhimost/kvartiry/", sheet)
