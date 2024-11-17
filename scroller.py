from bs4 import BeautifulSoup
import requests
import os
import time
import pandas as pd
import zipfile
from pymongo import MongoClient, UpdateOne
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager


class Scroller: # TODO: rename to TradeDataDownloader
    def __init__(self, mongo_addr, download_folder, extracted_folder):
        self.download_folder = download_folder
        self.extracted_folder = extracted_folder

        self.mongo_addr = mongo_addr
        self.import_export_collection = None
        self.files_collection = None

        self.driver = None

    def setup(self):
        # Set up MongoDB connection
        client = MongoClient(self.mongo_addr)
        db = client['trade_data']

        # Define collections
        self.files_collection = db['files_metadata']
        self.import_export_collection = db['import_export_data']
        self.import_export_collection.create_index(
            [('year', 1), ('month', 1), ('partner_country', 1), ('product_code', 1), ('value', 1), ('direction', 1)],
            unique=True
        )

        # Set up Selenium WebDriver
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

        # Create necessary directories for downloads and extracted files.
        if not os.path.exists(self.download_folder):
            os.makedirs(self.download_folder)
        if not os.path.exists(self.extracted_folder):
            os.makedirs(self.extracted_folder)

    def get_soup(self, url, files_type):
        self.driver.get(url)

        # Wait for the page to load if necessary
        time.sleep(5)

        # Locate the dropdown element first
        subject_dropdown_element = self.driver.find_element(By.XPATH, '//select[@ng-model="curerntSubject"]')
        first_year_dropdown_element = self.driver.find_element(By.XPATH, '//select[@ng-model="firstYear"]')
        last_year_dropdown_element = self.driver.find_element(By.XPATH, '//select[@ng-model="lastYear"]')

        # Make the dropdown visible using JavaScript
        self.driver.execute_script("arguments[0].style.display = 'block';", subject_dropdown_element)
        self.driver.execute_script("arguments[0].style.display = 'block';", first_year_dropdown_element)
        self.driver.execute_script("arguments[0].style.display = 'block';", last_year_dropdown_element)

        # Wrap it in a Select object
        subject_dropdown = Select(subject_dropdown_element)
        first_year_dropdown = Select(first_year_dropdown_element)
        last_year_dropdown = Select(last_year_dropdown_element)

        # Select the earliest year (first option)
        subject_dropdown.select_by_value(files_type)
        first_year_dropdown.select_by_index(1)
        last_year_dropdown.select_by_index(len(last_year_dropdown.options) - 2)
        first_year_dropdown.select_by_index(0)
        last_year_dropdown.select_by_index(len(last_year_dropdown.options) - 1)

        # Get page source and parse with BeautifulSoup
        time.sleep(5)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        return soup

    def parse_file(self, file_metadata, file_path):
        # Extract the .zip file
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(self.extracted_folder)

        parsed = True
        # Process each extracted file
        for extracted_file in os.listdir(self.extracted_folder):
            extracted_file_path = os.path.join(self.extracted_folder, extracted_file)
            print(f"Processing file: {extracted_file_path}")

            try:
                # Read the content based on the file extension
                if extracted_file_path.endswith('.xls') or extracted_file_path.endswith('.xlsx'):
                    df = pd.read_excel(extracted_file_path, dtype={'Product_code': str, 'Partner_country': str})
                elif extracted_file_path.endswith('.csv'):
                    df = pd.read_csv(extracted_file_path)
                else:
                    print(f"Unsupported file type: {extracted_file_path}, skipping.")
                    continue

                # Prepare a list of new documents to insert
                documents = []
                for _, row in df.iterrows():
                    data_year = row.get('year', None)
                    data_month = row.get('Period', None)
                    partner_country = row.get('Partner_country', None)
                    product_code = row.get('Product_code', None)
                    value = row.get('Value')
                    direction = 'Import' if row.get('Flow', 0) == 1 else 'Export'

                    # Create the document structure
                    document = {
                        'year': data_year,
                        'month': data_month,
                        'partner_country': partner_country,
                        'product_code': product_code,
                        'value': value,
                        'direction': direction
                    }
                    documents.append(document)

                # Insert all documents at once
                if documents:
                    self.import_export_collection.insert_many(documents)
                    print(f"Inserted {len(documents)} new documents from {extracted_file_path}.")
            except Exception as e:
                print(f"Error processing file {extracted_file}: {e}")
                parsed = False
            finally:
                os.remove(os.path.join(self.extracted_folder, extracted_file))

        if parsed:
            print(f"Successfully processed and stored data for file: {file_path}")
        return parsed

    def parse_table(self, url, files_type):
        # Locate the table containing the data
        soup = self.get_soup(url, files_type)
        table = soup.find('table', {'class': 'zebraTable'})
        rows = table.find_all('tr')

        year = None
        for row in rows[1:]:  # Skip the header row
            cells = row.find_all('td')
            if len(cells) >= 5:
                # Extract year and month from the table
                year = cells[0].get_text(strip=True) if cells[0].get_text(strip=True).isdigit() else year
                month = cells[1].get_text(strip=True)
                file_size = cells[2].get_text(strip=True)
                last_update_date = cells[3].get_text(strip=True)
                download_link = cells[4].find('a')['href']

                # Construct the download URL
                file_url = f"https://www.cbs.gov.il{download_link}" if download_link.startswith('/') else download_link
                file_name = download_link.split('/')[-1]
                file_path = os.path.join(self.download_folder, file_name)

                # Check for file updates in the `files_metadata` collection
                file_metadata = self.files_collection.find_one(
                    {'file_name': file_name, 'year': int(year), 'month': int(month)})

                # Determine if the file should be downloaded
                if file_metadata:
                    if file_metadata['last_update_date'] == last_update_date: # TODO: Check if parsed
                        print(f"File '{file_name}' is up-to-date. Skipping download.")
                        continue
                    else:
                        print(f"File '{file_name}' has been updated. Downloading the new version.")
                else:
                    print(f"New file '{file_name}' found. Downloading.")

                # Store file metadata in the `files_metadata` collection
                file_metadata = {
                    'file_name': file_name,
                    'year': int(year),
                    'month': int(month),
                    'file_size': file_size,
                    'last_update_date': last_update_date,
                    'download_link': file_url,
                    'data_type': files_type
                }

                # Download the updated file
                print(f"Downloading {file_name} from {file_url}")
                response = requests.get(file_url)
                with open(file_path, 'wb') as file:
                    file.write(response.content)

                # Parse the content of the file and store in the trade database
                file_metadata['parsed'] = self.parse_file(file_metadata, file_path)

                # Update the metadata of the file in the files description database
                self.files_collection.update_one(
                    {'file_name': file_name, 'year': int(year), 'month': int(month)},
                    {'$set': file_metadata},
                    upsert=True
                )
                print(f"Stored file metadata: {file_metadata}")

    def __del__(self):
        self.driver.quit()
