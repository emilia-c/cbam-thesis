from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import json
from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
import unicodedata
import os

class MEP:
    def __init__(self, url):
        self.url = url                  # mep's website url
        self.name = None                # mep name
        self.group = None               # mep political group
        self.mep_country = None         # mep country of origin
        self.mep_national_party = None  # mep national party

        # Setup Selenium with robust options
        chrome_options = Options()
        chrome_options.add_argument("--headless")                                       # run in headless mode (no GUI)
        chrome_options.add_argument("start-maximized")              
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        self.driver = webdriver.Chrome(service=ChromeService(), options=chrome_options)

    def get_mep_data(self):
        self.driver.get(self.url)
        time.sleep(3)                   # wait for the page to load
        soup = BeautifulSoup(self.driver.page_source, 'html.parser') # define soup
        
        # scrape mep name
        try: 
            name_tag = self.driver.find_element(By.CSS_SELECTOR, 'span.sln-member-name')
            if name_tag:
                self.name = name_tag.text.strip()
            else: 
                self.name = "N/A"
        except NoSuchElementException: 
            print(f'Warning: Name not found for {self.url}')
        
        # scrape mep group
        try: 
            group_tag = self.driver.find_element(By.CSS_SELECTOR, 'h3.erpl_title-h3.mt-1.sln-political-group-name')
            self.group = group_tag.text.strip() if group_tag else "N/A" 
        
        except Exception:
            # Fallback: use BeautifulSoup to locate the "Political groups" section
            political_groups = []
            status_divs = soup.find_all('div', class_='erpl_meps-status')
            for div in status_divs:
                header = div.find('h4')
                if header and "Political groups" in header.text:
                    lis = div.find_all('li')
                    for li in lis:
                        # Example li text: "02-07-2019 / 28-04-2021 : Non-attached Members"
                        # or "29-04-2021 / 15-07-2024 : Group of the European People's Party (Christian Democrats) - Member"
                        text = li.get_text(separator=" ").strip()
                        parts = text.split(":")
                        if len(parts) > 1:
                            # Remove role information (e.g. "- Member") if it exists
                            group_text = parts[1].split("-")[0].strip()
                            if group_text:
                                political_groups.append(group_text)
            self.group = ", ".join(political_groups) if political_groups else "N/A"
            
        # scrape mep country of origin
        try: 
            mep_country_tag = self.driver.find_element(By.CSS_SELECTOR, 'div.erpl_title-h3.mt-1.mb-1')
            if mep_country_tag:
                country_text = mep_country_tag.text.strip()
                # If a hyphen is present, assume the country is before it.
                if " - " in country_text:
                    self.mep_country = country_text.split(" - ")[0].strip()
                # Otherwise, if there are parentheses, it might be the national party, so just take the whole text.
                else:
                    self.mep_country = country_text
            else:
                self.mep_country = "N/A"
        except NoSuchElementException: 
            print(f'Warning: Country not found for {self.url}')
            self.mep_country = "N/A"
     
        # scrape mep national party
        #try: 
        #    np_tag = self.driver.find_element(By.CSS_SELECTOR, 'div.erpl_title-h3.mt-1.mb-1')
        #    if np_tag:
        #        national_party_text = np_tag.text.strip()
        #        if " - " in national_party_text:
        #            self.mep_national_party = national_party_text.split(" - ")[1].split(" (")[0]
        #        else:
        #            print(f"Warning: MEP National Party information not found or formatted incorrectly for {self.url}")
        #            self.mep_national_party = "N/A"
        #except Exception:
            # Fallback: use BeautifulSoup to extract multiple national parties
        try: 
            national_parties = []
            status_divs = soup.find_all('div', class_='erpl_meps-status')
            for div in status_divs:
                header = div.find('h4')
                if header and "National parties" in header.text:
                    lis = div.find_all('li')
                    for li in lis:
                        # Example li text: "02-07-2019 / 28-04-2021 : Movimento 5 Stelle (Italy)"
                        # or "29-04-2021 / 15-07-2024 : Forza Italia (Italy)"
                        text = li.get_text(separator=" ").strip()
                        parts = text.split(":")
                        if len(parts) > 1:
                            # Remove any parenthetical country info, if present
                            party_text = parts[1].split(" (")[0].strip()
                            if party_text:
                                national_parties.append(party_text)
            self.mep_national_party = ", ".join(national_parties) if national_parties else "N/A"
        except NoSuchElementException: 
            print(f'Warning: National party not found for {self.url}')
        
    def to_dict(self):
        return {
            "name": self.name,
            "group": self.group,  
            "origin_country": self.mep_country,
            "national_party": self.mep_national_party
        }

    def close(self):
        self.driver.quit()  # Close the browser when done

# construct the MEP url using the historical page containing information about the mep and thier id and name that were scraped
def get_mep_links(json_file_path):
    def remove_non_ascii(text):
        # Normalize text to remove accents and other non-ASCII characters
        return ''.join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != 'Mn'
        )

    def construct_mep_url(mep_name, mep_id):
        # Split name into components
        names = mep_name.split()
        first_names = []
        last_names = []

        # Separate first and last names based on capitalization
        for name in names:
            if name.isupper():
                last_names.append(name)
            else:
                first_names.append(name)

        # Construct the name part in FIRSTNAME_LASTNAME format
        first_name_part = '+'.join(first_names)
        last_name_part = '+'.join(last_names)

        # Combine parts into the required format
        if first_names and last_names:
            name_part = f"{first_name_part}_{last_name_part}"
        elif first_names:
            name_part = first_name_part
        else:
            name_part = last_name_part

        # Construct the full URL
        return f"https://www.europarl.europa.eu/meps/en/{mep_id}/{name_part}/history/9"

    # Load JSON data from the file
    with open(json_file_path, 'r', encoding='utf-8') as f:
        meps_data = json.load(f)
    
    mep_links = []
    for mep in meps_data:
        mep_id = mep["mep_id"]
        
        # Clean and format the MEP name
        mep_name = mep["mep_name"]
        cleaned_name = remove_non_ascii(mep_name.upper())
        
        # Construct the MEP URL
        mep_url = construct_mep_url(cleaned_name, mep_id)
        mep_links.append(mep_url)

    return mep_links

def scrape_meps(url, mep_name):
    mep = MEP(url)
    mep.get_mep_data()
    
    return mep.to_dict()

def load_existing_data(file_path):
    """Load existing MEP data if the JSON file already exists."""
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as infile:
            try:
                return json.load(infile)
            except json.JSONDecodeError:
                print("Error decoding JSON. Starting with an empty list.")
                return []
    return []

def main():
    # File paths
    json_path = 'cbam_meeting_information/data/mep_list/NAMES_9term_meps.json'
    output_file = "cbam_meeting_information/data/mep_list/9term_meps_all_info.json"

    # Load MEP links and names
    mep_links = get_mep_links(json_path)

    # test MEP links 
    #mep_links = mep_links[:5]

    # Load MEP data from JSON
    with open(json_path, 'r', encoding='utf-8') as f:
        meps_data = json.load(f)

    # Load existing data from the output file
    existing_data = load_existing_data(output_file)
    processed_meps = {mep["name"] for mep in existing_data if "name" in mep}

    # Initialize list with existing data
    all_mep_data = existing_data

    # Iterate and scrape only new MEPs
    for mep_info, mep_url in tqdm(zip(meps_data, mep_links), desc="Scraping MEPs"):
        mep_name = mep_info["mep_name"]

        # Skip if already processed
        if mep_name in processed_meps:
            print(f"Skipping already processed MEP: {mep_name}")
            continue

        try:
            mep_data = scrape_meps(mep_url, mep_name)
            all_mep_data.append(mep_data)
            processed_meps.add(mep_name)

            # Save progress after each MEP
            with open(output_file, "w", encoding="utf-8") as outfile:
                json.dump(all_mep_data, outfile, ensure_ascii=False, indent=4)

        except Exception as e:
            print(f"Error scraping {mep_name}: {e}")

        time.sleep(1)  # Optional delay to avoid server overload

    print("Scraping completed successfully!")

if __name__ == "__main__":
    main()