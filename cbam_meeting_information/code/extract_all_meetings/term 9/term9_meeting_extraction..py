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
        self.url = url
        self.name = None
        #self.mep_party = None          REMOVE BECAUSE CAUSING BUGS
        #self.mep_country = None        REMOVE BECAUSE CAUSING BUGS
        self.meetings = {}
        
    # Setup Selenium
       # Setup Selenium with robust options
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")                                 # Use the new headless mode
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")                                   # Helps in Linux environments
        chrome_options.add_argument("--disable-dev-shm-usage")                        # Prevent crashes due to shared memory limits
        chrome_options.add_argument("--remote-debugging-port=9222")                   # Enable remote debugging
        chrome_options.add_argument("--disable-extensions")                           # Disable extensions for stability
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")  # Avoid detection
        chrome_options.add_argument("--window-size=1920,1080")                        # Set a larger window size
        chrome_options.add_argument("--log-level=3")                                  # Reduce Chrome log verbosity
        chrome_options.add_argument("--disable-software-rasterizer")                  # Reduce GPU usage

        self.driver = webdriver.Chrome(service=ChromeService(), options=chrome_options)
        self.driver.set_page_load_timeout(120)  # Increase page load timeout to 120 seconds


    def get_mep_data(self):
        # Navigate to the URL
        self.driver.get(self.url)
        
        try:
            # Wait until the name element is available, with a max timeout of 10 seconds
            name_container = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'h3.erpl_title-h3'))
            )
            
            # Extract and clean the text if found
            if name_container:
                name_text = name_container.find_element(By.TAG_NAME, 'em').text.strip()
                self.name = " ".join(name_text.split())  # Join to handle any new lines or extra spaces
            else:
                self.name = None

        except (TimeoutException, NoSuchElementException) as e:
            print(f"MEP name element not found or page took too long to load: {e}")
            self.name = None

        # Call the load_meetings function to load all meetings (if it’s implemented in your class)
        self.load_meetings()

    def load_meetings(self):
        while True:
            try:
                # Wait for the Load More button to be present
                #print("Waiting for the Load More button...")
                load_more_button = WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, "//button[contains(@class, 'europarl-expandable-async-loadmore')]"))
                )
                #print("Load More button found")

                # Scroll into view
                self.driver.execute_script("arguments[0].scrollIntoView(true);", load_more_button)
                time.sleep(1)  # Ensure the button is visible

                # Wait until the button is visible and enabled
                WebDriverWait(self.driver, 10).until(
                    EC.visibility_of(load_more_button)
                )
                WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable(load_more_button)
                )

                # Click using JavaScript
                self.driver.execute_script("arguments[0].click();", load_more_button)
                #print("LOAD MORE button clicked")

                # Allow time for new meetings to load
                time.sleep(2)

            except TimeoutException:
                #print("No more LOAD MORE button to be clicked or button not found")
                break
            except Exception as e:
                print(f"An error occurred: {e}")
                break

        # Now extract all meeting data
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        meeting_elements = soup.find_all('div', class_='erpl_document')
        
        for idx, meeting_element in enumerate(meeting_elements):
            meeting_data = {}

            # Meeting reason (title within span.t-item inside h3)
            reason_tag = meeting_element.find('h3', class_='erpl_document-title erpl_title-h3 mb-1 a-i')
            if reason_tag:
                reason_text = reason_tag.find('span', class_='t-item')
                if reason_text:
                    meeting_data['reason'] = reason_text.text.strip()

            # Date and place
            date_place_tag = meeting_element.find('div', class_='erpl_document-subtitle d-inline')
            if date_place_tag:
                date_tag = date_place_tag.find('time')
                place_tag = date_place_tag.find('span', class_='erpl_document-subtitle-location')
                if date_tag:
                    meeting_data['date'] = date_tag.text.strip()
                if place_tag:
                    meeting_data['place'] = place_tag.text.strip()

            # Capacity
            capacity_tag = meeting_element.find('span', class_='erpl_document-subtitle-capacity')
            if capacity_tag:
                meeting_data['capacity'] = capacity_tag.text.strip()

            # Committee code
            committee_tag = meeting_element.find('span', class_='erpl_badge erpl_badge-committee')
            if committee_tag:
                meeting_data['committee_code'] = committee_tag.text.strip()

            # Meeting with
            meeting_with_tag = meeting_element.find('span', class_='erpl_document-subtitle-author')
            if meeting_with_tag:
                meeting_data['meeting_with'] = meeting_with_tag.text.strip()

            # Store meeting data in self.meetings with a unique key
            self.meetings[f"{idx}"] = meeting_data

    def to_dict(self):
        return {
            "name": self.name,
            #"party": self.mep_party,
            #"origin_country": self.mep_country,
            "meetings": self.meetings
        }

    def close(self):
        self.driver.quit()  # Close the browser when done

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
        return f"https://www.europarl.europa.eu/meps/en/{mep_id}/{name_part}/all-meetings/9"

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
    
    # If no meetings were found, add a note with the MEP's name from the JSON
    if not mep.meetings:
        print(f"No meetings found for {mep_name}. Saving name for reference.")
        mep.name = mep_name  # Ensure the MEP name is set if no data is scraped
        mep.meetings = {"note": "No meetings found for this MEP."}
    
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
    output_file = "cbam_meeting_information/data/all_meetings/MEETINGS_9term.json"
    
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