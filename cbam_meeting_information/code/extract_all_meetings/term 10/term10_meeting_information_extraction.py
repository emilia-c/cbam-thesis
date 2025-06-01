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

class MEP:
    def __init__(self, url):
        self.url = url                  # mep's website url
        self.name = None                # mep name
        self.group = None               # mep political group
        self.mep_country = None         # mep country of origin
        self.mep_national_party = None  # mep national party
        self.meetings = {}              # dic to store meetings
        
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
        group_tag = self.driver.find_element(By.CSS_SELECTOR, 'h3.erpl_title-h3.mt-1.sln-political-group-name')
        if group_tag:
            self.group = group_tag.text.strip()
        else: 
            self.group = "N/A"

        # scrape mep country of origin
        mep_country_tag = self.driver.find_element(By.CSS_SELECTOR, 'div.erpl_title-h3.mt-1.mb-1')
        if mep_country_tag:
            country_text = mep_country_tag.text.strip()
            last_open_index = country_text.rfind("(")
            last_close_index = country_text.rfind(")")
        
            if last_open_index != -1 and last_close_index != -1 and last_open_index < last_close_index:
                self.mep_country = country_text[last_open_index + 1:last_close_index].strip()
            else:
                self.mep_country = "N/A"  # fallback in case the format is unexpected

        # scrape mep national party
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        mep_national_party_tag = soup.find('div', class_='erpl_title-h3 mt-1 mb-1')
        if mep_national_party_tag:
            national_party_text = mep_national_party_tag.text.strip()
            if " - " in national_party_text:
                # extract the text before the parentheses and after the hyphen
                self.mep_national_party = national_party_text.split(" - ")[1].split(" (")[0]
            else:
                print(f"Warning: MEP National Party information not found or formatted incorrectly for {self.url}")
                self.mep_national_party = "N/A"
        
        # load all MEP meetings
        self.load_meetings()

    def load_meetings(self): 
        while True:
            try:
                # wait for the Load More button to be present
                load_more_button = WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, "//button[contains(@class, 'europarl-expandable-async-loadmore')]"))
                )

                # scroll into view
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

                # Allow time for new meetings to load
                time.sleep(2)

            except TimeoutException:
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
            "group": self.group,  
            "origin_country": self.mep_country,
            "national_party": self.mep_national_party,  
            "meetings": self.meetings
        }

    def close(self):
        self.driver.quit()  # Close the browser when done

# construct the MEP url from the base url of a list of all MEPs, need to extract their name from here as the page with assistant information includes their information
def construct_mep_url(mep_name, mep_id):
    names = mep_name.split()
    first_names = []
    last_names = []

    for name in names:
        if name.isupper():
            last_names.append(name)
        else:
            first_names.append(name)

    first_name_part = '+'.join(first_names)
    last_name_part = '+'.join(last_names)

    if last_names:
        if first_names:
            return f"https://www.europarl.europa.eu/meps/en/{mep_id}/{first_name_part}+{last_name_part}/meetings/past#detailedcardmep"
        else:
            return f"https://www.europarl.europa.eu/meps/en/{mep_id}/{last_name_part}/meetings/past#detailedcardmep"
    else:
        return f"https://www.europarl.europa.eu/meps/en/{mep_id}/{first_name_part}/home"

def get_mep_links():
    base_url = "https://www.europarl.europa.eu/meps/en/full-list/all"
    response = requests.get(base_url)
    response.raise_for_status()  # Raise an error for bad responses
    soup = BeautifulSoup(response.content, 'html.parser')
    
    mep_links = []
    for mep in soup.select('a.erpl_member-list-item-content'):
        mep_url_base = mep['href']
        mep_name = mep.select_one('.erpl_title-h4.t-item').text.strip()
        mep_id = mep_url_base.split('/')[-1]
        mep_url = construct_mep_url(mep_name, mep_id)
        mep_links.append(mep_url)

    return mep_links

#def scrape_meps(url):
#    mep = MEP(url)
#    try: 
#        mep.get_mep_data()
#        return mep.to_dict()
#    finally: 
#        mep.close() # close broswer

def scrape_meps(url):
    mep = MEP(url)
    try:
        mep.get_mep_data()
        
        # Filter meetings that contain CBAM or Carbon Border Adjustment Mechanism
        filtered_meetings = {
            k: v for k, v in mep.meetings.items()
            if "CBAM" in v.get("meeting_with", "") or "CBAM" in v.get("reason", "") 
            or "Carbon Border Adjustment Mechanism" in v.get("meeting_with", "") 
            or "Carbon Border Adjustment Mechanism" in v.get("reason", "")
        }

        # Modify MEP data based on whether relevant meetings were found
        mep_data = mep.to_dict()
        if filtered_meetings:
            mep_data["meetings"] = filtered_meetings  # Store only CBAM-related meetings
        else:
            mep_data["meetings"] = "No CBAM meetings recorded"  # Default message
        
        return mep_data  # Always return MEP data
    
    finally:
        mep.close()  # Ensure browser closes

#def main():
#    mep_links = get_mep_links()  # Get all MEP links
    # limit to the first five meps for testing
    #mep_links = mep_links[:5]

    # Try to resume from existing data
#    try:
#        with open("MEP_MEETINGS_01.04.2025.json", "r", encoding="utf-8") as infile:
#            all_mep_data = json.load(infile)
#    except (FileNotFoundError, json.JSONDecodeError):
#        all_mep_data = []  # Start fresh if file is missing or corrupted

#    for mep_url in tqdm(mep_links, desc="Scraping MEPs"):
#        try:
#            mep_data = scrape_meps(mep_url)
#            all_mep_data.append(mep_data)

            # Save after every MEP
#            with open("MEP_MEETINGS_01.04.2025.json", "w", encoding="utf-8") as outfile:
#                json.dump(all_mep_data, outfile, indent=4, ensure_ascii=False)

#        except Exception as e:
#            print(f"Error scraping {mep_url}: {e}")

#        time.sleep(1)  # Avoid overwhelming the server
def main():
    mep_links = get_mep_links()  # Get all MEP links
    mep_links = mep_links[:5]
    
    # Try to resume from existing filtered data
    try:
        with open("MEP_MEETINGS_CBAM_01.04.2025.json", "r", encoding="utf-8") as infile:
            all_mep_data = json.load(infile)
    except (FileNotFoundError, json.JSONDecodeError):
        all_mep_data = []  # Start fresh if file is missing or corrupted

    for mep_url in tqdm(mep_links, desc="Scraping MEPs"):
        try:
            mep_data = scrape_meps(mep_url)
            if mep_data:  # Save only if there are relevant meetings
                all_mep_data.append(mep_data)

        except Exception as e:
            print(f"Error scraping {mep_url}: {e}")

        time.sleep(1)  # Avoid overwhelming the server

    # Save only filtered results
    with open("MEP_MEETINGS_CBAM_01.04.2025.json", "w", encoding="utf-8") as outfile:
        json.dump(all_mep_data, outfile, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    main()