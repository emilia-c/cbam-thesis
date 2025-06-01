import json
import time
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException

JSON_FILENAME = "MEP_MEETINGS_CBAM_01.04.2025.json"

""" CHAT GPT PROMPT: how can I update the code so that it runs faster? I also want to save the json after every mep so that I don't lose the data if the script crashes"""
class MEP:
    def __init__(self, url):
        self.url = url
        self.name = None
        self.group = None
        self.mep_country = None
        self.mep_national_party = None
        self.meetings = {}

        # **Optimized Selenium Setup**
        chrome_options = Options()
        chrome_options.add_argument("--headless")  
        chrome_options.add_argument("--disable-gpu")  # Speed up headless mode
        chrome_options.add_argument("--no-sandbox")  # Prevent unnecessary security overhead
        chrome_options.add_argument("--disable-dev-shm-usage")  # Avoid shared memory issues
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        self.driver = webdriver.Chrome(service=ChromeService(), options=chrome_options)

    def get_mep_data(self):
        self.driver.get(self.url)
        
        # **Wait for key elements to load (instead of time.sleep)**
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'span.sln-member-name'))
            )
        except TimeoutException:
            print(f"Warning: Page elements not found for {self.url}")

        # **Scrape MEP details**
        try: 
            self.name = self.driver.find_element(By.CSS_SELECTOR, 'span.sln-member-name').text.strip()
        except NoSuchElementException: 
            self.name = "N/A"

        try:
            self.group = self.driver.find_element(By.CSS_SELECTOR, 'h3.sln-political-group-name').text.strip()
        except NoSuchElementException: 
            self.group = "N/A"

        try:
            country_text = self.driver.find_element(By.CSS_SELECTOR, 'div.erpl_title-h3.mt-1.mb-1').text.strip()
            self.mep_country = country_text.split("(")[-1].split(")")[0] if "(" in country_text else "N/A"
        except NoSuchElementException:
            self.mep_country = "N/A"

        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        try:
            national_party_text = soup.select_one('div.erpl_title-h3.mt-1.mb-1').text.strip()
            self.mep_national_party = national_party_text.split(" - ")[1].split(" (")[0] if " - " in national_party_text else "N/A"
        except AttributeError:
            self.mep_national_party = "N/A"

        # **Scrape Meetings**
        self.load_meetings()

    def load_meetings(self): 
        while True:
            try:
                load_more_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'europarl-expandable-async-loadmore')]"))
                )
                self.driver.execute_script("arguments[0].click();", load_more_button)
                time.sleep(1)  # Allow time for content to load
            except TimeoutException:
                break  # No more meetings to load

        # **Extract Meetings**
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        meeting_elements = soup.find_all('div', class_='erpl_document')

        for idx, meeting_element in enumerate(meeting_elements):
            meeting_data = {}

            reason_tag = meeting_element.select_one('h3.erpl_document-title span.t-item')
            if reason_tag:
                meeting_data['reason'] = reason_tag.text.strip()

            date_tag = meeting_element.select_one('div.erpl_document-subtitle time')
            if date_tag:
                meeting_data['date'] = date_tag.text.strip()

            place_tag = meeting_element.select_one('div.erpl_document-subtitle-location')
            if place_tag:
                meeting_data['place'] = place_tag.text.strip()

            capacity_tag = meeting_element.select_one('span.erpl_document-subtitle-capacity')
            if capacity_tag:
                meeting_data['capacity'] = capacity_tag.text.strip()

            committee_tag = meeting_element.select_one('span.erpl_badge-committee')
            if committee_tag:
                meeting_data['committee_code'] = committee_tag.text.strip()

            meeting_with_tag = meeting_element.select_one('span.erpl_document-subtitle-author')
            if meeting_with_tag:
                meeting_data['meeting_with'] = meeting_with_tag.text.strip()

            self.meetings[f"{idx}"] = meeting_data

    def to_dict(self):
        # **Filter CBAM-related meetings**
        filtered_meetings = {
            k: v for k, v in self.meetings.items()
            if "CBAM" in v.get("meeting_with", "") or "CBAM" in v.get("reason", "")
            or "Carbon Border Adjustment Mechanism" in v.get("meeting_with", "") 
            or "Carbon Border Adjustment Mechanism" in v.get("reason", "")
        }

        return {
            "name": self.name,
            "group": self.group,  
            "origin_country": self.mep_country,
            "national_party": self.mep_national_party,  
            "meetings": filtered_meetings if filtered_meetings else "No CBAM meetings recorded"
        }

    def close(self):
        self.driver.quit()

def load_existing_meps(filename):
    """ Load existing MEPs from the JSON file. If it doesn’t exist, create an empty list. """
    try:
        with open(filename, "r", encoding="utf-8") as infile:
            data = json.load(infile)
            return {mep["name"] for mep in data}, data  # Return MEP names as a set + existing data
    except (FileNotFoundError, json.JSONDecodeError):
        return set(), []  # Return empty set & list if no file

def save_mep_data(mep_data, existing_data, filename):
    """ Save new MEP data to JSON, preserving existing records. """
    existing_data.append(mep_data)
    with open(filename, "w", encoding="utf-8") as outfile:
        json.dump(existing_data, outfile, indent=4, ensure_ascii=False)

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
    soup = BeautifulSoup(response.content, 'html.parser')
    
    mep_links = []
    for mep in soup.select('a.erpl_member-list-item-content'):
        mep_url_base = mep['href']
        mep_name = mep.select_one('.erpl_title-h4.t-item').text.strip()
        mep_id = mep_url_base.split('/')[-1]
        mep_url = construct_mep_url(mep_name, mep_id)
        mep_links.append((mep_name, mep_url))  # ✅ Now stores (name, url)

    return mep_links

def scrape_meps(url):
    mep = MEP(url)
    try:
        mep.get_mep_data()
        return mep.to_dict()
    finally:
        mep.close()

def main():
    """ Main function: Loads existing data, skips processed MEPs, and scrapes only new ones. """
    existing_meps, existing_data = load_existing_meps(JSON_FILENAME)
    mep_links = get_mep_links()

    for mep_name, mep_url in tqdm(mep_links, desc="Scraping MEPs"):
        if mep_name in existing_meps:
            print(f"Skipping {mep_name} (Already processed)")
            continue  

        try:
            mep_data = scrape_meps(mep_url)
            save_mep_data(mep_data, existing_data, JSON_FILENAME)
            print(f"✅ Saved data for {mep_name}")
        except Exception as e:
            print(f"❌ Error scraping {mep_name}: {e}")

        time.sleep(1)  # Reduce request load

if __name__ == "__main__":
    main()

#def main():
#    mep_links = get_mep_links()
#    mep_links = mep_links[:5]
    
#    for mep_url in tqdm(mep_links, desc="Scraping MEPs"):
#        try:
#            mep_data = scrape_meps(mep_url)
#            save_mep_data(mep_data)
#        except Exception as e:
#            print(f"Error scraping {mep_url}: {e}")

#        time.sleep(1)  # Reduce request load

#if __name__ == "__main__":
#    main()
