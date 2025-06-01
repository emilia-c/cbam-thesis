import requests
from bs4 import BeautifulSoup
import json
import time
from tqdm import tqdm
import pandas as pd

# STEP 1: SCRAPE ALL OF THE DATA 
class MEP:
    def __init__(self, url):
        """ Define elements to be included in MEP class"""
        
        self.url = url
        self.name = None
        self.mep_group = None
        self.mep_national_party = None
        self.mep_country = None
        self.mep_dob = None
        self.mep_linkedin = None
        self.mep_email = None
        self.mep_fb = None
        self.mep_instagram = None
        self.mep_twitter = None

    def get_mep_data(self):
        """ Scrape all the MEP information of interest including: 
            - MEP name
            - MEP group
            - MEP national party 
            - MEP country of origin
            - MEP social media handles 
            HTML tags were found via manual exploration of the page
            MEP URL format also discovered via manual exploration of MEP's pages
            """
        
        response = requests.get(self.url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # ccrape MEP Name
        name_tag = soup.find('span', class_='sln-member-name')
        if name_tag:
            self.name = name_tag.text.strip()
        else:
            print(f"Warning: MEP name not found for {self.url}")
            self.name = "N/A"

        # Scrape MEP Group
        mep_group_tag = soup.find('h3', class_='erpl_title-h3 mt-1 sln-political-group-name')
        if mep_group_tag:
            self.mep_group = mep_group_tag.text.strip()
        else:
            print(f"Warning: MEP Group information not found for {self.url}")
            self.mep_group = "N/A"
        
        # Scrape MEP National Party
        mep_national_party_tag = soup.find('div', class_='erpl_title-h3 mt-1 mb-1')
        if mep_national_party_tag:
            national_party_text = mep_national_party_tag.text.strip()
            if " - " in national_party_text:
                # Extract the text before the parentheses and after the hyphen (format found via manual exploration)
                self.mep_national_party = national_party_text.split(" - ")[1].split(" (")[0]
            else:
                print(f"Warning: MEP National Party information not found or formatted incorrectly for {self.url}")
                self.mep_national_party = "N/A"

        # Scrape MEP Country
        if mep_national_party_tag:
            country_text = mep_national_party_tag.text.strip()
            if "(" in country_text and ")" in country_text:
                self.mep_country = country_text[country_text.find("(")+1:country_text.find(")")]
            else: 
                self.mep_country = "N/A"
        
        # Scrape MEP dob
        mep_dob_tag = soup.find('time', class_='sln-birth-date')
        if mep_dob_tag:
            self.mep_dob = mep_dob_tag['datetime']  # the "datetime" attribute contains the DOB
        else:
            print(f"Warning: Date of birth not found for {self.url}")
            self.mep_dob = "N/A"

        # Extract the LinkedIn Profile URL
        mep_linkedin_tag = soup.find('a', class_='link_linkedin')
        if mep_linkedin_tag:
            self.mep_linkedin = mep_linkedin_tag['href']  # The "href" attribute contains the URL
        else:
            print(f"Warning: LinkedIn URL not found for {self.url}")
            self.mep_linkedin = "N/A"

        # Extract Email
        mep_email_tag = soup.find('a', class_='link_email')
        if mep_email_tag:
            email_href = mep_email_tag['href']  # Extract the 'href' attribute from the tag
            email_cleaned = email_href.replace('mailto:', '')  
            email_cleaned = email_cleaned.replace('[at]', '@')  
            email_cleaned = email_cleaned.replace('[dot]', '.')  
            self.mep_email = email_cleaned  # Assign the cleaned email to self.mep_email
        else:
            print(f"Warning: Email not found for {self.url}")
            self.mep_email = "N/A"

        # Extract Instagram URL
        mep_instagram_tag = soup.find('a', class_='link_instagram')
        if mep_instagram_tag:
            self.mep_instagram = mep_instagram_tag['href']  # Get the href attribute for the Instagram link
        else:
            print(f"Warning: Instagram URL not found for {self.url}")
            self.mep_instagram = "N/A"

        # Extract X (formerly Twitter) URL
        mep_twitter_tag = soup.find('a', class_='link_twitt')
        if mep_twitter_tag:
            self.mep_twitter = mep_twitter_tag['href']  # Get the href attribute for the X link
        else:
            print(f"Warning: X (Twitter) URL not found for {self.url}")
            self.mep_twitter = "N/A"

        # Extract Facebook URL
        mep_facebook_tag = soup.find('a', class_='link_fb')
        if mep_facebook_tag:
            self.mep_fb = mep_facebook_tag['href']  # Get the href attribute for the Facebook link
        else:
            print(f"Warning: Facebook URL not found for {self.url}")
            self.mep_fb = "N/A"

    def to_dict(self):
        # Create a dictionary representation of the MEP data
        return {
            "name": self.name,
            "mep_group": self.mep_group,
            "mep_national_party": self.mep_national_party,
            "country": self.mep_country,
            "date of birth": self.mep_dob, 
            "mep_linkedin": self.mep_linkedin,
            "mep_email": self.mep_email, 
            "mep_facebook": self.mep_fb, 
            "mep_instagram": self.mep_instagram,
            "mep_x": self.mep_twitter
        }

# construct the MEP url from the base url of a list of all MEPs, need to extract their name from here
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
            return f"https://www.europarl.europa.eu/meps/en/{mep_id}/{first_name_part}+{last_name_part}/home"
        else:
            return f"https://www.europarl.europa.eu/meps/en/{mep_id}/{last_name_part}/home"
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

# RUNNING THE THING
def scrape_meps(url):
    mep = MEP(url)
    try: 
        mep.get_mep_data()
        return mep.to_dict() # return dict directly
    finally: 
        mep.close() # close broswer

def main():
    mep_links = get_mep_links()  # Get all MEP links

    # Limit to the first five MEPs for testing
    #mep_links = mep_links[:5]

    all_mep_data = []  # List to hold all MEP data as dictionaries

    # Use tqdm to show a progress bar for scraping MEPs
    for mep_url in tqdm(mep_links, desc="Scraping MEPs"):
        mep_data = scrape_meps(mep_url)  # Get data for each MEP
        all_mep_data.append(mep_data)    # Append MEP data dictionary to the list
        time.sleep(1)  

    # Save the data to a JSON file
    with open("10term_meps.json", "w", encoding="utf-8") as outfile:
        json.dump(all_mep_data, outfile, ensure_ascii=False, indent=4)

    print(json.dumps(all_mep_data, indent=4, ensure_ascii=False))  # print result to console for debugging

if __name__ == "__main__":
    main()    