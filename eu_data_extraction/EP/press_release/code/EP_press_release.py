import json
import time
import feedparser
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# set up selenium options
chrome_options = Options()
chrome_options.add_argument("--headless=new")          # use new headless mode
chrome_options.add_argument("--disable-gpu")           # disable gpu
chrome_options.add_argument("--no-sandbox")            # helps in linux envs
chrome_options.add_argument("--disable-dev-shm-usage") # prevent crashes due to shared memory
chrome_options.add_argument("--window-size=1920,1080") # set to larger window size
chrome_options.add_argument("--log-level=3")           # reduce chrome log verbosity

# ----------------------------------
#          1. Define URLs
# ----------------------------------

# base url for press releases
press_release_url = "https://www.europarl.europa.eu/news/en?searchQuery=CBAM" # filtering kind of terrible, got best results with CBAM

# global dictionary to hold article details from the search page (url mapped to date)
article_details = {}

# ----------------------------------
#       2. Extract article urls 
# ----------------------------------

# function to extract article urls from the current page
def get_article_info(driver):
    """
    Extracts article URLs and dates from the search results page using the new HTML structure.
    Returns a dictionary where keys are URLs and values are the publication date.
    """
    articles_info = {}
    # Find all article containers; adjust the selector to match the new structure
    article_containers = driver.find_elements(By.CSS_SELECTOR, "div.ep_gridcolumn-content")
    
    for container in article_containers:
        try:
            # The article link is inside an h3 with class "ep-a_heading"
            link_element = container.find_element(By.CSS_SELECTOR, "h3.ep-a_heading a")
            url = link_element.get_attribute("href")
            
            # Extract the publication date from the sibling "ep-layout_date" element
            date_element = container.find_element(By.CSS_SELECTOR, "div.ep-layout_date time")
            date_text = date_element.get_attribute("datetime") or date_element.text.strip()
            
            if url and "/press-room/" in url:  # filtering for press releases
                articles_info[url] = date_text
        except Exception:
            continue  # if any element is not found, skip this container
    return articles_info

# function to navigate through pagination and extract all article urls
def scrape_all_article_info():
    """
    Navigates through paginated search result pages to collect all article URLs and dates.
    Updates the global article_details dictionary.
    """
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    driver.get(press_release_url)
    
    while True:
        article_details.update(get_article_info(driver))
        
        try:
            # Find the "Load more" button by its ID
            load_more_button = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, "continuesLoading_button"))
            )
            # Scroll into view and click
            driver.execute_script("arguments[0].scrollIntoView(true);", load_more_button)
            time.sleep(1)
            WebDriverWait(driver, 10).until(EC.visibility_of(load_more_button))
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable(load_more_button))
            driver.execute_script("arguments[0].click();", load_more_button)
            time.sleep(2)  # allow time for new articles to load
            
        except Exception:
            print("No more pages to load.")
            break
    
    driver.quit()

# Call function to extract URLs and dates from all pages
scrape_all_article_info()

# ---------------------------------
#     3. Scrape Article Content
# ---------------------------------

# function to scrape article content
def scrape_article(url):
    """
    Scrapes article data including title, document type, and text content
    from the article page using the new HTML structure.
    Combines text from fact lists, chapo, and main content paragraphs.
    """
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    driver.get(url)
    
    try:
        # Wait for the page to fully load
        WebDriverWait(driver, 20).until(lambda d: d.execute_script("return document.readyState") == "complete")
        # Optionally, wait for a key element; if title is not available, adjust accordingly
        # For instance, if there is a header for the article, wait on it
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.ep_gridrow"))
        )
        # Scroll to the bottom to trigger any lazy-loaded content
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
    except Exception:
        print(f"Timeout loading page: {url}")
        driver.quit()
        return None
    
    soup = BeautifulSoup(driver.page_source, "html.parser")
    
    # Extract title if available. This selector may need adjusting based on actual page structure.
    title_elem = soup.find("h1", class_="ep_title")
    if title_elem:
        span_elem = title_elem.find("span", class_="ep_name")  # Find the specific span inside
        title_text = span_elem.get_text(strip=True) if span_elem else "no title found"
    else:
        title_text = "no title found"

    # Extract document type from the category element
    doc_type_elem = soup.find("div", class_="ep-p_text ep-layout_category")
    document_type = doc_type_elem.find("span", class_="ep_name").get_text(strip=True) if doc_type_elem else "unknown"
    
    # Combine text from different sections
    
    # 1. Fact list extraction
    fact_texts = []
    fact_block = soup.find("div", class_="ep-a_facts")
    if fact_block:
        fact_items = fact_block.find_all("li")
        fact_texts = [item.get_text(" ", strip=True) for item in fact_items]
    
    # 2. Chapo extraction (short intro text)
    chapo_text = ""
    chapo_block = soup.find("div", class_="ep-a_text ep-layout_chapo")
    if chapo_block:
        p = chapo_block.find("p")
        if p:
            chapo_text = p.get_text(" ", strip=True)
    
    # 3. Main text extraction from paragraphs with class "ep-wysiwig_paragraph"
    main_paragraphs = soup.find_all("p", class_="ep-wysiwig_paragraph")
    main_text = "\n".join([p.get_text(" ", strip=True) for p in main_paragraphs])
    
    # Combine the extracted texts
    combined_text = "\n\n".join(
        ["\n".join(fact_texts)] if fact_texts else [] +
        ([chapo_text] if chapo_text else []) +
        ([main_text] if main_text else [])
    )
    
    # If no text found, assign default message
    if not combined_text.strip():
        combined_text = "no content found"
    
    driver.quit()
    
    # Use the date from the search page stored in the global article_details
    date_text = article_details.get(url, "no date found")
    
    return {
        "title": title_text,
        "date": date_text,
        "document_type": document_type,
        "text": combined_text,
        "url": url
    }

def scrape_and_store(url):
    return scrape_article(url)

# --------------------------------
#   4. Run parallel scraper 
# --------------------------------

# main function to handle parallel scraping
def main():
    article_list = list(article_details.keys()) #[:5]  limit to first 5 articles for testing

    with ThreadPoolExecutor(max_workers=5) as executor:  # scrape in parallel with 5 threads
        results = list(tqdm(executor.map(scrape_and_store, article_list), 
                                         total=len(article_list), 
                                         desc="scraping articles"))

    print("scraping completed successfully!")

    # print first two results for debugging
    for result in results[:2]:
        print(result)

    # save results to json
    with open("EP_CBAM_articles.json", "w", encoding="utf-8") as f:
        json.dump([r for r in results if r], f, indent=4, ensure_ascii=False)

    print("data saved to EP_CBAM_articles.json")

# run main function
if __name__ == "__main__":
    main()