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
press_release_url = "https://ec.europa.eu/commission/presscorner/home/en?dotyp=&keywords=cbam"

# rss feed url
rss_url = "https://ec.europa.eu/commission/presscorner/api/rss?search?language=en&text=cbam&datefrom=16062019&dateto=12032025&pagesize=20"
feed = feedparser.parse(rss_url)
article_urls = set(entry.link for entry in feed.entries)  # collect article links from rss


# ----------------------------------
#       2. Extract article urls 
# ----------------------------------

# function to extract article urls from the current page
def get_article_urls(driver):
    """extracts all article urls from the current page."""
    page_articles = set()
    articles = driver.find_elements(By.CSS_SELECTOR, "a.ecl-link")

    for article in articles:
        link = article.get_attribute("href")
        if link and "/detail/en/" in link:  # filter press release links
            page_articles.add(link)

    return page_articles

# function to navigate through pagination and extract all article urls
def scrape_all_article_urls():
    """navigates through paginated pages and extracts all article urls."""
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    driver.get(press_release_url)

    while True:
        article_urls.update(get_article_urls(driver))  # update global article_urls set

        try:
            next_button = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "//a[contains(@class, 'ecl-pager__link') and contains(text(), 'Next')]"))
            )

            driver.execute_script("arguments[0].click();", next_button)  # click next page
            time.sleep(2)  # allow time for new articles to load

        except Exception:
            print("no more pages to load.")  # exit loop if no next button
            break

    driver.quit()  # close driver after scraping all pages

# call function to extract urls from all pages
scrape_all_article_urls()

# ---------------------------------
#     3. Scrape Article Content
# ---------------------------------

# function to scrape article content
def scrape_article(url):
    """scrapes article data including title, date, location, and content."""
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    driver.get(url)

    try:
        WebDriverWait(driver, 20).until(
            lambda d: d.execute_script("return document.readyState") == "complete"  # wait for full page load
        )

        WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.CLASS_NAME, "ecl-page-header__title"))  # wait for title to appear
        )

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")  # scroll to trigger dynamic content
        time.sleep(2)  # wait for elements to load

    except Exception:
        print(f"timeout loading page: {url}")
        driver.quit()
        return None

    soup = BeautifulSoup(driver.page_source, "html.parser")  # parse page source with BeautifulSoup

    # extract details
    title = soup.find("h1", class_="ecl-page-header__title")
    title_text = title.get_text(strip=True) if title else "no title found"

    meta_items = soup.find_all("span", class_="ecl-page-header__meta-item")
    date_text = meta_items[1].get_text(strip=True) if len(meta_items) > 1 else "no date found"
    location_text = meta_items[2].get_text(strip=True) if len(meta_items) > 2 else "no location found"

    # extract content specifically from the ecl-content-block__description
    content_block = soup.find("div", class_="ecl-content-block__description")
    text_content = ""

    if content_block:
        # get text only within the ecl-content-block__description, avoiding unwanted elements
        paragraphs = content_block.find_all("p")
        text_content = "\n".join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])

    # if no text found in the content block, we assign a default message
    text_content = text_content if text_content else "no content found in the description block"

    driver.quit()  # close driver after scraping

    return {
        "title": title_text,
        "date": date_text,
        "location": location_text,
        "text": text_content,
        "url": url
    }

# function to scrape and return data for a single url
def scrape_and_store(url):
    return scrape_article(url)

# --------------------------------
#   4. Run parallel scraper 
# --------------------------------

# main function to handle parallel scraping
def main():
    article_list = list(article_urls) #[:5]  limit to first 5 articles for testing

    with ThreadPoolExecutor(max_workers=5) as executor:  # scrape in parallel with 5 threads
        results = list(tqdm(executor.map(scrape_and_store, article_list), 
                                         total=len(article_list), 
                                         desc="scraping articles"))

    print("scraping completed successfully!")

    # print first two results for debugging
    #for result in results[:2]:
    #    print(result)

    # save results to json
    with open("EC_CBAM_articles.json", "w", encoding="utf-8") as f:
        json.dump([r for r in results if r], f, indent=4, ensure_ascii=False)

    print("data saved to EC_CBAM_articles.json")

# run main function
if __name__ == "__main__":
    main()