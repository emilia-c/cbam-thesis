import os

# base directory of the project  # all file paths will be relative to this
BASE_DIR = "master_thesis_2025/stakeholder_data_extraction_pipeline"

# directories and file paths  
DATA_DIR = os.path.join(BASE_DIR, "data")                          # folder to store downloads and intermediate files
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw_data")                  # folder to store raw data
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed_data")      # folder to store processed data
URL_DOWNLOADS_DIR = os.path.join(RAW_DATA_DIR, "url_downloads")    # folder to store url downloaded 
LOGS_DIR = os.path.join(RAW_DATA_DIR, "logs")                      # folder to store records/error logs
EXTRACTED_HTML_DIR = os.path.join(PROCESSED_DATA_DIR, "html_text") # folder to store extracted html json
EXTRACTED_PDF_DIR = os.path.join(PROCESSED_DATA_DIR, "pdf_text")   # folder to store extracted pdf json
DB_PATH = os.path.join(DATA_DIR, "cbam_data.db")                   # sqlite database file

# stakeholder settings
STAKEHOLDER_FILE = "master_thesis_2025/stakeholder_data_extraction_pipeline/data/input_data/organisation_titles.xlsx" # excel with stakeholder info
STAKEHOLDER_SHEET = "clean"  # sheet name in excel

# search setting
SEARCH_QUERY = "+CBAM future" # FIRST TRY "CBAM (future OR issue OR concern OR strategy OR feedback)"  # keyword for ddg search
MAX_RESULTS = 20    # max ddg results per organization

# filtering settings  
PLATFORMS = ['linkedin', 'facebook', 'instagram', 'twitter', 'tiktok', 'tandfonline', 'wikipedia', 'sciencedirect', 'springer', 'researchgate', 'glassdoor']  # platforms to filter
GOOGLE_SEARCH_PATTERN = r"https?://(www\.)?google\.com/search\?.*"         # pattern to remove google search urls

# extraction settings  # paywall keywords and pdf extraction batch size
PAYWALL_WORDS = ["log in to read", "paywall", "membership required", "register to continue"]  # paywall indicators
PDF_BATCH_SIZE = 8  # batch size for pdf extraction tasks