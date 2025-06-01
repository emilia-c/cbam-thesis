import os
import time

# where all file paths, keyword search stored
from stakeholder_data_extraction_pipeline import config 

# for ddg url extraction, download 
from .ddg_urls.ddg_search import run_search_pipeline, load_existing_results
from .ddg_urls.ddg_download import run_downloader
from .ddg_urls import ddg_file_detection

# for creating, updating and cleaning the database
from .database.database_setup import setup_db
from .database.database_update import read_org_data, add_ddg_urls_csv
from .database.database_clean import remove_noresults_urls, remove_duplicate_urls, filter_unwanted_urls

# for extracting text from identified pdf and html files
from .text_extraction import html_text_extraction 
from .text_extraction import pdf_text_extraction 

# OBS MUST RUN FROM TERMINAL FROM DATA FOLDER VIA CODE python -m stakeholder_data_extraction_pipeline.main

def main():
    " Main pipeline for running url data extraction"
    
    # ensure required directories exist  # create directories if they don't exist
    os.makedirs(config.DATA_DIR, exist_ok=True)             # data directory
    os.makedirs(config.URL_DOWNLOADS_DIR, exist_ok=True)    # url downloads directory
    os.makedirs(config.RAW_DATA_DIR, exist_ok=True)         # MIGHT NOT NEED
    os.makedirs(config.PROCESSED_DATA_DIR, exist_ok=True)   # MIGHT NOT NEED
    os.makedirs(config.LOGS_DIR, exist_ok=True)             # record and error log directory
    os.makedirs(config.EXTRACTED_HTML_DIR, exist_ok=True)   # html extraction directory
    os.makedirs(config.EXTRACTED_PDF_DIR, exist_ok=True)    # pdf extraction directory       

    # 1. Set up database
    print("Setting up database...", flush=True)
    setup_db()  # create database (if they don't already exist)
    
    # 2. Search DDG for stakeholder documents
    print("\nSearching DDG for stakeholder documents...")

    # Loop until no organizations remain in a "rate_limited" or "error" state.
    # OBS: need to play around with this because the DDGs backend often can return ratelimit error
    
    while True:
        ddg_results, org_df = run_search_pipeline()  # Run the search pipeline
        # Check CSV for any rate-limited orgs remaining.
        _, rate_limited_orgs = load_existing_results()
        if not rate_limited_orgs:
            print(" 🎉 All organizations processed successfully 🎉 ")
            break  # Exit loop if no org is rate-limited
        
        print("Some organizations are still rate-limited. Retrying in 10 seconds...")
        time.sleep(10)  # Wait before retrying

    # 3. Add organization and url into database
    print("\nAdding organization and DDG url data into database...")
    read_org_data (org_df)    # add stakeholder information into db
    add_ddg_urls_csv()        # add url information in db
    
    # 4. Clean url database
    print("\nCleaning up database: filtering unwanted urls...")
    filter_unwanted_urls()   # remove social media and google searches
    remove_noresults_urls()  # remove orgs with no urls extracted, or extra error/timed_out messages
    remove_duplicate_urls()  # remove duplicate urls              ** THINK WHAT INFORMATION LOST

    # 5. Download files (locally) from extracted urls
    print("\nDownloading files from ddg urls...")
    run_downloader() # download pending urls asynchronously
    
    # 6. Extracting text from downloaded files
    print("\nExtracting text from HTML and PDF files")
    html_text_extraction.run_html_extraction() # process html files
    pdf_text_extraction.run_pdf_extraction()   # process pdf files 

    print("\nStakeholder document pipeline completed successfully!")

if __name__ == "__main__":
    main()  # start pipeline