import os
import json
import csv
import sqlite3
from stakeholder_data_extraction_pipeline import config
from trafilatura import extract

# define error log file path
HTML_ERROR_LOG = os.path.join(config.LOGS_DIR, "html_extract_errors.csv")  # error log for html extraction

def add_html_url_data():
    """ add html extraction pending data from urls table into html_text table, only for html files with success download""" 
    conn = None # ensure conn exists
    
    try: 
        # connect to db
        conn = sqlite3.connect(config.DB_PATH)
        c = conn.cursor()

        # insert pending htmls
        c.execute("""INSERT INTO html_text (id, organization_id, html_file, extract_status)
                    SELECT u.id, u.organization_id, u.file_path, 'pending'
                    FROM urls u
                    WHERE u.paywall_status = 'unknown'
                    AND u.download_status = 'success'
                    AND u.file_type = 'html'
                """)
        
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"Error adding html info: {e}")
    
    finally:
        if conn:
            conn.close() # close db connection

def get_pending_htmls():
    """ get all html records pending extraction, returns list of tuples (id, organization_id, html_file)"""
    conn = None # ensure conn exists
    
    try: 
        # connect to db
        conn = sqlite3.connect(config.DB_PATH)
        c = conn.cursor()

        # get all html records with pending extraction status
        c.execute("SELECT id, organization_id, html_file FROM html_text WHERE extract_status = 'pending'")  # query pending
        rows = c.fetchall()

        if not rows: 
            print("no pending html records")
        return rows

    except sqlite3.Error as e:
        print(f"Error when getting pending htmls: {e}")
    
    finally:
        if conn:
            conn.close() # close db connection
    
    return rows # returns list of tuples (id, organization_id, html_file)

def extract_html_content(html_file_path):
    """ using library trafilatura (Barbaresi, 2021) extract text and metadata from html file """
    try: 
        # read in raw html and decode as utf-8 (except errors and replace if encoding erros)
        with open(html_file_path, "rb") as file:
            raw = file.read()  # read raw content
        try:
            html_text = raw.decode("utf-8")  # try decoding as utf-8
        except UnicodeDecodeError:
            html_text = raw.decode("utf-8", errors="replace")  # replace encoding errors
        
        # extract html content using trafilatura's extract
        extracted = extract(html_text, 
                            output_format="json", # set output format
                            with_metadata=True,   # include metadata
                            include_images=True)  # include images
        
        return extracted  # return json string
    except FileNotFoundError:
        print(f'File not found: {html_file_path}') 
        return None
    except Exception as e: 
        print(f'Error extracting html content from {html_file_path}: {e}')
        return None

def clean_json(raw_json):
    """ keep only relevant fields from extraction  # returns cleaned dict """
    try:
        data = json.loads(raw_json)  # parse json
        cleaned = {
            "title": data.get("title", ""),             # get title
            "author": data.get("author") or "unknown",  # get author
            "source": data.get("source-hostname", ""),  # get source
            "date": data.get("date", ""),               # get date
            "text": data.get("raw_text", ""),           # get text
            "images": data.get("image", ""),            # get images
            "tags": data.get("tags", ""),               # get tags
            "excerpt": data.get("excerpt", ""),         # get excerpt
            "categories": data.get("categories", "")    # get categories
        }
        return cleaned  # return cleaned json
    
    except Exception as e:
        print(f"error cleaning json: {e}")  # print error
        return None

def save_extracted_json(file_id, organization_id, json_data):
    """ save cleaned json to file and return path, filename based on org and id """
    
    cleaned = clean_json(json_data)  # clean json
    if cleaned:
        filename = f"{organization_id}_{file_id}.json"                 # build filename
        json_path = os.path.join(config.EXTRACTED_HTML_DIR, filename)  # build full path
        
        # save json path and cleaned json 
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(cleaned, f, ensure_ascii=False, indent=4)  # save json
        return json_path  # return saved file path
    
    return None

def log_html_error(file_id, organization_id, file_path, error_message):
    """ log extraction error to csv """ 
    if not os.path.exists(HTML_ERROR_LOG):
        # if the log file doesn't exist, create it and add a header row
        with open(HTML_ERROR_LOG, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["file_id", "organization_id", "file_path", "error_message"])  # write header

    # if errors occur, log them in html_error log along with the file id, org id and file path
    with open(HTML_ERROR_LOG, "a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([file_id, organization_id, file_path, error_message])  # log error

def extract_all_html():
    """ run extraction for all pending html files, update html_text table with result"""
    conn = None # ensure conn exists 
    
    try: 
        # connect to db
        conn = sqlite3.connect(config.DB_PATH)
        c = conn.cursor()
        pending = get_pending_htmls()  # get pending htmls from db
        
        for file_id, org_id, file_path in pending:
            full_path = os.path.join(config.URL_DOWNLOADS_DIR, file_path)  # full downloaded file path
            
            try:
                # try to extract html content, update db if successful or not
                extracted = extract_html_content(full_path)  # extract html content
                if extracted:
                    json_path = save_extracted_json(file_id, org_id, extracted)  # save extracted json
                    status = "success"  # mark success
                else:
                    json_path = None
                    status = "failure"  # mark failure
                c.execute("""UPDATE html_text
                            SET extracted_text_path = ?, extract_status = ?, timestamp = CURRENT_TIMESTAMP
                            WHERE id = ?""", (json_path, status, file_id))  # update record
            
            except Exception as e:
                log_html_error(file_id, org_id, file_path, str(e))  # log error
        
        conn.commit()  # commit changes

    except sqlite3.Error as e:
            print(f"Error extracting html: {e}")
        
    finally:
        if conn:
            conn.close() # close db connection

def run_html_extraction():
    """ run the full html extraction pipeline, add pending data then extract """
    add_html_url_data()  # insert pending html records from urls
    extract_all_html()  # process all pending html extractions


# SOURCES 
#@inproceedings{barbaresi-2021-trafilatura,
#  title = {{Trafilatura: A Web Scraping Library and Command-Line Tool for Text Discovery and Extraction}},
#  author = "Barbaresi, Adrien",
#  booktitle = "Proceedings of the Joint Conference of the 59th Annual Meeting of the Association for Computational Linguistics and the 11th International Joint Conference on Natural Language Processing: System Demonstrations",
#  pages = "122--131",
#  publisher = "Association for Computational Linguistics",
#  url = "https://aclanthology.org/2021.acl-demo.15",
#  year = 2021,
#}