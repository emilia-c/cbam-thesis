import os
import json
import sqlite3
import gc
from stakeholder_data_extraction_pipeline import config
import deepdoctection as dd
from PyPDF2 import PdfReader
from deepdoctection.extern.d2detect import D2FrcnnDetector

#d2_detector = D2FrcnnDetector(...)

# TO DO - OPTIMIZE THIS PIPELINE 
# 1) sometimes the language is detected incorrectly (ukrainan or russain often)

# -------------------------
# Setup deepdoctection pipeline components
# -------------------------
categories=dd.ModelCatalog.get_profile("fasttext/lid.176.bin").categories
categories_orig = dd.ModelCatalog.get_profile("fasttext/lid.176.bin").categories_orig

dd.ModelCatalog.get_profile("fasttext/lid.176.bin").model_wrapper

path_weights=dd.ModelDownloadManager.maybe_download_weights_and_configs("fasttext/lid.176.bin")

# load the Tesseract config
tess_ocr_config_path = dd.get_configs_dir_path() / "dd/conf_tesseract.yaml"
tesseract_ocr = dd.TesseractOcrDetector(tess_ocr_config_path) # initialize ocr with loaded config

# define language component
fast_text = dd.FasttextLangDetector(path_weights, categories, categories_orig)
lang_detect_comp = dd.LanguageDetectionService(fast_text,text_detector=tesseract_ocr)

# define text extraction component
text_comp = dd.TextExtractionService(tesseract_ocr, 
                                     run_time_ocr_language_selection=True) # dynamically select ocr based on language

# define reading order component 
order_comp = dd.TextOrderService(text_container=dd.LayoutType.WORD)

# define layout component 
path_weights = dd.ModelCatalog.get_full_path_weights("layout/d2_model_0829999_layout_inf_only.pt")
path_configs = dd.ModelCatalog.get_full_path_configs("layout/d2_model_0829999_layout_inf_only.pt")
categories = dd.ModelCatalog.get_profile("layout/d2_model_0829999_layout_inf_only.pt").categories

layout_detector = dd.D2FrcnnDetector(path_configs,path_weights,categories,device="cpu")
layout_comp = dd.ImageLayoutService(layout_detector)

from deepdoctection.pipe import IntersectionMatcher
from deepdoctection.datapoint import Relationships
map_comp = dd.MatchingService(
    parent_categories=["text", "title", "list"], # exclude tables and figures
    child_categories=["word"],
    matcher=IntersectionMatcher(matching_rule="ioa", 
                                threshold=0.6),
    relationship_key=Relationships.CHILD
)

# define order component
order_comp = dd.TextOrderService(text_container=dd.LayoutType.WORD,
                                 floating_text_block_categories=["text", "title", "list"],
                                 text_block_categories=["text", "title", "list"])

# Define the pipeline                                
pipe_comp_list = [layout_comp, lang_detect_comp, 
                  text_comp, map_comp, order_comp]

pipe = dd.DoctectionPipe(pipeline_component_list=pipe_comp_list)

def ensure_pdf_extensions():
    """
    Extract PDF paths from the database and ensure that the file names in the downloads directory
    have the correct '.pdf' extension. If not, rename the files.
    """
    conn = None
    try:
        conn = sqlite3.connect(config.DB_PATH)
        c = conn.cursor()
        c.execute("SELECT pdf_file FROM pdf_text")
        pdf_paths = c.fetchall()
    except Exception as e:
        print(f"Error fetching PDF paths: {e}")
        pdf_paths = []
    
    finally:
        if conn:
            conn.close()
    
    # Build full paths based on the downloads directory
    raw_pdf_paths = [os.path.join(config.URL_DOWNLOADS_DIR, path[0].strip()) for path in pdf_paths]

    # Debugging: print the raw pdf paths
    #print("Raw PDF paths:", raw_pdf_paths)

    # Ensure correct path formatting and add .pdf extension if missing
    for path in raw_pdf_paths:
        full_path = os.path.normpath(path)
        if not full_path.endswith(".pdf") and os.path.exists(full_path):
            new_file_path = full_path + ".pdf"
            try:
                print(f"Attempting to rename: {full_path} -> {new_file_path}")  # Debug print
                os.rename(full_path, new_file_path)
                print(f"Renamed file: {full_path} -> {new_file_path}")
            except FileNotFoundError:
                print(f"Skipping (not found): {full_path}")
            except Exception as e:
                print(f"Error renaming {full_path}: {e}")
        elif os.path.exists(full_path):
            print(f"PDF already exists: {full_path}")

# -------------------------
#   Database functions
# -------------------------

def add_pdf_url_data():
    """ add pending pdf records from urls table into pdf_text table, for pdf files with success download""" 
    
    conn = None # ensure conn exists
    
    try: 
        conn = sqlite3.connect(config.DB_PATH)
        c = conn.cursor()
        c.execute("""INSERT INTO pdf_text (id, organization_id, pdf_file, extract_status)
                    SELECT u.id, u.organization_id, u.file_path, 'pending'
                    FROM urls u
                    WHERE u.download_status = 'success'
                    AND u.file_type = 'pdf'
                """)  # insert pending pdf records
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"Error adding pdf info: {e}")
    
    finally: 
        if conn: 
            conn.close() # close db connection

def update_pdf_database(c, file_id, json_path, status):
    """ update pdf_text record in db  # with extracted json path and status """
    c.execute("""UPDATE pdf_text
                 SET extracted_text_path = ?, extract_status = ?, timestamp = CURRENT_TIMESTAMP
                 WHERE id = ?""", (json_path, status, file_id))  # update record

def get_pending_pdf_ids():
    """ get ids of pdfs pending extraction, returns list of pending ids """
    conn = None # ensure db connection 
    rows = []

    try: 
        conn = sqlite3.connect(config.DB_PATH)
        c = conn.cursor()
        c.execute("""SELECT id FROM pdf_text 
                    WHERE extract_status = 'pending' OR extract_status = 'failure'
                    """)  # query pending pdfs
        rows = c.fetchall()
    
    except sqlite3.Error as e:
        print(f"Error getting pending PDF ids: {e}")
    finally:
        if conn:
            conn.close()
    return [row[0] for row in rows]

def is_pdf_encrypted(pdf_path):
    """ check if pdf is encrypted using pypdf https://pypdf.readthedocs.io/en/stable/modules/PdfReader.html """
    try:
        reader = PdfReader(pdf_path)
        return reader.is_encrypted  # return encryption status
    
    except Exception as e:
        print(f"error checking encryption for {pdf_path}: {e}")  # print error
        return True  # assume encrypted if error

# ------------------------------
# Main PDF Extraction Pipeline
# ------------------------------

def run_pdf_extraction():
    """
    Run the pdf extraction pipeline:
      - Ensure PDF filenames are correctly formatted (with .pdf extension).
      - Process pending PDFs and update the database with extraction results.
    """
    # first check that all pdfs have .pdf extension
    ensure_pdf_extensions()
    add_pdf_url_data()                        # add pending pdf records from urls
    pending_ids = set(get_pending_pdf_ids())  # get pending pdf ids
    
    # gather pdf file paths from downloads dir  # based on file naming convention
    pdf_files = [f for f in os.listdir(config.URL_DOWNLOADS_DIR) if f.lower().endswith(".pdf")]
    pending_pdf_paths = []  # list to hold tuples (pdf_path, file_id)
    
    for file in pdf_files:
        try:
            # file naming convention: "orgid_urlid.pdf", extract urlid as file_id
            file_id = int(file.split("_")[-1].replace(".pdf", ""))  # parse file id
        except Exception as e:
            print(f"could not parse file id from {file}: {e}")  # error parsing
            continue
        if file_id in pending_ids:
            full_path = os.path.join(config.URL_DOWNLOADS_DIR, file)  # full pdf path
            if is_pdf_encrypted(full_path):
                print(f"skipping encrypted pdf: {file}")  # skip if encrypted
                # update status as decryption_failure here if desired
                continue
            pending_pdf_paths.append((full_path, file_id))  # add to list
    #print("pending pdf paths (not encrypted):", pending_pdf_paths)  # debug print

    try: 
        conn = sqlite3.connect(config.DB_PATH)
        c = conn.cursor()
        
        # run the data pipeline on each pending pdf
        for pdf_path, file_id in pending_pdf_paths:
            file_name = os.path.basename(pdf_path)  # get file name
            print(f"processing pdf: {file_name}")   # debug print
            
            try:
                df = pipe.analyze(path=pdf_path)    # analyze pdf with deepdoctection
                if not df:
                    print("no data found")          # nothing extracted
                    continue
                df.reset_state()  # free memory
                
                # save pdf pages in json
                total_pages = 0
                first_page_lang = "unknown"
                pages = {}
                temp_json_path = os.path.join(config.EXTRACTED_PDF_DIR, f"{file_name}.tmp.json")  # temporary json file
                
                with open(temp_json_path, "w", encoding="utf-8") as f:
                    f.write("{\n")  # start json
                    f.write(f'  "file_name": "{file_name}",\n')  # file name
                    
                    for i, page in enumerate(df, start=1):
                        try:
                            print(f"Processing page {i}...")
                            # Attempt to access language attribute on first page
                            if i == 1:
                                detected_lang = getattr(page, "language", "unknown")
                                first_page_lang = detected_lang
                                print(f"Detected language on page {i}: {first_page_lang}")
                            total_pages += 1
                            page_text = page.text.strip()
                            print(f"Page {i} text length: {len(page_text)}")
                            pages[f"page_{i}"] = page_text
                        except Exception as e:
                            print(f"Error processing page {i}: {e}")
                            raise  # Optionally re-raise so you see the full traceback
                                        
                    json_pages = json.dumps(pages, ensure_ascii=False, indent=2)  # convert pages to json string
                    f.write(f'  "pages": {json_pages},\n')            # write pages
                    f.write(f'  "language": "{first_page_lang}",\n')  # write language
                    f.write(f'  "total_pages": {total_pages}\n')      # write page count
                    f.write("}\n")                                    # end json
                
                # write json file, update db and clean memory (avoid ram crashes)
                final_json_path = os.path.join(config.EXTRACTED_PDF_DIR, os.path.splitext(file_name)[0] + ".json")  # final json file path
                os.rename(temp_json_path, final_json_path)  # rename temp file
                print(f"saved json to: {final_json_path}")  # debug print
                
                update_pdf_database(c, file_id, final_json_path, "success")  # update db as success
                conn.commit()     # commit after each pdf
                df.reset_state()  # free memory after processing pdf
                gc.collect()      # garbage collect
            
            except Exception as e:
                error_message = str(e)  # get error message
                print(f"error processing {file_name}: {error_message}")  # print error
                update_pdf_database(c, file_id, None, "failure")         # update db as failure
                conn.commit()  # commit update
    
    except sqlite3.Error as e:
        print(f"Error extracting PDFs: {e}")

    finally: 
        if conn: 
            conn.close()               # close connection
    print("pdf processing complete!")  # print completion message