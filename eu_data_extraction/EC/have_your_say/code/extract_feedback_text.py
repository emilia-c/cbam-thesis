import os
import json
import glob
import pandas as pd
from tqdm import tqdm
import deepdoctection as dd
from PyPDF2 import PdfReader
from deepdoctection.extern.d2detect import D2FrcnnDetector

# -------------------------
# 1. Setup deepdoctection pipeline components
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

# -------------------------
# 2) Function to extract text from a single PDF
# -------------------------
def extract_text_from_pdf(pdf_path: str, pipe) -> str:
    """
    Runs the DeepDoctection pipeline on a PDF and returns the extracted text.
    """
    df = pipe.analyze(path=pdf_path)
    if not df:
        return "empty"

    # extract text content on each page
    extracted_text = []
    for i, page in enumerate(df, start=1):
        try: 
            print(f'Processing page {i}...')
            page_text = page.text.strip() if page.text else "error"
            # add all of the page texts together
            extracted_text.append(page_text)
        
        except Exception as e:
            print(f"Error processing page {i}: {e}")
            raise  # re-raise so you see the full traceback
    df.reset_state() # free memory

    return "\n".join(extracted_text).strip()

# -------------------------
# 3) Main script
# -------------------------
def main():
    # Directories you want to process
    publications = [
        "7587254",
        "33194896",
        "33195975"
    ]
    
    base_dir = r"master_thesis_2025\eu_data_extraction\EC\have_your_say"
    
    # this will store all records for all publications
    all_data = []
    
    for pub_id in tqdm(publications, desc="Processing publications"):
        pub_dir = os.path.join(base_dir, pub_id)
        
        # -------------------------
        # Define csv variables
        # -------------------------
        csv_path = os.path.join(pub_dir, "feedbacks.csv") # path to feedback csv
        if not os.path.exists(csv_path):
            print(f"Could not find feedback.csv in {pub_dir}, skipping.")
            continue
        
        feedback_df = pd.read_csv(csv_path, encoding="utf-8") # read in the feedback CSV

        # -------------------------
        # Define pdf attachment variables
        # -------------------------
        attachments_dir = os.path.join(pub_dir, "attachments")   # Directory containing PDF attachments
        if not os.path.exists(attachments_dir):
            print(f"No attachments folder in {pub_dir}, skipping PDF extraction.")
            continue

        # -------------------------
        # Iterate over every row in the filtered CSV
        # -------------------------
        output_json_path = os.path.join(base_dir, "combined_hys.json")

        for idx, row in tqdm(feedback_df.iterrows(), total=len(feedback_df), desc=f"Processing {pub_id} feedbacks"):
            # Extract the necessary fields
            file_id = str(row.get("id", None))
            organization = row.get("organization", None)
            tr_number = row.get("tr_number", None)
            org_type = row.get("user_type", None)
            publication_id = row.get("publication_id", pub_id)  # Default to pub_id if missing
            
            # Choose the correct feedback text based on language
            if row.get("language") == "en": 
                feedback_text = row.get("feedback", None)  # use English feedback
                extract_pdf = True  # Extract PDF text
            else:
                feedback_text = row.get("feedback_text_user_language", None)  # use translated feedback
                extract_pdf = False  # skip PDF text extraction
            
            # check if the corresponding PDF file exists
            pdf_text = None # assume file does not exist
            if extract_pdf and file_id and attachments_dir:
                pdf_path = os.path.join(attachments_dir, f"{file_id}.pdf")
                if os.path.isfile(pdf_path):
                    pdf_text = extract_text_from_pdf(pdf_path, pipe)
            
            record = {
                "file_id": file_id,
                "organization": organization,
                "tr_number": tr_number,
                "org_type": org_type,
                "publication_id": publication_id,
                "feedback": feedback_text,
                "pdf_text": pdf_text
            }

            # -------------------------
            # 4) Save everything to one JSON
            # -------------------------
            if os.path.exists(output_json_path):
                with open(output_json_path, "r", encoding="utf-8") as f:
                    try:
                        all_data = json.load(f)
                    except json.JSONDecodeError:
                        all_data = []  # Handle empty or corrupted files
            else:
                all_data = []

            all_data.append(record)

            with open(output_json_path, "w", encoding="utf-8") as f:
                json.dump(all_data, f, ensure_ascii=False, indent=4)

            print(f"Saved record {idx + 1}/{2} to {output_json_path}")

if __name__ == "__main__":
    main()