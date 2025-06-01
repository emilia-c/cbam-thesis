import os
import json
import glob
import deepdoctection as dd
from PyPDF2 import PdfReader
from deepdoctection.extern.d2detect import D2FrcnnDetector

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

# define the pipeline                                
pipe_comp_list = [layout_comp, lang_detect_comp, 
                  text_comp, map_comp, order_comp]

pipe = dd.DoctectionPipe(pipeline_component_list=pipe_comp_list)

# -------------------------
# Extract Data from PDFs
# -------------------------

# Define base directory
base_dir = r"master_thesis_2025\eu_data_extraction\EC\legislation"

# Find all PDFs recursively
pdf_files = glob.glob(os.path.join(base_dir, "**", "*.pdf"), recursive=True)

# Store extracted data
pdf_data_list = []

for pdf_path in pdf_files:
    try:
        file_name = os.path.basename(pdf_path).replace(".pdf", "")

        print(f"Processing PDF: {file_name}")

        # Analyze the PDF
        df = pipe.analyze(path=pdf_path)

        if not df:
            print(f"No data found in {file_name}")
            continue
        df.reset_state() # free memory

        # extract text content on each page
        extracted_text = ""
        for i, page in enumerate(df, start=1):
            try: 
                print(f'Processing page {i}...')
                page_text = page.text.strip()
                # add all of the page texts together
                extracted_text += page_text + "\n"
            
            except Exception as e:
                print(f"Error processing page {i}: {e}")
                raise
                
        # store metadata and extracted text
        pdf_data_list.append({
            "title": file_name,
            "text": extracted_text.strip() # remove trailing newline
        })

    except Exception as e:
        print(f"Error processing {pdf_path}: {str(e)}")

# save to JSON file
output_json_path = os.path.join(base_dir, "legislation_data.json")

with open(output_json_path, "w", encoding="utf-8") as json_file:
    json.dump(pdf_data_list, json_file, ensure_ascii=False, indent=4)

print(f"Data successfully saved to {output_json_path}")