from stakeholder_data_extraction_pipeline import config 

def is_pdf(filepath):
    """ check if file is a pdf  # by extension"""
    return filepath.lower().endswith(".pdf")  # return true if pdf

def detect_paywall(filepath):
    """ detect paywall indicators in html file, using file length and paywall keywords """
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as file:
            content = file.read()   # read file content
            if len(content) < 200:  # too short might indicate paywall
                return "possible paywall"  # flag as possible paywall
            
            for word in config.PAYWALL_WORDS:
                if word in content.lower():  # check each keyword
                    return "paywall detected"  # flag detected
            return None  # no paywall detected
    
    except Exception as e:
        print(f"error reading {filepath}: {e}")  # print error
        return "unreadable"  # mark unreadable

def detect_file_type(filepath):
    # determine file type (pdf or html) and check for paywall  # returns tuple (type, paywall_status)
    try:
        with open(filepath, "rb") as file:
            header = file.read(1024)  # read first 1kb
            if header.startswith(b"%PDF-"):
                return "pdf", None  # pdf detected
            elif b"<html" in header.lower() or b"<!doctype html" in header.lower():
                return "html", detect_paywall(filepath)  # html detected, check paywall
            else:
                return "unknown", None  # unknown type
    
    except Exception as e:
        print(f"error reading {filepath}: {e}")  # print error
        return "unreadable", None  # mark as unreadable