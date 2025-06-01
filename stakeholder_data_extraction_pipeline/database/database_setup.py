import sqlite3
from stakeholder_data_extraction_pipeline import config

def setup_db():
    """Creates the SQLite database and tables if they do not exist"""
    conn = None
    try:
        conn = sqlite3.connect(config.DB_PATH)
        c = conn.cursor()

        # create organizations table
        c.execute('''CREATE TABLE IF NOT EXISTS organizations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        registered_organisation_title TEXT NOT NULL UNIQUE,
                        search_title TEXT NOT NULL,
                        category TEXT NOT NULL)''')

        # create URLs table
        c.execute('''CREATE TABLE IF NOT EXISTS urls (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        organization_id INTEGER,
                        url TEXT NOT NULL,
                        file_path TEXT,
                        download_status TEXT,
                        file_type TEXT,
                        paywall_status TEXT,
                        timestamp TEXT,
                        FOREIGN KEY(organization_id) REFERENCES organizations(id))''')

        # create HTML text table
        c.execute('''CREATE TABLE IF NOT EXISTS html_text (
                        id INTEGER PRIMARY KEY,
                        organization_id INTEGER,
                        html_file TEXT,
                        extracted_text_path TEXT,
                        extract_status TEXT,
                        timestamp TEXT)''')

        # create PDF text table
        c.execute('''CREATE TABLE IF NOT EXISTS pdf_text (
                        id INTEGER PRIMARY KEY,
                        organization_id INTEGER,
                        pdf_file TEXT,
                        extracted_text_path TEXT,
                        extract_status TEXT,
                        timestamp TEXT)''')

        conn.commit()
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    
    finally:
        if conn:
            conn.close()