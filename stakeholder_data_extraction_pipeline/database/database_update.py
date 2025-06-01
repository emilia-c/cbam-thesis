import sqlite3
import pandas as pd
from stakeholder_data_extraction_pipeline import config

def insert_organization(registered_title, search_title, category):
    """Inserts an organization into the organizations table (ignores duplicates)."""
    conn = None # ensure conn exists
    
    try:
        # connect to db
        conn = sqlite3.connect(config.DB_PATH)
        c = conn.cursor()
        
        # add orgs official title, search title and transparency registry category to the db
        c.execute('INSERT OR IGNORE INTO organizations (registered_organisation_title, search_title, category) VALUES (?, ?, ?)', 
                  (registered_title, search_title, category))
        conn.commit() # add info to db
    
    except sqlite3.Error as e:
        print(f"Error inserting organization: {e}")
    
    finally:
        if conn:
            conn.close() # close db connection

def get_organization_id(search_title):
    """Retrieves an organization ID by search title."""
    conn = None # ensure conn exists
    
    try:
        # connect to db
        conn = sqlite3.connect(config.DB_PATH)
        c = conn.cursor()
        
        # extract org id based on search title input
        c.execute('SELECT id FROM organizations WHERE search_title = ?', (search_title,))
        row = c.fetchone()
        return row[0] if row else None
    
    except sqlite3.Error as e:
        print(f"Error fetching organization ID: {e}")
        return None
    
    finally:
        if conn:
            conn.close() # close db connection

def read_org_data(df):
    """Inserts multiple organizations from a dataframe."""

    for _, row in df.iterrows():
        insert_organization(row['org_title'], row['search_title'], row['reg_category'])

def add_ddg_urls(json_data):
    """Inserts DuckDuckGo search results into the URLs table."""
    conn = None # ensure conn exists
    
    try:
        # connect to db
        conn = sqlite3.connect(config.DB_PATH) 
        c = conn.cursor()
        
        # loop through ddg search results (from ddg_search)
        for data in json_data:
            org_name = data['org']                          # extract org name
            url = data['url']                               # extract url
            organization_id = get_organization_id(org_name) # extract org id (for metadata)
            
            # if org exists, create url id and file names for each url
            if organization_id is not None:
                # find largest current id, and if none exist then start at 1
                c.execute('SELECT MAX(id) FROM urls')
                result = c.fetchone()
                url_id = result[0] + 1 if result[0] is not None else 1
                file_name = f"{organization_id}_{url_id}"
                
                # add data to urls table (if doesn't already exist)
                c.execute('SELECT 1 FROM urls WHERE organization_id = ? AND url = ?', (organization_id, url))
                exists = c.fetchone()

                if not exists:
                    c.execute('''INSERT INTO urls (organization_id, url, file_path, download_status, timestamp)
                                VALUES (?, ?, ?, ?, ?)''', 
                            (organization_id, url, file_name, 'pending', None))
        
        conn.commit() # add changes to db
    
    except sqlite3.Error as e:
        print(f"error inserting URLs: {e}")
    
    finally:
        if conn:
            conn.close() # close db connection

def add_ddg_urls_csv():
    """Reads URLs from a CSV file and inserts them into the database.
    
    - Keeps duplicates across different organizations.
    - Prevents duplicates for the same organization.
    """

    # read in csv file
    csv_file = config.LOGS_DIR + "/ddg_search_results.csv"
    df = pd.read_csv(csv_file, encoding='utf-8')

    # Ensure required columns exist
    if not {'organisation', 'url'}.issubset(df.columns):
        raise ValueError("CSV file must contain 'org' and 'url' columns")

    conn = sqlite3.connect(config.DB_PATH)
    cursor = conn.cursor()

    for _, row in df.iterrows():
        org_name = row['organisation'].strip()
        url = row['url'].strip()

        # Get organization ID
        organization_id = get_organization_id(org_name)

        if organization_id is not None:
            # Check if this URL already exists for this specific organization
            cursor.execute("SELECT 1 FROM urls WHERE organization_id = ? AND url = ?", (organization_id, url))
            exists = cursor.fetchone()

            if not exists:  # Insert only if this URL is new for the organization
                # Generate a new ID for the URL (if needed)
                cursor.execute("SELECT MAX(id) FROM urls")
                result = cursor.fetchone()
                url_id = result[0] + 1 if result[0] is not None else 1

                # File name based on org ID and new URL ID
                file_name = f"{organization_id}_{url_id}"

                # Insert the URL into the database
                cursor.execute('''INSERT INTO urls (organization_id, url, file_path, download_status, timestamp)
                                  VALUES (?, ?, ?, ?, ?)''',
                               (organization_id, url, file_name, 'pending', None))

    conn.commit()
    conn.close()
    print("✅ URLs inserted successfully from CSV!")