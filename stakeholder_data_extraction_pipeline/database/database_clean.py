import sqlite3
from stakeholder_data_extraction_pipeline import config
import re

def remove_noresults_urls():
    """Removes URLs that are labeled as 'no_results'."""
    conn = None # ensure conn exists
    
    try:
        # connect to db and remove orgs without any urls saved
        conn = sqlite3.connect(config.DB_PATH)
        c = conn.cursor()
        c.execute("DELETE FROM urls WHERE url = 'no_results'")
        c.execute("DELETE FROM urls WHERE url = 'error'")
        c.execute("DELETE FROM urls WHERE url = 'timed_out'")
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"Error removing 'no_results' URLs: {e}")
    
    finally:
        if conn:
            conn.close() # close db connection

def remove_duplicate_urls():
    """Removes duplicate URLs, keeping the first instance."""
    conn = None # ensure conn exists
    
    try:
        # connect to db and remove duplicate urls
        conn = sqlite3.connect(config.DB_PATH)
        c = conn.cursor()
        c.execute('''
            DELETE FROM urls
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM urls
                GROUP BY organization_id, url
            )
        ''')
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"Error removing duplicate URLs: {e}")
    
    finally:
        if conn:
            conn.close() # close db connection

def regexp(pattern, string): 
    """ Custom REGEXP function for SQLite queries"""
    
    if string is None: 
        return False
    return re.search(pattern, string) is not None

def filter_unwanted_urls():
    """Removes social media URLs and Google search results."""
    
    conn = None # ensure conn exists
    
    try:
        # connect to db 
        conn = sqlite3.connect(config.DB_PATH)
        conn.create_function("REGEXP", 2, regexp) # create user regexp user function
        c = conn.cursor()
        
        # remove social media URLs (out of scope )
        platform_pattern = '|'.join(config.PLATFORMS)  
        c.execute("DELETE FROM urls WHERE url REGEXP ?", (platform_pattern,))
        
        # remove Google search results (found during manual exploration)
        c.execute("DELETE FROM urls WHERE url REGEXP ?", (config.GOOGLE_SEARCH_PATTERN,))
        
        conn.commit()
    
    except sqlite3.Error as e:
        print(f"Error filtering unwanted URLs: {e}")
    
    finally:
        if conn:
            conn.close() # close db connection
