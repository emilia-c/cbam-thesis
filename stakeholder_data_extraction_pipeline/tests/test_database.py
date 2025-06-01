import sqlite3
from stakeholder_data_extraction_pipeline import config 
from stakeholder_data_extraction_pipeline.database.database_setup import setup_db
from stakeholder_data_extraction_pipeline.database.database_update import insert_organization, get_organization_id

def test_database_setup():
    """Test if the database and tables are created successfully."""
    setup_db()
    conn = sqlite3.connect(config.DB_PATH)
    c = conn.cursor()
    
    # Check if tables exist
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in c.fetchall()]
    
    assert "organizations" in tables
    assert "urls" in tables
    assert "html_text" in tables
    assert "pdf_text" in tables
    
    conn.close()

def test_insert_organization():
    """Test inserting a new organization into the database."""
    setup_db()
    insert_organization("Test Org", "Test Org Search", "NGO")
    
    conn = sqlite3.connect(config.DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT * FROM organizations WHERE search_title = 'Test Org Search';")
    result = c.fetchone()
    
    assert result is not None
    assert result[1] == "Test Org"  # registered_organisation_title
    assert result[2] == "Test Org Search"  # search_title
    assert result[3] == "NGO"  # category

    conn.close()
