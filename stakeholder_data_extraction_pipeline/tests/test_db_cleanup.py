import sqlite3
from stakeholder_data_extraction_pipeline import config 
from stakeholder_data_extraction_pipeline.database.database_clean import filter_unwanted_urls
from stakeholder_data_extraction_pipeline.database.database_setup import setup_db

def test_filter_unwanted_urls():
    """Test that social media and Google search results are removed."""
    setup_db()
    
    conn = sqlite3.connect(config.DB_PATH)
    c = conn.cursor()
    
    # Insert test URLs
    test_urls = [
        (1, "https://linkedin.com/example"),
        (2, "https://google.com/search?q=cbam"),
        (3, "https://eurofer.eu/publications"), 
        (4, "http://www.google.com/search?q=test"),
        (5, "https://www.example.com/page"), 
        (6, "https://instagram.com/example"),
        (7, "http://twitter.com/example"),
        (8, "www.facebook.com/example")
        ]
    
    c.executemany("INSERT INTO urls (organization_id, url) VALUES (?, ?)", test_urls)
    conn.commit()
    conn.close()
    
    # Run cleanup
    filter_unwanted_urls()
    
    conn = sqlite3.connect(config.DB_PATH)
    c = conn.cursor()
    c.execute("SELECT url FROM urls;")
    remaining_urls = [row[0] for row in c.fetchall()]
    
    assert "https://linkedin.com/example" not in remaining_urls
    assert "https://google.com/search?q=cbam" not in remaining_urls
    assert "https://eurofer.eu/publications" in remaining_urls
    assert "http://www.google.com/search?q=test" not in remaining_urls
    assert "https://www.example.com/page" in remaining_urls
    assert "https://instagram.com/example" not in remaining_urls
    assert "http://twitter.com/example" not in remaining_urls
    assert "www.facebook.com/example" not in remaining_urls

    
    conn.close()
