import os
import pytest
from stakeholder_data_extraction_pipeline import config
import sqlite3
from stakeholder_data_extraction_pipeline.text_extraction import html_text_extraction
from stakeholder_data_extraction_pipeline.text_extraction import pdf_text_extraction

# --- Dummy classes to replace the deepdoctection pipeline ---
class DummyPage:
    def __init__(self, text, language="eng"):
        self.text = text
        self.language = language
    def reset_state(self):
        pass

class DummyDoc:
    def __init__(self, pages):
        self.pages = pages
    def __iter__(self):
        return iter(self.pages)
    def reset_state(self):
        pass

class DummyPipe:
    def analyze(self, path):
        # Return a dummy document with one page saying "Hello World"
        return DummyDoc([DummyPage("Hello World", "eng")])

# --- Test functions ---
def test_html_extraction(tmp_path):
    """Test that HTML extraction runs without errors."""
    test_file = tmp_path / "sample.html"
    test_file.write_text("<html><body><h1>Test</h1></body></html>")
    extracted_text = html_text_extraction.extract_html_content(str(test_file))
    assert "Test" in extracted_text

def test_pdf_extraction(tmp_path, monkeypatch):
    """
    Test that run_pdf_extraction:
      - Renames PDF files to include the .pdf extension.
      - Processes pending PDFs (using the deepdoctection pipeline) and creates JSON output.
      
    Sets up two sample PDF files (without .pdf extensions) in a temporary downloads directory.
    """
    # Create temporary directories for PDF downloads and for extracted output.
    downloads_dir = tmp_path / "temp_pdf"
    downloads_dir.mkdir()
    extracted_dir = tmp_path / "extracted"
    extracted_dir.mkdir()

    # Create a temporary SQLite database with the proper schema.
    temp_db = tmp_path / "temp.db"
    conn = sqlite3.connect(str(temp_db))
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS pdf_text (
            id INTEGER PRIMARY KEY,
            organization_id INTEGER,
            pdf_file TEXT,
            extract_status TEXT,
            extracted_text_path TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Insert records for two sample PDFs.
    # The naming convention is "orgid_urlid" so that the file id is parsed from the part after '_'
    c.execute(
        "INSERT INTO pdf_text (id, pdf_file, organization_id, extract_status) VALUES (?, ?, ?, ?)",
        (123, "31_123", 31, "pending")
    )
    c.execute(
        "INSERT INTO pdf_text (id, pdf_file, organization_id, extract_status) VALUES (?, ?, ?, ?)",
        (456, "32_456", 32, "pending")
    )
    conn.commit()
    conn.close()

    # Override configuration values so that the extraction uses our temporary directories and DB.
    monkeypatch.setattr(config, "URL_DOWNLOADS_DIR", str(downloads_dir))
    monkeypatch.setattr(config, "EXTRACTED_PDF_DIR", str(extracted_dir))
    monkeypatch.setattr(config, "DB_PATH", str(temp_db))
    # Override add_pdf_url_data to be a no-op (we already inserted our records).
    monkeypatch.setattr(pdf_text_extraction, "add_pdf_url_data", lambda: None)

    # Create two sample PDF files (without the .pdf extension).
    pdf_content = b"""%PDF-1.4
1 0 obj
<< /Type /Catalog /Pages 2 0 R >>
endobj
2 0 obj
<< /Type /Pages /Kids [3 0 R] /Count 1 >>
endobj
3 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>
endobj
4 0 obj
<< /Length 55 >>
stream
BT
/F1 24 Tf
100 700 Td
(Hello World) Tj
ET
endstream
endobj
5 0 obj
<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>
endobj
xref
0 6
0000000000 65535 f 
0000000010 00000 n 
0000000053 00000 n 
0000000102 00000 n 
0000000209 00000 n 
0000000281 00000 n 
trailer
<< /Size 6 /Root 1 0 R >>
startxref
350
%%EOF
"""
    # Write two sample PDF files (initially without .pdf extension).
    sample_pdf1 = downloads_dir / "31_123"
    sample_pdf1.write_bytes(pdf_content)
    sample_pdf2 = downloads_dir / "32_456"
    sample_pdf2.write_bytes(pdf_content)

    print("Files before renaming:", os.listdir(str(downloads_dir)))

    # Call the actual extraction pipeline.
    pdf_text_extraction.run_pdf_extraction()

    print("Files after renaming:", os.listdir(str(downloads_dir)))

    # Verify that the PDF files have been renamed (i.e. have the .pdf extension).
    assert (downloads_dir / "31_123.pdf").exists(), "Expected file 31_123.pdf to exist"
    assert (downloads_dir / "32_456.pdf").exists(), "Expected file 32_456.pdf to exist"

    # Verify that JSON output files were created in the extracted directory.
    output1 = extracted_dir / "31_123.json"
    output2 = extracted_dir / "32_456.json"
    assert output1.exists(), "Expected extracted file 31_123.json to exist"
    assert output2.exists(), "Expected extracted file 32_456.json to exist"

    # Optionally, verify that the JSON files are not empty.
    with open(output1, "r", encoding="utf-8") as f:
        content1 = f.read().strip()
        assert content1, "Extracted file 31_123.json is empty"
    with open(output2, "r", encoding="utf-8") as f:
        content2 = f.read().strip()
        assert content2, "Extracted file 32_456.json is empty"