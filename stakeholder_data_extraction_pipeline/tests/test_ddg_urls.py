from stakeholder_data_extraction_pipeline.ddg_urls import ddg_search
from unittest.mock import patch, MagicMock

def test_ddg_search():
    # Patch the DDGS class in the ddg_search module (not ddg_urls)
    with patch('stakeholder_data_extraction_pipeline.ddg_urls.ddg_search.DDGS') as MockDDGS:
        # Create a mock instance to simulate the DDGS context manager
        mock_ddgs_instance = MagicMock()
        
        # Simulate a successful return for the `text` attribute
        mock_ddgs_instance.text.return_value = [{"href": "https://example.com"}]
        
        # Mocking `__enter__` so that it doesn't hang and simply returns the mock instance
        MockDDGS.return_value.__enter__.return_value = mock_ddgs_instance
        MockDDGS.return_value.__exit__.return_value = None  # Simulate context exit without issues
        
        # Run the pipeline, which will now use the mocked DDGS
        results, org_df = ddg_search.run_search_pipeline()

        # Validate the results
        assert isinstance(results, list)
        assert len(results) > 0
        assert "org" in results[0]
        assert "url" in results[0]