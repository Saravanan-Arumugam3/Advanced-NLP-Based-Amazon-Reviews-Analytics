import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import gzip  # Make sure gzip is imported
import json
import csv

# Append the path where the actual module is located
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'dags', 'src'))

# Import the module after adjusting the path
import Convert_json_csv

class TestProcessFiles(unittest.TestCase):
    def setUp(self):
        """Set up test environment by creating necessary dummy files."""
        self.test_dir = os.path.dirname(__file__)
        self.sample_gz_path = os.path.join(self.test_dir, 'sample_data.gz')
        self.extracted_json_path = self.sample_gz_path.replace('.gz', '.json')
        self.extracted_csv_path = self.extracted_json_path.replace('.json', '.csv')

        # Sample data to be written into a .gz file
        sample_data = json.dumps({
            "overall": 5,
            "verified": True,
            "reviewTime": "2021-01-01",
            "reviewerID": "AB123",
            "asin": "123456",
            "reviewerName": "John Doe",
            "reviewText": "Great product!",
            "summary": "Loved it!",
            "unixReviewTime": 161987
        })
        with gzip.open(self.sample_gz_path, 'wt') as gz_file:
            gz_file.write(sample_data)

    def tearDown(self):
        """Clean up by removing the files created for testing."""
        os.remove(self.sample_gz_path)
        os.remove(self.extracted_json_path)
        os.remove(self.extracted_csv_path)

    @patch('Convert_json_csv.storage.Client')  # Mock the GCS Client
    def test_process_files_end_to_end(self, mock_gcs_client):
        """Test processing from .gz to JSON to CSV without actual GCS interaction."""
        # Bypass GCS client initialization
        mock_gcs_client.from_service_account_json.return_value = MagicMock()

        # Mocking os.path.exists to always return True to avoid directory error
        with patch('os.path.exists', return_value=True):
            # Execute the process
            Convert_json_csv.process_files()

            # Check if JSON and CSV files were created as expected
            self.assertTrue(os.path.exists(self.extracted_json_path))
            self.assertTrue(os.path.exists(self.extracted_csv_path))

            # Verify the contents of the CSV file
            with open(self.extracted_csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]['reviewText'], "Great product!")

if __name__ == '__main__':
    unittest.main()
