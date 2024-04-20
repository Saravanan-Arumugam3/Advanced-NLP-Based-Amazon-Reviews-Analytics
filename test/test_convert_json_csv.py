import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import gzip
import json
import csv

# Adjust the path where the actual module is located
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'dags', 'src'))

# Mock Google Cloud Storage client before importing the module
with patch('google.cloud.storage.Client') as MockClient:
    MockClient.from_service_account_json = MagicMock()
    import Convert_json_csv

class TestProcessFiles(unittest.TestCase):
    def setUp(self):
        """Set up test environment by creating necessary dummy files."""
        self.test_dir = os.path.join(os.path.dirname(__file__), 'test_data')
        os.makedirs(self.test_dir, exist_ok=True)
        self.sample_gz_path = os.path.join(self.test_dir, 'sample_data.gz')
        self.extracted_json_path = self.sample_gz_path.replace('.gz', '.json')
        self.extracted_csv_path = self.extracted_json_path.replace('.json', '.csv')

        # Write sample data into a .gz file
        sample_data = json.dumps({
            "overall": 5, "verified": True, "reviewTime": "2021-01-01",
            "reviewerID": "AB123", "asin": "123456", "reviewerName": "John Doe",
            "reviewText": "Great product!", "summary": "Loved it!", "unixReviewTime": 161987
        })
        with gzip.open(self.sample_gz_path, 'wt') as gz_file:
            gz_file.write(sample_data)

    def tearDown(self):
        """Clean up by removing the files and directories created for testing."""
        os.remove(self.sample_gz_path)
        if os.path.exists(self.extracted_json_path):
            os.remove(self.extracted_json_path)
        if os.path.exists(self.extracted_csv_path):
            os.remove(self.extracted_csv_path)
        os.rmdir(self.test_dir)

    def test_process_files_end_to_end(self):
        """Test processing from .gz to JSON to CSV without actual GCS interaction."""
        Convert_json_csv.process_files(self.test_dir, '', '')  # Adjusted for no actual JSON or bucket name

        # Assert that JSON and CSV files were created as expected
        self.assertTrue(os.path.exists(self.extracted_json_path), "JSON file was not created.")
        self.assertTrue(os.path.exists(self.extracted_csv_path), "CSV file was not created.")

        # Verify the contents of the CSV file
        with open(self.extracted_csv_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['reviewText'], "Great product!")

if __name__ == '__main__':
    unittest.main()
