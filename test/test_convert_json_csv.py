import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import gzip
import json
import csv
import importlib.util

# Set the path to where the actual module is located
module_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'dags', 'src', 'Convert_json_csv.py')

class TestProcessFiles(unittest.TestCase):
    def setUp(self):
        """Set up test environment by creating necessary dummy files."""
        self.test_dir = os.path.join(os.path.dirname(__file__), 'test_data')
        os.makedirs(self.test_dir, exist_ok=True)
        self.sample_gz_path = os.path.join(self.test_dir, 'sample_data.gz')
        self.extracted_json_path = self.sample_gz_path.replace('.gz', '.json')
        self.extracted_csv_path = self.extracted_json_path.replace('.json', '.csv')

        sample_data = json.dumps({
            "overall": 5, "verified": True, "reviewTime": "2021-01-01",
            "reviewerID": "AB123", "asin": "123456", "reviewerName": "John Doe",
            "reviewText": "Great product!", "summary": "Loved it!", "unixReviewTime": 161987
        })
        with gzip.open(self.sample_gz_path, 'wt') as gz_file:
            gz_file.write(sample_data)

    def tearDown(self):
        """Clean up by removing the files created for testing."""
        os.remove(self.sample_gz_path)
        os.remove(self.extracted_json_path)
        os.remove(self.extracted_csv_path)
        os.rmdir(self.test_dir)

    def test_process_files_end_to_end(self):
        """Test processing from .gz to JSON to CSV without actual GCS interaction."""
        # Dynamically import the required function
        spec = importlib.util.spec_from_file_location("process_files", module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        process_files = module.process_files

        # Mock the GCS Client within the dynamically loaded module
        with patch('Convert_json_csv.storage.Client') as mock_gcs_client:
            mock_gcs_client.from_service_account_json.return_value = MagicMock()

            # Execute the process
            process_files()

            # Asserts
            self.assertTrue(os.path.exists(self.extracted_json_path))
            self.assertTrue(os.path.exists(self.extracted_csv_path))

            with open(self.extracted_csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]['reviewText'], "Great product!")

if __name__ == '__main__':
    unittest.main()
