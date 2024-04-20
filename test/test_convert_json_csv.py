import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import gzip
import json
import csv

# Append the path where the actual module is located
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'dags', 'src'))

import convert_json_csv

class TestProcessFiles(unittest.TestCase):
    def setUp(self):
        """Set up test environment"""
        self.test_dir = os.path.dirname(__file__)
        self.sample_gz_path = os.path.join(self.test_dir, 'sample_data.gz')
        self.extracted_json_path = self.sample_gz_path.replace('.gz', '.json')
        self.extracted_csv_path = self.extracted_json_path.replace('.json', '.csv')

        # Sample data to be written into a .gz file
        sample_data = json.dumps({"overall": 5, "verified": True, "reviewTime": "2021-01-01",
                                  "reviewerID": "AB123", "asin": "123456", "reviewerName": "John Doe",
                                  "reviewText": "Great product!", "summary": "Loved it!", "unixReviewTime": 161987})
        with gzip.open(self.sample_gz_path, 'wt') as gz_file:
            gz_file.write(sample_data)

    def tearDown(self):
        """Clean up after tests"""
        os.remove(self.sample_gz_path)
        os.remove(self.extracted_json_path)
        os.remove(self.extracted_csv_path)

    def test_process_files_end_to_end(self):
        """Test the processing of files from .gz to JSON to CSV"""
        # Adjust the function to save JSON and CSV in the test directory
        convert_json_csv.process_files()

        # Ensure the JSON and CSV files were created
        self.assertTrue(os.path.exists(self.extracted_json_path))
        self.assertTrue(os.path.exists(self.extracted_csv_path))

        # Verify contents of the CSV file
        with open(self.extracted_csv_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            rows = list(reader)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['reviewText'], 'Great product!')

if __name__ == '__main__':
    unittest.main()
