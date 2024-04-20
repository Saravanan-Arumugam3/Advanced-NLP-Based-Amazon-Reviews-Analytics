import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import csv

# Mock Google Cloud Storage client before importing the module
@patch('google.cloud.storage.Client.from_service_account_json', MagicMock(return_value=MagicMock()))
class TestProcessFiles(unittest.TestCase):
    def setUp(self):
        """Set up test environment by ensuring the correct test directory and files exist."""
        self.test_dir = os.path.join(os.path.dirname(__file__), 'test_data')
        os.makedirs(self.test_dir, exist_ok=True)
        self.sample_gz_path = os.path.join(self.test_dir, 'sample_data.gz')  # Ensure this file exists
        self.extracted_json_path = self.sample_gz_path.replace('.gz', '.json')
        self.extracted_csv_path = self.extracted_json_path.replace('.json', '.csv')

    def tearDown(self):
        """Clean up by removing the files created during the test."""
        if os.path.exists(self.extracted_json_path):
            os.remove(self.extracted_json_path)
        if os.path.exists(self.extracted_csv_path):
            os.remove(self.extracted_csv_path)
        os.rmdir(self.test_dir)

    def test_process_files_end_to_end(self):
        """Test the processing from .gz to JSON to CSV without actual GCS interaction."""
        # Adjust the import to be within the test method after the GCS client has been mocked
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'dags', 'src'))
        import Convert_json_csv

        # Mock os.path.exists and os.listdir to simulate file presence
        with patch('os.path.exists', MagicMock(return_value=True)), \
             patch('os.listdir', MagicMock(return_value=['sample_data.gz'])):
            Convert_json_csv.process_files(self.test_dir, '', '')  # Process the existing .gz file

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
