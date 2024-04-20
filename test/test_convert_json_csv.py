import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import csv

# Adjust the path where the actual module is located
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'dags', 'src'))

# Import the module
import Convert_json_csv

class TestProcessFiles(unittest.TestCase):
    def setUp(self):
        """Set up test environment."""
        self.test_dir = os.path.join(os.path.dirname(__file__), 'test_data')
        # Ensure this file exists in 'test_data' directory before running the test
        self.sample_gz_path = os.path.join(self.test_dir, 'sample_data.gz')  
        self.extracted_json_path = self.sample_gz_path.replace('.gz', '.json')
        self.extracted_csv_path = self.extracted_json_path.replace('.json', '.csv')

    def tearDown(self):
        """Clean up by removing the files created during the test."""
        if os.path.exists(self.extracted_json_path):
            os.remove(self.extracted_json_path)
        if os.path.exists(self.extracted_csv_path):
            os.remove(self.extracted_csv_path)

    @patch('Convert_json_csv.get_gcs_client')
    @patch('Convert_json_csv.get_bucket')
    @patch('Convert_json_csv.upload_to_gcs')
    def test_process_files_end_to_end(self, mock_upload_to_gcs, mock_get_bucket, mock_get_gcs_client):
        """Test processing from .gz to JSON to CSV without actual GCS interaction."""
        mock_bucket = MagicMock()
        mock_get_gcs_client.return_value = MagicMock()
        mock_get_bucket.return_value = mock_bucket

        # Ensure all paths and directories are perceived as existent
        with patch('os.path.exists', MagicMock(return_value=True)), \
             patch('os.listdir', MagicMock(return_value=['sample_data.gz'])):
            Convert_json_csv.process_files(self.test_dir, '', '')  # Assuming you adjust process_files to accept parameters

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
