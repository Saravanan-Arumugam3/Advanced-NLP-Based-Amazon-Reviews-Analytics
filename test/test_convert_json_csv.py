import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import csv

# Assuming the following path setup based on your project structure:
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'dags', 'src'))

class TestProcessFiles(unittest.TestCase):
    def setUp(self):
        """Set up test environment by locating the test directory and ensuring it exists."""
        self.test_dir = os.path.join(os.path.dirname(__file__), 'test_data')
        os.makedirs(self.test_dir, exist_ok=True)
        self.sample_gz_path = os.path.join(self.test_dir, 'sample_data.gz')
        self.extracted_json_path = self.sample_gz_path.replace('.gz', '.json')
        self.extracted_csv_path = self.extracted_json_path.replace('.json', '.csv')

    def tearDown(self):
        """Clean up by removing the files created during the test, if they exist."""
        if os.path.exists(self.extracted_json_path):
            os.remove(self.extracted_json_path)
        if os.path.exists(self.extracted_csv_path):
            os.remove(self.extracted_csv_path)
        # Check if the directory is empty now and remove it
        if not os.listdir(self.test_dir):
            os.rmdir(self.test_dir)

    @patch('google.cloud.storage.Client.from_service_account_json')
    def test_process_files_end_to_end(self, mock_gcs_client):
        """Test the processing from .gz to JSON to CSV without actual GCS interaction."""
        # Mocking the GCS client and related operations
        mock_gcs_client.return_value = MagicMock()
        mock_bucket = MagicMock()
        mock_gcs_client.return_value.bucket.return_value = mock_bucket
        mock_upload = MagicMock()
        mock_bucket.blob.return_value.upload_from_filename = mock_upload

        # Import the module inside the test to ensure it uses the mocked client
        import Convert_json_csv

        with patch('os.path.exists', MagicMock(return_value=True)), \
             patch('os.listdir', MagicMock(return_value=['sample_data.gz'])):
            Convert_json_csv.process_files()

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
