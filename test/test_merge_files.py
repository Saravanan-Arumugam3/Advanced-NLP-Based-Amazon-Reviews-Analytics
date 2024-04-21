import unittest
from unittest.mock import patch, MagicMock
import os
import sys

# Adjust the path to include the directory where the module under test is located
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'dags', 'src'))

from Merge_Files import merge_files  # Import the function or module you're testing

class TestMergeFiles(unittest.TestCase):
    @patch('merge_csv_files.storage.Client')
    def test_merge_files(self, mock_storage_client):
        # Set up the mock objects
        mock_bucket = MagicMock()
        mock_blob1 = MagicMock(spec=['download_to_filename', 'name'], name='CSV/file1.csv')
        mock_blob2 = MagicMock(spec=['download_to_filename', 'name'], name='CSV/file2.csv')
        mock_storage_client.return_value.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2]
        
        # Prepare the paths for the fake CSV files in the test_data directory
        test_data_dir = os.path.join(os.path.dirname(__file__), 'test_data')
        file1_path = os.path.join(test_data_dir, 'file1.csv')
        file2_path = os.path.join(test_data_dir, 'file2.csv')
        tmp_dir = os.path.join(os.environ.get('AIRFLOW_HOME', '/Users/phanibhavanaatluri'), 'temp')
        
        # Ensure the mock download_to_filename method places the CSV files in the right temporary directory
        def mock_download_to_filename(destination):
            if 'file1.csv' in destination:
                os.link(file1_path, destination)
            else:
                os.link(file2_path, destination)
        mock_blob1.download_to_filename.side_effect = mock_download_to_filename
        mock_blob2.download_to_filename.side_effect = mock_download_to_filename

        # Mock uploading back to GCS
        mock_blob_upload = MagicMock()
        mock_bucket.blob.return_value = mock_blob_upload
        
        # Run the function under test
        merge_files()
        
        # Check calls to ensure files were "downloaded" and "uploaded"
        mock_blob1.download_to_filename.assert_called()
        mock_blob2.download_to_filename.assert_called()
        mock_blob_upload.upload_from_filename.assert_called()

        # Optionally check the content of the merged file
        # This would require reading from the file system and verifying its content matches expectations.

if __name__ == '__main__':
    unittest.main()
