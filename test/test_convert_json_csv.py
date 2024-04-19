import unittest
from unittest.mock import patch, mock_open, MagicMock
import sys
import os

# Adjust the path to include the directory where convert_json_csv.py is located
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'dags', 'src'))

import Convert_json_csv

class TestUploadToGCS(unittest.TestCase):
    @patch('convert_json_csv.bucket.blob')
    def test_upload_to_gcs(self, mock_blob):
        mock_blob_instance = mock_blob.return_value
        convert_json_csv.upload_to_gcs('path/to/local/file', 'destination_blob_name')
        mock_blob_instance.upload_from_filename.assert_called_once_with('path/to/local/file')

class TestJsonToCsvAndUpload(unittest.TestCase):
    @patch('builtins.open', new_callable=mock_open, read_data='{"overall": 5, "reviewText": "Great!"}\n')
    @patch('convert_json_csv.csv.DictWriter')
    @patch('convert_json_csv.upload_to_gcs')
    def test_json_to_csv_and_upload(self, mock_upload, mock_writer, mock_file):
        mock_writer_instance = mock_writer.return_value
        mock_writer_instance.writerow = MagicMock()
        convert_json_csv.json_to_csv_and_upload('path/to/json', 'path/to/csv', 'destination_blob_name')
        mock_writer_instance.writeheader.assert_called_once()
        mock_writer_instance.writerow.assert_called()
        mock_upload.assert_called_with('path/to/csv', 'destination_blob_name')

class TestProcessFiles(unittest.TestCase):
    @patch('convert_json_csv.os.path.exists', return_value=True)
    @patch('convert_json_csv.os.listdir', return_value=['file1.gz'])
    @patch('convert_json_csv.upload_to_gcs')
    @patch('convert_json_csv.json_to_csv_and_upload')
    @patch('convert_json_csv.gzip.open', new_callable=mock_open, read_data='some data')
    @patch('builtins.open', new_callable=mock_open)
    def test_process_files(self, mock_open, mock_gzip_open, mock_json_to_csv, mock_upload, mock_listdir, mock_exists):
        convert_json_csv.process_files()
        mock_json_to_csv.assert_called_once()

if __name__ == '__main__':
    unittest.main()
