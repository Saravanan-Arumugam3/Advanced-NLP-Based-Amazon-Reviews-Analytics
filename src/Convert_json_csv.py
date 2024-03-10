import gzip
import json
import csv
import os

current_directory = os.getcwd()

# Function to convert JSON file to CSV with a consistent column order
def json_to_csv_ordered(json_file_path, csv_file_path, columns):
    with open(json_file_path, 'r', encoding='utf-8') as jfile, open(csv_file_path, 'w', newline='', encoding='utf-8') as cfile:
        writer = csv.DictWriter(cfile, fieldnames=columns)
        writer.writeheader()
        for line in jfile:
            jdata = json.loads(line)
            writer.writerow({col: jdata.get(col, "") for col in columns})

# List of JSON keys for CSV columns in the desired order
columns = ["overall", "verified", "reviewTime", "reviewerID", "asin", "reviewerName", "reviewText", "summary", "unixReviewTime"]

# Unzip and convert each .json.gz file to CSV, then delete the original .gz file
for gz_file in os.listdir(current_directory):
    if gz_file.endswith('.json.gz'):
        # Define the file paths
        json_file_name = gz_file.replace('.gz', '')
        csv_file_name = json_file_name.replace('.json', '.csv')
        json_file_path = os.path.join(current_directory, json_file_name)
        csv_file_path = os.path.join(current_directory, csv_file_name)
        gz_file_path = os.path.join(current_directory, gz_file)

        # Unzip the file
        with gzip.open(gz_file_path, 'rb') as f_in, open(json_file_path, 'wb') as f_out:
            f_out.write(f_in.read())

        # Convert JSON to CSV
        json_to_csv_ordered(json_file_path, csv_file_path, columns)

        # Delete the original .json.gz file
        os.remove(gz_file_path)

        # Optional: Delete the .json file as well, uncomment the line below if desired
        os.remove(json_file_path)
