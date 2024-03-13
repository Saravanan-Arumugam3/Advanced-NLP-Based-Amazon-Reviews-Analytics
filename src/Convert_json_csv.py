import os
import json
import csv
import logging

# Set the directory where your .json files are located
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/Users/yasir/Desktop/MLops_Project/Advanced-NLP-Based-Amazon-Reviews-Analytics/data')
DIRECTORY_PATH = AIRFLOW_HOME

def json_csv():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f"Starting JSON to CSV conversion in directory: {DIRECTORY_PATH}")

    if not os.path.exists(DIRECTORY_PATH):
        logging.error(f"Directory does not exist: {DIRECTORY_PATH}")
        return

    json_files = [f for f in os.listdir(DIRECTORY_PATH) if f.endswith('.json')]
    if not json_files:
        logging.info("No .json files found.")
        return

    for json_file in json_files:
        try:
            csv_file_name = json_file.replace('.json', '.csv')
            json_file_path = os.path.join(DIRECTORY_PATH, json_file)
            csv_file_path = os.path.join(DIRECTORY_PATH, csv_file_name)

            # Define your JSON keys here
            columns = ["overall", "verified", "reviewTime", "reviewerID", "asin", "reviewerName", "reviewText", "summary", "unixReviewTime"]

            with open(json_file_path, 'r', encoding='utf-8') as jfile, open(csv_file_path, 'w', newline='', encoding='utf-8') as cfile:
                writer = csv.DictWriter(cfile, fieldnames=columns)
                writer.writeheader()
                for line in jfile:
                    # Strip the newline character from each line and load the JSON object
                    jdata = json.loads(line.strip())
                    # Write the data to the CSV file, getting each column's value from the JSON object
                    writer.writerow({col: jdata.get(col, "") for col in columns})
            logging.info(f"Converted {json_file} to {csv_file_name}")

            os.remove(json_file_path)
            logging.info(f"Deleted {json_file} after conversion.")

        except Exception as e:
            logging.error(f"Failed to process {json_file}: {e}")

# Call the function to start the conversion process
json_csv()