import pandas as pd
import os
import glob

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/Users/phanibhavanaatluri')
current_directory = os.path.join(AIRFLOW_HOME, 'dags')

def merge_files():
    # Get the current working directory

    # Get a list of all CSV files in the current directory
    csv_files = glob.glob(os.path.join(current_directory, '*.csv'))

    # Initialize a list to store DataFrames
    dfs = []

    # Loop through the files and add them to the dfs list
    for csv_file in csv_files:
        # Read the CSV file
        df = pd.read_csv(csv_file)
        # Add the DataFrame to the list
        dfs.append(df)

    # Concatenate all DataFrames in the list
    df_merged = pd.concat(dfs, ignore_index=True)

    # Save the merged DataFrame to a new CSV file
    merged_csv_file = os.path.join(current_directory, 'Merger_data.csv')
    df_merged.to_csv(merged_csv_file, index=False)

    # Print the file path of the saved merged file
    print(f"Merged file saved as '{merged_csv_file}'")

    # Optional: Print the head and tail of the merged DataFrame
    print(df_merged.head(10))
    print(df_merged.tail(10))

# Call the function to perform the merging and saving process
merge_files()

