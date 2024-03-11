import pandas as pd
import os
import glob

# Define the directory containing your files
current_directory = os.getcwd()

def read_csv(folder_path):
  # Get a list of all CSV files in the current directory
  csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
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
  return df_merged

merged_df = read_csv(current_directory)

# Save the merged DataFrame to a new CSV file
merged_csv_file = os.path.join(current_directory, 'Data.csv')
merged_df.to_csv(merged_csv_file, index=False)

print(f"Merged file saved as '{merged_csv_file}'")


print(merged_df.head(10))
print(merged_df.tail(10))

