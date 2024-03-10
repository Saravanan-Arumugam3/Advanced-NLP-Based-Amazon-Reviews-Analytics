import pandas as pd
import os

# Define the directory containing your files
current_directory = os.getcwd()

# Get a list of all CSV files in the current directory
csv_files = [file for file in os.listdir(current_directory) if file.endswith('.csv')]

# Initialize a list to store DataFrames
dfs = []

# Loop through the files and add them to the dfs list
for file in csv_files:
    # Read the CSV file
    temp_df = pd.read_csv(os.path.join(current_directory, file))
    
    # Create a new column with the name of the file (without the extension)
    temp_df['Product_Type'] = os.path.splitext(file)[0]
    
    # Add the DataFrame to the list
    dfs.append(temp_df)

# Concatenate all DataFrames in the list
merged_df = pd.concat(dfs, ignore_index=True)

# Save the merged DataFrame to a new CSV file
merged_csv_file = os.path.join(current_directory, 'Data.csv')
merged_df.to_csv(merged_csv_file, index=False)

print(f"Merged file saved as '{merged_csv_file}'")


print(merged_df.head(10))
print(merged_df.tail(10))
print(merged_df.Product_Type.unique())

