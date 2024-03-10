import pandas as pd
import os

# Define the directory containing your file
current_directory = os.getcwd()

# Assuming the merged file is named 'merged_files.csv'
file_path = os.path.join(current_directory, 'Clean_1.csv')

# Read the merged CSV file into a DataFrame
df = pd.read_csv(file_path)

#duplicates = df[df.duplicated(keep=False)]


#duplicates_sorted = duplicates.sort_values(by=list(df.columns))


#duplicates_sorted.to_csv('All_Duplicates.csv')

#print(df.shape)

df_clean = df.drop_duplicates()

#print(df_clean)

df_clean.to_csv("Clean_2.csv")

os.remove("Clean_1.csv")


