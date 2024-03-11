import pandas as pd
import os

# Define the directory containing your file
current_directory = os.getcwd()

def delete_columns(dataframe, columns):
  dataframe = dataframe.drop(columns, axis=1)
  dataframe = dataframe.drop_duplicates()
  dataframe = dataframe.dropna()
  return dataframe

# Assuming the merged file is named 'Data.csv'
file_path = os.path.join(current_directory, 'Data.csv')

# Read the merged CSV file into a DataFrame
df = pd.read_csv(file_path)

df['reviewText']=df['reviewText'].fillna('Missing')

df = delete_columns(df, ['reviewerID', 'reviewerName', 'unixReviewTime'])

print(df.head())


# Check for missing values in the DataFrame
missing_values_count = df.isnull().sum()

# Calculate the percentage of missing values for each column
total_rows = len(df)
missing_percentage = (missing_values_count / total_rows) * 100


# Print the percentage of missing values for each column
print("Percentage of missing values per column after removing specified columns:")
print(missing_percentage)

duplicates = df[df.duplicated(keep=False)]


duplicates_sorted = duplicates.sort_values(by=list(df.columns))


duplicates_sorted.to_csv('All_Duplicates.csv')

print(df.shape)

#print(df_clean)

df.to_csv("Clean_1.csv")

os.remove("Data.csv")

