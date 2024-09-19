import glob
import os
import pandas as pd

# Get the current working directory
path = os.getcwd()

# Get list of CSV files in the current directory
file_list = glob.glob(path + "/*.csv")

print('File list:', file_list)

# Define the columns to be read and reordered
columns = ["bp name", "destination_network_negated", "rule_name", "logged"]
column_order = ["bp name", "logged", "destination_network_negated", "rule_name"]

dfs = []

for file in file_list:
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file)

    # If the columns exist, keep them, otherwise keep the whole DataFrame
    df = df[columns] if all(col in df.columns for col in columns) else df

    # Reorder the columns as needed
    df = df[column_order]

    # Filter the DataFrame to only keep rows where 'destination_network_negated' is True or False
    df = df[df['destination_network_negated'].isin([True, False])]

    # Append the DataFrame to the list
    dfs.append(df)

# Merge all the DataFrames in the list into one DataFrame
merged_data = pd.concat(dfs, ignore_index=True)

# Save the merged data to an Excel file
merged_data.to_excel("merged.xlsx", index=False)
