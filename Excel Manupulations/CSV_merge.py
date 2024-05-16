import pandas as pd
import glob

# Specify the path to your CSV files
csv_files = glob.glob('/path/to/csv/files/*.csv')

# Read and concatenate CSV files into a single DataFrame
df = pd.concat([pd.read_csv(file) for file in csv_files], ignore_index=True)

# Remove blank rows
df = df.dropna(how='all')

# Write the merged and cleaned DataFrame to a new CSV file
df.to_csv('/path/to/output/merged_data.csv', index=False)
