import glob
import os
import pandas as pd

path = os.getcwd()

file_list = glob.glob(path + "/*.xlsx")

print("", file_list)

columns = ["bp name", "destination_network_negated", "rule_name", "logged"]
column_order = ["bp name", "logged", "destination_network_negated", "rule_name", "logged"]

dfs = []

for file in file_list:
    df = pd.read_excel(file)

    # Ensure columns exist before selecting them
    if all(col in df.columns for col in columns):
        df = df[columns]

    # Filter rows where "destination_network_negated" is either "True" or "False"
    df = df[(df["destination_network_negated"] == "True") | (df["destination_network_negated"] == "False")]

    df = df[column_order]
    dfs.append(df)

merged_data = pd.concat(dfs, ignore_index=True)

merged_data.to_excel("merged.xlsx", index=False)
