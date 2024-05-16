import pandas as pd
import re

def identify_table_type(headers):
    """
    Identifies the table type based on unique keywords or patterns in headers.

    Args:
        headers (list): List of table header strings.

    Returns:
        int: Unique identifier for the table type (0 for unknown).
    """

    # Update your logic to assign unique identifiers based on header patterns
    if "Device1" in headers:
        return 1
    elif "Device2" in headers:
        return 2
    else:
        return 0

def keep_relevant_headers(headers, relevant_headers):
    """
    Filters headers to keep only the specified ones.

    Args:
        headers (list): List of table header strings.
        relevant_headers (list): List of relevant headers to keep.

    Returns:
        list: List of filtered headers.
    """

    return [header for header in headers if header in relevant_headers]

def main():
    # Read data
    df = pd.read_excel("your_file.xlsx")

    # Initialize empty lists
    tables = []
    start_rows = [0]

    # Identify table starting rows
    for i in range(1, len(df)):
        if df.iloc[i, 0] == "":
            start_rows.append(i + 1)

    # Define relevant headers (update with your list)
    relevant_headers = ["bp name", "policy_ name", "al name", "rule number", "rule_name", "other_relevant_header1", "other_relevant_header2", ...]

    # Extract and combine data for each table
    for start_row in start_rows:
        end_row = start_row + 1
        while end_row < len(df) and df.iloc[end_row, 0] != "":
            end_row += 1
        table_data = df.iloc[start_row:end_row, :]

        # Handle empty tables (optional)
        if not table_data.empty:
            # Keep relevant headers
            table_data = table_data[keep_relevant_headers(table_data.columns, relevant_headers)]
            table_type = identify_table_type(table_data.columns)
            tables.append((table_data, table_type))

    # Group tables by type and merge
    merged_tables = {}
    for data, type_id in tables:
        if type_id not in merged_tables:
            merged_tables[type_id] = data
        else:
            merged_tables[type_id] = pd.concat([merged_tables[type_id], data], ignore_index=True)

    # Further process or save the merged tables
    for type_id, merged_table in merged_tables.items():
        # Perform your desired operations on each merged table based on its type_id
        print(f"Merged table for type {type_id}:")
        print(merged_table)

if __name__ == "__main__":
    main()
