import pandas as pd
from ipaddress import IPv4Network

def extract_ip_info(filename, sheet_name=None):
    """Extracts IP range in CIDR and network mask from an Excel sheet.

    Args:
        filename (str): The path to the Excel file.
        sheet_name (str, optional): The name of the worksheet to process.
                                   If None, all sheets are processed.
                                   Defaults to None.

    Returns:
        pandas.DataFrame: A DataFrame containing extracted data,
                          or an empty DataFrame if no data is found.
    """

    try:
        df = pd.read_excel(filename, sheet_name=sheet_name)
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found. Skipping.")
        return pd.DataFrame()

    if df.empty:
        print(f"Warning: No data found in sheet '{sheet_name}' of file '{filename}'. Skipping.")
        return pd.DataFrame()

    # Find columns with CIDR and network mask
    cidr_col = df.columns[df.columns.str.contains('CIDR', case=False)].tolist()
    mask_col = df.columns[df.columns.str.contains('Mask', case=False)].tolist()

    if not cidr_col or not mask_col:
        print(f"Warning: CIDR or Mask columns not found in sheet '{sheet_name}' of file '{filename}'. Skipping.")
        return pd.DataFrame()

    cidr_col = cidr_col[0] if len(cidr_col) == 1 else cidr_col[0]
    mask_col = mask_col[0] if len(mask_col) == 1 else mask_col[0]

    extracted_data = []
    for index, row in df.iterrows():
        cidr_value = row[cidr_col]
        mask_value = row[mask_col]

        # Handle potential errors and invalid data formats
        try:
            if cidr_value:
                network = IPv4Network(cidr_value)
                cidr = network.with_prefixlen
                mask = network.netmask.with_prefixlen
            elif mask_value:
                # If CIDR is missing, attempt to create it from the mask
                try:
                    network = IPv4Network(mask=mask_value)
                    cidr = network.with_prefixlen
                    mask = network.netmask.with_prefixlen
                except ValueError:
                    print(f"Warning: Invalid mask format in row {index+1} of file '{filename}'. Skipping row.")
                    continue
            else:
                print(f"Warning: Missing CIDR or network mask in row {index+1} of file '{filename}'. Skipping row.")
                continue
        except ValueError:
            print(f"Warning: Invalid CIDR format in row {index+1} of file '{filename}'. Skipping row.")
            continue

        extracted_data.append({'CIDR': cidr, 'Network Mask': mask})

    return pd.DataFrame(extracted_data)

def combine_excel_files(input_files, output_file=None, sheet_name=None):
    """Combines data from multiple Excel files into a single pandas DataFrame
    and optionally saves it to an Excel file.

    Args:
        input_files (list): A list of paths to the input Excel files.
        output_file (str, optional): The path to the output Excel file (optional).
        sheet_name (str, optional): The name of the worksheet to process in
                                   each file. If None, all sheets are processed.
                                   Defaults to None.
    """

    all_data = pd.DataFrame()

    for file in input_files:
        data = extract_ip_info(file, sheet_name)
        all_data = pd.concat([all_data, data], ignore_index=True)  # Concatenate DataFrames

    if output_file:
        all_data.to_excel(output_file, index=False)  # Save to Excel without index
        print(f"Combined IP information saved to '{output_file}'.")
    else:
        print("Combined IP information processed:")

    print(all_data)

if __name__ == '__main__':
    input_files = ['file1.xlsx', 'file2.xlsx', 'file3.xlsx']  # Replace with your actual file paths
    output_file = 'combined_ip_info.xlsx'  # Optional output file path
    # sheet_name = 'Sheet1'  # Optional sheet name (if different from default)
    combine_excel_files(input_files, output_file)  # Adjust sheet_name if needed
