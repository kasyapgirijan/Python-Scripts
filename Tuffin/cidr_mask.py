import pandas as pd

def cidr_to_subnet_mask(cidr):
    try:
        prefix, suffix = cidr.split('/')
        prefix = int(prefix)
        suffix = int(suffix)
        if prefix < 0 or prefix > 32 or suffix < 0 or suffix > 32:
            raise ValueError("Invalid CIDR notation")
        mask = ["0"] * 32
        for i in range(suffix):
            mask[i] = "1"
        subnet_mask = ".".join(
            [str(int("".join(mask[i * 8:i * 8 + 8]), 2)) for i in range(4)]
        )
        return subnet_mask
    except ValueError as e:
        return str(e)

# Read CIDR notations from Excel sheet
def process_excel_sheet(file_path):
    try:
        df = pd.read_excel(file_path)  # Assuming the CIDR notations are in the first column
        cidr_column = df.iloc[:, 0]
        subnet_masks = []
        for cidr in cidr_column:
            subnet_mask = cidr_to_subnet_mask(str(cidr))
            subnet_masks.append(subnet_mask)
        df['Subnet Mask'] = subnet_masks
        return df
    except Exception as e:
        print("Error:", str(e))
        return None

# Example usage
file_path = "cidr_sheet.xlsx"  # Path to your Excel sheet
result_df = process_excel_sheet(file_path)
if result_df is not None:
    print("CIDR Notations with Subnet Masks:")
    print(result_df)
