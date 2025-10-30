import pandas as pd
import glob
import os
from datetime import datetime

# --- Define the headers to keep (only for primary data file) ---
headers_to_keep = [
    'First Name', 'Last Name', 'Campaign Guid', 'Users Guid',
    'Primary Email Opened', 'Primary Clicked', 'Primary Compromised Login',
    'Email Address', 'Date Sent', 'Campaign Title', 'Reported',
    'Email Bounced', 'Department', 'Location', 'Clicked Browser', 'Clicked OS'
]

# --- Map CSV filenames → Excel sheet names ---
csv_to_sheet = {
    'Mod_Phishing_data.csv': 'Mod_Phishing_data',
    'Mod_Phishing_repeat_click.csv': 'Mod_Phishing_repeat_click',
    'Mod_Phishing_repeat_compromised.csv': 'Mod_Phishing_repeat_compromised',
    'Mod_Repeat_reports.csv': 'Mod_Repeat_reports'
}

# --- Determine script directory ---
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    script_dir = os.getcwd()

# --- Create Excel output filename ---
output_file = os.path.join(script_dir, 'Combined_Mod_Phishing_Data.xlsx')

# --- Start writing Excel workbook ---
with pd.ExcelWriter(output_file, engine='openpyxl', datetime_format='mm/dd/yyyy hh:mm:ss') as writer:
    for csv_name, sheet_name in csv_to_sheet.items():
        csv_path = os.path.join(script_dir, csv_name)
        if not os.path.exists(csv_path):
            print(f"⚠️  File not found: {csv_name}")
            continue

        print(f"Processing: {csv_name} → Sheet: {sheet_name}")
        df = pd.read_csv(csv_path)

        # Only refine headers for the main (primary) file
        if csv_name == 'Mod_Phishing_data.csv':
            cols_present = [c for c in headers_to_keep if c in df.columns]
            df = df[cols_present].copy()

            # Convert 'Date Sent' to datetime and remove timezone
            if 'Date Sent' in df.columns:
                df['Date Sent'] = pd.to_datetime(df['Date Sent'], utc=True, errors='coerce').dt.tz_localize(None)

        # Write to Excel sheet
        df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
        print(f"✅ Written {csv_name} to '{sheet_name}'")

print(f"\n🎉 Combined workbook created: {output_file}")
