import pandas as pd
import os
from datetime import datetime

# --- Define your data files here ---
primary_data_file = "Mod_Phishing_data.csv"
repeat_click_file = "Mod_Phishing_repeat_click.csv"
repeat_compromised_file = "Mod_Phishing_repeat_compromised.csv"
repeat_reports_file = "Mod_Repeat_reports.csv"

# --- Map CSV files → Excel sheet names ---
csv_to_sheet = {
    primary_data_file: "Mod_Phishing_data",
    repeat_click_file: "Mod_Phishing_repeat_click",
    repeat_compromised_file: "Mod_Phishing_repeat_compromised",
    repeat_reports_file: "Mod_Repeat_reports"
}

# --- Define the headers to keep (only for primary data file) ---
headers_to_keep = [
    'First Name', 'Last Name', 'Campaign Guid', 'Users Guid',
    'Primary Email Opened', 'Primary Clicked', 'Primary Compromised Login',
    'Email Address', 'Date Sent', 'Campaign Title', 'Reported',
    'Email Bounced', 'Department', 'Location', 'Clicked Browser', 'Clicked OS'
]

# --- Determine script directory ---
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    script_dir = os.getcwd()

# --- Create Excel output filename (with date) ---
timestamp = datetime.now().strftime("%Y%m%d")
output_file = os.path.join(script_dir, f"Combined_Mod_Phishing_{timestamp}.xlsx")

# --- Start writing Excel workbook ---
with pd.ExcelWriter(output_file, engine='openpyxl', datetime_format='mm/dd/yyyy hh:mm:ss') as writer:
    for csv_name, sheet_name in csv_to_sheet.items():
        csv_path = os.path.join(script_dir, csv_name)
        if not os.path.exists(csv_path):
            print(f"⚠️  File not found: {csv_name}")
            continue

        print(f"Processing: {csv_name} → Sheet: {sheet_name}")
        df = pd.read_csv(csv_path)

        # Only refine headers for the primary file
        if csv_name == primary_data_file:
            cols_present = [c for c in headers_to_keep if c in df.columns]
            df = df[cols_present].copy()

            # Convert 'Date Sent' to datetime and remove timezone
            if 'Date Sent' in df.columns:
                df['Date Sent'] = pd.to_datetime(
                    df['Date Sent'], utc=True, errors='coerce'
                ).dt.tz_localize(None)

        # Write DataFrame to Excel sheet
        df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
        print(f"✅ Written {csv_name} to '{sheet_name}'")

print(f"\n🎉 Combined workbook created successfully: {output_file}")
