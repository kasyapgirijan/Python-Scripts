import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

BASE_URL = "https://results.us.securityeducation.com/api/reporting/v0.3.0/"
API_KEY = "<your_api_key>"

def extract_user_tags(attributes):
    user_tags_data = {}
    user_tags = attributes.get('usertags', {})
    if user_tags:
        for tag_category, tag_values in user_tags.items():
            # Renaming logic can be applied here
            # For example, if you want to replace spaces with underscores and convert to lowercase
            tag_category = tag_category.lower().replace(' ', '_')
            user_tags_data.update({f'{tag_category}_{i+1}': value for i, value in enumerate(tag_values)})
    return user_tags_data

def extract_attributes(api_url, report_type, page_size=100):
    extracted_data = []

    try:
        page = 1
        while True:
            url = f"{api_url}&page[number]={page}&page[size]={page_size}"
            headers = {"Authorization": f"Bearer {API_KEY}"}
            response = requests.get(url, headers=headers)
            data = response.json()

            # Check if there are records in the current page
            if not data['data']:
                break  # No more pages

            for item in data['data']:
                attributes = item.get('attributes', {})
                item_data = {
                    'type': item.get('type'),
                    'id': item.get('id'),
                    **attributes
                }

                # Conditionally include user tags only for 'user' report type
                if report_type == 'user':
                    item_data.update(extract_user_tags(attributes))

                extracted_data.append(item_data)

            page += 1  # Move to the next page

    except Exception as e:
        error_message = f"Error extracting attributes for {report_type}: {e}"
        print(f"\033[91m{error_message}\033[0m")  # ANSI escape code for red text

    return extracted_data

def process_report(report_type, workbook):
    if report_type == 'user':
        api_url = f"{BASE_URL}{report_type}?filter[user_specific_filter]=value"
    else:
        api_url = f"{BASE_URL}{report_type}?filter[a]=b"

    extracted_data = extract_attributes(api_url, report_type)

    if extracted_data:
        # Convert the extracted data to a DataFrame
        df = pd.DataFrame(extracted_data)

        # Add the DataFrame as a sheet to the workbook
        df.to_excel(workbook, sheet_name=report_type.capitalize(), index=False)

        print(f"{report_type.capitalize()} data saved to workbook.")

    else:
        print(f"Failed to retrieve {report_type.capitalize()} data.")

def main():
    report_types = ['training', 'user', 'phishing']  # Add more report types as needed

    # Create a timestamp-based folder
    timestamp_folder = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    folder_path = os.path.join("./output_data", timestamp_folder)
    os.makedirs(folder_path, exist_ok=True)

    # Create a single workbook
    workbook_path = os.path.join(folder_path, "reports.xlsx")
    with pd.ExcelWriter(workbook_path, engine='xlsxwriter') as writer:
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(process_report, report_type, writer): report_type for report_type in report_types}

            for future in futures:
                future.result()

    print(f"All reports saved to {workbook_path}")

if __name__ == '__main__':
    main()
