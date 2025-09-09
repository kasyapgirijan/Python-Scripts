import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

with open("api_key.txt", "r") as file:
    api_key = file.read().strip()

datetime_now = datetime.datetime.now()
date_string = datetime_now.strftime("Y-%m-%d_%H-%M-%S")

base_url = "https://results.us.securityeducation.com/api/reporting/v0.3.0/"

def extract_user_tags(attributes):
    user_tags_data = {}
    user_tags = attributes.get('usertags', {})
    if user_tags:
        for tag_category, tag_values in user_tags.items():
            tag_category = tag_category.lower().replace('_1', '')
            user_tags_data.update(
                {f'{tag_category}_{i+1}': value for i, value in enumerate(tag_values)})
    return user_tags_data

def extract_attributes(api_url, report_type):
    extracted_data = []
    try:
        page = 1
        while True:
            url = f"{api_url}&page[number]={page}&page[size]=8000&filter[_includedeletedusers]=TRUE"
            print(url)
            headers = {"x-apikey-token":f"{api_key}"}
            response = requests.get(url, headers=headers)
            data = response.json()

            if not data['data']:
                break  # No more pages

            for item in data['data']:
                attributes = item.get('attributes', {})
                item_data = {
                    'type': item.get('type'),
                    'id': item.get('id'),
                    **attributes
                }

                if report_type == 'users':
                    item_data.update(extract_user_tags(attributes))

                extracted_data.append(item_data)
            page += 1  # Move to the next page

    except Exception as e:
        print(f"Error extracting attributes:{e}")

    return extracted_data

def process_report(report_type, workbook):
    
    if report_type == 'users':
        api_url = f"{base_url}{report_type}?user_tag_enabled&"
    else:
        api_url = f"{base_url}{report_type}?"

    extracted_data = extract_attributes(api_url, report_type)

    print(f"Collecting {report_type.capitalize()}")

    if extracted_data:
        df = pd.DataFrame(extracted_data)

        # Remove Rows where uernemail matches sso_id@whateverdomain.com
        if report_type == 'users':
            df['sso_id_email'] = df['sso_id'] + '@organization.com'
            df = df[~df['useremailaddress'].isin(df['sso_id_email'])]

        df.to_excel(workbook, sheet_name=report_type.capitalize(), index=False)

        print(f"{report_type.capitalize()} data saved to workbook.")

    else:
        print(f"Failed to retrieve attributes for {report_type.capitalize()} data.")
        return None

def main():
    report_types = ['training', 'users', 'phishing']  # Add more report types as needed
    date_folder = date_string
    f_path = os.getcwd()
    folder_path = os.path.join(f_path, date_folder)
    os.makedirs(folder_path, exist_ok=True)
    # Create a single workbook
    workbook_path = os.path.join(folder_path, "Proofpoint.xlsx")
    with pd.ExcelWriter(workbook_path, engine='xlsxwriter') as writer:
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(process_report, report_type, writer): report_type for report_type in report_types}

            for future in futures:
                future.result()

    print(f"All reports saved to {workbook_path}")

if __name__ == '__main__':
    main()
