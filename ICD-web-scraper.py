import requests
import time
from bs4 import BeautifulSoup
import csv
import pandas as pd
import numpy as np

print("Welcome to the ICD-10 scraper.\nPlease choose from the options below:")

billable = 'Billable_Specific_Codes/'
nonbillable = 'Non_Billable_Specific_Codes/'
base_url = 'https://www.icd10data.com/ICD10CM/Codes/Rules/'
name = ''

choice = input("1: scrape non-billable codes \n2: scrape billable codes \n3: check ccm.csv for invalid entries\n ")

scrape = True

if choice == '1':
    base_url = base_url + nonbillable
    name = 'non-billable'
elif choice == '2':
    base_url = base_url + billable
    name = 'billable'

elif choice == '3':
    'CCM Check'
    scrape = False
    try:
        # Load the non-billable and billable data with error handling
        try:
            nonbillable_df = pd.read_csv("non-billable-icd-codes.csv", encoding='utf-8')
        except UnicodeDecodeError:
            nonbillable_df = pd.read_csv("non-billable-icd-codes.csv", encoding='ISO-8859-1')

        try:
            billable_df = pd.read_csv("billable-icd-codes.csv", encoding='utf-8')
        except UnicodeDecodeError:
            billable_df = pd.read_csv("billable-icd-codes.csv", encoding='ISO-8859-1')

        # Convert the first column of each DataFrame to sets, handling potential missing data
        nonbillable_set = set(nonbillable_df.iloc[:, 0].dropna().str.upper())
        billable_set = set(billable_df.iloc[:, 0].dropna().str.upper())

    except FileNotFoundError as e:
        print(f"Error: {e}")
        exit()
    except Exception as e:
        print(f"Unexpected error when reading non-billable or billable files: {e}")
        exit()

    # Load the CCM CSV with error handling
    try:
        ccm_csv = pd.read_csv("ccm.csv", encoding='utf-8')
    except UnicodeDecodeError:
        ccm_csv = pd.read_csv("ccm.csv", encoding='ISO-8859-1')
    except FileNotFoundError as e:
        print(f"Error: {e}")
        exit()
    except Exception as e:
        print(f"Unexpected error when reading CCM file: {e}")
        exit()

    invalid_rows = []

    codes = []

    ccm_csv.replace("NAN", np.nan, inplace=True)

    # Iterate through each row in the CCM CSV
    for index, row in ccm_csv.iterrows():
        valid = False
        try:

            if pd.isna(row.iloc[0]):
                continue 

            col_b = str(row.iloc[2]).upper()
            col_c = str(row.iloc[3]).upper() 
            col_d = str(row.iloc[4]).upper()

            codes.append(col_b)
            codes.append(col_c)
            codes.append(col_d)

            if col_b in billable_set:
                valid = True
            if col_c in billable_set:
                valid = True
            if  col_d in billable_set:
                valid = True
            if col_b == "NAN" and col_c == "NAN" and col_d == "NAN":
                valid = True

            if valid == False:
                invalid_rows.append(row)

        except KeyError as e:
            print(f"Missing expected column: {e}")
            continue
        except Exception as e:
            print(f"Unexpected error processing row {index}: {e}")
            continue

    # Save the invalid rows to a new CSV, with error handling
    try:
        invalid_df = pd.DataFrame(invalid_rows)
        invalid_df.to_csv("invalid_ccm_entries.csv", index=False)
        print("Invalid CCM entries have been saved to 'invalid_ccm_entries.csv'.")
    except Exception as e:
        print(f"Error saving invalid entries: {e}")

else:
    print("Invalid choice. Please enter 1 or 2.")
    url = None


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'    
}

icd_codes = []

page_number = 1

previous_count = 0

if scrape == True:
    while True:

        url = base_url + str(page_number)

        page = requests.get(url, headers=headers)

        soup = BeautifulSoup(page.text, 'html.parser')

        if page.status_code == 200:

            print(f"Connected to page {page_number}")

            #retrieve all <li> elements
            list_items = soup.find_all('li')

            # Loop through each 'li' element and extract the code and description
            for item in list_items:
                code_tag = item.find('a', class_='identifier')
                description_tag = item.select_one('span')
            
                if code_tag and description_tag:
                    code = code_tag.text.strip()
                    description = description_tag.text.strip()
                    icd_codes.append((code, description))

            if len(icd_codes) == previous_count:
                print(f"No new items found on page: {page_number}")
                break
            
            previous_count = len(icd_codes)
            page_number += 1
            time.sleep(2)

        else:
            print(f"Connection failed. Status code: {page.status_code}")

    filename = name + '-icd-codes.csv'
    line = 0

    with open(filename, mode='w',newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Code', 'Description'])
        for code, desc in icd_codes:
            writer.writerow([code,desc])
            line += 1

    print(f"CSV created. Total codes: {line}")

print("Goodbye!")
