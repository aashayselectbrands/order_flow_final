import requests
import json
import pandas as pd
import time
from datetime import datetime, timedelta, date
import zipfile
import io
import gspread as gs
from oauth2client.service_account import ServiceAccountCredentials
import aiohttp
import asyncio
from tqdm.asyncio import tqdm
from Functions.Script_updates import update_script_date
from Functions.google_sheet import authenticate_google_sheets

# Constants
max_retries = 3
max_re = 3

# Your Google Sheets credentials file
CREDENTIALS_FILE = 'credentials.json'

# Define the scope
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

def get_data_from_google_sheets(sheet_name, worksheet_name):
    """Retrieve data from a specific Google Sheets worksheet."""
    client = authenticate_google_sheets()
    sheet = client.open(sheet_name)
    worksheet = sheet.worksheet(worksheet_name)
    data = worksheet.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])
    return df, worksheet, client

# Status mapping conditions
def determine_status(row):
    if pd.notna(row['delivery_date']) and len(row['delivery_date']) > 0:
        return 'status_completed'
    elif pd.notna(row['rto_delivered_date']) and len(row['rto_delivered_date']) > 0:
        return 'status_completed'
    else:
        return 'active'

def processing_ats(df):
    df['awb_num'] = pd.to_numeric(df['awb_num'], errors='coerce')

    df['pickup_date'] = pd.to_datetime(df['pickup_date'], errors="coerce").dt.strftime("%d-%m-%Y")
    df['delivery_date'] = pd.to_datetime(df['delivery_date'], errors="coerce").dt.strftime("%d-%m-%Y")
    df['rto_initiated_date'] = pd.to_datetime(df['rto_initiated_date'], errors="coerce").dt.strftime("%d-%m-%Y")
    df['rto_delivered_date'] = pd.to_datetime(df['rto_delivered_date'], errors="coerce").dt.strftime("%d-%m-%Y")

    df['mapped_status'] = df.apply(determine_status, axis=1)
    df['updated_at'] = date.today().strftime("%d-%m-%Y")

    df['mapped_status'].fillna("no_mapping_avl", inplace=True)
    df.fillna("", inplace=True)

    return df

def updating_primary_ats(primary_df, new_df):
    primary_df["awb_num"] = primary_df["awb_num"].astype(int)
    new_df_proc = processing_ats(new_df)
    new_df_proc["awb_num"] = new_df_proc["awb_num"].astype(int)

    condition = (primary_df['mapped_status'] != 'status_completed') & (primary_df['awb_num'].isin(new_df_proc['awb_num'])) & (~primary_df['awb_num'].isna())
    df_to_update = primary_df[condition].reset_index(drop=True)

    awbs_to_update = new_df_proc[new_df_proc['awb_num'].isin(df_to_update['awb_num'])].reset_index(drop=True) 
    new_awbs = new_df_proc[~new_df_proc['awb_num'].isin(primary_df['awb_num'])].reset_index(drop=True)

    primary_df = primary_df[~primary_df['awb_num'].isin(df_to_update['awb_num'])]
    primary_df = pd.concat([primary_df, awbs_to_update, new_awbs], ignore_index=True)

    return primary_df

# LAST 30 DAYS DATA FETCHING

BASE_URL_EASYECOM = "https://api.easyecom.io"
AUTH_EMAIL_EASYECOM = "dhruv.pahuja@selectbrands.in"
AUTH_PASS_EASYECOM = "Analyst@123#"

def authenticate_easyecom():
    url = f"{BASE_URL_EASYECOM}/getApiToken"
    payload = {
        "email": AUTH_EMAIL_EASYECOM,
        "password": AUTH_PASS_EASYECOM,
        "location_key": "en11797218225"
    }

    response = requests.post(url, data=payload)
    data = response.json()["data"]
    return data["api_token"], data["jwt_token"]

def generate_sales_report(api_token, jwt_token, start_date, end_date):
    url = f"{BASE_URL_EASYECOM}/reports/queue"
    headers = {
        "x-api-key": api_token,
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json"
    }

    payload = json.dumps({
        "reportType": "MINI_SALES_REPORT",
        "params": {
            "invoiceType": "ALL",
            "warehouseIds": "en11797218225,ne14939928441,ne30022839441,ur11851370496,ix27964535076,en29424942369,wo31538918464",
            "dateType": "ORDER_DATE",
            "startDate": start_date,
            "endDate": end_date
        }
    })
    while True:
        try:
            response = requests.post(url, headers=headers, data=payload)
            report_id = response.json()["data"]["reportId"]
            break
        except:
            print("Retrying for report id in 30 seconds...")
            time.sleep(30)
    return report_id

def download_report(api_token, jwt_token, report_id):
    url = f"{BASE_URL_EASYECOM}/reports/download?reportId={report_id}"
    headers = {
        "x-api-key": api_token,
        "Authorization": f"Bearer {jwt_token}"
    }

    while True:
        try:
            response = requests.get(url, headers=headers)
            if "data" in response.json() and "downloadUrl" in response.json()["data"]:
                download_url = response.json()["data"]["downloadUrl"]
                break
            else:
                print("Report data not available yet. Retrying in 30 seconds...")
                time.sleep(30)
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error: {e}. Retrying in 30 seconds...")
            time.sleep(30)

    return download_url

def get_ee_df():
    # Initial setup
    api_token, jwt_token = authenticate_easyecom()
    print("Authentication successful!")

    today = datetime.now()
    start_date_str = (today - timedelta(days=30)).strftime('%Y-%m-%d')
    end_date_str = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    # start_date_str = '2024-09-15'
    # end_date_str = '2024-09-20'

    # Generate report for the desired date
    report_id = generate_sales_report(api_token, jwt_token, start_date_str, end_date_str)
    print(f"Sales report for {start_date_str} to {end_date_str} generated. Report ID:", report_id)

    download_url = ""
    while len(download_url) == 0:
        print("Retrying for report in 30 seconds...")
        time.sleep(30)
        download_url = download_report(api_token, jwt_token, report_id)

    print(f"Download URL for report: {download_url}")

    if not download_url:
        raise ValueError("Download URL is empty. Unable to fetch report data.")

    response = requests.get(download_url, stream=True)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        file_names = z.namelist()
        if not file_names:
            raise ValueError("The ZIP file is empty. Unable to proceed.")
        csv_content = z.open(file_names[0]).read()

    try:
        df = pd.read_csv(io.BytesIO(csv_content))
        print("CSV data fetched and read successfully.")
    except Exception as e:
        raise ValueError(f"Failed to read CSV file. Error: {e}")
    
    # Filtering for just Bluedart orders
    df = df[df['Courier Name'] == "ATS"]

    # Extract date part from 'Order Date' column
    df['Order Date'] = pd.to_datetime(df['Order Date']).dt.strftime('%Y-%m-%d')

    df["Order Number"] = df["Order Number"].str.replace("`", "")
    df["Tracking Number"] = df["Tracking Number"].str.replace("`", "")

    df = df[["Order Date", "Order Number", "Courier Name", "Tracking Number","SKU", "Order Status", "Shipping Status", "Selling Price", "Suborder Quantity"]]
    
    df['Suborder Quantity'] = pd.to_numeric(df['Suborder Quantity'], errors='coerce').fillna(0).astype(int)

    # Convert 'Selling Price' to numeric
    df['Selling Price'] = pd.to_numeric(df['Selling Price'], errors='coerce').fillna(0).astype(float)

    # Round 'Selling Price' to 2 decimal places
    df['Selling Price'] = df['Selling Price'].round(2)

    print(df)
    df.fillna("", inplace=True)

    return df

def get_awb_from_ee(ee_df):
    awb_nums = [num for num in ee_df['Tracking Number']]
    awb_nums_unique = list(set(awb_nums))
    return awb_nums_unique

def get_tracking_details(api_token, jwt_token, reference_code):
    url = f"{BASE_URL_EASYECOM}/Carriers/getTrackingDetails?awb_number={reference_code}"

    headers = {
        "x-api-key": api_token,
        "Authorization": f"Bearer {jwt_token}"
    }

    response = requests.get(url, headers=headers)
    return response.json()

def _create_df(awb, shipping_history):
    df = pd.DataFrame(shipping_history)
    # Step 1: Get pickup_date (first occurrence of "PickupDone")
    pickup_date = None
    delivery_date = None
    rto_initiated_date = ""
    rto_delivered_date = ""
    status = ""

    pickup_date = df[df['status'] == 'PickupDone']['time'].min()
    status = df['status'].iloc[-1]

    # Step 2: Get delivery_date (last occurrence of "Delivered")
    if len(df[df['status'] == 'ReturnInitiated']) > 0:
        status = "RTO " + status
        delivery_date = ""
        # Step 3: Get rto_initiated_date (first occurrence of "ReturnInitiated")
        rto_initiated_date = df[df['status'] == 'ReturnInitiated']['time'].min()

        # Step 4: Get rto_delivered_date (last occurrence of "Delivered" after RTO)
        rto_delivered_date = df[(df['status'] == 'Delivered')]['time'].max()
        if not rto_delivered_date:
            rto_delivered_date = ""
    else:
        delivery_date = df[(df['status'] == 'Delivered')]['time'].max()
        if not delivery_date:
            delivery_date = ""
        
    
    # Create the final DataFrame with the required columns
    result_df = pd.DataFrame({
        "awb_num": awb,
        "status": status,
        'pickup_date': [pickup_date],
        'delivery_date': [delivery_date],
        'rto_initiated_date': [rto_initiated_date],
        'rto_delivered_date': [rto_delivered_date]
    })

    return result_df

def process_awb(api_token, jwt, awb):
    try:
        response = get_tracking_details(api_token, jwt, awb)
        
        if not response:
            print(f"Empty response for AWB {awb}. Skipping...")
            return None
        
        # Ensure response has valid data
        if "data" not in response or not response["data"]:
            print(f"No data found in response for AWB {awb}. Skipping...")
            return None
        
        shipping_data_raw = response["data"][0].get("shippingHistory")
        if not shipping_data_raw:
            data = response['data'][0]
            awb_num = data['awbNumber']
            status = data['orderStatus']
            result_df = pd.DataFrame({
                "awb_num": [awb_num],  
                "status": [status],  
                'pickup_date': [""],  
                'delivery_date': [""],  
                'rto_initiated_date': [""],  
                'rto_delivered_date': [""],  
            })
            return result_df
        else:
            shipping_data = json.loads(shipping_data_raw)
            df = _create_df(awb, shipping_data)
            return df

    except Exception as e:
        print(f"Error for AWB {awb}: {e}. Skipping...")
        return None

def get_awb_details(awb_nums, api_token, jwt):
    all_df = []
    failed_df = []

    # Wrap awb_nums with tqdm for progress tracking
    for awb in tqdm(awb_nums, desc="Processing AWBs", unit="AWB"):
        df_1 = process_awb(api_token, jwt, awb)
        if df_1 is not None:
            all_df.append(df_1)
        else:
            failed_df.append(awb)
    
    if all_df: 
        final_df_ats = pd.concat(all_df, ignore_index=True)  # Ignore index to reset the index
        return final_df_ats, failed_df
    else:
        return None, failed_df  # Return None for final_df_ats if no successful data

def main():
    # Get data from Google Sheets
    print("Getting primary df data from Google Sheet")
    df_primary, worksheet, client = get_data_from_google_sheets("ATS Order Flow", "Till_date_data")
    df_primary = df_primary.drop(columns=['total_charges'])
    worksheet_failed = client.open("ATS Order Flow").worksheet("Failed AWBs")
    primary_active_awbs = [num for num in df_primary[df_primary['mapped_status'] == 'active']['awb_num']]
    # awb_failed, _, _ = get_data_from_google_sheets("ATS Order Flow", "Failed AWBs")
    # awb_failed_list = [num for num in awb_failed['Failed']]
    # print("Failed AWBs", len(awb_failed_list))

    print("Getting AWB Numbers for past 30 days orders")
    ee_df_awb = get_ee_df()
    awb_nums_unique = get_awb_from_ee(ee_df_awb)
    print("30 Days AWBs", len(awb_nums_unique))
    final_awbs = list(set(awb_nums_unique + primary_active_awbs))
    print("Total AWBs", len(final_awbs))
    # Authenticate and get JWT
    api_token, jwt = authenticate_easyecom()

    # df_primary_raw, failed_awb_primary = get_awb_details(till_date_awbs_list, api_token, jwt)
    # print(f"Number of AWBs failed in Primary df {len(failed_awb_primary)}")
    # df_primary = processing_ats(df_primary_raw)

    # df_primary.to_csv("ats_primary_example.csv", index=False)

    # Run the async function to get AWB details
    final_df_ats, failed_df = get_awb_details(final_awbs, api_token, jwt)

    # Process and update the primary DataFrame 
    if final_df_ats is not None: 
        final_ats_op = updating_primary_ats(df_primary, final_df_ats)
        final_ats_op.fillna("",inplace=True)
        final_ats_op.to_csv("sample_ats_2.csv", index=False)
        print(f"Number of failed AWBs are: {len(failed_df)}")

        # Update Google Sheets
        print("Clearing existing data on Google Sheet")
        worksheet.batch_clear(["A:H"])
        print("Uploading updated data on Google Sheet")
        worksheet.update([final_ats_op.columns.values.tolist()] + final_ats_op.values.tolist())
        update_script_date("ATS")

        # Append failed AWBs
        if failed_df:  # Check if there are any failed AWBs to append
            print("Uploading Failed AWBs to Google Sheet")
            rows_to_append = [[awb] for awb in failed_df]  # Create a list of lists
            worksheet_failed.append_rows(rows_to_append)  # Append the rows to the worksheet
        else: 
            print("No Failed AWBs!")
    else:
        print("No data present in final updated df! Please check.")
    print("Updated successfully. Exiting script.")

if __name__ == "__main__":
   main()