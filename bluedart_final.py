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

max_retries = 3
max_re = 3

# Get Bluedart data till date from Google Sheets -----------------------

# Your Google Sheets credentials file
CREDENTIALS_FILE = 'credentials.json'

# Define the scope
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

# Authenticate with Google Sheets API
creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPE)
client = gs.authorize(creds)

# Open the Google Sheets document and worksheet
sheet = client.open("Bluedart Order Flow")
worksheet = sheet.worksheet("Till_date_data")  # Specific worksheet
worksheet_failed = sheet.worksheet("Failed AWBs")

data = worksheet.get_all_values()
df_primary = pd.DataFrame(data[1:], columns=data[0])
df_primary = df_primary.drop(columns=['total_charges'])

print("Primary df", df_primary.head())

status_mapping = {
    "RTO In Transit. Await delivery information": "active",
    "RTO NETWORK DELAY, WILL IMPACT DELIVERY": "active",
    "SHIPMENT DELIVERED": "status_completed",
    "OUT OF DELIVERY AREA": "active",
    "RTO SHIPMENT DELIVERED": "status_completed",
    "In Transit. Await delivery information": "active",
    "CONSIGNEE'S ADDRESS INCORRECT/INCORRECT": "active",
    "CONSIGNEE REFUSED TO ACCEPT": "active",
    "CONSIGNEE ADD IS EDUCATIONAL INSTITUTION": "active",
    "CONTACT CUSTOMER SERVICE": "status_completed",
    "SHIPPER INSTRUCTED TO RTO THE SHIPMENT": "active",
    "Online shipment booked": "active",
    "PICKUP EMPLOYEE IS OUT TO P/U SHIPMENT": "active",
    "CONSIGNEE NOT AVAILABLE": "active",
    "SHIPMENT PICKED UP": "active",
    "NEED DEPARTMENT NAME/EXTENTION NUMBER": "active",
    "DELIVERY ATTEMPTED-PREMISES CLOSED": "active",
    "RTO SHIPMENT ARRIVED": "active",
    "PROHIBITED AREA-ENTRY RESTRICTED FOR DELIVERY": "active",
    "DELAY CAUSED BEYOND OUR CONTROL": "active",
    "RETURN TO SHIPPER": "status_completed",
    "DELIVERED BACK TO SHIPPER": "status_completed",
    "NETWORK ISSUES": "active",
    "NECESSARY CHARGES PENDING FROM CONSIGNEE": "active",
    "LOAD ON HOLD;SPACE CONSTRAINT IN NET VEH": "active",
    "NETWORK DELAY, WILL IMPACT DELIVERY": "active",
    "SHIPMENT REDIRECTED ON SAME AWB": "active",
    "DELIVERY  SCHEDULED FOR NEXT WORKING DAY": "active",
    "CUSTOMER REQUESTED FUTURE DELIVERY: HAL": "active",
    "NO SUCH CONSIGNEE AT THE GIVEN ADDRESS": "active",
    "OUT FOR DELIVERY,  DETAILS AWAITED": "active",
    "DELIVERY DELAYED": "active",
    "MISROUTED-DELIVERY DELAYED": "active",
    "DELIVERY ON NEXT BUSINESS DAY": "active",
    "WRONG PINCODE, WILL IMPACT DELIVERY": "active",
    "RTO LOAD ON HOLD;SPACE CONSTRAINT IN NET VEH": "active",
    "C'NEE SHIFTED FROM THE GIVEN ADDRESS": "active",
    "SHIPMENT MANIFESTED - NOT RECEIVED": "active",
    "RTO CONSIGNEE'S ADDRESS INCORRECT/INCORRECT": "active",
    "RTO HELD AT ORIGIN FOR FURTHER PROCESSING": "active",
    "BULK ORDER, REFUSED BY CONSIGNEE": "active",
    "LOAD ON HOLD;SPACE CONSTRAINT-COMML FLT": "active",
    "RTO NETWORK ISSUES": "active",
    "RTO CUSTOMER REQUESTED FUTURE DELIVERY: HAL": "active",
    "OFFICE CLOSED; UNABLE TO DELIVER": "active",
    "RTO SHIPMENT REDIRECTED ON SAME AWB": "active",
    "NETWORK DELAY, WILL IMPACT TIMELY DELIVERY": "active",
    "RTO at Origin outscan for Delivery": "active",
    "LOAD/VEHICLE ARRIVAL AT TRANSIT HUB": "active",
    "SHIPMENT FURTHER CONNECTED": "active",
    "SHIPMENT ARRIVED": "active",
    "LOAD/VEHICLE ARRIVED AT DELIVERY LOC": "active",
    "CONSIGNEE HAS GIVEN BDE HAL ADDRESS": "active",
    "PICKUP HAS BEEN REGISTERED": "active",
    "SHIPMENT DESTROYED": "status_completed",
    "PICKUP NOT ATTEMPTED, ADDRESS INCOMPLETE": "active",
    "RTO PROCESS COMPLETED, READY TO DESPATCH": "active",
    "REDIRECTED ON SAME AWB TO SHIPPER": "active",
    "Rejected AWB Inscanned": "active",
    "CONSIGNEE REFUSED TO PAY NECESSARY CHARGES": "active",
    "RTO CONTACT CUSTOMER SERVICE": "status_completed",
    "Incorrect Waybill number or No Information": "active",
    "RTO SHIPMENT MANIFESTED - NOT RECEIVED": "active",
    "SHIPMENT MISPLACED": "active",
    "UNDELIVERED RETURN/REDIRECT INITIATED": "active",
    "ON HOLD FOR REGULATORY PAPERWORK": "active",
    "CANVAS BAG RECEIVED AS OVERAGE": "active",
    "CNEE REFUSED ID/OTP NOT SHARED-INCORRECT": "active",
    "ON HOLD;SPACE CONSTRAINT AT DELVRY LOC": "active",
    "AUTO PICKUP REGISTERED": "active",
    "DELIVERY SCHEDULED THROUGH APPOINTMENT": "active",
    "": "active",
    "NETWORK ISSUE, WILL IMPACT DELIVERY": "active",
    "PICKUP CANCELLED BY CALL": "status_completed",
    "RTO DELIVERY DELAYED": "active",
    "UD SHIPMENT RECEIVED FOR FURTHER PROCESS": "active"
}

def processing_bluedart(bd_raw):
    bd_raw['Pickup Date'] = pd.to_datetime(bd_raw['Pickup Date'], format="mixed").dt.strftime("%d-%m-%Y")
    bd_raw['Last Status Date'] = pd.to_datetime(bd_raw['Last Status Date'], format="mixed").dt.strftime("%d-%m-%Y")
    bd_raw['Delivery Date'] = pd.to_datetime(bd_raw['Delivery Date'], format="mixed").dt.strftime("%d-%m-%Y")
    bd_raw['RTO Initiation Date'] = pd.to_datetime(bd_raw['RTO Initiation Date'], format="mixed").dt.strftime("%d-%m-%Y")
    bd_raw['RTO Delivered Date'] = pd.to_datetime(bd_raw['RTO Delivered Date'], format="mixed").dt.strftime("%d-%m-%Y")
    bd_raw['Updated at'] = pd.to_datetime(bd_raw['Updated at']).dt.strftime("%d-%m-%Y")

    bd_raw['mapped_status'] = bd_raw['Status'].map(status_mapping)
    bd_raw['mapped_status'].fillna("no_mapping_avl", inplace=True)
    
    return bd_raw

def updating_primary_bluedart(primary_df, new_df):
    primary_df["AWB Number"] = primary_df["AWB Number"].astype(int)
    new_df_proc = processing_bluedart(new_df)
    print(new_df_proc.head())
    new_df_proc["AWB Number"] = new_df_proc["AWB Number"].astype(int)

    condition = (primary_df['mapped_status'] != 'status_completed') & (primary_df['AWB Number'].isin(new_df_proc['AWB Number'])) & (~primary_df['AWB Number'].isna())
    df_to_update = primary_df[condition].reset_index(drop=True)

    awbs_to_update = new_df_proc[new_df_proc['AWB Number'].isin(df_to_update['AWB Number'])].reset_index(drop=True)
    new_awbs = new_df_proc[~new_df_proc['AWB Number'].isin(primary_df['AWB Number'])].reset_index(drop=True)

    primary_df = primary_df[~primary_df['AWB Number'].isin(df_to_update['AWB Number'])].reset_index(drop=True)

    if 'index' in primary_df.columns:
        primary_df.drop(columns=['index'], inplace=True)
    if 'index' in awbs_to_update.columns:
        awbs_to_update.drop(columns=['index'], inplace=True)
    if 'index' in new_awbs.columns:
        new_awbs.drop(columns=['index'], inplace=True)

    print(primary_df.columns.values)
    print(awbs_to_update.columns.values)
    print(new_awbs.columns.values)

    primary_df = pd.concat([primary_df, awbs_to_update, new_awbs], ignore_index=True)

    if 'index' in primary_df.columns: 
        primary_df.drop(columns=['index'], inplace=True)
    
    return primary_df

# LAST 30 DAYS DATA FETCHING
BASE_URL_EASYECOM = "https://api.easyecom.io"
AUTH_EMAIL_EASYECOM = "saksham@selectbrands.in"
AUTH_PASS_EASYECOM = "Easyecom@123!"

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
    # start_date_str = '2024-10-01'
    # end_date_str = '2024-10-31'

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

    # df = pd.read_csv("minisales_aug_raw.csv")
    
    # Filtering for just Bluedart orders
    df = df[df['Courier Name'] == "BLUEDART"]

    # Extract date part from 'Order Date' column
    df['Order Date'] = pd.to_datetime(df['Order Date']).dt.strftime('%Y-%m-%d')

    df["Order Number"] = df["Order Number"].str.replace("`", "")
    df["Tracking Number"] = df["Tracking Number"].str.replace("`", "")

    df = df[["Order Date", "Order Number", "Courier Name", "Tracking Number", "SKU", "Order Status", "Shipping Status", "Selling Price", "Suborder Quantity"]]
    
    df['Suborder Quantity'] = pd.to_numeric(df['Suborder Quantity'], errors='coerce').fillna(0).astype(int)

    # Convert 'Selling Price' to numeric
    df['Selling Price'] = pd.to_numeric(df['Selling Price'], errors='coerce').fillna(0).astype(float)

    # Round 'Selling Price' to 2 decimal places
    df['Selling Price'] = df['Selling Price'].round(2)

    df.to_csv('past_30_days.csv', index=False)
    df.fillna("", inplace=True)

    return df

def get_awb_from_ee(ee_df):
    awb_nums = [num for num in ee_df['Tracking Number']]
    print(f"Number of AWBs: {len(awb_nums)}")
    awb_nums_unique = list(set(awb_nums))
    print(f"Number of unique AWBs: {len(awb_nums_unique)}")
    return awb_nums_unique

def process_new_awbs(awbs_new, df_primary):
    df_common = df_primary[df_primary['AWB Number'].isin(awbs_new)]
    df_primary_active = df_primary[(df_primary['AWB Number'].isin(awbs_new)) & (df_primary['mapped_status'] == 'active')]
    df_primary_completed = df_primary[(df_primary['AWB Number'].isin(awbs_new)) & (df_primary['mapped_status'] == 'status_completed')]
    active_awbs_new = [num for num in df_primary_active['AWB Number']]
    common_awbs = [num for num in df_common['AWB Number']]
    completed_awbs_new = [num for num in df_primary_completed['AWB Number']]
    new_awbs_new = [item for item in awbs_new if item not in completed_awbs_new and item not in active_awbs_new]
    final_awbs_new = active_awbs_new + new_awbs_new
    print("Number of awbs for 30 days: ", len(awbs_new))
    print("Number of common awbs: ", len(common_awbs))
    print("Number of common active awbs: ", len(active_awbs_new))
    print("Number of common completed awbs: ", len(completed_awbs_new))
    print("Number of new awbs: ", len(new_awbs_new))
    print("Number of final awbs: ", len(final_awbs_new))

    return final_awbs_new

def BD_auth_jwt():
    url = "https://apigateway.bluedart.com/in/transportation/token/v1/login"

    headers = {
        "ClientID": "CCU82270"
    }

    auth_res = requests.request("GET", url, headers=headers)
    print(auth_res.text)
    return auth_res.json()["JWTToken"]

async def make_request(awb, session, jwt):
    retries = 0
    url = "https://apigateway.bluedart.com/in/transportation/tracking/v1"
    
    # Set up the parameters for the API request
    params = {
        'handler': 'tnt',
        'action': 'custawbquery',
        'loginid': 'CCU82270',  # Replace 'APIID' with your login ID
        'awb': 'awb',
        'numbers': awb,
        'format': 'json',
        'lickey': 'uq5qitlevuspgojivuujmxngt5lmtoql',  # Replace with your API key
        'verno': '1',
        'scan': '1',
    }
    
    headers = {"JWTToken": jwt}  # Replace 'jwt' with your token
    
    while retries < max_retries:
        try:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 500:
                    retries += 1
                    await asyncio.sleep(2 ** retries)  # Exponential backoff
                else:
                    return None
        except Exception as e:
            return None

async def get_awb_data(awb, session, jwt):
    retries = 0
    while retries < max_re:
        try:
            data = await make_request(awb, session, jwt)

            if not data or 'ShipmentData' not in data or 'Shipment' not in data['ShipmentData']:
                retries += 1
                await asyncio.sleep(3)
            else:
                shipment = data['ShipmentData']['Shipment']
                pickup_date = ""
                status_final = ""
                status_date_final = ""
                status_type = ""
                waybill_no = ""
                rto_initiation_dt = ""
                rto_del_dt = ""
                del_dt = ""
                updated_dt = date.today()

                if shipment[0].get('StatusType') == "RT":
                    pickup_date = shipment[0].get('PickUpDate', "")
                    rto_initiation_dt = shipment[0].get('StatusDate', "")
                    status_type = shipment[0].get('StatusType', "")
                    waybill_no = shipment[0].get('WaybillNo', "")
                    if len(shipment) > 1:
                        status_final = "RTO " + shipment[1].get('Status', "")
                        if 'Scans' in shipment[1] and len(shipment[1]['Scans']) > 0:
                            status_date_final = shipment[1]['Scans'][0]['ScanDetail'].get('ScanDate', "")
                            if shipment[1]['StatusType'] == 'DL':
                                rto_del_dt = shipment[1]['StatusDate']                        
                    else:
                        status_final = shipment[0].get('Status', "")
                        status_date_final = shipment[0].get('StatusDate', "")
                    
                else:
                    if shipment[0].get('StatusType', "") == "DL":
                        del_dt = shipment[0].get('StatusDate', "")
                    pickup_date = shipment[0].get('PickUpDate', "")
                    status_final = shipment[0].get('Status', "")
                    status_date_final = shipment[0].get('StatusDate', "")
                    status_type = shipment[0].get('StatusType', "")
                    waybill_no = shipment[0].get('WaybillNo', "") 

                final_dict = [{
                    'AWB Number': waybill_no,
                    'Status': status_final,
                    'Status Type': status_type,                    
                    'Pickup Date': pickup_date, 
                    'Last Status Date': status_date_final, 
                    'Delivery Date': del_dt,
                    'RTO Initiation Date': rto_initiation_dt,
                    'RTO Delivered Date': rto_del_dt,
                    'Updated at': updated_dt
                }]

                df = pd.DataFrame(final_dict)
                return df

        except Exception as e:
            return None

    return None

async def get_awb_details(awb_nums, jwt):
    all_df = []
    failed_awbs = []  # List to store failed AWBs
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        async for awb in tqdm(awb_nums, desc="Processing AWBs", unit="awb", ncols=100):
            df = await get_awb_data(awb, session, jwt)
            if df is not None:
                all_df.append(df)
            else:
                failed_awbs.append(awb)

            # Time estimation logic
            elapsed_time = time.time() - start_time
            avg_time_per_awb = elapsed_time / (awb_nums.index(awb) + 1)
            remaining_time = avg_time_per_awb * (len(awb_nums) - (awb_nums.index(awb) + 1))

    # Save the final dataframe
    if all_df:
        final_df_bd = pd.concat(all_df, ignore_index=True)
        return final_df_bd, failed_awbs

    else:  # Return or process the failed AWBs
        return failed_awbs

# Main function to orchestrate the entire process
async def main():
    # Get AWB numbers from EasyEcom data
    ee_df = get_ee_df()
    awb_nums_new = get_awb_from_ee(ee_df)
    print("Number of AWBs for past 30 days before processing", len(awb_nums_new))
    awb_nums_new = process_new_awbs(awb_nums_new, df_primary)
    awb_nums_new = list(set(awb_nums_new))
    print("Unique new awbs final", len(awb_nums_new))
    #Filter out primary df for all active awbs
    df_primary_active = df_primary[df_primary['mapped_status'] == 'active']
    #Get the active awb numbers
    awb_nums_primary_active = list(set([num for num in df_primary_active['AWB Number']]))
    awb_nums_for_new = list(set(awb_nums_new + awb_nums_primary_active))
    print(f"Number of common AWBs are: {len(awb_nums_new + awb_nums_primary_active) - len(set(awb_nums_new + awb_nums_primary_active))}")

    # Authenticate and get JWT
    jwt = BD_auth_jwt()
    print(jwt)

    # Run the async function to get AWB details
    new_df, failed_awbs = await get_awb_details(awb_nums_for_new, jwt)

    new_df.to_csv('new_df_test_bd.csv',index=False)

    # new_df = pd.read_csv('new_df_test_bd.csv')

    # Update the primary DataFrame with new data
    final_bd_op = updating_primary_bluedart(df_primary, new_df)

    final_bd_op.to_csv("sample_bd_2.csv", index=False)
    # print(f"Failed AWBs are: {failed_awbs}")

    final_bd_op.fillna("", inplace=True)
    print(final_bd_op.head(10))
    # Update Google Sheets
    print("Clearing existing data on Google Sheet")
    worksheet.batch_clear(['A:J'])
    print("Uploading updated data on Google Sheet")
    worksheet.update([final_bd_op.columns.values.tolist()] + final_bd_op.values.tolist())
    update_script_date("Bluedart")
    if failed_awbs:  # Check if there are any failed AWBs to append
        print("Uploading Failed AWBs to Google Sheet")
        # Convert the list to the format required by append_rows (list of lists)
        rows_to_append = [[awb] for awb in failed_awbs]  # Create a list of lists
        worksheet_failed.append_rows(rows_to_append)  # Append the rows to the worksheet
    else: 
        print("No Failed AWBs!")
    print("Updated successfully. Exiting script.")

# Execute the main function
if __name__ == "__main__":
    asyncio.run(main())