import pandas as pd
import requests
import json
import time
from datetime import datetime, timedelta, date
import zipfile
import os
from gspread_dataframe import set_with_dataframe
import io
from Functions.ee import authenticate_easyecom, generate_sales_report, download_report
from dotenv import load_dotenv
from Functions.Script_updates import update_script_date
from Functions.google_sheet import get_data_from_google_sheets

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
    df = df[df['Courier Aggregator Name'] == "Delhivery"]

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

    df.fillna("", inplace=True)

    return df

def get_awb_from_ee(ee_df):
    awb_nums = [num for num in ee_df['Tracking Number']]
    print(f"Number of AWBs: {len(awb_nums)}")
    awb_nums_unique = list(set(awb_nums))
    print(f"Number of unique AWBs: {len(awb_nums_unique)}")
    return awb_nums_unique

def process_response(resp, tracking_details):
    for order in resp['ShipmentData']:
        shipment = order.get('Shipment', {})
        status = shipment['Status']['Status']
        status_type = shipment['Status']['StatusType']
        pickup_date = shipment['PickedupDate']
        if pickup_date is not None:
            pickup_date = pd.to_datetime(pickup_date).strftime("%d-%m-%Y")
        delivery_date = shipment['DeliveryDate']
        if delivery_date is not None:
            delivery_date = pd.to_datetime(delivery_date).strftime("%d-%m-%Y")
        rto_initiation_date = shipment['RTOStartedDate']
        if rto_initiation_date is not None:
            rto_initiation_date = pd.to_datetime(rto_initiation_date).strftime("%d-%m-%Y")
        rto_delivered_date = shipment['ReturnedDate']
        if rto_delivered_date is not None:
            rto_delivered_date = pd.to_datetime(rto_delivered_date).strftime("%d-%m-%Y")
        awb_num = shipment['AWB']
        updated_at = date.today().strftime("%d-%m-%Y")
        final_dict = {
                    'awb_number': awb_num,
                    'status': status,
                    'status_type': status_type,                    
                    'pickup_date': pickup_date, 
                    'delivery_date': delivery_date,
                    'rto_initiation_date': rto_initiation_date,
                    'rto_delivered_date': rto_delivered_date,
                    'updated_at': updated_at
                }
        tracking_details.append(final_dict)

def fetch_awb_details(awbs, mode, tracking_details):
    # Define the base URL and query parameters
    base_url = "https://track.delhivery.com/api/v1/packages/json/"
    
    load_dotenv()

    if mode == 'surface':
        token = os.getenv("SURFACE_TOKEN")
    else:
        token = os.getenv("EXPRESS_TOKEN")

    # Define the query parameters
    params = {
        "waybill": awbs,  # Replace with the actual AWB number
        "token": token
    }

    # Send the GET request
    response = requests.get(base_url, params=params)

    # Check the response status
    if response.status_code == 200:
        # Parse and display the JSON data
        tracking_data = response.json()
        process_response(tracking_data, tracking_details)
    else:
        print(f"Failed to fetch data. HTTP Status Code: {response.status_code}")
        print("Response:", response.text)

def chunk_awbs(awbs, chunk_size=50):
    """
    Yield successive chunks of size `chunk_size` from the list `awbs`.
    """
    for i in range(0, len(awbs), chunk_size):
        yield awbs[i : i + chunk_size]

def update_df(old_df, new_df):
    old_filtered_df = old_df[~old_df['awb_number'].isin(new_df['awb_number'])]
    final_df = pd.concat([old_filtered_df, new_df])

    return final_df

def status_mapping(row):
    if row['status_type'] == 'DL':
        return 'status_completed'
    else:
        return 'active'

def main():
    print("Getting primary df from worksheet.")
    df_primary, ws = get_data_from_google_sheets('Delhivery Order Flow', 'Till_date_data')

    print("Getting the AWBs for the past 30 days from EE.")
    df_ee = get_ee_df()
    awb_nums = get_awb_from_ee(df_ee)
    print("Number of AWBs from past 30 days:", len(awb_nums))
    df_primary_active = df_primary[df_primary['mapped_status'] == 'active']
    awb_nums_primary_active = list(set([num for num in df_primary_active['awb_number']]))
    awb_nums_for_new = list(set(awb_nums + awb_nums_primary_active))
    print("Total number of AWBs to track:", len(awb_nums_for_new))

    surface_awbs = [awb for awb in awb_nums_for_new if awb.startswith('35')]
    express_awbs = [awb for awb in awb_nums_for_new if awb.startswith('34')]
    print("Number of Surface AWBs are:",len(surface_awbs))
    print("Number of Express AWBs are:",len(express_awbs))

    tracking_details = []

    for chunk in chunk_awbs(surface_awbs, 50):
        awbs_str = ",".join(chunk)  # Convert list to comma-separated string
        fetch_awb_details(awbs_str, mode='surface', tracking_details=tracking_details)

    # Process Express AWBs in batches
    for chunk in chunk_awbs(express_awbs, 50):
        awbs_str = ",".join(chunk)
        fetch_awb_details(awbs_str, mode='express', tracking_details=tracking_details)

    df = pd.DataFrame(tracking_details)
    df['mapped_status'] = df.apply(status_mapping, axis=1)
    print("The shape of the processed df is:", df.shape)
    print("The number of AWBs which could not be processed are:", len(awb_nums_for_new)-len(df))

    print("Updating the old df with the new data.")
    df_final = update_df(df_primary, df)
    df_final.to_csv('csv files/delhivery_test.csv', index=False)
    print("Clearing the Delhivery worksheet.")
    ws.batch_clear(['A:I'])
    print("Updating the Delhivery worksheet.")
    set_with_dataframe(worksheet=ws, dataframe=df_final)
    update_script_date('Delhivery')

if __name__ == "__main__":
    main()