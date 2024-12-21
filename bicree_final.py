import requests
import json
import pandas as pd
import time
from datetime import datetime, timedelta, date
import zipfile
import io
import gspread as gs
from oauth2client.service_account import ServiceAccountCredentials
from Functions.Script_updates import update_script_date

CREDENTIALS_FILE = 'credentials.json'
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
RAW_FILE_PATH = r"Raw data\Bicree\Bicree_90_days_till_21_dec.csv"

# Function to authenticate with Google Sheets API
def authenticate_google_sheets():
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPE)
    client = gs.authorize(creds)
    return client

# Function to get data from Google Sheets
def get_data_from_google_sheets(sheet_name, worksheet_name):
    client = authenticate_google_sheets()
    sheet = client.open(sheet_name)
    worksheet = sheet.worksheet(worksheet_name)  # Specific worksheet
    data = worksheet.get_all_values()
    return pd.DataFrame(data[1:], columns=data[0]), worksheet  # Skip header row

status_mapping = {
    'Cancelled': 'status_completed',
    'Damaged': 'status_completed',
    'Delivered': 'status_completed',
    'In Transit': 'active',
    'Lost': 'status_completed',
    'NDR': 'active',
    'Out For Delivery': 'active',
    'Out For Pickup': 'active',
    'Picked Up': 'active',
    'Pickup Exception': 'active',
    'Pickup Scheduled': 'active',
    'Queued': 'active',
    'RTO Delivered': 'status_completed',
    'RTO In Transit': 'active',
    'RTO Initiated': 'active',
    'RTO Out For Delivery': 'active',
    'RTO Picked Up': 'active',
    'Undelivered': 'active',
    'ZFM': 'active',
    'ZLM': 'active',
    'RTO Undelivered': 'active',
    'Z': 'active'
}

def processing_bicree_raw(bicree_df_raw):

    df_1 = bicree_df_raw[['Order No','Delivery Name', 'Delivery No', 'Invoice Amount', 'Item Count', 'Gr Provider', 'Awb No', 'Status', 'Status Ts', 'Order Date', 'Cod Charges', 'Cod Payout Amount', 'Shipping Charges', 'Rto Cod Charges', 'Rto Charges', 'Actual Pickup Date', 'Delivery Date', 'Rto Delivery Date']]

    df_1['Status Ts'] = pd.to_datetime(df_1['Status Ts'], format="mixed").dt.strftime("%d-%m-%Y")
    df_1['Order Date'] = pd.to_datetime(df_1['Order Date'], format="mixed").dt.strftime("%d-%m-%Y")
    df_1['Actual Pickup Date'] = pd.to_datetime(df_1['Actual Pickup Date'], format="mixed").dt.strftime("%d-%m-%Y")
    df_1['Delivery Date'] = pd.to_datetime(df_1['Delivery Date'], format="mixed").dt.strftime("%d-%m-%Y")
    df_1['Rto Delivery Date'] = pd.to_datetime(df_1['Rto Delivery Date'], format="mixed").dt.strftime("%d-%m-%Y")
    df_1['updated_at'] = date.today().strftime("%d-%m-%Y")

    df_1['mapped_status'] = df_1['Status'].map(status_mapping)
    df_1['mapped_status'].fillna("no_mapping_avl", inplace=True)

    final_df = df_1[['Order No','Delivery Name', 'Delivery No', 'Invoice Amount', 'Item Count', 'Gr Provider', 'Awb No', 'Status', 'mapped_status', 'Status Ts', 'Order Date', 'Cod Charges', 'Cod Payout Amount', 'Shipping Charges', 'Rto Cod Charges', 'Rto Charges', 'Actual Pickup Date', 'Delivery Date', 'Rto Delivery Date', 'updated_at']]
    
    return final_df
    
def updating_primary_bic(primary_df, new_df):
    # primary_df = pd.read_csv("df1.csv")
    new_df_proc = processing_bicree_raw(new_df)
    # print(new_df_proc.head())
    
    condition = (primary_df['mapped_status'] != 'status_completed') & (primary_df['Awb No'].isin(new_df['Awb No'])) & (~primary_df['Awb No'].isna())
    df_to_update = primary_df[condition].reset_index(inplace=False)

    awbs_to_update = new_df_proc[new_df_proc['Awb No'].isin(df_to_update['Awb No'])].reset_index(inplace=False) 
    new_awbs = new_df_proc[~new_df_proc['Awb No'].isin(primary_df['Awb No'])].reset_index(inplace=False)

    primary_df = primary_df[~primary_df['Awb No'].isin(df_to_update['Awb No'])]
    primary_df = pd.concat([primary_df, awbs_to_update, new_awbs], ignore_index=True)

    if 'index' in primary_df.columns: 
        primary_df.drop(columns=['index'], inplace=True)

    return primary_df

def main():
    # Get data from Google Sheets
    print("Getting primary data from Google Sheet")
    df_primary, worksheet = get_data_from_google_sheets("Bicree Order Flow", "Till_date_data")
    df_primary = df_primary[~df_primary['Status'].isin(['Z','ZFM','ZLM'])]

    # Get new Shiprocket data for past 30 days and process it 
    print("Getting new Shiprocket data")
    bicree_new = pd.read_csv(RAW_FILE_PATH)
    bicree_new = bicree_new[~bicree_new['Status'].isin(['Z','ZFM','ZLM'])]

    # Get the final output df after updating the primary df
    print("Getting the final updated output from the 2 dfs")
    final_df = updating_primary_bic(df_primary, bicree_new)

    final_df.fillna("", inplace=True)
    final_df.to_csv("csv files/sample_bicree_2.csv", index=False)

    # Uploading the final ouptput to Google Sheet
    print("Clearing the existing data on Google Sheet")
    worksheet.clear()
    print("Uploading the updated data to Google Sheet")
    worksheet.update([final_df.columns.values.tolist()] + final_df.values.tolist())
    update_script_date("Bicree")
    print("Data uploaded. Exiting script.")
    # Upload final output to CSV
    # print("Processed data saved to sample_bicree_2.csv")

# Execute the main function
if __name__ == "__main__":
    main()