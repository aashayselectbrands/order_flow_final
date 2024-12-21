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

# Constants
CREDENTIALS_FILE = 'credentials.json'
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
RAW_FILE_PATH = r"Raw data\shiprocket\sr_2_ac.csv"

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

# Status mapping for processing the new df
status_mapping = {
    'OUT FOR PICKUP': 'active',
    'IN TRANSIT-AT DESTINATION HUB': 'active',
    'PICKED UP': 'active',
    'NEW ORDER': 'active',
    'PICKUP EXCEPTION': 'active',
    'PICKUP SCHEDULED': 'active',
    'IN TRANSIT': 'active',
    'OUT FOR DELIVERY': 'active',
    'DELIVERED': 'status_completed',
    'MISROUTED': 'active',
    'REACHED DESTINATION HUB': 'active',
    'SHIPPED': 'active',
    'IN TRANSIT-EN-ROUTE': 'active',
    'RTO IN TRANSIT': 'active',
    'RTO DELIVERED': 'status_completed',
    'UNDELIVERED-2nd Attempt': 'active',
    'RTO INITIATED': 'active',
    'RTO OFD': 'active',
    'CANCELED': 'status_completed',
    'LOST': 'status_completed',
    'UNDELIVERED-3rd Attempt': 'active',
    'UNDELIVERED-1st Attempt': 'active',
    'UNDELIVERED': 'active',
    'UNTRACEABLE': 'status_completed',
    'DESTROYED': 'status_completed',
    'READY TO SHIP': 'active',
    'RETURN PENDING': 'active',
    'DISPOSED OFF': 'status_completed',
    'RETURN DELIVERED': 'status_completed',
    "": "no_status"
}

# Function to process raw shiprocket df and map status_mapping
def processing_raw_sr(sr_dff):
    final_df = sr_dff[["Status", "Courier Company", "AWB Code", "Order Picked Up Date", "Order Delivered Date", "RTO Delivered Date", "COD Remittance Date"]]
    
    final_df.loc[:, 'Order Picked Up Date'] = pd.to_datetime(final_df['Order Picked Up Date'], format = 'mixed', dayfirst=True).dt.strftime('%d-%m-%Y')
    final_df.loc[:, 'Order Delivered Date'] = pd.to_datetime(final_df['Order Delivered Date'], format = 'mixed', dayfirst=True).dt.strftime('%d-%m-%Y')
    final_df.loc[:, 'RTO Delivered Date'] = pd.to_datetime(final_df['RTO Delivered Date'], format = 'mixed', dayfirst=True).dt.strftime('%d-%m-%Y')
    final_df['updated_at'] = date.today().strftime("%d-%m-%Y")
    final_df.drop_duplicates(inplace=True)
    
    # MAPPING THE STATUS
    final_df["mapped_status"] = final_df["Status"].map(status_mapping)
    final_df["mapped_status"].fillna("no_mapping_avl", inplace=True)

    # Helper df for shipment charges 
    charges_df = sr_dff[["AWB Code", "COD Charges", "Freight Total Amount"]].drop_duplicates()

    # Mapping the charges
    final_df["COD Charges"] = final_df["AWB Code"].map(charges_df.set_index("AWB Code")["COD Charges"])  
    final_df["Freight Total Amount"] = final_df["AWB Code"].map(charges_df.set_index("AWB Code")["Freight Total Amount"])  

    final_df = final_df[["Status", "Courier Company", "AWB Code", "Order Picked Up Date", "Order Delivered Date", "RTO Delivered Date", "COD Remittance Date", "mapped_status", "COD Charges", "Freight Total Amount","updated_at"]].drop_duplicates()

    return final_df

# Function to update the primary df 
def updating_primary_sr(sr_new, df_primary):
    new_df = processing_raw_sr(sr_new)

    condition = (df_primary['mapped_status'] != 'status_completed') & (df_primary['AWB Code'].isin(new_df['AWB Code'])) & (~df_primary['AWB Code'].isna())
    df_to_update = df_primary[condition]

    awbs_to_update = new_df[new_df['AWB Code'].isin(df_to_update['AWB Code'])]
    new_awbs = new_df[~new_df['AWB Code'].isin(df_primary['AWB Code'])]

    df_primary = df_primary[~df_primary['AWB Code'].isin(df_to_update['AWB Code'])]    
    df_primary = pd.concat([df_primary, awbs_to_update, new_awbs], ignore_index=True)
    final_df = df_primary[["Status", "Courier Company", "AWB Code", "Order Picked Up Date", "Order Delivered Date", "RTO Delivered Date", "COD Remittance Date", "mapped_status", "COD Charges", "Freight Total Amount", "updated_at"]].drop_duplicates()

    return final_df

def main():
    # Get data from Google Sheets
    print("Getting primary data from Google Sheet")
    df_primary, worksheet = get_data_from_google_sheets("Shiprocket Order Flow", "Till_date_data")
    # df_primary = pd.read_csv("sample_sr_1.csv")
    df_primary.rename(columns={'Order Picked up Date': 'Order Picked Up Date'}, inplace=True)
    print("Getting new Shiprocket data")
    # Get new Shiprocket data for past 30 days and process it 
    sr_new = pd.read_csv(RAW_FILE_PATH)

    print("Getting the final updated output from the 2 dfs")
    # Get the final output df after updating the primary df
    final_df = updating_primary_sr(sr_new, df_primary)
    final_df.fillna("", inplace=True)
    # Uploading the final ouptput to Google Sheet
    final_df.to_csv("sr_upload.csv", index=False)
    print("Clearing the existing data on Google Sheet")
    worksheet.clear()
    print("Uploading the updated data to Google Sheet")
    worksheet.update([final_df.columns.values.tolist()] + final_df.values.tolist())
    update_script_date("Shiprocket")
    print("Data uploaded. Exiting script.")

# Execute the main function
if __name__ == "__main__":
    main()
