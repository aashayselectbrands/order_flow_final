import requests
import json
import pandas as pd
from calendar import monthrange
import numpy as np
import time
from datetime import datetime, timedelta, date
import zipfile
import io
import gspread as gs
from oauth2client.service_account import ServiceAccountCredentials

BASE_URL_EASYECOM = "https://api.easyecom.io"
AUTH_EMAIL_EASYECOM = "dhruv.pahuja@selectbrands.in"
AUTH_PASS_EASYECOM = "Analyst@123#"

CREDENTIALS_FILE = 'Creds.json'
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

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

def get_order_details(api_token, jwt_token, reference_code):
    url = f"{BASE_URL_EASYECOM}/orders/V2/getOrderDetails?reference_code={reference_code}"

    headers = {
        "x-api-key": api_token,
        "Authorization": f"Bearer {jwt_token}"
    }

    response = requests.get(url, headers=headers)
    
    return response

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
            print(f"Retrying for report id for {start_date} to {end_date} in 30 seconds...")
            time.sleep(30)
    return report_id

def download_report(api_token, jwt_token, report_id):
    url = f"{BASE_URL_EASYECOM}/reports/download?reportId={report_id}"
    headers = {
        "x-api-key": api_token,
        "Authorization": f"Bearer {jwt_token}"
    }
    while True:
        response = requests.get(url, headers=headers)
        if "data" in response.json() and "downloadUrl" in response.json()["data"]:
            download_url = response.json()["data"]["downloadUrl"]
            break
        else:
            print("Report data not available yet. Retrying in 30 seconds...")
            time.sleep(30)
    return download_url


def final_sales_df(api_token, jwt, start_date, end_date):
    # today.strftime("%Y-%m-%d")
    report_id = generate_sales_report(api_token, jwt, start_date, end_date)
    print(f"Sales report for {start_date} to {end_date} generated. Report ID:", report_id)

    download_url = ""
    while len(download_url) == 0:
        print("Retrying for report in 30 seconds...")
        time.sleep(30)
        download_url = download_report(api_token, jwt, report_id)

    print("Download URL for 45 days report:", download_url)
    response = requests.get(download_url, stream=True)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        file_names = z.namelist()
        if not file_names:
            raise ValueError("The ZIP file is empty. Unable to proceed.")
        csv_content = z.open(file_names[0]).read()
        
    df = pd.read_csv(io.BytesIO(csv_content))

    return df

def process_easyecom_data(raw_df, corp=False, shopify=True):
    if shopify:
        raw_df = raw_df[raw_df.loc[:, 'MP Name'].str.lower() == "shopify"]
    elif corp:
        raw_df = raw_df[(raw_df.loc[:, 'MP Name'].str.lower() == "offline") | (raw_df.loc[:, 'MP Name'].str.lower() == "b2b")]
    else:
        raw_df = raw_df[raw_df.loc[:, 'MP Name'].str.lower() == "offline"]

    raw_df.loc[:, "Order Number"] = raw_df.loc[:, "Order Number"].str.replace("`", "")
    raw_df.loc[:, "Tracking Number"] = raw_df.loc[:, "Tracking Number"].str.replace("`", "")

    # basic column and type change  
    raw_df["combine_id"] = raw_df["Order Number"] + "_" + raw_df["SKU"]
    raw_df["combine_id_mp"] = raw_df["Order Number"] + "_" + raw_df["Marketplace Sku"]
    raw_df["pickup_canceled"] = ""	
    raw_df["rto_initiated"] = ""
    raw_df["Order Date"] = pd.to_datetime(raw_df["Order Date"]).dt.date
    raw_df["Manifested At"] = pd.to_datetime(raw_df["Manifested At"]).dt.date
    raw_df["Cancelled At"] = pd.to_datetime(raw_df["Cancelled At"]).dt.date

    raw_df["Shipping Status new"] = raw_df.apply(
        lambda x: "No" if pd.isna(x["Shipping Status"]) or x["Shipping Status"] == "" or (x["Shipping Status"] in ["Cancelled","Out For Pickup","Shipment Created","Pickup Exception","Pickup Scheduled"] and x["Order Status"] in ["Ready to dispatch"] ) or x["Shipping Status"] in ["Cancelled","Out For Pickup","Shipment Created","Pickup Exception","Pickup Scheduled"] else "Yes",
        axis=1
    )
    # Fill 'Cancelled Status' based on conditions
    raw_df["Cancelled Status"] = raw_df.apply(
        lambda x: "Yes" if x["Order Status"] in ["Cancelled", "CANCELLED"] or x["Shipping Status"] in 
        ["Cancelled"]   else "No",
        axis=1
    )

    # Fill 'Delivered' based on conditions
    raw_df["Delivered"] = raw_df.apply(
        lambda x: "Yes" if x["Shipping Status"] in ["Delivered"]
        else ("RTO" if  x["Shipping Status"] in 
        ["Delivered To Origin", "RTO initiated", "RTO In-Transit", "Returned"] else "Active"),
        axis=1
    )

    raw_df = raw_df[["Suborder No", "Client Location", "Order Date", "Order Number", "SKU","Marketplace Sku","combine_id", "combine_id_mp", "Suborder Quantity", 'Selling Price', 'Courier Aggregator Name', 'Courier Name', "Tracking Number",
            'Order Status', 'Shipping Status', 'Payment Mode', 'Payment Transaction ID', 'Manifested At', 'Cancelled At', 'Delivered At', "pickup_canceled", "rto_initiated", "Shipping Status new", "Cancelled Status", "Delivered", "Batch ID", "Shipping State","Shipping Zip Code", "Message", "Shipping Customer Name", "Mobile No"
    ]]
    return raw_df

def process_offline_data(raw_df):
    offline_data = process_easyecom_data(raw_df,corp=False, shopify=False)
    offline_data = offline_data[~offline_data["Order Number"].str.lower().str.contains("corp")]
    offline_data["clean_id"] = offline_data["Order Number"].str.split("_").str[0]
    offline_data["clean_id_sku_comb"] = offline_data['clean_id'] + '_' + offline_data['SKU']
    reship_df = offline_data[offline_data["Order Number"].str.contains("_RS")]
    replace_df = offline_data[offline_data["Order Number"].str.contains("_RP")]
    return reship_df, replace_df

def process_offline_data_missing(raw_df):
    offline_data = process_easyecom_data(raw_df,corp=False, shopify=False)
    offline_data = offline_data[~offline_data["Order Number"].str.lower().str.contains("corp")]
    offline_data["clean_id"] = offline_data["Order Number"].str.split("_").str[0]
    offline_data["clean_id_sku_comb"] = offline_data['clean_id'] + '_' + offline_data['SKU']
    ms_df = offline_data[offline_data["Order Number"].str.contains("_MS")]
    return ms_df

def process_corp_data(raw_df):
    corp_df = process_easyecom_data(raw_df,corp=True, shopify=False)
    corp_df = corp_df[corp_df["Order Number"].str.lower().str.contains("corp|samp", na=False)]
    return corp_df

def pd_cost_calc(row):
    if row['pd_cost'] != 'no_cost_avl':
        pd_cost = row['pd_cost']
        if "," in pd_cost:
            pd_cost = row['pd_cost'].replace(",", "")
        return (pd.to_numeric(pd_cost) * pd.to_numeric(row['Suborder Quantity']))
    else:
        return 'no_cost_avl'