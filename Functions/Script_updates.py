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

CREDENTIALS_FILE = 'credentials.json'
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
SHEET_NAME = "Scripts Update"
WORKSHEET_NAME = "updates"

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
    return pd.DataFrame(data[1:], columns=data[0]), worksheet

def update_script_date(script_name):
    today = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    row_to_append = [script_name, today]
    _, ws = get_data_from_google_sheets(SHEET_NAME, WORKSHEET_NAME)
    ws.append_row(row_to_append)
    print(f"Updated {script_name} on {today}")