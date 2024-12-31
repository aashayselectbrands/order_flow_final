import pandas as pd
import gspread as gs
from oauth2client.service_account import ServiceAccountCredentials

CREDENTIALS_FILE = 'Creds.json'
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

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
    df = pd.DataFrame(data[1:], columns=data[0])
    return df, worksheet