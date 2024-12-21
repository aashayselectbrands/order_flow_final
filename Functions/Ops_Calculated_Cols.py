import pandas as pd
from datetime import datetime, timedelta, timezone

def date_missing(row):
    if pd.isna(row['pickup_date']):  # Check if pickup_date is NaN or empty
        pickup_date = row['Manifested At']
    else:
        pickup_date =  row['pickup_date']
        
    if row['final_status'] == 'Delivered':
        if pd.isna(pickup_date) or pd.isna(row['delivered_date']):
            return 'Yes'
        else:
            return 'No'
    elif 'forced_closure' in row['final_status']:
        return 'No'
    elif row['final_status'] == 'Cancelled':
        if pd.isna(row['Cancelled At']):
            return 'Yes'
        else:
            return 'No'
    elif row['final_status'] == 'Active':
        if pd.isna(pickup_date):
            return 'Yes'
        else:
            return 'No'
    elif row['final_status'] == 'Lost or Damaged':
        if pd.isna(pickup_date):
            return 'Yes'
        else:
            return 'No'
    elif row['final_status'] == 'RTO' and row['mapped_status'] == 'status_completed':
        if pd.isna(row['rto_delivered_date']) or row['rto_delivered_date'] == "" or pd.isna(pickup_date):
            return 'Yes'
        else:
            return 'No'
    elif row['final_status'] == 'RTO':
        if pd.isna(pickup_date):
            return 'Yes'
        else:
            return 'No'
    else:
        return 'No'
    
def o2c(row):
    if row['final_status'] == 'Cancelled':
        if row['date_missing'] == 'Yes':
            return 'date_missing'
        else:
            return (row['Cancelled At'] - row['Order Date']).days
    else:
        return 'NA'

def o2s(row):
    if pd.isna(row['pickup_date']): 
        # if pd.isna(row['Manifested At']):
        #     return 'date_missing'
        # else:
        pickup_date = row['Manifested At']
    else:
        pickup_date =  row['pickup_date']
    
    if row['final_status'] not in ["Not Shipped", "Cancelled"]:
        if row['date_missing'] == 'Yes':
            return 'date_missing'
        else:
            if pd.isna(pickup_date):
                return 'NA'
            else:
                return (pickup_date - row['Order Date']).days
    else:
        return 'NA'
    
def o2ns(row):
    if row['final_status'] == 'Not Shipped':
        if row['date_missing'] == 'Yes':
            return 'date_missing'
        else:
            return (datetime.today() - row['Order Date']).days
    else:
        return 'NA'

def s2d(row):
    if pd.isna(row['pickup_date']):  # Check if pickup_date is NaN or empty
        pickup_date = row['Manifested At']
    else:
        pickup_date =  row['pickup_date']
    if row['final_status'] == 'Delivered':
        if row['date_missing'] == 'Yes':
            return 'date_missing'
        else:
            return (row['delivered_date'] - pickup_date).days
    elif row['final_status'] == 'RTO' and row['mapped_status'] == 'status_completed':
        if row['date_missing'] == 'Yes':
            return 'date_missing'
        else:
            return (row['rto_delivered_date'] - pickup_date).days
    else:
        return 'NA'
    
def s2nd(row):
    if pd.isna(row['pickup_date']):  # Check if pickup_date is NaN or empty
        pickup_date = row['Manifested At']
    else:
        pickup_date =  row['pickup_date']
    if row['final_status'] not in ['Cancelled', 'Not Shipped', 'Delivered'] and not (row['final_status'] == 'RTO' and row['mapped_status'] == 'status_completed'):
        if row['date_missing'] == 'Yes':
            return 'date_missing'
        else:
            return (datetime.today() - pickup_date).days
    else:
        return 'NA'

def o2d(row):

    if row['final_status'] == 'Delivered':
        if row['date_missing'] == 'Yes':
            return 'date_missing'
        else:
            return (row['delivered_date'] - row['Order Date']).days
    else:
        return 'NA'

def change_date_format(df):
    df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
    df['Cancelled At'] = pd.to_datetime(df['Cancelled At'], format='mixed', dayfirst=True)
    df['pickup_date'] = pd.to_datetime(df['pickup_date'], format='%d-%m-%Y', errors='coerce')
    df['delivered_date'] = pd.to_datetime(df['delivered_date'], format='%d-%m-%Y', errors='coerce')
    df['rto_delivered_date'] = pd.to_datetime(df['rto_delivered_date'], format='%d-%m-%Y', errors='coerce')
    df['Manifested At'] = pd.to_datetime(df['Manifested At'], errors='coerce')

    return df