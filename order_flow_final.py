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
from Functions.Ops_Calculated_Cols import o2c, o2d, o2ns, o2s, s2d, s2nd, date_missing, change_date_format
from Functions.Script_updates import update_script_date
from Functions.ee import authenticate_easyecom, final_sales_df, process_easyecom_data, process_offline_data, process_corp_data, pd_cost_calc, process_offline_data_missing
from Functions.google_sheet import get_data_from_google_sheets
from drive import authenticate_drive, get_file_id_by_name, overwrite_csv_on_drive, read_csv_from_drive, upload_csv_to_drive

BASE_URL_EASYECOM = "https://api.easyecom.io"
AUTH_EMAIL_EASYECOM = "dhruv.pahuja@selectbrands.in"
AUTH_PASS_EASYECOM = "Analyst@123#"

CREDENTIALS_FILE = 'credentials.json'
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

def final_status(row):  
    if "lost" in row['status'].lower() or "damaged" in row['status'].lower() or row['status'] in ["CONTACT CUSTOMER SERVICE"]:
        return "Lost or Damaged"
    elif "RTO" in row['status'].upper() or row['status'] in ['DELIVERED BACK TO SHIPPER']:
        return "RTO"
    elif row['status'] in ['RETURN TO SHIPPER', "PickupCancelled", "Cancelled", "PICKUP EMPLOYEE IS OUT TO P/U SHIPMENT", "PICKUP CANCELLED BY CALL"] or (row['Cancelled Status'] == 'Yes' and (row['Shipping Status new'] == "No" and row['delivered_date'] == "")):
        return "Cancelled"
    elif row['status'] in ['Delivered', 'SHIPMENT DELIVERED', 'DELIVERED']:
        return "Delivered"
    elif row['Shipping Status new'] == "No" and row['delivered_date'] == "":
        if row['Order Status'] == 'Pending':
            return 'Not Shipped (No Inventory)'
        else:
            if row['pickup_date'] != "":
                return 'Active'
            else:
                return 'Not Shipped'
    else:
        return "Active"

def merge_dfs(df,df_courier):
    df_cleaned = df.replace({"Tracking Number": ""}, float("NaN")).dropna(subset=['Tracking Number'])
    df_blanks = df[df['Tracking Number'].isna()]
    df_merged = pd.merge(left=df_cleaned, right=df_courier, left_on='Tracking Number', right_on='awb_no', how='left')
    df_final = pd.concat([df_merged, df_blanks])
    df_final.fillna("", inplace=True)

    for column in df_final.select_dtypes(include=['object']):
        try:
            df_final[column] = df_final[column].astype(str)
        except Exception as e:
            print(f"Error converting column {column}: {e}")

    df_final['final_status'] = df_final.apply(final_status, axis = 1)
    return df_final

def get_suborder_id(df_order_flow, df_rs):
    suborderno_mapping = df_rs.set_index('clean_id_sku_comb')['Suborder No'].to_dict()
    df_order_flow['rs_suborder_no'] = df_order_flow['combine_id'].map(suborderno_mapping)
    return df_order_flow

def ops_calculated_cols(df):
    df['date_missing'] = df.apply(date_missing, axis=1)
    df['O2S'] = df.apply(o2s, axis=1)
    df['O2NS'] = df.apply(o2ns, axis=1)
    df['S2D'] = df.apply(s2d, axis=1)
    df['S2ND'] = df.apply(s2nd, axis=1)
    df['O2C'] = df.apply(o2c, axis=1)
    df['O2D'] = df.apply(o2d, axis=1)
    return df

def day_first_date(df):
    df['Order Date'] = df['Order Date'].dt.strftime('%d-%m-%Y')
    df['Cancelled At'] = df['Cancelled At'].dt.strftime('%d-%m-%Y')
    df['pickup_date'] = df['pickup_date'].dt.strftime('%d-%m-%Y')
    df['delivered_date'] = df['delivered_date'].dt.strftime('%d-%m-%Y')
    df['rto_delivered_date'] = df['rto_delivered_date'].dt.strftime('%d-%m-%Y')
    df['Manifested At'] = df['Manifested At'].dt.strftime('%d-%m-%Y')
    return df

def get_start_end_dates(month, year):
    today = datetime.now()
    start_date = date(year, month, 1)
    
    # If the input month is the current month, set the end date as today - 1
    if today.month == month and today.year == year:
        end_date = today - timedelta(days=1)
    else:
        # Otherwise, set the end date as the last day of the input month
        last_day = monthrange(year, month)[1]
        end_date = date(year, month, last_day)
    
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

def update_rs_rp(old_df, new_df):
    old_df['Suborder No'] = old_df['Suborder No'].astype(str)
    new_df['Suborder No'] = new_df['Suborder No'].astype(str)

    old_filtered_df = old_df[~old_df['Suborder No'].isin(new_df['Suborder No'])]
    final_df = pd.concat([old_filtered_df, new_df])

    return final_df

def courier_charges_adjusted(df):
    tracking_counts = df['Tracking Number'].value_counts()
    def adjust_charges(row):
        count = tracking_counts[row['Tracking Number']]
        if row['total_charges'] != 'no_cost_avl':
            return round((pd.to_numeric(row['total_charges']) / count),2)
        return row['total_charges']
    df['total_charges_adj'] = df.apply(adjust_charges, axis=1)
    df['total_charges'] = df['total_charges_adj']
    df.drop(columns=['total_charges_adj'], inplace=True)

    return df

def main(month, year):
    month_sheet_mapping = {
        4: 'apr',
        5: 'may',
        6: 'jun',
        7: 'jul',
        8: 'aug',
        9: 'sept', 
        10: 'oct',
        11: 'nov',
        12: 'dec'
    }
    
    if month not in month_sheet_mapping:
        print(f"No Order Flow data available for the entered month {month}. Please enter valid month.")
        return
    
    # api_token, jwt  = authenticate_easyecom()
    # start_date, end_date = get_start_end_dates(month, year)
    # print("Getting shopify data for entered month.")
    # df_raw = final_sales_df(api_token, jwt, start_date, end_date)
    df_raw = pd.read_csv(f"minisales_{month_sheet_mapping[month]}_raw.csv")
    # df_raw.to_csv(f"minisales_{month_sheet_mapping[month]}_raw.csv", index=False)
    
    reship_df, replace_df = process_offline_data(df_raw)
    ms_df = process_offline_data_missing(df_raw)
    df_order_flow = process_easyecom_data(raw_df=df_raw)
    df_sr, _ = get_data_from_google_sheets("Shiprocket Order Flow", "Till_date_data")
    df_bicree, _ = get_data_from_google_sheets("Bicree Order Flow", "Till_date_data")
    df_bd, _ = get_data_from_google_sheets("Bluedart Order Flow", "Till_date_data")
    df_ats, _ = get_data_from_google_sheets("ATS Order Flow", "Till_date_data")

    selected_cols = ['awb_no','status','pickup_date','delivered_date','rto_initiation_date','rto_delivered_date','mapped_status','updated_at','cod_charges','total_charges']

    df_sr.rename(columns={
    'AWB Code': 'awb_no',
    'Status': 'status',
    'Order Picked Up Date': 'pickup_date',
    'Order Delivered Date': 'delivered_date',
    'RTO Delivered Date': 'rto_delivered_date',
    'COD Charges': 'cod_charges'
    }, inplace=True)
    df_sr.insert(1,'rto_initiation_date', '')
    df_sr['total_charges'] = df_sr['cod_charges'].str.replace(',', '').astype(float) + df_sr['Freight Total Amount'].str.replace(',', '').astype(float)
    df_sr = df_sr[selected_cols]

    df_bicree.rename(columns={
    'Awb No': 'awb_no',
    'Status': 'status',
    'Actual Pickup Date': 'pickup_date',
    'Delivery Date': 'delivered_date',
    'Rto Delivery Date': 'rto_delivered_date',
    'Cod Charges': 'cod_charges'
    }, inplace=True)
    df_bicree.insert(1,'rto_initiation_date', '')
    df_bicree['total_charges'] = df_bicree['cod_charges'].astype(float) + df_bicree['Shipping Charges'].astype(float) + df_bicree['Rto Cod Charges'].astype(float) + df_bicree['Rto Charges'].astype(float)
    df_bicree = df_bicree[selected_cols]

    df_bd.rename(columns={
    'AWB Number': 'awb_no',
    'Status': 'status',
    'Pickup Date': 'pickup_date',
    'Delivery Date': 'delivered_date',
    'RTO Initiation Date': 'rto_initiation_date',
    'RTO Delivered Date': 'rto_delivered_date',
    'Updated at': 'updated_at'
    }, inplace=True)
    df_bd.insert(len(df_bd.columns),'cod_charges', '')
    # df_bd.insert(len(df_bd.columns),'total_charges', '')
    df_bd = df_bd[selected_cols]

    df_ats.rename(columns={
    'awb_num': 'awb_no',
    'delivery_date': 'delivered_date',
    'rto_initiated_date': 'rto_initiation_date'
    }, inplace=True)
    df_ats.insert(len(df_ats.columns),'cod_charges', '')
    df_ats = df_ats[selected_cols]

    df_shopify, _ = get_data_from_google_sheets("shopify_order_wise shipping_discount", "SHOPIFY_DATA")
    df_shopify = df_shopify[['Order ID', 'Shipping']]
    df_courier_concat = pd.concat([df_sr, df_bicree, df_bd, df_ats])
    df_final = merge_dfs(df_order_flow,df_courier_concat)
    df_pd_cost_1, _ = get_data_from_google_sheets("cost_automated", "bundle_sku_cost")
    df_pd_cost_2, _ = get_data_from_google_sheets("cost_automated", "single_sku_cost")
    df_pd_cost_1 = df_pd_cost_1[df_pd_cost_1['b2c_other'] == 'cost_avl']
    df_pd_cost_2 = df_pd_cost_2[df_pd_cost_2['tagging'] == 'all_completed']
    df_pd_cost_1 = df_pd_cost_1[['sku', 'TOTAL_B2C_COST_WITH_TAX', 'tax_rate']]
    df_pd_cost_2 = df_pd_cost_2[['sku', 'TOTAL_B2C_COST_WITH_TAX', 'tax_rate']]
    df_pd_cost_1.rename(columns={'TOTAL_B2C_COST_WITH_TAX':'pd_cost'}, inplace=True)
    df_pd_cost_2.rename(columns={'TOTAL_B2C_COST_WITH_TAX': 'pd_cost'}, inplace=True)
    df_pd_cost = pd.concat([df_pd_cost_1,df_pd_cost_2])
    df_final = pd.merge(left=df_final, right=df_pd_cost, left_on='SKU', right_on='sku', how='left')
    df_final = pd.merge(left=df_final, right=df_shopify, left_on='Order Number', right_on='Order ID', how='left')
    df_final['tax_rate'] = df_final['tax_rate'].replace('', np.nan)
    df_final['Shipping'] = df_final['Shipping'].replace('', np.nan)
    df_final['Shipping'].fillna("no_shipping_avl", inplace=True)
    df_final['tax_rate'].fillna("no_tax_avl", inplace=True)

    df_final['Selling Price'] = df_final['Selling Price'].replace('', np.nan)
    df_final['Selling Price'] = pd.to_numeric(df_final['Selling Price'], errors='coerce').fillna(0)

    df_rs, ws_rs = get_data_from_google_sheets('RS Order Flow', 'rs_order_flow')
    df_final = get_suborder_id(df_final, df_rs)

    for idx, row in df_final.iterrows():
        tax_rate = row['tax_rate']
        shipping = row['Shipping']
        if shipping != "no_shipping_avl" and tax_rate != "no_tax_avl":
            if isinstance(tax_rate,str):
                tax_rate = float(tax_rate)
            if isinstance(shipping,str):
                shipping = float(shipping)
            sp_total = df_final.loc[(df_final['Order Number'] == row['Order Number']),'Selling Price'].astype(float).sum()
            sp = float(row['Selling Price'])
            if sp_total != 0:
                shipping_suborder_lvl = round((sp/sp_total)*shipping,2)
                shipping_tax_val = round((shipping_suborder_lvl/118)*18,2)
            else:
                shipping_suborder_lvl = 0
                shipping_tax_val = 0
            sp_minus_shipping = round(sp - shipping_suborder_lvl,2)
            shipping_minus_tax = round(shipping_suborder_lvl - shipping_tax_val,2)
            pd_tax = round((sp_minus_shipping/(1+tax_rate))*tax_rate,2)
            sp_minus_tax = sp_minus_shipping - round((sp_minus_shipping/(1+tax_rate))*tax_rate,2)
            df_final.at[idx,'pd_taxable_value'] = sp_minus_tax
            df_final.at[idx,'pd_tax'] = pd_tax
            df_final.at[idx,'shipping_value'] = shipping_minus_tax
            df_final.at[idx, 'shipping_tax'] = shipping_tax_val
        else:
            if shipping == "no_shipping_avl":
                df_final.at[idx,'pd_taxable_value'] = 'no_shipping_avl'
                df_final.at[idx,'pd_tax'] = 'no_shipping_avl'
                df_final.at[idx,'shipping_value'] = 'no_shipping_avl'
                df_final.at[idx, 'shipping_tax'] = 'no_shipping_avl'

            elif tax_rate == "no_tax_avl":
                df_final.at[idx,'pd_taxable_value'] = "no_tax_avl"
                df_final.at[idx,'pd_tax'] = "no_tax_avl"

                if isinstance(shipping,str):
                    shipping = float(shipping)
                sp_total = df_final.loc[(df_final['Order Number'] == row['Order Number']),'Selling Price'].astype(float).sum()
                sp = float(row['Selling Price'])
                if sp_total != 0:
                    shipping_suborder_lvl = round((sp/sp_total)*shipping,2)
                    shipping_tax_val = round((shipping_suborder_lvl/118)*18,2)
                else:
                    shipping_suborder_lvl = 0
                    shipping_tax_val = 0
                sp_minus_shipping = sp - shipping_suborder_lvl
                shipping_minus_tax = shipping_suborder_lvl - shipping_tax_val

                df_final.at[idx,'shipping_value'] = shipping_minus_tax
                df_final.at[idx, 'shipping_tax'] = shipping_tax_val

    df_final['pd_cost'].fillna("no_cost_avl", inplace=True)
    df_final['pd_cost'] = df_final.apply(pd_cost_calc, axis=1)
    df_final['sku'].fillna("", inplace=True)
    sel_cols_df_final = ['Suborder No','rs_suborder_no','Client Location','Order Date','Order Number','SKU','Marketplace Sku','combine_id','combine_id_mp','Suborder Quantity','Selling Price','Courier Aggregator Name','Courier Name','Tracking Number','Order Status','Shipping Status','status','final_status','Payment Mode','Payment Transaction ID','Manifested At','Cancelled At','pickup_date','delivered_date','rto_initiation_date','rto_delivered_date','forced_closure_date','mapped_status','updated_at','total_charges','pd_cost', 'date_missing', 'O2S', 'O2NS', 'S2D', 'S2ND','O2C', 'O2D','pd_taxable_value', 'pd_tax', 'shipping_value', 'shipping_tax', "Batch ID", "Shipping State", "Shipping Zip Code", "Message", "Shipping Customer Name", "Mobile No"]

    for idx, row in df_final.iterrows():
        if (row['status'] == "" or pd.isna(row['status'])) and ("RTO" in row['Shipping Status'].upper() or row['Shipping Status'] == 'Delivered To Origin'):
            df_final.loc[idx, 'final_status'] = "RTO"
        elif (row['status'] == "" or pd.isna(row['status'])) and row['Shipping Status'] == 'Delivered':
            df_final.loc[idx, 'final_status'] = "Delivered"

        if row['final_status'] not in ['Cancelled', 'Not Shipped'] and (row['total_charges'] == "" or pd.isna(row['total_charges'])):
            df_final.loc[idx, 'total_charges'] = "no_cost_avl"

    df_final = courier_charges_adjusted(df_final)
    df_final['forced_closure_date'] = ""

    # Changing the values given in the Checker sheet for given suborder IDs
    checker_df, _ = get_data_from_google_sheets('manual_data_and_checker_flow', 'CHANGES_SHEET')
    filtered_df = checker_df[checker_df['PARENT_ORDER_MONTH'].astype(int)  == month]
    print("Checking Checker sheet for any Suborder IDs to be updated")
    if not filtered_df.empty:
        for idx, row in filtered_df.iterrows():
            suborder_no = row['SUBORDER_ID']
            col_name = row['COLUMN_NAME']
            value = row['CHANGE_VALUE_TO']
            forced_closure_dt = None
            # if 'forced_closure' in value:
            #     forced_closure_dt = row['CHANGE_DATE']
            row_index = df_final[df_final['Suborder No'] == suborder_no].index
            if not row_index.empty:
                # Get column index and update the value
                col_index = df_final.columns.get_loc(col_name)

                if 'forced_closure' in value:
                    manifested_at_index = df_final.columns.get_loc('Manifested At')
                    manifested_at_dt = df_final.iloc[row_index[0], manifested_at_index]
                    pickup_index = df_final.columns.get_loc('pickup_date')
                    pickup_dt = df_final.iloc[row_index[0], pickup_index]
                    order_dt_index = df_final.columns.get_loc('Order Date')
                    order_dt = df_final.iloc[row_index[0], order_dt_index]

                    if (pd.isna(pickup_dt) or pickup_dt == '') and (pd.isna(manifested_at_dt) or manifested_at_dt == ''):
                        forced_closure_dt = (pd.to_datetime(order_dt, dayfirst=True) + timedelta(days=20)).strftime('%d-%m-%Y')
                    elif (pd.isna(pickup_dt) or pickup_dt == '') and ~(pd.isna(manifested_at_dt) or manifested_at_dt == ''):
                        forced_closure_dt = (pd.to_datetime(manifested_at_dt, dayfirst=True) + timedelta(days=20)).strftime('%d-%m-%Y')
                    else:
                        forced_closure_dt = (pd.to_datetime(pickup_dt, dayfirst=True) + timedelta(days=20)).strftime('%d-%m-%Y')
                df_final.iloc[row_index, col_index] = value
                if forced_closure_dt is not None:
                    col_index_closure = df_final.columns.get_loc('forced_closure_date')
                    df_final.iloc[row_index,col_index_closure] = forced_closure_dt
            else:
                print(f"Could not find the Suborder No {suborder_no} in order flow")
    else:
        print("No Suborder ID to update from Checker sheet")

    df_final = change_date_format(df_final)
    df_final = ops_calculated_cols(df_final)
    df_final = df_final[sel_cols_df_final]
    df_final = day_first_date(df_final)

    df_final.fillna("", inplace=True)
    df_final = df_final[~df_final['Suborder No'].str.contains('cancel', case=False, na=False)]
    df_final.to_csv("ops_check.csv", index=False)

    df_final_ms = merge_dfs(ms_df,df_courier_concat)
    df_final_ms.fillna("", inplace=True)
    df_final_ms = pd.merge(left=df_final_ms, right=df_pd_cost, left_on='SKU', right_on='sku', how='left')
    df_final_ms = df_final_ms.drop('tax_rate', axis=1)
    df_final_ms['pd_cost'].fillna("no_cost_avl", inplace=True)
    df_final_ms['pd_cost'] = df_final_ms.apply(pd_cost_calc, axis=1)
    df_final_ms['sku'].fillna("", inplace=True)

    for idx, row in df_final_ms.iterrows():
        if row['final_status'] not in ['Cancelled', 'Not Shipped'] and (row['total_charges'] == "" or pd.isna(row['total_charges'])):
            df_final_ms.loc[idx, 'total_charges'] = "no_cost_avl"

    df_final_ms = courier_charges_adjusted(df_final_ms)

    df_final_ms = change_date_format(df_final_ms)
    df_final_ms = ops_calculated_cols(df_final_ms)
    df_final_ms = day_first_date(df_final_ms)
    df_final_ms.fillna("", inplace=True)

    df_ms, ws_rp = get_data_from_google_sheets("MS Order Flow", "ms_order_flow")
    df_ms = df_ms.drop('MS_COGS', axis=1)

    df_final_ms = update_rs_rp(df_ms, df_final_ms)
    df_final_ms = df_final_ms[~df_final_ms['Suborder No'].str.contains('cancel', case=False, na=False)]
    df_final_ms.to_csv("ops_ms_check.csv", index=False)

    df_final_rs = merge_dfs(reship_df,df_courier_concat)
    df_final_rp = merge_dfs(replace_df,df_courier_concat)

    # df_final_rs = get_suborder_id(df_final, df_final_rs)
    df_final_rs.fillna("", inplace=True)
    df_final_rs = pd.merge(left=df_final_rs, right=df_pd_cost, left_on='SKU', right_on='sku', how='left')
    df_final_rs = df_final_rs.drop('tax_rate', axis=1)
    df_final_rs['pd_cost'].fillna("no_cost_avl", inplace=True)
    df_final_rs['pd_cost'] = df_final_rs.apply(pd_cost_calc, axis=1)
    df_final_rs['sku'].fillna("", inplace=True)

    for idx, row in df_final_rs.iterrows():
        if row['final_status'] not in ['Cancelled', 'Not Shipped'] and (row['total_charges'] == "" or pd.isna(row['total_charges'])):
            df_final_rs.loc[idx, 'total_charges'] = "no_cost_avl"
    
    df_final_rs = courier_charges_adjusted(df_final_rs)

    df_final_rs = change_date_format(df_final_rs)
    df_final_rs = ops_calculated_cols(df_final_rs)
    df_final_rs = day_first_date(df_final_rs)
    # df_final_rp = get_suborder_id(df_final, df_final_rp)
    df_final_rp.fillna("", inplace=True)
    df_final_rp = pd.merge(left=df_final_rp, right=df_pd_cost, left_on='SKU', right_on='sku', how='left')
    df_final_rp = df_final_rp.drop('tax_rate', axis=1)
    df_final_rp['pd_cost'].fillna("no_cost_avl", inplace=True)
    df_final_rp['pd_cost'] = df_final_rp.apply(pd_cost_calc, axis=1)
    df_final_rp['sku'].fillna("", inplace=True)

    for idx, row in df_final_rp.iterrows():
        if row['final_status'] not in ['Cancelled', 'Not Shipped'] and (row['total_charges'] == "" or pd.isna(row['total_charges'])):
            df_final_rp.loc[idx, 'total_charges'] = "no_cost_avl"

    df_final_rp = courier_charges_adjusted(df_final_rp)

    df_final_rp = change_date_format(df_final_rp)
    df_final_rp = ops_calculated_cols(df_final_rp)
    df_final_rp = day_first_date(df_final_rp)
    df_final_rp.fillna("", inplace=True)

    # df_rs, ws_rs = get_data_from_google_sheets("RS Order Flow", "rs_order_flow")
    df_rp, ws_rp = get_data_from_google_sheets("RP Order Flow", "rp_order_flow")
    df_rs = df_rs.drop('RS_COGS', axis=1)
    df_rp = df_rp.drop('RP_COGS', axis=1)

    df_final_rs = update_rs_rp(df_rs, df_final_rs)
    df_final_rp = update_rs_rp(df_rp, df_final_rp)
    df_final_rs = df_final_rs[~df_final_rs['Suborder No'].str.contains('cancel', case=False, na=False)]
    df_final_rp = df_final_rp[~df_final_rp['Suborder No'].str.contains('cancel', case=False, na=False)]
    df_final_rs.to_csv("ops_rs_check.csv", index=False)
    df_final_rp.to_csv("ops_rp_check.csv", index=False)

    df_final_corp = process_corp_data(df_raw)
    df_final_corp = merge_dfs(df_final_corp,df_courier_concat)
    df_final_corp = pd.merge(left=df_final_corp, right=df_pd_cost, left_on='SKU', right_on='sku', how='left')

    df_final_corp['Selling Price'] = df_final_corp['Selling Price'].replace('', np.nan)
    df_final_corp['Selling Price'] = pd.to_numeric(df_final_corp['Selling Price'], errors='coerce').fillna(0)

    df_final_corp['pd_cost'].fillna("no_cost_avl", inplace=True)
    df_final_corp['pd_cost'] = df_final_corp.apply(pd_cost_calc, axis=1)
    df_final_corp['sku'].fillna("", inplace=True)
    sel_cols_df_final_corp = ['Suborder No','Client Location','Order Date','Order Number','SKU','Marketplace Sku','combine_id','combine_id_mp','Suborder Quantity','Selling Price','Courier Aggregator Name','Courier Name','Tracking Number','Order Status','Shipping Status','status','final_status','Payment Mode','Payment Transaction ID','Manifested At','Cancelled At','pickup_date','delivered_date','rto_initiation_date','rto_delivered_date','mapped_status','updated_at','total_charges','pd_cost', 'date_missing', 'O2S', 'O2NS', 'S2D', 'S2ND','O2C', 'O2D']

    for idx, row in df_final_corp.iterrows():
        if (row['status'] == "" or pd.isna(row['status'])) and ("RTO" in row['Shipping Status'].upper() or row['Shipping Status'] == 'Delivered To Origin'):
            df_final_corp.loc[idx, 'final_status'] = "RTO"
        elif (row['status'] == "" or pd.isna(row['status'])) and row['Shipping Status'] == 'Delivered':
            df_final_corp.loc[idx, 'final_status'] = "Delivered"

        if row['final_status'] not in ['Cancelled', 'Not Shipped'] and (row['total_charges'] == "" or pd.isna(row['total_charges'])):
            df_final_corp.loc[idx, 'total_charges'] = "no_cost_avl"

    df_final_corp = change_date_format(df_final_corp)
    df_final_corp = ops_calculated_cols(df_final_corp)
    df_final_corp = df_final_corp[sel_cols_df_final_corp]
    df_final_corp = day_first_date(df_final_corp)
    df_final_corp.fillna("", inplace=True)
    df_corp_primary, ws_corp = get_data_from_google_sheets("Corporate_Order_Flow", "final_order_flow")
    df_final_corp = update_rs_rp(df_corp_primary, df_final_corp)
    df_final_corp = df_final_corp[~df_final_corp['Suborder No'].str.contains('cancel', case=False, na=False)]
    df_final_corp.to_csv("ops_corp_check.csv", index=False)

    for column in df_final.select_dtypes(include=['object']):
        try:
            df_final[column] = df_final[column].astype(str)
        except Exception as e:
            print(f"Error converting column {column}: {e}")

    for column in df_final_rs.select_dtypes(include=['object', 'datetime64']):
        try:
            df_final_rs[column] = df_final_rs[column].astype(str)
        except Exception as e:
            print(f"Error converting column {column}: {e}")

    for column in df_final_rp.select_dtypes(include=['object', 'datetime64']):
        try:
            df_final_rp[column] = df_final_rp[column].astype(str)
        except Exception as e:
            print(f"Error converting column {column}: {e}")      

    for column in df_final_corp.select_dtypes(include=['object', 'datetime64']):
        try:
            df_final_corp[column] = df_final_corp[column].astype(str)
        except Exception as e:
            print(f"Error converting column {column}: {e}") 

    for column in df_final_ms.select_dtypes(include=['object', 'datetime64']):
        try:
            df_final_ms[column] = df_final_ms[column].astype(str)
        except Exception as e:
            print(f"Error converting column {column}: {e}")   

    _, ws = get_data_from_google_sheets(f"{month_sheet_mapping[month]}_order_flow", "final_order_flow")
    print("Clearing the order flow worksheet")
    ws.batch_clear(['A:AV'])
    print("Uploading updated data on order flow worksheet")
    ws.update([df_final.columns.values.tolist()] + df_final.values.tolist())
    print("Data uploaded for order flow.")
    update_script_date(f"Order Flow - {month_sheet_mapping[month]}")

    print("Clearing the RS order flow worksheet")
    ws_rs.batch_clear(['A:BB'])
    print("Uploading updated data on RS order flow worksheet")
    ws_rs.update([df_final_rs.columns.values.tolist()] + df_final_rs.values.tolist())    
    print("Data uploaded for RS order flow.")
    update_script_date("RS Order Flow")

    print("Clearing the RP order flow worksheet")
    ws_rp.batch_clear(['A:BB'])
    print("Uploading updated data on RP order flow worksheet")
    ws_rp.update([df_final_rp.columns.values.tolist()] + df_final_rp.values.tolist())    
    print("Data uploaded for RP order flow.")
    update_script_date("RP Order Flow")

    _, ws_corp = get_data_from_google_sheets("Corporate_Order_Flow", "final_order_flow")
    print("Clearing the Corporate order flow worksheet")
    ws_corp.batch_clear(['A:AJ'])
    print("Uploading updated data on Corporate order flow worksheet")
    ws_corp.update([df_final_corp.columns.values.tolist()] + df_final_corp.values.tolist())    
    print("Data uploaded for Corporate order flow.")
    update_script_date(f"Order Flow - Corporate")

    _, ws_ms = get_data_from_google_sheets("MS Order Flow", "ms_order_flow")
    print("Clearing the MS order flow worksheet")
    ws_ms.batch_clear(['A:BA'])
    print("Uploading updated data on order flow worksheet")
    ws_ms.update([df_final_ms.columns.values.tolist()] + df_final_ms.values.tolist())    
    print("Data uploaded for MS order flow.")
    update_script_date(f"Order Flow - MS")                

    folder_id = '1AIxxqpA_lhogQLYwodMUccF0tY-9d9tF'  
    file_name = 'combined_order_flow.csv'
    drive = authenticate_drive()

    file_id = get_file_id_by_name(drive, folder_id, file_name)
    if file_id is None:
        print(f"No file named '{file_name}' found in the folder.")
    else:
        df_old = read_csv_from_drive(drive, file_id)
        print("Original file shape:")
        print(df_old.shape)

        df_overall_updated = update_rs_rp(df_old, df_final)
        print("Updated file shape:")
        print(df_overall_updated.shape)
        overwrite_csv_on_drive(drive, file_id, df_overall_updated)

    print("All data uploaded. Exiting the script!")

if __name__ == "__main__":
    #Enter the month and the year for which you want the Order Flow. 
    month = int(input("Enter the month (1-12): "))
    year = 2024
    main(month, year)