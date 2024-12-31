import pandas as pd
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from gspread_dataframe import set_with_dataframe
from calendar import monthrange
from Functions.ee import authenticate_easyecom, final_sales_df
from Functions.google_sheet import get_data_from_google_sheets
from drive import authenticate_drive, get_file_id_by_name, overwrite_csv_on_drive, read_csv_from_drive, upload_csv_to_drive

BASE_URL_EASYECOM = "https://api.easyecom.io"
AUTH_EMAIL_EASYECOM = "dhruv.pahuja@selectbrands.in"
AUTH_PASS_EASYECOM = "Analyst@123#"

CREDENTIALS_FILE = 'Creds.json'
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

def process_easyecom_data(raw_df, df_combined_of):
    df_rs, _ = get_data_from_google_sheets('RS Order Flow', 'rs_order_flow')
    df_rp, _ = get_data_from_google_sheets('RP Order Flow', 'rp_order_flow')
    sel_cols = ['Suborder No', 'final_status']
    df_combined_of = df_combined_of[sel_cols]
    df_rs = df_rs[sel_cols]
    df_rp = df_rp[sel_cols]
    df_concat = pd.concat([df_combined_of, df_rs, df_rp])
    df_concat.rename(columns={
        'Suborder No': 'Sub No'
    }, inplace=True)
    df_concat.to_csv('csv files/batch_concat.csv', index=False)
    # df_concat = df_concat.set_index('Suborder No')

    raw_df.loc[:, "Order Number"] = raw_df.loc[:, "Order Number"].str.replace("`", "")
    raw_df.loc[:, "Tracking Number"] = raw_df.loc[:, "Tracking Number"].str.replace("`", "")

    raw_df["Order Date"] = pd.to_datetime(raw_df["Order Date"]).dt.date
    raw_df["Manifested At"] = pd.to_datetime(raw_df["Manifested At"]).dt.date
    raw_df["Cancelled At"] = pd.to_datetime(raw_df["Cancelled At"]).dt.date

    def shipping_status_logic(row):
        if ('Vendor Central' in row['MP Name']) or (row['MP Name'] in ['Amazon.in', 'B2B', 'Production Order']):
            if row['Order Status'] == 'Shipped':
                return 'Yes'
            else:
                return 'No'
        else:
            if pd.isna(row["Shipping Status"]) or row["Shipping Status"] == "" or (row["Shipping Status"] in ["Cancelled","Out For Pickup","Shipment Created","Pickup Exception","Pickup Scheduled"] and row["Order Status"] in ["Ready to dispatch"]) or row["Shipping Status"] in ["Cancelled","Out For Pickup","Shipment Created","Pickup Exception","Pickup Scheduled"]:
                return 'No'
            else:
                return 'Yes'

    raw_df["Shipping Status new"] = raw_df.apply(
        shipping_status_logic,
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

    raw_df['NEW_MP_NAME'] = raw_df.apply(NEW_MP_NAME_name, axis=1)
    raw_df = pd.merge(left=raw_df, right=df_concat, left_on='Suborder No', right_on='Sub No', how='left')

    raw_df.to_csv('csv files/batch_raw_df.csv', index=False)

    raw_df = raw_df[["Batch ID", "Order Date", "Suborder No","MP Name", "Client Location", "Order Number", "SKU", "Suborder Quantity",'Order Status', 'final_status', 'Courier Aggregator Name', 'Tracking Number', 'Shipping Status', "Shipping Status new", "Cancelled Status", "Delivered",'Selling Price', 'Message', 'Shipping City', 'Shipping Zip Code', 'Payment Mode','Shipping Customer Name','Mobile No', 'NEW_MP_NAME']]
    
    return raw_df

def get_start_end_dates():
    today = datetime.now()
    start_date = today - relativedelta(months=2) - timedelta(days=28)
    end_date = today
    
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

def tagging(row):
    if pd.isna(row['final_status']) or row['final_status'] == "":
        if row['Cancelled Status'] == 'No':
            if row['Shipping Status new'] == 'No':
                return 'Not Shipped'
            else:
                return 'Shipped'
        else:
            return 'Cancelled'
    else:
        if row['final_status'] == 'Cancelled':
            return 'Cancelled'
        elif row['final_status'] == 'Not Shipped':
            return 'Not Shipped'
        else:
            return 'Shipped'

def NEW_MP_NAME_name(row):
    if row['MP Name'] in ['Vendor Central Dropship 2', 'Vendor Central Dropship 3']:
        return 'amz-rb'
    elif row['MP Name'] == 'Vendor Central Dropship':
        return 'amz-kyari'
    elif row['MP Name'] == 'Shopify':
        return 'web-kyari'
    elif row['MP Name'] == 'Shopify13':
        return 'web-rb'
    elif row['MP Name'] in ['Myntra PPMP', 'Production Order']:
        return 'other-MP'
    elif row['MP Name'] == 'Amazon.in':
        if row['Brand'] == 'KYARI':
            return 'amz-kyari'
        else:
            return 'amz-rb'
    elif row['MP Name'] == 'Offline':
        if ('SAMP' in row['Order Number']) or ('CORP' in row['Order Number']):
            return 'corp-mp'
        else:
            return 'rs-rp-order'
    elif row['MP Name'] == 'B2B':
        if ('SAMP' in row['Order Number']) or ('CORP' in row['Order Number']):
            return 'corp-mp'
        else:
            return 'bulk-order'
    
def warehouse_name(row):
    if row['Client Location'] == 'Select Brands International Pvt. Ltd.':
        return 'Indore'
    elif row['Client Location'] == 'SELECT BRANDS (I) PVT. LTD. (PUNE)':
        return 'Pune'
    elif row['Client Location'] == 'SELECT BRANDS (I) PVT. LTD. (KOLKATA)':
        return 'Kolkata'
    elif row['Client Location'] == 'DELHI WAREHOUSE':
        return 'Delhi'
    elif row['Client Location'] == 'KYARI AURA':
        return 'Aura'
    elif row['Client Location'] == 'SELECT BRANDS (I) PVT. LTD. (MORADABAD)':
        return 'Moradabad'
    elif row['Client Location'] == 'CORPORATE WAREHOUSE':
        return 'Corporate WH'
    
def update_rs_rp(old_df, new_df):
    old_df['Suborder No'] = old_df['Suborder No'].astype(str)
    new_df['Suborder No'] = new_df['Suborder No'].astype(str)

    old_filtered_df = old_df[~old_df['Suborder No'].isin(new_df['Suborder No'])]
    final_df = pd.concat([old_filtered_df, new_df])

    return final_df
    
# def check_and_update(df_proc):
#     df_primary, ws_batch = get_data_from_google_sheets('batch_daily_clear_action', 'batch_data_daily')
#     df_primary.drop(columns=['Pincode Servicability', 'State'], inplace=True)
#     df_primary['Suborder No'] = df_primary['Suborder No'].astype(str)
#     df_proc['Suborder No'] = df_proc['Suborder No'].astype(str)

#     old_filtered_df = df_primary[~df_primary['Suborder No'].isin(df_proc['Suborder No'])]
#     final_df = pd.concat([old_filtered_df, df_proc])
#     # df_primary['Suborder No'] = df_primary['Suborder No'].astype(int)
#     # df_proc['Suborder No'] = df_proc['Suborder No'].astype(int)
#     ws_batch.batch_clear(['A:W'])
#     set_with_dataframe(worksheet=ws_batch, dataframe=final_df, include_index=False, include_column_header=True, row=1, col=1)

# def update_shipped(df_shipped):
#     df_primary, ws_shipped = get_data_from_google_sheets('batch_daily_clear_action', 'batch_data_shipped')
#     # df_primary = df_primary.astype(str)
#     # df_shipped = df_shipped.astype(str)
#     print("old shipped shape", df_primary.shape)

#     cols = ['Batch ID', 'Week', 'NEW_MP_NAME']
#     old_filtered_df = df_primary[
#         ~df_primary[cols].apply(tuple, axis=1).isin(df_shipped[cols].apply(tuple, axis=1))
#     ]
#     # print("old_filtered_df shape", old_filtered_df.shape)
#     # old_filtered_df = df_primary[~df_primary['Batch ID'].isin(df_shipped['Batch ID'])]
#     final_df = pd.concat([df_primary, df_shipped], ignore_index=True)
#     print("final shipped shape before dupl dropping", final_df.shape)
#     print(f"Number of duplicates: {final_df.duplicated().sum()}")
#     final_df.to_csv('batch_dup.csv', index=False)
#     final_df.drop_duplicates(inplace=True)
#     final_df = final_df.astype({
#         'Week': 'int64',
#         'Suborder No': 'int64',
#         'Order Number': 'int64',
#         'Suborder Quantity': 'int64',
#     })
#     print("final shipped shape", final_df.shape)
    # ws_shipped.clear()
    # set_with_dataframe(worksheet=ws_shipped, dataframe=final_df, include_index=False, include_column_header=True, row=1, col=1)

def main():
    api_token, jwt  = authenticate_easyecom()
    start_date, end_date = get_start_end_dates()
    print("Getting shopify data.")
    df_raw = final_sales_df(api_token, jwt, start_date, end_date)
    # df_raw = pd.read_csv(f"csv files/minisales_last_3_months_raw.csv")
    df_raw.to_csv(f"csv files/minisales_last_3_months_raw.csv", index=False)

    print("Getting the combined order flow from drive.")
    folder_id = '1AIxxqpA_lhogQLYwodMUccF0tY-9d9tF'  
    file_name = 'combined_order_flow.csv'
    drive = authenticate_drive()
    file_id = get_file_id_by_name(drive, folder_id, file_name)
    df_combined_of = read_csv_from_drive(drive, file_id)

    print("Processing easyecom data.")
    df_proc = process_easyecom_data(df_raw, df_combined_of)
    df_proc.to_csv('csv files/processed_test.csv', index=False)

    batch_date_dict = {}
    batch_id_list = list(set(df_proc['Batch ID'].dropna()))

    for batch_id in batch_id_list:
        df_filtered = df_proc[df_proc['Batch ID'] == batch_id]
        batch_date_dict[batch_id] = df_filtered['Order Date'].max().strftime("%d-%m-%Y")

    df_proc.loc[:, 'Batch Date'] = df_proc['Batch ID'].map(batch_date_dict)

    df_proc['Batch Date'].fillna('', inplace=True)

    df_proc['Tagging'] = df_proc.apply(tagging, axis=1)

    df_proc['Week'] = df_proc.apply(
        lambda x: pd.to_datetime(x['Batch Date'], dayfirst=True).strftime("%W") if x['Batch Date'] != '' else x['Order Date'].strftime("%W")
        ,axis=1
    )

    df_proc['days_diff'] = df_proc.apply(
        lambda x: (pd.Timestamp(date.today()) - pd.to_datetime(x['Order Date'], dayfirst=True)).days, axis=1
    )

    df_proc['NEW_WH_NAME'] = df_proc.apply(warehouse_name, axis=1)

    df_proc = df_proc[['Client Location', 'MP Name','Tagging','Week','days_diff','NEW_WH_NAME','NEW_MP_NAME','Batch ID','Batch Date','Order Date','Order Number','Suborder No','Order Status','final_status','Courier Aggregator Name', 'Tracking Number', 'Payment Mode','Message','SKU','Suborder Quantity','Selling Price','Shipping Zip Code','Shipping City','Shipping Customer Name','Mobile No']]

    df_proc['Batch ID'] = pd.to_numeric(df_proc['Batch ID'], errors='coerce')
    df_proc['Batch Date'] = pd.to_datetime(df_proc['Batch Date'], dayfirst=True).dt.date
    df_proc['Suborder Quantity'] = pd.to_numeric(df_proc['Suborder Quantity'], errors='coerce')
    df_proc['Week'] = pd.to_numeric(df_proc['Week'], errors='coerce')

    df_proc['Batch ID'].fillna('no_batch_created', inplace=True)

    df_proc.fillna('', inplace=True)

    df_proc.to_csv('csv files/batch_test_1.csv', index=False)

    folder_id = '1V2CVlOYjj0osXzE55XKirZdVN2gELOOm'  
    file_name = 'batch_id_flow.csv'

    file_id = get_file_id_by_name(drive, folder_id, file_name)
    if file_id is None:
        print(f"No file named '{file_name}' found in the folder.")
    else:
        df_old = read_csv_from_drive(drive, file_id)
        print("Original file shape:")
        print(df_old.shape)

        df_overall_updated = update_rs_rp(df_old, df_proc)
        print("Updated file shape:")
        print(df_overall_updated.shape)
        overwrite_csv_on_drive(drive, file_id, df_overall_updated, 'csv files/batch_id_flow.csv')
    
    df_shipped = df_proc[df_proc['Tagging'] == 'Shipped']
    df_shipped_grpd = df_shipped.groupby(['Client Location','MP Name','Week','NEW_WH_NAME', 'NEW_MP_NAME','Batch ID', 'Order Date', 'Batch Date']).agg({
        'Suborder No': 'count',
        'Order Number': pd.Series.nunique,
        'Suborder Quantity': 'sum'
        }).reset_index()
    df_shipped_grpd.to_csv('csv files/batch_shipped_grouped.csv', index=False)
    print("df_shipped processed shape", df_shipped_grpd.shape)

    df_pending = df_proc[df_proc['Tagging'] != 'Shipped']
    df_pending.to_csv('csv files/batch_pending.csv', index=False)

    # check_and_update(df_pending)
    # update_shipped(df_shipped_grpd)
    _, ws_batch = get_data_from_google_sheets('batch_daily_clear_action', 'batch_data_daily')
    ws_batch.batch_clear(['A:Y'])
    set_with_dataframe(worksheet=ws_batch, dataframe=df_pending, include_index=False, include_column_header=True, row=1, col=1)
    _, ws_shipped = get_data_from_google_sheets('batch_daily_clear_action', 'batch_data_shipped')
    ws_shipped.clear()
    set_with_dataframe(worksheet=ws_shipped, dataframe=df_shipped_grpd, include_index=False, include_column_header=True, row=1, col=1)

if __name__ == "__main__":
    main()