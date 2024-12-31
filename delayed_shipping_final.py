import pandas as pd
import requests
from drive import authenticate_drive, get_file_id_by_name, overwrite_csv_on_drive, read_csv_from_drive, upload_csv_to_drive
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
import os
from gspread_dataframe import set_with_dataframe
import time
from Functions.google_sheet import get_data_from_google_sheets

load_dotenv()

phone_number_id = os.getenv("PHONE_NUMBER_ID")
access_token = os.getenv("ACCESS_TOKEN")

def make_request(name, prod_name, order_id, mob_no):
    # Define the required parameters
    url = f"https://graph.facebook.com/v21.0/{phone_number_id}/messages"

    headers = {
        "Authorization": f"Bearer {access_token}",
        'Content-Type': 'application/json'
    }

    # Prepare the payload
    payload = {
    "messaging_product": "whatsapp",
    "to": mob_no,
    "type": "template",
    "template": {
        "name": "delay_shipment",
        "language": {
            "code": "en"
        },
        "components": [
            {
                "type": "body",
                "parameters": [
                    {
                        "type": "text",
                        "text": name
                    },
                    {
                        "type": "text",
                        "text": prod_name
                    },
                    {
                        "type": "text",
                        "text": order_id
                    }
                ]
            }
        ]
    }
}
    
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        print(f"Message sent successfully for order id {order_id}, mob no {mob_no}!")
        print("Response:", response.json())
    else:
        print(f"Failed to send message for order id {order_id}, mob no {mob_no}!.")
        print("Status Code:", response.status_code)
        print("Response:", response.text)

    return response

def main():
    folder_id = '1AIxxqpA_lhogQLYwodMUccF0tY-9d9tF'  
    file_name = 'combined_order_flow.csv'
    drive = authenticate_drive()
    file_id = get_file_id_by_name(drive, folder_id, file_name)
    df = read_csv_from_drive(drive, file_id)
    today_minus_3 = (datetime.now().date() - timedelta(days=3)).strftime("%d-%m-%Y")
    df_filtered = df[df['Order Date'] == today_minus_3]
    df_filtered_1 = df_filtered[(df_filtered['Manifested At'] == "") | (pd.isna(df_filtered['Manifested At']))]
    df_filtered_1 = df_filtered_1[df_filtered_1['final_status'] == 'Not Shipped']
    df_prod_master, _ = get_data_from_google_sheets('Productmaster_data', 'raw_data')
    df_prod_master = df_prod_master[['sku', 'product_name']]
    df_merged = pd.merge(df_filtered_1, df_prod_master, left_on='SKU', right_on='sku', how='left')
    df_merged['Mobile No'] = df_merged['Mobile No'].astype(str)
    
    details = []
    seen_orders = set()

    for _, row in df_merged.iterrows():
        order_num = row["Order Number"]
        if order_num in seen_orders:
            continue

        df_1 = df_merged[df_merged['Order Number'] == row['Order Number']]
        plant_list = ['🌱'+ plant for plant in df_1['product_name']]
        order_details = {
            'order_id': row['Order Number'],
            'plants_list': plant_list, 
            'cust_name': row['Shipping Customer Name'],
            'mob_no': row['Mobile No']
        }
        details.append(order_details)
        seen_orders.add(order_num)

    print(f'Number of Messages to be sent: {len(details)}')

    results = []
    todays_date = datetime.now().strftime('%d/%m/%Y')
    _, ws = get_data_from_google_sheets('Delayed Shipping Messaging', 'Sheet1')

    for order in details:
        cust_name = order['cust_name']
        mob_no = '+91'+order['mob_no'].strip()[-10:]
        order_no = order['order_id']
        plant_list_str = ', '.join(order['plants_list'])
        response = make_request(cust_name, plant_list_str, order_no, mob_no)
        msg_status = 'sent' if response.status_code == 200 else 'not_sent'
        results.append({
            'cust_name': cust_name,
            'mob_no': mob_no,
            'order_no': order_no,
            'response': response.json(),
            'msg_status': msg_status,
            'msg_send_date': todays_date
        })
        print("Waiting for 10 seconds")
        time.sleep(10)

    df_results = pd.DataFrame(results)
    existing_data = ws.get_all_values()
    next_row = len(existing_data) + 1
    print(f"Messages sent: {len(df_results[df_results['msg_status'] == 'sent'])}")
    print(f"Messages not sent: {len(df_results[df_results['msg_status'] == 'not_sent'])}")
    print("Adding the message results to sheet.")
    set_with_dataframe(
            worksheet=ws,
            dataframe=df_results,
            row=next_row,
            col=1,
            include_index=False,
            include_column_header=False
        )
    print("Exiting the script.")

if __name__ == "__main__":
    main()