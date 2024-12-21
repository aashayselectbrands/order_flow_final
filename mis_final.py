import pandas as pd
from Functions.google_sheet import get_data_from_google_sheets
new_df_data = []

def new_df_update(date, order_id, suborder_no,sku, tagging, value, pd_taxable_value, pd_tax, shipping_value,shipping_tax, state, warehouse):
    new_df_data.append({
        "Date": date,
        "Order ID": order_id, 
        "Suborder No": suborder_no, 
        "SKU": sku,
        "Tagging": tagging, 
        "Value": value, 
        "Product Taxable Value": pd_taxable_value,
        "Product Tax": pd_tax, 
        "Shipping Value": shipping_value, 
        "Shipping Tax": shipping_tax, 
        "Shipping State": state, 
        "Warehouse": warehouse
    })

def process(df, rs_flag=False):
    for _, row in df.iterrows():
        date = row['Order Date']
        order_no = row['Order Number']
        suborder_no = row['Suborder No']
        sp = row['Selling Price']
        manifested_at = row['Manifested At']
        picked_up = row['pickup_date']
        final_status = row['final_status']
        tracking_no = row['Tracking Number']
        sku = row['SKU']
        if not rs_flag:
            pd_taxable_value = row['pd_taxable_value']
            pd_tax = row['pd_tax']
            shipping_value = row['shipping_value']
            shipping_tax = row['shipping_tax']
        else:
            pd_taxable_value = ""
            pd_tax = ""
            shipping_value = ""
            shipping_tax = ""
        state = row['Shipping State']
        warehouse = row['Client Location']
        # rs_suborder_no = row['rs_suborder_no']
        
        if rs_flag:
            new_df_update(date, order_no, suborder_no, sku, 'RS_Sales', sp, "", "", "", "", state, warehouse)
        else:
            new_df_update(date, order_no, suborder_no, sku, 'Sales', sp, pd_taxable_value, pd_tax, shipping_value, shipping_tax, state, warehouse)
        if final_status == 'Cancelled':
            if rs_flag:
                if pd.isna(row['Cancelled At']) or row['Cancelled At'] == "":
                    new_df_update(date, order_no, suborder_no, sku, 'RS_Cancelled', sp, "", "", "", "", state, warehouse)
                else:
                    new_df_update(row['Cancelled At'], order_no, suborder_no, sku, 'RS_Cancelled', sp, "", "", "", "", state, warehouse)
            else:
                new_df_update(row['Cancelled At'], order_no, suborder_no, sku, 'Cancelled', sp, pd_taxable_value, pd_tax, shipping_value, shipping_tax, state, warehouse)
        elif final_status == 'RTO' and row['mapped_status'] == 'status_completed':
            if rs_flag:
                new_df_update(row['rto_delivered_date'], order_no, suborder_no, sku, 'RS_RTO_delivered', sp, "", "", "", "", state, warehouse)
            else:
                new_df_update(row['rto_delivered_date'], order_no, suborder_no, sku, 'RTO_delivered', sp, pd_taxable_value, pd_tax, shipping_value, shipping_tax, state, warehouse)
        elif final_status == 'Lost or Damaged':
            if rs_flag:
                new_df_update(row['Manifested At'], order_no, suborder_no, sku, 'RS_Lost/Damaged', sp, "", "", "", "", state, warehouse)
            else:
                new_df_update(row['Manifested At'], order_no, suborder_no, sku, 'Lost/Damaged', sp, pd_taxable_value, pd_tax, shipping_value, shipping_tax, state, warehouse)
        elif 'forced_closure' in final_status:
            if rs_flag:
                new_df_update(row['forced_closure_date'], order_no, suborder_no, sku, f'RS_{final_status}', sp, "", "", "", "", state, warehouse)
            else:
                new_df_update(row['forced_closure_date'], order_no, suborder_no, sku, final_status, sp, pd_taxable_value, pd_tax, shipping_value, shipping_tax, state, warehouse)
        if rs_flag:
                if row['final_status'] not in ['Cancelled', 'Not Shipped', 'Not Shipped (No Inventory)']:
                    if pd.isna(picked_up) or picked_up == "":
                        new_df_update(manifested_at, order_no, suborder_no, sku, 'rs_cogs', row['RS_COGS'], "", "", "", "", state, warehouse)
                    else:
                        new_df_update(picked_up, order_no, suborder_no, sku, 'rs_cogs', row['RS_COGS'], "", "", "", "", state, warehouse)
        else:
            if final_status not in ['Not Shipped','Not Shipped (No Inventory)', 'Cancelled']:
                new_df_update(date, order_no, suborder_no, sku, 'pd_cogs', row['pd_cost'], "", "", "", "", state, warehouse)
                # df_filtered = df[df['Tracking Number'] == tracking_no]
                # no_of_tracking_nums = len(df_filtered)
                # if row['total_charges'] != 'no_cost_avl':
                #     courier_charges_adjusted = pd.to_numeric(row['total_charges'])/no_of_tracking_nums
                # else:
                #     courier_charges_adjusted = row['total_charges']
                new_df_update(date, order_no, suborder_no, sku, 'courier_cogs', row['total_charges'], "", "", "", "", state, warehouse)

def main():
    print('Getting order flows for all the months!')
    # df_apr, _ = get_data_from_google_sheets('apr_order_flow', 'final_order_flow')
    # df_may, _ = get_data_from_google_sheets('may_order_flow', 'final_order_flow')
    # df_jun, _ = get_data_from_google_sheets('jun_order_flow', 'final_order_flow')
    # df_jul, _ = get_data_from_google_sheets('jul_order_flow', 'final_order_flow')
    # df_aug, _ = get_data_from_google_sheets('aug_order_flow', 'final_order_flow')
    # df_sept, _ = get_data_from_google_sheets('sept_order_flow', 'final_order_flow')
    # df_oct, _ = get_data_from_google_sheets('oct_order_flow', 'final_order_flow')
    # df_nov, _ = get_data_from_google_sheets('nov_order_flow', 'final_order_flow')

    print('Getting order flows for RS RP Orders!')
    df_rs, _ = get_data_from_google_sheets('RS Order Flow', 'rs_order_flow')
    df_rp, _ = get_data_from_google_sheets('RP Order Flow', 'rp_order_flow')
    df_ms, _ = get_data_from_google_sheets('MS Order Flow', 'ms_order_flow')

    # df_list = [df_apr, df_may, df_jun, df_jul, df_aug, df_sept, df_oct, df_nov]

    # print('Processing monthwise data')
    # for df in df_list:
    #     process(df)

    print('Processing RS df')
    process(df_rs, True)
    # for _, row in df_rs.iterrows():
    #     if row['final_status'] not in ['Cancelled', 'Not Shipped', 'Not Shipped (No Inventory)']:
    #         order_no = row['Order Number']
    #         suborder_no = row['Suborder No']
    #         sku = row['SKU']
    #         manifested_at = row['Manifested At']
    #         picked_up = row['pickup_date']
    #         rs_cogs = row['RS_COGS']
    #         state = row['Shipping State']
    #         warehouse = row['Client Location']
    #         if pd.isna(picked_up) or picked_up == "":
    #             new_df_update(manifested_at, order_no, suborder_no, sku, 'rs_cogs', rs_cogs, "", "", "", "", state, warehouse)
    #         else:
    #             new_df_update(picked_up, order_no, suborder_no, sku, 'rs_cogs', rs_cogs, "", "", "", "", state, warehouse)

    print('Processing RP df')
    for _, row in df_rp.iterrows():
        if row['final_status'] not in ['Cancelled', 'Not Shipped', 'Not Shipped (No Inventory)']:
            order_no = row['Order Number']
            suborder_no = row['Suborder No']
            sku = row['SKU']
            manifested_at = row['Manifested At']
            picked_up = row['pickup_date']
            rp_cogs = row['RP_COGS']
            state = row['Shipping State']
            warehouse = row['Client Location']
            if pd.isna(picked_up) or picked_up == "":
                new_df_update(manifested_at, order_no, suborder_no, sku, 'rp_cogs', rp_cogs, "", "", "", "", state, warehouse)
            else:
                new_df_update(picked_up, order_no, suborder_no, sku, 'rp_cogs', rp_cogs, "", "", "", "", state, warehouse)
    
    print('Processing MS df')
    for _, row in df_ms.iterrows():
        if row['final_status'] not in ['Cancelled', 'Not Shipped', 'Not Shipped (No Inventory)']:
            order_no = row['Order Number']
            suborder_no = row['Suborder No']
            sku = row['SKU']
            manifested_at = row['Manifested At']
            picked_up = row['pickup_date']
            ms_cogs = row['MS_COGS']
            state = row['Shipping State']
            warehouse = row['Client Location']
            if pd.isna(picked_up) or picked_up == "":
                new_df_update(manifested_at, order_no, suborder_no, sku, 'ms_cogs', ms_cogs, "", "", "", "", state, warehouse)
            else:
                new_df_update(picked_up, order_no, suborder_no, sku, 'ms_cogs', ms_cogs, "", "", "", "", state, warehouse)
    
    print('Processing Refund df')
    df_refund, _ = get_data_from_google_sheets('Kyari Order Refunds', "Feb'24 to Present")
    df_refund['Amount'].replace({
    '':0,
    ',':''
    }, inplace=True, regex=True)
    df_refund['Amount'] = df_refund['Amount'].astype(float)
    df_refund = df_refund[
    (df_refund['REFUND REASON'].isin(['Damaged Plant', 'Damaged Replacement'])) & 
    (df_refund['APRROVAL'] == "Yes") & 
    (df_refund['Amount'] != 0.00)
    ]

    for _, row in df_refund.iterrows():
        date = row['Date']
        order_no = "K-" + row['ORDER ID']
        suborder_no = ""
        sku = row['SKU NO. AS PER SHOPIFY']
        refund_val = row['Amount']
        new_df_update(date, order_no, suborder_no, sku, 'Refund', refund_val, "", "", "", "", "", "")
    
    print('Making final df')
    df_final = pd.DataFrame(new_df_data)

    df_cat, _ = get_data_from_google_sheets('Productmaster_data', 'CATEGORY_MAPPING')

    df_final_merged = pd.merge(left=df_final, right=df_cat, left_on='SKU', right_on='sku', how='left')

    df_final_merged = df_final_merged[['Date','Order ID','Suborder No','SKU','Tagging','Value','Product Taxable Value','Product Tax','Shipping Value','Shipping Tax', 'Shipping State','Warehouse','category_name','Plant','Pot','Color','units (units_cx_will_receive)','weight actual']]

    print('Saving final df to csv')
    df_final_merged.to_csv('csv files/mis_1.csv', index=False)
    print('All done!')

if __name__ == "__main__":
    main()