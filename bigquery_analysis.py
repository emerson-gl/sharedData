import os
import pandas as pd
import numpy as np
import re
from datetime import datetime

# Setup
os.chdir('C:\\Users\\Graphicsland\\Spyder\\cartBuilder')
today_str = datetime.today().strftime('%Y-%m-%d')
max_gap_minutes = 20

# Load and prep
df = pd.read_feather('data/bigquery.feather')
df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], unit='us')
df['param_engagement_time_sec'] = df['param_engagement_time_msec'] / 1000
df = df.sort_values(by=['param_ga_session_id', 'event_timestamp'])

# Mark page types
df['page_type'] = None
df.loc[df['param_page_location'].str.contains('/account/order-history', na=False), 'page_type'] = 'order_history'
df.loc[df['param_page_location'].str.contains(r'/account/?(?:\?.*)?$', na=False), 'page_type'] = 'my_dashboard'
df.loc[df['param_page_location'].str.contains(r'/orders/\d{7,}', na=False), 'page_type'] = 'specific_order'

# Identify click-like events
click_events = [ 'click', 'Click', 'internal_link_click', 'internal_link_clicked',
    'Sticker Template Selected', 'Design: Upload Design Button Clicked', 'Continue to Shipping',
    'Continue to Delivery', 'Continue to Payment', 'Reorder Item', 'Add Order to Cart',
    'Add Order to Cart Without Design', 'Share link via URL', 'FAQ Search', 'Filter Templates',
    'Template Paging', 'Show More Options', 'Clicked play' ]
click_df = df[df['event_name'].isin(click_events)]

# Get first reorder per session
reorder_clicks = df[df['event_name'] == 'Reorder Item'].groupby('param_ga_session_id').first().reset_index()[
    ['param_ga_session_id', 'event_timestamp', 'param_page_location'] ]
reorder_clicks.rename(columns={'event_timestamp': 'reorder_time'}, inplace=True)

# First transaction per session
reorder_tx = df[df['param_transaction_id'].notna()].groupby('param_ga_session_id').first().reset_index()[
    ['param_ga_session_id', 'param_transaction_id']]

# Merge and infer reorder path
reorders = reorder_clicks.merge(reorder_tx, on='param_ga_session_id', how='left')

def classify_path(x):
    x = str(x)
    if '/account/order-history' in x:
        return 'order_history'
    elif '/orders/' in x:
        return 'specific_order'
    elif '/account' in x and '/account/order-history' not in x:
        return 'my_dashboard'
    else:
        return 'unknown'

reorders['reorder_path'] = reorders['param_page_location'].apply(classify_path)



# Session timing
first_session_start = df[df['event_name'] == 'session_start'].groupby('param_ga_session_id')['event_timestamp'].first().reset_index().rename(columns={'event_timestamp': 'session_start_time'})
last_session_event = df.groupby('param_ga_session_id')['event_timestamp'].last().reset_index().rename(columns={'event_timestamp': 'session_end_time'})
page_hits = df[df['page_type'].isin(['order_history', 'my_dashboard'])].groupby(['param_ga_session_id', 'page_type'])['event_timestamp'].first().unstack()

# Merge core info
merged = reorders.merge(first_session_start, on='param_ga_session_id', how='left') \
    .merge(last_session_event, on='param_ga_session_id', how='left') \
    .merge(page_hits, on='param_ga_session_id', how='left')

# Specific order chain logic
def get_specific_chain_and_engagement(row):
    if row['reorder_path'] != 'specific_order':
        return pd.Series([np.nan, np.nan])

    match = re.search(r'/orders/(\d{7,})', str(row['param_page_location']))
    if not match:
        return pd.Series([np.nan, np.nan])

    order_id = match.group(1)
    url_pattern = f'/orders/{order_id}'
    
    session_df = df[df['param_ga_session_id'] == row['param_ga_session_id']]
    session_df = session_df[session_df['event_timestamp'] <= row['reorder_time']].copy()
    session_df = session_df.sort_values('event_timestamp').reset_index(drop=True)
    
    session_df['matches'] = session_df['param_page_location'].str.contains(re.escape(url_pattern), na=False)

    matching_indices = session_df.index[session_df['matches']].tolist()
    if not matching_indices:
        return pd.Series([np.nan, np.nan])
    
    # Now walk backward to find the start of the last contiguous chain
    last_idx = matching_indices[-1]
    chain = [last_idx]
    
    for i in range(len(matching_indices) - 2, -1, -1):
        curr_idx = matching_indices[i + 1]
        prev_idx = matching_indices[i]
        
        gap = (session_df.loc[curr_idx, 'event_timestamp'] - session_df.loc[prev_idx, 'event_timestamp']).total_seconds() / 60
        
        if gap <= max_gap_minutes:
            chain.insert(0, prev_idx)
        else:
            break

    first_idx = chain[0]
    total_engagement = session_df.loc[chain, 'param_engagement_time_sec'].sum() / 60

    return pd.Series([session_df.loc[first_idx, 'event_timestamp'], total_engagement])


merged[['correct_specific_order_time', 'engagement_min_from_path_to_first_reorder']] = merged.apply(get_specific_chain_and_engagement, axis=1)
# Compute engagement from my_dashboard or order_history path (non-specific_order paths)
def get_engagement_from_other_paths(row):
    if row['reorder_path'] == 'specific_order':
        return row['engagement_min_from_path_to_first_reorder']
    
    path_time = row.get(row['reorder_path'])
    if pd.isna(path_time):
        return np.nan

    session_df = df[df['param_ga_session_id'] == row['param_ga_session_id']]
    in_range = session_df[
        (session_df['event_timestamp'] >= path_time) &
        (session_df['event_timestamp'] <= row['reorder_time'])
    ]
    return in_range['param_engagement_time_sec'].sum() / 60

merged['engagement_min_from_path_to_first_reorder'] = merged.apply(get_engagement_from_other_paths, axis=1)


# Total session engagement time
def session_engagement(row):
    session_df = df[df['param_ga_session_id'] == row['param_ga_session_id']]
    return session_df[session_df['event_timestamp'] <= row['reorder_time']]['param_engagement_time_sec'].sum() / 60

merged['engagement_min_from_session_start_to_first_reorder'] = merged.apply(session_engagement, axis=1)

# Time calculations
merged['time_from_session_start_to_reorder'] = (merged['reorder_time'] - merged['session_start_time']).dt.total_seconds() / 60
merged['time_from_order_history_to_reorder'] = (merged['reorder_time'] - merged.get('order_history')).dt.total_seconds() / 60
merged['time_from_my_dashboard_to_reorder'] = (merged['reorder_time'] - merged.get('my_dashboard')).dt.total_seconds() / 60
merged['time_from_specific_order_to_reorder'] = (merged['reorder_time'] - merged['correct_specific_order_time']).dt.total_seconds() / 60
merged['session_elapsed_minutes'] = (merged['session_end_time'] - merged['session_start_time']).dt.total_seconds() / 60

# Unified time field
def select_time(row):
    return {
        'order_history': row['time_from_order_history_to_reorder'],
        'my_dashboard': row['time_from_my_dashboard_to_reorder'],
        'specific_order': row['time_from_specific_order_to_reorder']
    }.get(row['reorder_path'], np.nan)

merged['time_from_path_to_first_reorder'] = merged.apply(select_time, axis=1)

# Clicks to reorder
click_df = click_df.merge(merged[['param_ga_session_id', 'reorder_time']], on='param_ga_session_id', how='left')
click_df = click_df[click_df['event_timestamp'] <= click_df['reorder_time']]
click_counts = click_df.groupby('param_ga_session_id').size().reset_index(name='clicks_from_session_start_to_first_reorder')
merged = merged.merge(click_counts, on='param_ga_session_id', how='left')
merged['clicks_from_session_start_to_first_reorder'] = merged['clicks_from_session_start_to_first_reorder'].fillna(0).astype(int)

# Path-specific click count
def count_path_clicks(row):
    path = row['reorder_path']
    if path not in ['order_history', 'my_dashboard', 'specific_order']:
        return 0
    start = row.get(path) if path != 'specific_order' else row.get('correct_specific_order_time')
    if pd.isna(start):
        return 0
    mask = (click_df['param_ga_session_id'] == row['param_ga_session_id']) & \
           (click_df['event_timestamp'] >= start) & \
           (click_df['event_timestamp'] <= row['reorder_time'])
    return click_df.loc[mask].shape[0]

merged['clicks_from_path_to_first_reorder'] = merged.apply(count_path_clicks, axis=1)

# Rename transaction ID to make it easier to join in Power BI (should be automatic)
merged.rename(columns={'param_transaction_id': 'OrderNumber'}, inplace=True)

# Join CustomerId into merged via OrderNumber
df_order = pd.read_feather('data/raw_order_df.feather')
df_order['OrderNumber'] = pd.to_numeric(df_order['OrderNumber'], errors='coerce')
merged['OrderNumber'] = pd.to_numeric(merged['OrderNumber'], errors='coerce')
merged = merged.merge(df_order[['OrderNumber', 'CustomerId', 'PriceTotal']], on='OrderNumber', how='left')


# Export merged
merged.to_csv(f'outputs/order_history_reorder_analysis_{today_str}.csv', index=False)

# Merge in customer data
merged_subset = merged.dropna(subset=['OrderNumber'])[['OrderNumber']]
merged_subset['OrderNumber'] = pd.to_numeric(merged_subset['OrderNumber'], errors='coerce')

df_order = pd.read_feather('data/raw_order_df.feather')
df_order['OrderNumber'] = pd.to_numeric(df_order['OrderNumber'], errors='coerce')
df_order_subset = df_order.merge(merged_subset, left_on='OrderNumber', right_on='OrderNumber', how='right')[
    ['OrderNumber', 'CustomerId']]

df_customer = pd.read_feather('outputs/customer_order_history.feather')
df_customer['CustomerId'] = df_customer['CustomerId'].astype('Int64')
df_customer['LifetimeCustomerOrdersGrouped'] = df_customer['LifetimeCustomerOrdersGrouped'].fillna('0')
df_customer['LifetimeCustomerItemsGrouped'] = df_customer['LifetimeCustomerItemsGrouped'].fillna('0')

orders_sort_map = {'0': 0, '1': 1, '2-5': 2, '6+': 3}
items_sort_map = {'0': 0, '1': 1, '2-9': 2, '10+': 3}
df_customer['LifetimeCustomerOrdersSortOrder'] = df_customer['LifetimeCustomerOrdersGrouped'].map(orders_sort_map)
df_customer['LifetimeCustomerItemsSortOrder'] = df_customer['LifetimeCustomerItemsGrouped'].map(items_sort_map)

df_customer_export = df_customer.merge(df_order_subset, on='CustomerId', how='right')
df_customer_export = df_customer_export.dropna(subset=['CustomerId', 'OrderNumber']).drop_duplicates()
df_customer_export.to_csv(f'outputs/customer_history_{today_str}.csv', index=False)
