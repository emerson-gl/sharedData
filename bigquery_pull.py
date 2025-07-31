import os
import pandas as pd
from jinja2 import Template
from datetime import datetime, timedelta
from google.cloud import bigquery

# Set working directory
os.chdir('C:\\Users\\Graphicsland\\Spyder\\sharedData')

client = bigquery.Client()

### Events ###
df_existing = pd.read_feather('data/bigquery_events.feather')
df_existing['event_date'] = pd.to_datetime(df_existing['event_date'], format='%Y%m%d')


# Load SQL template
with open('scripts/bigquery_events.sql', 'r') as f:
    raw_sql = f.read()

# Prepare the time window
last_event_date = df_existing['event_date'].max()
start_date = (last_event_date - timedelta(days=2)).strftime('%Y%m%d')
today = datetime.utcnow().strftime('%Y%m%d')

# Render the SQL with Jinja
template = Template(raw_sql)
rendered_sql = template.render(start_date=start_date, today=today)

# Use rendered_sql in your BigQuery query
df = client.query(rendered_sql).to_dataframe()

# 1. Archive the new pull
timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')
archive_path = f"data/archive/bigquery_events_{timestamp}.feather"
df.to_feather(archive_path)

# Ensure event_date is saved as string (Feather-safe format)
df_existing['event_date'] = df_existing['event_date'].dt.strftime('%Y%m%d')


# 2. Combine and de-duplicate
df_combined = pd.concat([df_existing, df], ignore_index=True)
df_combined.drop_duplicates(
    subset=['event_timestamp', 'event_name', 'user_pseudo_id'], 
    keep='last',
    inplace=True
)

# 3. Sort by event_date and timestamp (optional but useful for review)
df_combined.sort_values(by=['event_date', 'event_timestamp'], inplace=True)


# 4. Save final result
df_combined.to_feather('data/bigquery_events.feather')


### Users ###
# Load existing user data if available
user_data_path = 'data/bigquery_users.feather'
if os.path.exists(user_data_path):
    df_users_existing = pd.read_feather(user_data_path)
else:
    df_users_existing = pd.DataFrame()

# Load SQL template
with open('scripts/bigquery_users.sql', 'r') as f:
    raw_sql = f.read()

# If needed: prepare date range filters based on existing user data
# (Optional, but consistent with your events logic)
if not df_users_existing.empty and 'last_updated_date' in df_users_existing.columns:
    df_users_existing['last_updated_date'] = pd.to_datetime(df_users_existing['last_updated_date'])
    last_update = df_users_existing['last_updated_date'].max()
    start_date = (last_update - timedelta(days=2)).strftime('%Y%m%d')
else:
    # Fall back to a broad range if there's no existing data
    start_date = '20250101'

today = datetime.utcnow().strftime('%Y%m%d')

# Render the SQL
template = Template(raw_sql)
rendered_sql = template.render(start_date=start_date, today=today)

# Run the query
df_users_new = client.query(rendered_sql).to_dataframe()

# Archive the new pull
timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H%M%S')
archive_path = f"data/archive/bigquery_users_{timestamp}.feather"
df_users_new.to_feather(archive_path)

# Combine and de-duplicate
df_users_combined = pd.concat([df_users_existing, df_users_new], ignore_index=True)

# De-duplicate by pseudo_user_id and last_updated_date
# If available, use both; otherwise, just pseudo_user_id
dedup_columns = ['pseudo_user_id']
if 'last_updated_date' in df_users_combined.columns:
    dedup_columns.append('last_updated_date')
    df_users_combined['last_updated_date'] = pd.to_datetime(df_users_combined['last_updated_date'], errors='coerce')

df_users_combined.drop_duplicates(
    subset=dedup_columns,
    keep='last',
    inplace=True
)

# Sort for consistency
df_users_combined.sort_values(by=dedup_columns, inplace=True)

# Save the combined file
df_users_combined.to_feather(user_data_path)
