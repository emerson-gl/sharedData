import pandas as pd
from sqlalchemy import create_engine
import urllib
import os

# Set working directory
os.chdir('C:\\Users\\Graphicsland\\Spyder\\sharedData')

# Format the connection string
params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={os.getenv('DB_SERVER')},1433;"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "ApplicationIntent=ReadOnly;"
)


# Create SQLAlchemy engine
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# Query and save tables
def query_and_save(table_name, file_name):
    df = pd.read_sql(f"SELECT * FROM {table_name};", engine)
    df.to_feather(f"{file_name}.feather")
    

# Query and save tables
query_and_save("[Order]", "raw_order_df")
query_and_save("OrderItem", "raw_order_item_df")
query_and_save("Design", "raw_design_df")
query_and_save("Note", "raw_note_df")
query_and_save("Feedback", "raw_feedback_df")
# query_and_save("ProductProductOption", "raw_product_product_option_df")

# ad_campaigns = pd.read_csv("AdCampaigns.csv")
# order_and_analytics = pd.read_csv("OrderAndAnalytics.csv")
# order_and_analytics = order_and_analytics.fillna("").astype(str)

# ad_campaigns.to_feather("raw_ad_campaigns.feather")
# order_and_analytics.to_feather("raw_order_and_analytics.feather")
