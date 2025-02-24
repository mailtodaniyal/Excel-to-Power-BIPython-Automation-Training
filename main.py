import pandas as pd
import requests
import sqlalchemy
import schedule
import time
from sqlalchemy import create_engine

DB_URI = "postgresql://username:password@localhost:5432/database_name"
engine = create_engine(DB_URI)

API_URL = "https://api.example.com/data"

EXCEL_FILE = "data_report.xlsx"

def fetch_from_database():
    """Fetch data from the database."""
    query = "SELECT * FROM your_table"
    df = pd.read_sql(query, engine)
    print("Database data fetched successfully!")
    return df

def fetch_from_api():
    """Fetch data from an API."""
    response = requests.get(API_URL)
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)  
        print("API data fetched successfully!")
        return df
    else:
        print("Failed to fetch API data.")
        return pd.DataFrame()

def fetch_from_excel():
    """Fetch data from an existing Excel file."""
    df = pd.read_excel(EXCEL_FILE, sheet_name="Sheet1")
    print("Excel data loaded successfully!")
    return df

def process_data(df1, df2, df3):
    """Transform and merge data from multiple sources."""
    df = pd.concat([df1, df2, df3], ignore_index=True)
    df.drop_duplicates(inplace=True)
    df.fillna(0, inplace=True)
    print("Data processing complete!")
    return df

def generate_excel_report(df):
    """Generate an automated Excel report."""
    with pd.ExcelWriter("automated_report.xlsx", engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Report", index=False)
        print("Excel report generated successfully!")

def automate_report():
    """Complete automation process."""
    db_data = fetch_from_database()
    api_data = fetch_from_api()
    excel_data = fetch_from_excel()
    
    final_data = process_data(db_data, api_data, excel_data)
    generate_excel_report(final_data)

schedule.every().day.at("19:00").do(automate_report)

print("Report automation scheduled. Running in the background...")

while True:
    schedule.run_pending()
    time.sleep(60) 
