import requests
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv() 

# =========================================
# CONFIG
# =========================================

FLOORSHEET_URL = os.environ['floorsheet_url']
sheet_id = os.environ['sheet_id']
PRICE_HISTORY_URL =os.environ['price_history_url']

# =========================================
# GOOGLE SHEET CONNECTION
# =========================================

def connect_google_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
    client = gspread.authorize(creds)
    workbook = client.open_by_key(sheet_id)
    return workbook


def get_trade_dates(symbol, limit_days=10):
    """
    Returns sorted list of oldest trade dates (ascending order)
    """
    
    # First request to know total pages
    base = requests.get(
        f"{PRICE_HISTORY_URL}?pageSize=10&symbol={symbol}"
    ).json()['data']

    total_pages = base['totalPages']

    all_records = []

    # Fetch last 2–3 pages to ensure we get enough recent history
    for page in range(max(1, total_pages - 2), total_pages + 1):
        response = requests.get(
            f"{PRICE_HISTORY_URL}?pageSize=10&symbol={symbol}&page={page}"
        ).json()['data']
        
        all_records.extend(response['content'])

    # Convert date strings to datetime
    for record in all_records:
        record['parsed_date'] = datetime.strptime(record['date'], "%Y-%m-%d")

    # Sort ascending
    all_records.sort(key=lambda x: x['parsed_date'])

    # Keep only latest `limit_days`
    recent_records = all_records[:limit_days]

    return recent_records


def get_expected_trade_price(trade_date, price_records):
    """
    price_records = output of get_trade_dates()
    """
    
    for i, record in enumerate(price_records):
        
        if record['date'] == trade_date:
            
            # If it's the first ever record
            if i == 0:
                return None  # First trading day
            
            prev_close = float(price_records[i - 1]['close'])
            
            expected_price = prev_close * 1.02  # +2%
            
            return int(expected_price * 10) / 10.0

    return None

def fetch_first_trades(symbol, trade_date):
    params = {
        "Size": 10,
        "symbol": symbol,
        "orderBy": "TradeTime",
        "order": "asc",
        "date": trade_date
    }

    response = requests.get(FLOORSHEET_URL, params=params)

    if response.status_code != 200:
        return []

    data = response.json()['data']

    # Return raw trade list
    return data.get("content", [])

def classify_session(trade_time_str):
    """
    trade_time_str example: '10:59:58'
    """
    trade_time = datetime.strptime(trade_time_str, "%H:%M:%S").time()

    if trade_time < datetime.strptime("11:00:00", "%H:%M:%S").time():
        return "PRE_OPEN"
    else:
        return "NORMAL"


