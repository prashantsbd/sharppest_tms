# make sure that it's at bull run still yet to fix
import requests
import pandas as pd
from datetime import datetime, time
import gspread
from google.oauth2.service_account import Credentials
import os
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common import keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options


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

def get_recent_listing(limit=10):
    """
    Analyze the most recent securities trade
    """
    option = webdriver.ChromeOptions()
    option.add_argument("--windows-size=1100,660")
    option.add_argument("--disable-notification")
    option.add_argument("--disable-blink-feature=AutomationControlled")
    option.add_argument("--ignore-certificate-errors")
    option.add_argument("--ignore-ssl-errors")
    option.add_argument("--allow-insecure-localhost")

    driver = webdriver.Chrome(options=option)
    driver.get("https://nepalstock.com/company")
    driver.implicitly_wait(5)
    driver.find_element(By.XPATH, "//div[@class='table__perpage']/select[1]").click()
    driver.implicitly_wait(5)
    driver.find_element(By.XPATH, "//div[@class='table__perpage']/select/option[6]").click()
    driver.implicitly_wait(5)
    table_data = driver.execute_script("""
        let rows = [...document.querySelectorAll("table tbody tr")];
        return rows
            .slice(-10)   // take last 10 rows
            .map(r => r.querySelectorAll("td")[2]?.innerText.trim());
    """)
    driver.quit()
    return table_data

    
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

def next_price(num: float):
    return int(num * 10) / 10.0


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
            return next_price(expected_price)

    return None

def fetch_first_trades(symbol, trade_date):
    params = {
        "Size": 20,
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
    dt_object = datetime.fromisoformat(trade_time_str.replace('Z', '+00:00'))
    trade_time = dt_object.time()

    if trade_time < time(11, 0, 0):
        return "PRE_OPEN"
    else:
        return "NORMAL"

stocks = get_recent_listing(limit=10)
for stock in stocks:
    price_record = get_trade_dates(symbol=stock, limit_days=10)
    for each in price_record:
        thatDayTrades = fetch_first_trades(symbol=stock, trade_date=each['date'])
        exp_open_price = get_expected_trade_price(trade_date=each['date'], price_records=price_record)
        gotPreOrder = False
        gotNormalOrder = False
        for eachTrade in thatDayTrades:
            tradeTime = eachTrade['tradeTime']
            price = eachTrade['contractRate']
            tradeSession = classify_session(tradeTime)
            normal_open_price = exp_open_price
            if tradeSession == "PRE_OPEN":
                normal_open_price = next_price(num=price)
                if not gotPreOrder and (exp_open_price is None or exp_open_price == price):
                    gotPreOrder = True
                    normal_open_price = next_price(num=price)
                    print(f"got PreOrder of {stock} on {each['date']} by Broker no. {eachTrade['buyerMemberId']}")
            elif tradeSession == "NORMAL" and not gotNormalOrder and (normal_open_price is None or normal_open_price == price):
                gotNormalOrder = True
                print(f"got Normal Order of {stock} on {each['date']} by Broker no. {eachTrade['buyerMemberId']}\n\n")
                break
