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

    # Extra filtering:
    filtered_records = []
    for i in range(0, len(recent_records)):
        close = float(recent_records[i]['close'])
        high = float(recent_records[i]['high'])

        if close != high:
            break

        filtered_records.append(recent_records[i])
    return filtered_records

def next_price(num: float):
    return int((num * 1.02) * 10) / 10.0


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
            return next_price(prev_close)

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
workbook = connect_google_sheet()
worksheet = workbook.worksheet('Raw_First_trades')
all_rows = []
for stock in stocks:
    price_record = get_trade_dates(symbol=stock, limit_days=10)
    for each in price_record:
        trade_date = each['date']
        thatDayTrades = fetch_first_trades(symbol=stock, trade_date=trade_date)
        exp_open_price = get_expected_trade_price(trade_date=trade_date, price_records=price_record)
        normal_open_price = exp_open_price
        gotPreOrder = False
        gotNormalOrder = False
        for eachTrade in thatDayTrades:
            tradeTime = eachTrade['tradeTime']
            price = eachTrade['contractRate']
            tradeSession = classify_session(tradeTime)
            if tradeSession == "PRE_OPEN":
                normal_open_price = next_price(num=price)
                if not gotPreOrder and exp_open_price == price:
                    gotPreOrder = True
                    all_rows.append([
                        trade_date,
                        stock,
                        "PRE_OPEN",
                        tradeTime,
                        price,
                        eachTrade['buyerMemberId'],
                        eachTrade['sellerMemberId'],
                    ])
            elif tradeSession == "NORMAL" and not gotNormalOrder and normal_open_price == price:
                gotNormalOrder = True
                all_rows.append([
                        trade_date,
                        stock,
                        "NORMAL",
                        tradeTime,
                        price,
                        eachTrade['buyerMemberId'],
                        eachTrade['sellerMemberId'],
                    ])
                break


if all_rows:

    # Optional: clear previous data except header
    worksheet.batch_clear(["A2:I1000"])

    worksheet.update(
        range_name=f"A2:G{len(all_rows)+1}",
        values=all_rows
    )

print("Bulk update completed.")
