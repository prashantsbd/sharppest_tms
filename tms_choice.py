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

FLOORSHEET_URL = "https://sharehubnepal.com/live/api/v2/floorsheet"
sheet_id = os.environ['sheet_id']

# =========================================
# GOOGLE SHEET CONNECTION
# =========================================

def connect_google_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
    client = gspread.authorize(creds)
    workbook = client.open_by_key(sheet_id)
    return workbook

