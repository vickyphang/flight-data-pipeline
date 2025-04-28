import os
import pandas as pd
import requests
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def fetch_aviationstack_data():
    params = {
        'access_key': os.getenv('AVIATIONSTACK_API_KEY'),
        'flight_status': 'active',
        'limit': 100
    }
    response = requests.get("http://api.aviationstack.com/v1/flights", params=params)
    return response.json()

def fetch_ourairports_data():
    return pd.read_csv("https://ourairports.com/data/airports.csv")

def save_raw_data():
    date_str = datetime.today().strftime('%Y-%m-%d')
    os.makedirs(f"data/bronze/{date_str}", exist_ok=True)
    
    # Save AviationStack data
    aviation_data = fetch_aviationstack_data()
    with open(f"data/bronze/{date_str}/aviationstack.json", 'w') as f:
        json.dump(aviation_data, f)
    
    # Save OurAirports data
    airports = fetch_ourairports_data()
    airports.to_parquet(f"data/bronze/{date_str}/ourairports.parquet")

if __name__ == "__main__":
    save_raw_data()