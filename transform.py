import pandas as pd
import json
from datetime import datetime

def transform_data():
    date_str = datetime.today().strftime('%Y-%m-%d')
    os.makedirs(f"data/silver/{date_str}", exist_ok=True)
    
    # Transform AviationStack data
    with open(f"data/bronze/{date_str}/aviationstack.json") as f:
        aviation_data = json.load(f)
    
    flights = pd.json_normalize(aviation_data['data'])
    flights.to_parquet(f"data/silver/{date_str}/flights.parquet")
    
    # Transform OurAirports data
    airports = pd.read_parquet(f"data/bronze/{date_str}/ourairports.parquet")
    airports = airports[airports['type'].isin(['large_airport', 'medium_airport'])]
    airports.to_parquet(f"data/silver/{date_str}/airports.parquet")

if __name__ == "__main__":
    transform_data()