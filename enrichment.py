import pandas as pd
from datetime import datetime

def create_gold_data():
    date_str = datetime.today().strftime('%Y-%m-%d')
    os.makedirs(f"data/gold/{date_str}", exist_ok=True)
    
    flights = pd.read_parquet(f"data/silver/{date_str}/flights.parquet")
    airports = pd.read_parquet(f"data/silver/{date_str}/airports.parquet")
    
    # Simple enrichment: Add airport details to flights
    enriched = flights.merge(
        airports[['iata_code', 'name', 'latitude_deg', 'longitude_deg']],
        left_on='departure.iata',
        right_on='iata_code',
        how='left'
    )
    
    enriched.to_parquet(f"data/gold/{date_str}/enriched_flights.parquet")

if __name__ == "__main__":
    create_gold_data()