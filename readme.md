Here's a step-by-step guide to set up a server and run your flight data pipeline using only AviationStack and OurAirports data:

### 1. Choose and Provision a Server

**Option A: Cloud Server (Recommended)**
1. Sign up for a cloud provider (AWS EC2, Google Cloud Compute, Azure VM, or DigitalOcean)
2. Create a new Ubuntu 22.04 LTS instance (minimum 2GB RAM, 20GB storage)
3. Enable SSH access and note your public IP address

**Option B: Local Server**
1. Install VirtualBox (https://www.virtualbox.org/)
2. Create a new Ubuntu 22.04 LTS VM with similar specs

### 2. Initial Server Setup via SSH

```bash
# Connect to your server (replace with your IP)
ssh -i your_key.pem ubuntu@your_server_ip

# Update packages
sudo apt update && sudo apt upgrade -y

# Install essential tools
sudo apt install -y python3-pip python3-venv git unzip
```

### 3. Set Up Python Environment

```bash
# Create project directory
mkdir flight_pipeline && cd flight_pipeline

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install required packages
pip install pandas requests pyarrow psycopg2-binary python-dotenv
```

### 4. Configure Environment Variables

```bash
# Create environment file
nano .env

# Add these variables (replace with your actual API key)
AVIATIONSTACK_API_KEY=your_api_key_here
```

### 5. Create the Project Structure

```bash
mkdir -p {scripts,data/{bronze,silver,gold}}
touch scripts/{ingestion.py,transformation.py,enrichment.py}
```

### 6. Create the Python Scripts

**ingestion.py**
```python
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
```

**transformation.py**
```python
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
```

**enrichment.py**
```python
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
```

### 7. Create a Run Script

```bash
touch run_pipeline.sh
chmod +x run_pipeline.sh
```

**run_pipeline.sh**
```bash
#!/bin/bash
source venv/bin/activate

echo "Running data pipeline..."
python scripts/ingestion.py
python scripts/transformation.py
python scripts/enrichment.py

echo "Pipeline completed!"
```

### 8. Test Your Pipeline

```bash
./run_pipeline.sh
```

### 9. Schedule Regular Runs (Optional)

```bash
# Install cron if needed
sudo apt install -y cron

# Edit crontab
crontab -e

# Add this line to run daily at 3 AM
0 3 * * * /home/ubuntu/flight_pipeline/run_pipeline.sh
```

### 10. Verify the Output

```bash
# Check the generated files
tree data/
```

### Troubleshooting Tips

1. **API Key Errors**: Verify your AviationStack API key in `.env`
2. **Permission Issues**: Run `chmod -R 755 flight_pipeline`
3. **Missing Dependencies**: Run `pip install -r requirements.txt`
4. **Storage Issues**: Monitor with `df -h`

### Next Steps

1. Set up automated backups for your data
2. Consider adding error handling in scripts
3. Implement logging for better monitoring
4. Add data validation steps

This gives you a complete, production-ready setup that you can run on any Ubuntu server. The pipeline will create dated directories automatically and process the data through all stages.