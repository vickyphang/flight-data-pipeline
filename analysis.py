import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import os
import sys
import warnings
warnings.filterwarnings('ignore')  # Suppress benign warnings

def analyze_flight_data():
    try:
        # 1. Load Data
        date_str = datetime.today().strftime('%Y-%m-%d')
        data_dir = f"data/gold/{date_str}"
        os.makedirs(f"{data_dir}/plots", exist_ok=True)
        
        df = pd.read_parquet(f"{data_dir}/enriched.parquet")
        
        # Convert timestamps
        time_cols = [col for col in df.columns if 'scheduled' in col or 'estimated' in col]
        for col in time_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # 2. Popular Flight Routes Analysis
        route_analysis = df.groupby(['departure.airport', 'arrival.airport'])\
            .agg({
                'flight.iata': 'count',
                'arrival.delay': 'mean'
            })\
            .rename(columns={
                'flight.iata': 'flight_count',
                'arrival.delay': 'avg_delay_minutes'
            })\
            .sort_values('flight_count', ascending=False)
        
        # Plot top routes
        plt.figure(figsize=(12, 6))
        route_analysis.head(10)['flight_count'].plot(kind='bar')
        plt.title('Top 10 Busiest Flight Routes')
        plt.ylabel('Number of Flights')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(f"{data_dir}/plots/top_routes.png")
        plt.close()
        
        # 3. Flight Delay Analysis
        delay_analysis = {}
        
        # Basic delay stats
        delay_analysis['basic_stats'] = df['arrival.delay'].describe().to_dict()
        
        # Delay by airline
        delay_analysis['by_airline'] = df.groupby('airline.name')['arrival.delay']\
            .agg(['mean', 'count'])\
            .rename(columns={
                'mean': 'avg_delay_minutes',
                'count': 'total_flights'
            })\
            .sort_values('avg_delay_minutes', ascending=False)\
            .head(10)
        
        # Delay distribution plot
        plt.figure(figsize=(10, 6))
        df['arrival.delay'].clip(lower=0, upper=180).hist(bins=30)
        plt.title('Flight Delay Distribution (0-180 mins)')
        plt.xlabel('Delay in Minutes')
        plt.ylabel('Number of Flights')
        plt.savefig(f"{data_dir}/plots/delay_distribution.png")
        plt.close()
        
        # 4. Airport Performance Metrics
        airport_metrics = df.groupby('departure.airport').agg({
            'flight.iata': 'count',
            'arrival.delay': ['mean', 'median'],
            'departure.delay': ['mean', 'median']
        })
        airport_metrics.columns = ['_'.join(col).strip() for col in airport_metrics.columns.values]
        airport_metrics = airport_metrics.rename(columns={
            'flight.iata_count': 'total_flights',
            'arrival.delay_mean': 'avg_arrival_delay',
            'arrival.delay_median': 'median_arrival_delay',
            'departure.delay_mean': 'avg_departure_delay',
            'departure.delay_median': 'median_departure_delay'
        }).sort_values('total_flights', ascending=False)
        
        # 5. Temporal Analysis
        temporal_analysis = {}
        
        # By hour
        df['departure_hour'] = df['departure.scheduled'].dt.hour
        temporal_analysis['by_hour'] = df.groupby('departure_hour').size()
        
        # By day of week
        df['departure_day'] = df['departure.scheduled'].dt.day_name()
        temporal_analysis['by_weekday'] = df.groupby('departure_day').size()
        
        # Plot temporal patterns
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        temporal_analysis['by_hour'].plot(kind='bar', ax=ax1)
        ax1.set_title('Flights by Hour of Day')
        ax1.set_xlabel('Hour')
        ax1.set_ylabel('Number of Flights')
        
        temporal_analysis['by_weekday'].plot(kind='bar', ax=ax2)
        ax2.set_title('Flights by Day of Week')
        ax2.set_xlabel('Day')
        ax2.set_ylabel('Number of Flights')
        
        plt.tight_layout()
        plt.savefig(f"{data_dir}/plots/temporal_patterns.png")
        plt.close()
        
        # 6. Aircraft Utilization (if data available)
        aircraft_analysis = {}
        if 'aircraft.registration' in df.columns:
            aircraft_analysis = df['aircraft.registration'].value_counts().head(10).to_dict()
        
        # 7. Generate Comprehensive Report
        report = f"""
        FLIGHT DATA ANALYSIS REPORT - {date_str}
        ===========================================
        
        1. SUMMARY STATISTICS
        ---------------------
        - Total flights: {len(df)}
        - Average delay: {delay_analysis['basic_stats']['mean']:.1f} minutes
        - Maximum delay: {delay_analysis['basic_stats']['max']:.1f} minutes
        - Flights with delays > 30 mins: {len(df[df['arrival.delay'] > 30])} ({len(df[df['arrival.delay'] > 30])/len(df)*100:.1f}%)
        
        2. BUSIEST ROUTES (TOP 5)
        -------------------------
        {route_analysis.head(5).to_string()}
        
        3. WORST PERFORMING AIRLINES BY DELAY (TOP 5)
        --------------------------------------------
        {delay_analysis['by_airline'].head(5).to_string()}
        
        4. AIRPORT PERFORMANCE (TOP 5 BY FLIGHT VOLUME)
        ----------------------------------------------
        {airport_metrics.head(5).to_string()}
        
        5. AIRCRAFT UTILIZATION
        -----------------------
        {aircraft_analysis if aircraft_analysis else "No aircraft registration data available"}
        
        Generated visualizations:
        - top_routes.png
        - delay_distribution.png
        - temporal_patterns.png
        """
        
        # Save report
        with open(f"{data_dir}/comprehensive_report.txt", 'w') as f:
            f.write(report)
            
        print(f"Analysis complete! Report saved to {data_dir}/comprehensive_report.txt")
        print(f"Visualizations saved to {data_dir}/plots/")
        
        return {
            'route_analysis': route_analysis,
            'delay_analysis': delay_analysis,
            'airport_metrics': airport_metrics,
            'temporal_analysis': temporal_analysis,
            'aircraft_analysis': aircraft_analysis
        }
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}", file=sys.stderr)
        return None

if __name__ == "__main__":
    analyze_flight_data()