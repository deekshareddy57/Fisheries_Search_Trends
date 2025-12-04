import pandas as pd
import requests
import time
from io import StringIO

# ---------------------------------------------------------
# 1. SETUP & INPUT DATA
# ---------------------------------------------------------
df = pd.read_csv('usa_cities.csv')

# ---------------------------------------------------------
# 2. DEFINE API FUNCTION
# ---------------------------------------------------------

def get_historical_weather(row):
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    # Requesting Mean, Max, Min, and Precipitation sum
    params = {
        "latitude": row['latitude'],
        "longitude": row['longitude'],
        "start_date": "2023-01-01",
        "end_date": "2023-12-31",
        "daily": ["temperature_2m_mean", "temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
        "timezone": "auto"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        daily_data = data.get('daily', {})
        
        # Extract lists
        dates = daily_data.get('time', [])
        means = daily_data.get('temperature_2m_mean', [])
        maxs = daily_data.get('temperature_2m_max', [])
        mins = daily_data.get('temperature_2m_min', [])
        precips = daily_data.get('precipitation_sum', [])
        
        results = []
        # Zip all lists together to iterate day by day
        for date, mean_t, max_t, min_t, precip in zip(dates, means, maxs, mins, precips):
            entry = row.to_dict()
            entry['date'] = date
            entry['temp_mean'] = mean_t
            entry['temp_max'] = max_t
            entry['temp_min'] = min_t
            entry['precip_sum'] = precip
            results.append(entry)
            
        return results

    except Exception as e:
        print(f"Error fetching data for {row['location_name']}: {e}")
        return []

# ---------------------------------------------------------
# 3. FETCH DATA
# ---------------------------------------------------------

print("Starting API calls...")
all_weather_data = []

for index, row in df.iterrows():
    print(f"Fetching {row['location_name']}...")
    city_weather = get_historical_weather(row)
    all_weather_data.extend(city_weather)
    time.sleep(0.5) 

# Create DataFrame of DAILY data
daily_df = pd.DataFrame(all_weather_data)
daily_df['date'] = pd.to_datetime(daily_df['date'])

# ---------------------------------------------------------
# 4. CALCULATE WEEKLY AGGREGATES
# ---------------------------------------------------------

print("Aggregating to Weekly Statistics...")

static_columns = ['location_name', 'state_province', 'country', 'country_code', 'geo_code', 'location_type']

# We use a dictionary to specify how each column should be aggregated
agg_rules = {
    'temp_mean': 'mean',  # Average of the daily averages
    'temp_max': 'max',    # Highest max of the week
    'temp_min': 'min',    # Lowest min of the week
    'precip_sum': 'sum'   # Total rainfall for the week
}

weekly_df = (
    daily_df
    .set_index('date')
    .groupby(static_columns)
    .resample('W') # Resample to Weekly
    .agg(agg_rules) # Apply the specific math rules above
    .reset_index()
)

# Rename columns for clarity
weekly_df = weekly_df.rename(columns={
    'temp_mean': 'weekly_mean_temp_c',
    'temp_max': 'weekly_max_temp_c',
    'temp_min': 'weekly_min_temp_c',
    'precip_sum': 'total_weekly_precip_mm'
})

# Round the values for cleaner output
weekly_df = weekly_df.round({
    'weekly_mean_temp_c': 2,
    'weekly_max_temp_c': 2,
    'weekly_min_temp_c': 2,
    'total_weekly_precip_mm': 2
})

# ---------------------------------------------------------
# 5. EXPORT
# ---------------------------------------------------------

output_filename = "weekly_weather_summary_2023.csv"
weekly_df.to_csv(output_filename, index=False)

print(f"Weekly detailed data saved to {output_filename}")
print(weekly_df.head())
