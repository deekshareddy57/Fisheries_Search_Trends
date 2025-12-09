import pandas as pd
import requests
import time

# 1. SETUP & INPUT DATA
input_filename = 'trends_checkpoint_2022_fixed.csv' # Replace for each year
output_filename = 'trends-with_weather_2022.csv'

print(f"Loading {input_filename}...")
df = pd.read_csv(input_filename)

# Ensure date is datetime
df['date'] = pd.to_datetime(df['date'])

# CREATE JOIN KEYS
# join on (Location + Year + Week Number).
# This handles the "Fixed Date" vs "Real Date" issue perfectly.
df['join_week'] = df['date'].dt.isocalendar().week.astype(int)
df['join_year'] = df['year'].astype(int) 

# Identify unique locations/years to fetch (Optimization)
unique_fetches = df[['location', 'latitude', 'longitude', 'join_year']].drop_duplicates()

print(f"Found {len(df)} rows. Need to fetch weather for {len(unique_fetches)} unique location-years.")

# 2. DEFINE WEATHER FETCHING

def get_yearly_weather(row):
    year = int(row['join_year'])
    lat = row['latitude']
    lon = row['longitude']
    location = row['location']
    
    # API Params
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "daily": ["temperature_2m_mean", "temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
        "timezone": "auto",
        "temperature_unit": "celsius"
    }

    try:
        r = requests.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        
        # Create a mini DataFrame for this single location-year
        daily = pd.DataFrame({
            'date': data['daily']['time'],
            'temp_mean': data['daily']['temperature_2m_mean'],
            'temp_max': data['daily']['temperature_2m_max'],
            'temp_min': data['daily']['temperature_2m_min'],
            'precip': data['daily']['precipitation_sum']
        })
        
        # Add metadata for merging later
        daily['location'] = location
        daily['join_year'] = year
        return daily

    except Exception as e:
        print(f"Error fetching {location} ({year}): {e}")
        return pd.DataFrame()

# 3. FETCH & PROCESS WEATHER

print("Starting Weather API calls...")
weather_chunks = []

for index, row in unique_fetches.iterrows():
    print(f"Fetching weather for: {row['location']} ({row['join_year']})")
    weather_chunk = get_yearly_weather(row)
    weather_chunks.append(weather_chunk)
    time.sleep(0.5) # Rate limiting

# Combine all daily weather data
all_weather = pd.concat(weather_chunks, ignore_index=True)
all_weather['date'] = pd.to_datetime(all_weather['date'])

# 4. AGGREGATE WEATHER TO WEEKLY
print("Aggregating daily weather to weekly...")

# Calculate Week Number for the weather data
all_weather['join_week'] = all_weather['date'].dt.isocalendar().week.astype(int)

# Group by [Location, Year, Week] and Aggregate
weather_weekly = all_weather.groupby(['location', 'join_year', 'join_week']).agg({
    'temp_mean': 'mean',
    'temp_max': 'max',
    'temp_min': 'min',
    'precip': 'sum'
}).reset_index()

# Rename columns
weather_weekly.columns = [
    'location', 'join_year', 'join_week', 
    'avg_temp_c', 'max_temp_c', 'min_temp_c', 'total_precip_mm'
]

# Round decimals
cols_to_round = ['avg_temp_c', 'max_temp_c', 'min_temp_c', 'total_precip_mm']
weather_weekly[cols_to_round] = weather_weekly[cols_to_round].round(2)

# 5. MERGE BACK TO ORIGINAL DATA
print("Merging weather data into original dataset...")

# Left Join: Keep all original rows, match on Location + Year + Week
final_df = pd.merge(
    df, 
    weather_weekly, 
    on=['location', 'join_year', 'join_week'], 
    how='left'
)

# Clean up helper columns
final_df.drop(columns=['join_week', 'join_year'], inplace=True)

# 6. SAVE
final_df.to_csv(output_filename, index=False)
print(f"Success! Saved merged data to: {output_filename}")
print(final_df[['location', 'date', 'search_term', 'avg_temp_c']].head())
