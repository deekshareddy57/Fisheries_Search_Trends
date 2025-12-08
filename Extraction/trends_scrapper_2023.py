import pandas as pd
import time
import random
import os
from pytrends.request import TrendReq
from io import StringIO
from datetime import datetime
import logging

# ==========================================
# 1. CONFIGURATION
# ==========================================
YEAR = 2021, 2022, 2023, 2024
SEARCH_TERMS = [
    "Fishing", "Bass Fishing", "Trout Fishing", "Fly Fishing", "Ice Fishing"
]
OUTPUT_FILE = "trends_checkpoint.csv"
LOG_FILE = "trends_scraper.log"
MAX_CITIES_PER_RUN = 50  # Adjust this to process fewer cities per run if needed

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()  # Also print to console
    ]
)
logger = logging.getLogger(__name__)

# Load your cities
df_cities = pd.read_csv('usa_cities.csv')
logger.info(f"Loaded {len(df_cities)} cities from usa_cities.csv")


# ==========================================
# 2. CHECKPOINT LOGIC
# ==========================================

def get_completed_cities():
    """Reads the output file to see which geo_codes are already done."""
    if not os.path.exists(OUTPUT_FILE):
        logger.info("No checkpoint file found. Starting fresh.")
        return set()
    
    # Read just the columns we need to identify finished work
    try:
        df_done = pd.read_csv(OUTPUT_FILE)
        if 'geo_code' in df_done.columns:
            completed = set(df_done['geo_code'].unique())
            logger.info(f"Found {len(completed)} completed cities in checkpoint file")
            return completed
    except pd.errors.EmptyDataError:
        logger.warning("Checkpoint file is empty")
        pass
    
    return set()

# ==========================================
# 3. GOOGLE TRENDS FUNCTION WITH RATE LIMIT HANDLING
# ==========================================
def fetch_city_trends(geo_code, keywords, year, max_retries=3):
    """
    Fetch Google Trends data. If data is empty, fill with Zeros.
    """
    pytrends = TrendReq(hl='en-US', tz=360, timeout=(10,25))
    timeframe = f'{year}-01-01 {year}-12-31'
    all_data = []

    # Create a standard date range for the year (Weekly, Sunday-based)
    # We use this to fill in blanks if Google returns nothing.
    # Note: Google Trends standard is Sunday-ending weeks.
    date_range = pd.date_range(start=f'{year}-01-01', end=f'{year}-12-31', freq='W-SUN')

    for term in keywords:
        print(f"    > Keyword: '{term}'", end=" ", flush=True)
        
        retry_count = 0
        while retry_count < max_retries:
            try:
                # Sleep 15-30 seconds to look human
                time.sleep(random.uniform(15, 30))
                
                pytrends.build_payload([term], cat=0, timeframe=timeframe, geo=geo_code)
                data = pytrends.interest_over_time()
                
                # --- LOGIC CHANGE HERE ---
                if data.empty:
                    print("⚠️ (Empty -> Saving as 0)")
                    logger.warning(f"No data for '{term}' in {geo_code}. Filling with Zeros.")
                    
                    # Create a dummy dataframe with zeros
                    data = pd.DataFrame({'date': date_range})
                    data['search_count'] = 0
                    data['search_term'] = term
                    data['is_partial'] = list(data.iloc[:, 0] == False) # Dummy column to match structure if needed
                else:
                    data = data.reset_index()
                    data = data.rename(columns={term: 'search_count', 'date': 'date'})
                    data['search_term'] = term
                    print("✅")
                    logger.info(f"Fetched '{term}' in {geo_code}")

                # Standardize columns
                data = data[['date', 'search_term', 'search_count']]
                all_data.append(data)
                
                break  # Success, exit retry loop
                
            except Exception as e:
                # ... (Same error handling as before) ...
                if "429" in str(e):
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = (2 ** retry_count) * 60
                        print(f"\n    ⏳ Rate limited. Waiting {wait_time/60:.1f} min...")
                        time.sleep(wait_time)
                    else:
                        print(f"\n    ❌ Max retries. Skipping.")
                        break
                else:
                    print(f"\n    ❌ Error: {e}")
                    time.sleep(5)
                    break

    if not all_data:
        return pd.DataFrame()
    return pd.concat(all_data, ignore_index=True)

# ==========================================
# 4. MAIN LOOP
# ==========================================

logger.info("="*60)
logger.info(f"Starting new scraping run for year {YEAR}")
logger.info(f"Search terms: {', '.join(SEARCH_TERMS)}")
logger.info("="*60)

completed_geos = get_completed_cities()
print(f"📊 Resume Status: {len(completed_geos)} cities already completed.")

# Filter list to only do remaining cities
cities_to_process = df_cities[~df_cities['geo_code'].isin(completed_geos)].head(MAX_CITIES_PER_RUN)
print(f"🚀 Starting processing for {len(cities_to_process)} remaining cities (max {MAX_CITIES_PER_RUN} per run)...\n")
logger.info(f"Processing {len(cities_to_process)} cities in this run (max {MAX_CITIES_PER_RUN})")

successful_cities = 0
failed_cities = 0

for index, row in cities_to_process.iterrows():
    city = row['location_name']
    geo = row['geo_code']
    
    print(f"📍 Processing {city} ({geo})...")
    logger.info(f"Starting city: {city} ({geo})")
    
    try:
        df_trends = fetch_city_trends(geo, SEARCH_TERMS, YEAR)
        
        if not df_trends.empty:
            # Add metadata
            df_trends['geo_code'] = geo
            df_trends['location'] = city
            df_trends['country'] = row['country']
            df_trends['state'] = row['state_province']
            df_trends['year'] = YEAR
            df_trends['latitude'] = row['latitude']
            df_trends['longitude'] = row['longitude']

            # Reorder columns to match request
            cols = ["date", "latitude", "longitude", "country", "state", "search_count", "location", "geo_code", "search_term", "year"]
            df_trends = df_trends[cols]
            
            # SAVE IMMEDIATELY (Append Mode)
            # If file doesn't exist, write header. If it does, skip header.
            write_header = not os.path.exists(OUTPUT_FILE)
            df_trends.to_csv(OUTPUT_FILE, mode='a', header=write_header, index=False)
            print(f"    💾 Saved data for {city}")
            logger.info(f"Successfully saved {len(df_trends)} rows for {city}")
            successful_cities += 1
        else:
            # If empty, we still want to mark it as 'done' so we don't retry forever?
            # Ideally, log it to a separate 'failed' list, but for now we skip.
            print(f"    ⚠️ No data retrieved for {city}")
            logger.warning(f"No data retrieved for {city} ({geo})")
            failed_cities += 1
        
        # Add delay between cities to avoid rate limits
        print(f"    ⏸️  Pausing before next city...")
        time.sleep(random.uniform(30, 60))

    except Exception as e:
        print(f"\n💥 CRITICAL FAILURE at {city}: {e}")
        logger.error(f"CRITICAL FAILURE at {city} ({geo}): {e}", exc_info=True)
        print("Script stopped. Fix the issue or wait, then run again. Progress is saved.")
        failed_cities += 1
        break

print("\n✅ Run completed! Check trends_checkpoint.csv for results.")
logger.info("="*60)
logger.info(f"Run completed - Successful: {successful_cities}, Failed: {failed_cities}")
print(f"📊 Total cities completed so far: {len(get_completed_cities())}")
remaining = len(df_cities) - len(get_completed_cities())
print(f"🔄 Remaining cities: {remaining}")
logger.info(f"Total progress: {len(get_completed_cities())}/{len(df_cities)} cities completed")
logger.info(f"Remaining cities: {remaining}")
logger.info("="*60)
