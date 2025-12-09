import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.ndimage import gaussian_filter1d
from scipy.interpolate import interp1d

# Set page configuration
st.set_page_config(page_title="Fishing Search Phenology", layout="wide")

# Title
st.title("🎣 Fishing Search Phenology Analysis with Temperature Data")

# Load the dataset
@st.cache_data
def load_data():
    df = pd.read_csv("visualization/trends-with_weather_2023.csv")
    df['date'] = pd.to_datetime(df['date'])
    df['day_of_year'] = df['date'].dt.dayofyear
    return df

df = load_data()

# Sidebar for filters
st.sidebar.header("Filter Options")

# Get unique values for filters
search_terms = sorted(df['search_term'].unique())
locations = sorted(df['location'].unique())
years = sorted(df['year'].unique())

# Create selection widgets
selected_search_term = st.sidebar.selectbox(
    "Select Search Term:",
    search_terms,
    index=search_terms.index('Fishing') if 'Fishing' in search_terms else 0
)

selected_location = st.sidebar.selectbox(
    "Select City:",
    locations,
    index=locations.index('Minneapolis') if 'Minneapolis' in locations else 0
)

selected_year = st.sidebar.selectbox(
    "Select Year:",
    years,
    index=years.index(2024) if 2024 in years else 0
)

# Advanced settings
st.sidebar.header("Advanced Settings")
sigma = st.sidebar.slider("Smoothing Factor (sigma):", 1, 10, 4)
threshold_pct = st.sidebar.slider("Threshold % above minimum:", 10, 50, 20)

# Filter the data
df_filtered = df[
    (df['search_term'] == selected_search_term) &
    (df['location'] == selected_location) &
    (df['year'] == selected_year)
].sort_values('day_of_year').reset_index(drop=True)

# Check if data exists
if len(df_filtered) == 0:
    st.error(f"No data found for {selected_search_term} in {selected_location} for {selected_year}")
else:
    # Extract the weekly values
    doy = df_filtered['day_of_year'].values
    search_counts = df_filtered['search_count'].values
    
    # Create continuous interpolated data
    doy_continuous = np.arange(doy.min(), doy.max() + 1)
    interpolator = interp1d(doy, search_counts, kind='linear', fill_value='extrapolate')
    search_counts_continuous = interpolator(doy_continuous)
    
    # Smooth the search trend
    smoothed_counts = gaussian_filter1d(search_counts_continuous, sigma=sigma)
    
    # Define threshold
    min_val = smoothed_counts.min()
    threshold = min_val + (threshold_pct / 100) * (smoothed_counts.max() - min_val)
    
    # Find Start (A), Peak (B), End (C)
    above_thresh = smoothed_counts > threshold
    
    # Find the peak
    peak_index = np.argmax(smoothed_counts)
    B_doy = doy_continuous[peak_index]
    
    # Find Start (A)
    indices_before_peak = np.where(doy_continuous < B_doy)[0]
    above_thresh_before_peak = above_thresh[indices_before_peak]
    if np.any(above_thresh_before_peak):
        start_index = indices_before_peak[np.argmax(above_thresh_before_peak)]
    else:
        start_index = 0
    
    # Find End (C)
    indices_after_peak = np.where(doy_continuous > B_doy)[0]
    below_thresh_after_peak = smoothed_counts[indices_after_peak] <= threshold
    if np.any(below_thresh_after_peak):
        end_index = indices_after_peak[np.argmax(below_thresh_after_peak)]
    else:
        end_index = len(smoothed_counts) - 1
    
    A_doy = doy_continuous[start_index]
    C_doy = doy_continuous[end_index]
    duration = C_doy - A_doy
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Start of Season (A)", f"Day {int(A_doy)}")
    with col2:
        st.metric("Peak Interest (B)", f"Day {int(B_doy)}")
    with col3:
        st.metric("End of Season (C)", f"Day {int(C_doy)}")
    with col4:
        st.metric("Duration", f"{int(duration)} days")
    
    # Check if weather data is available and has valid values
    has_weather = ('avg_temp_c' in df_filtered.columns and 
                   df_filtered['avg_temp_c'].notna().any())
    
    # Debug: Show what columns we have
    st.write(f"**Available columns:** {', '.join(df_filtered.columns.tolist())}")
    st.write(f"**Weather data detected:** {has_weather}")
    
    if 'temperature_2m_mean' in df_filtered.columns:
        valid_temp = df_filtered['temperature_2m_mean'].notna().sum()
        st.write(f"**Valid temperature values:** {valid_temp} out of {len(df_filtered)}")
    
    
    if has_weather:
        st.sidebar.header("Weather Display")
        show_temp = st.sidebar.checkbox("Show Temperature", value=True)
        show_precip = st.sidebar.checkbox("Show Precipitation", value=True)
    else:
        show_temp = False
        show_precip = False
        if 'temperature_2m_mean' in df_filtered.columns:
            st.warning("⚠️ Weather columns exist but contain no valid data for this selection. Try a different city/year.")
    
    # Create the plot
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # Plot original data
    ax1.plot(doy, search_counts, label='Original Weekly Search Count', 
            linestyle='--', alpha=0.5, linewidth=1.5, color='steelblue', 
            marker='o', markersize=3)
    
    # Plot smoothed trend
    line1 = ax1.plot(doy_continuous, smoothed_counts, label='Smoothed Trend', 
            linewidth=3, color='orange')
    
    # Plot threshold
    ax1.axhline(threshold, color='gray', linestyle=':', linewidth=1.5,
               label=f'Threshold = {threshold:.1f}')
    
    # Mark A, B, and C
    ax1.scatter([A_doy], [smoothed_counts[start_index]], color='green', 
               s=100, label='Start (A)', zorder=5, edgecolors='black')
    ax1.scatter([B_doy], [smoothed_counts[peak_index]], color='red', 
               s=100, label='Peak (B)', zorder=5, edgecolors='black')
    ax1.scatter([C_doy], [smoothed_counts[end_index]], color='blue', 
               s=100, label='End (C)', zorder=5, edgecolors='black')
    
    
    ax1.set_title(f"{selected_search_term} Search Phenology - {selected_location} ({selected_year})", 
                 fontsize=14, fontweight='bold')
    ax1.set_ylabel("Search Interest", fontsize=12)
    ax1.legend(loc='upper left')
    ax1.grid(True, alpha=0.3)
    
    # Add temperature overlay if weather data is available
    if has_weather:
            # Create secondary axis sharing the same x-axis
            ax2 = ax1.twinx()
            
            # 1. Plot Precipitation first (so it's behind the line)
            if show_precip and 'total_precip_mm' in df_filtered.columns:
                precip = df_filtered['total_precip_mm'].values
                # Using bar chart for precip, low alpha so it doesn't block the lines
                ax2.bar(doy, precip, color='blue', alpha=0.2, label='Precipitation (mm)', width=2)
                
            # 2. Plot Temperature
            if show_temp and 'avg_temp_c' in df_filtered.columns:
                temp_mean = df_filtered['avg_temp_c'].values
                ax2.plot(doy, temp_mean, label='Avg Temp (°C)', 
                        linewidth=2, color='red', alpha=0.6, linestyle='-.')
            
            # Configure secondary axis
            ax2.set_ylabel("Weather Metrics", fontsize=12, color='gray')
            ax2.tick_params(axis='y', labelcolor='gray')
            
            # Combine legends from both axes into one
            lines_1, labels_1 = ax1.get_legend_handles_labels()
            lines_2, labels_2 = ax2.get_legend_handles_labels()
            ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left')

    plt.tight_layout()
        
    # Display the plot
    st.pyplot(fig)
    
    # Display data summary
    with st.expander("📊 View Data Summary"):
        st.write(f"**Number of data points:** {len(df_filtered)}")
        st.write(f"**Search count range:** {df_filtered['search_count'].min()} to {df_filtered['search_count'].max()}")
        st.dataframe(df_filtered.head(10))
