# Fisheries_Search_Trends
This project analyzes fisheries-related search behavior using Google Trends data. It includes data extraction, temperature integration, and an interactive Streamlit dashboard to explore seasonal patterns and environmental relationships.

This repository contains the code and data processing pipeline used for extracting, cleaning, and visualizing fisheries-related Google Trends search data. The project is part of ongoing research collaboration with Iowa State University.

The workflow includes:
1. **Search Trends Data Extraction**
2. **Temperature Data Processing**
3. **Visualization Dashboard (Streamlit)**


## 📁 Project Structure
fisheries-trends/
│
├── extraction/
│ ├── extraction_code.py # Google Trends extractor
│ ├── city_list.csv # List of locations
│ └── outputs/ # Raw extracted search data
│
├── temperature/
│ ├── get_temperature.py # Temperature processing script
│ ├── input.csv # Input dataset
│ └── output.csv # Cleaned temperature dataset
│
├── visualization/
│ ├── streamlit_app.py # Streamlit visualization dashboard
│ └── data.csv # Input data for graphs
│
├── requirements.txt # Python dependencies
└── LICENSE # Open-source license

## How to Run the Project

### 1. **Install dependencies**
```bash
pip install -r requirements.txt

### 2. Run Google Trends Extraction
python extraction/extraction_code.py

### 3. Process Temparature Data
python tempararture/get_temperature.py

### 4. Start the Visualization Dashboard
streamlit run visualization/streamlit_app.py

## Components Overview
## Extraction
Pulls weekly Google Trends data for selected fisheries-related search terms.
Outputs CSV files stored in /extraction/outputs/.

## Temperature Processing
Reads raw location & trend data.
Adds temperature variables
Outputs merged dataset.

## Visualization
Interactive Streamlit dashboard.
Displays trend patterns, temperature relationships, and seasonal variation.
