"""
Download all station data for central valley from CIMIS. 
"""
import pandas as pd
import requests
import json
import numpy as np
from datetime import datetime, timedelta

# --- Configuration ---
# Your CIMIS API key (you can get one for free after registration)
file_path = './download/api_key.txt'
with open(file_path, 'r') as file:
    API_KEY = file.read()

# File name for the station information (Updated to use the filtered file if desired)
STATIONS_INFO_FILE = "all_stations_info.csv"  # Use your filtered file name here if you made one!

# Date Range for the query (MM/DD/YYYY)
START_DATE_STR = "09/28/2010"
END_DATE_STR = "09/28/2025"

# CIMIS Hourly Data Item Codes (Variables)
DATA_ITEMS = [
    "hly-asce-eto",  # ASCE Reference Evapotranspiration (ETo)
    "hly-precip",    # Precipitation
    "hly-sol-rad",   # Solar Radiation
    "hly-vap-pres",  # Vapor Pressure
    "hly-air-tmp",   # Air Temperature
    "hly-rel-hum",   # Relative Humidity
    "hly-dew-pnt",   # Dew Point
    "hly-wind-spd",  # Wind Speed
    "hly-wind-dir",  # Wind Direction
    "hly-soil-tmp"   # Soil Temperature
]
DATA_ITEMS_STR = ",".join(DATA_ITEMS)

# CIMIS API Endpoint
BASE_URL = "http://et.water.ca.gov/api/data"

# Maximum number of days to request at once (CIMIS API limit is ~72 days, 60 is safe)
CHUNK_DAYS = 60
# --- End Configuration ---

def parse_cimis_datetime(date_str, time_str):
    """
    Parses CIMIS Date (YYYY-MM-DD) and Hour (HH00) into a continuous datetime object.
    
    Handles the special case where CIMIS uses '2400' to denote midnight (00:00) 
    of the *next* day.
    """
    try:
        base_date = datetime.strptime(date_str, "%Y-%m-%d")
        hour_val = int(time_str[:2])
        
        if hour_val == 24:
            # Case 1: '2400' means 00:00 of the next day.
            timestamp = base_date + timedelta(days=1)
        else:
            # Case 2: '0100' through '2300' are standard hours on the base date.
            datetime_str = f"{date_str} {time_str[:-2]}:{time_str[-2:]}:00"
            timestamp = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
            
        return timestamp
    except ValueError as e:
        print(f"Datetime parsing error for Date: {date_str}, Hour: {time_str}. Error: {e}")
        return None

def fetch_cimis_data(station_id, start_date, end_date):
    """Fetches hourly CIMIS data for a single station within a date range."""
    
    # Format dates as YYYY-MM-DD for the API
    start_date_fmt = start_date.strftime("%Y-%m-%d")
    end_date_fmt = end_date.strftime("%Y-%m-%d")

    # Construct the API request URL
    url = f"{BASE_URL}?appKey={API_KEY}&targets={station_id}&startDate={start_date_fmt}&endDate={end_date_fmt}&dataItems={DATA_ITEMS_STR}"
    
    try:
        response = requests.get(url, headers={'Accept': 'application/json'})
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        data = response.json()
        
        if 'Data' in data and 'Providers' in data['Data'] and data['Data']['Providers'][0]['Records']:
            records = data['Data']['Providers'][0]['Records']
            df_records = []
            
            for record in records:
                timestamp = parse_cimis_datetime(record['Date'], record['Hour'])
                if timestamp is None:
                    continue # Skip this record if time parsing failed

                row = {
                    'Datetime': timestamp,
                    'StationId': record['Station'],
                }
                
                # Extract the value for each requested data item
                for item in DATA_ITEMS:
                    # Convert item code to JSON key (e.g., 'hly-asce-eto' -> 'HlyAsceEto')
                    json_key = "".join(x.capitalize() for x in item.split('-'))
                    
                    if json_key in record:
                        value = record[json_key].get('Value')
                        
                        # Use np.nan for missing/invalid data, as requested
                        if value and value.strip() not in ['M', '---', '####']:
                            try:
                                row[item] = float(value)
                            except ValueError:
                                row[item] = np.nan # Use np.nan if conversion fails
                        else:
                            row[item] = np.nan # Use np.nan for explicit missing values
                    else:
                        row[item] = np.nan # Use np.nan if item is missing in the record

                df_records.append(row)
            
            # Sort by Datetime to ensure correct chronological order after '2400' handling
            df_temp = pd.DataFrame(df_records)
            df_final = df_temp.set_index('Datetime').sort_index()
            return df_final
        else:
            print(f"-> No data records found for station {station_id} between {start_date_fmt} and {end_date_fmt}.")
            return pd.DataFrame()

    except requests.exceptions.HTTPError as errh:
        print(f"-> HTTP Error for station {station_id}: {errh}")
    except requests.exceptions.RequestException as err:
        print(f"-> Request Error for station {station_id}: {err}")
    except json.JSONDecodeError:
        print(f"-> Failed to decode JSON for station {station_id}. Response: {response.text[:100]}...")
    except Exception as e:
        print(f"-> An unexpected error occurred for station {station_id}: {e}")
        
    return pd.DataFrame()

def main():
    if API_KEY == "YOUR-CIMIS-API-KEY":
        print("\n!!! ERROR: Please replace 'YOUR-CIMIS-API-KEY' in the script with your actual CIMIS API key. !!!\n")
        return

    print(f"--- CIMIS Data Download Script (Revised) ---")
    print(f"Date Range: {START_DATE_STR} to {END_DATE_STR}")
    print(f"Missing data will be represented by 'np.nan'.")
    
    # 1. Read station IDs
    try:
        # Load the station info CSV
        stations_df = pd.read_csv(STATIONS_INFO_FILE)
        # Convert Stn Id to integer and filter for valid IDs
        station_ids = stations_df['Stn Id'].dropna().astype(int).unique().tolist()
        print(f"Found {len(station_ids)} station IDs to process from {STATIONS_INFO_FILE}.")
        
    except FileNotFoundError:
        print(f"\n!!! ERROR: The station information file '{STATIONS_INFO_FILE}' was not found. !!!\n")
        return
    except Exception as e:
        print(f"\n!!! ERROR reading station info file: {e} !!!\n")
        return

    # Convert date strings to datetime objects
    try:
        start_date = datetime.strptime(START_DATE_STR, "%m/%d/%Y").date()
        end_date = datetime.strptime(END_DATE_STR, "%m/%d/%Y").date()
    except ValueError:
        print("\n!!! ERROR: Date format is incorrect. Please use MM/DD/YYYY. !!!\n")
        return

    all_data = []

    # 2. Loop through all stations and date chunks
    for station_id in station_ids:
        print(f"\nProcessing Station ID: {station_id}...")
        station_data = []
        current_start = start_date
        
        while current_start <= end_date:
            current_end = current_start + timedelta(days=CHUNK_DAYS - 1)
            if current_end > end_date:
                current_end = end_date

            # print(f"  Fetching data chunk: {current_start} to {current_end}")
            
            df_chunk = fetch_cimis_data(station_id, current_start, current_end)
            if not df_chunk.empty:
                station_data.append(df_chunk)
            
            # Move to the next chunk
            current_start = current_end + timedelta(days=1)
        
        # Aggregate all chunks for the current station
        if station_data:
            df_station = pd.concat(station_data)
            all_data.append(df_station)
            print(f"  Successfully retrieved {len(df_station)} hourly records for station {station_id}.")
        else:
            print(f"  No data retrieved for station {station_id} over the entire period.")


    # 3. Aggregate all station data
    if all_data:
        final_df = pd.concat(all_data)
        
        # Merge with station info to include Lat/Lon/Name
        # Note: 'Stn Id' is integer in stations_df, 'StationId' is string in final_df records
        final_df = final_df.reset_index().merge(
            stations_df[['Stn Id', 'Stn Name', 'Latitude', 'Longitude']],
            left_on=final_df['StationId'].astype(int), # Convert back to int for merging
            right_on='Stn Id',
            how='left'
        ).drop(columns=['Stn Id', 'key_0']).set_index('Datetime')
        
        # 4. Save the final dataset
        output_filename = "cimis_hourly_data_2010_2025_revised.csv"
        final_df.to_csv(output_filename)
        print(f"\n--- SUCCESS ---")
        print(f"All data has been successfully combined and saved to: {output_filename}")
        print(f"Total records saved: {len(final_df)}")
    else:
        print("\n--- COMPLETE ---")
        print("No data was retrieved from the CIMIS API. Please check your configuration.")

if __name__ == "__main__":
    main()