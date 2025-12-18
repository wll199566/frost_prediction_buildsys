"""
Download all station data for central valley from CIMIS. 
"""
import os
import pandas as pd
import requests
import json
import numpy as np
from datetime import datetime, timedelta

# --- Configuration ---
# Your CIMIS API key (you can get one for free after registration)
file_path = './download/api_key.txt'
with open(file_path, 'r') as file:
    API_KEY = file.read().strip()

# File name for the station information
PATH = "./Data"
STATIONS_INFO_FILE = "cv_stations_info.csv"

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

def fetch_cimis_data(station_id, start_date, end_date):
    """
    Fetches hourly CIMIS data and QC codes for a single station, 
    keeping the original CIMIS Date and Hour strings.
    """
    
    start_date_fmt = start_date.strftime("%Y-%m-%d")
    end_date_fmt = end_date.strftime("%Y-%m-%d")

    url = f"{BASE_URL}?appKey={API_KEY.strip()}&targets={station_id}&startDate={start_date_fmt}&endDate={end_date_fmt}&dataItems={DATA_ITEMS_STR}&unitOfMeasure=M"
    print(f"station id: {station_id}, start_date: {start_date}, end_date: {end_date}")
    try:
        response = requests.get(url, headers={'Accept': 'application/json'})
        response.raise_for_status() 
        data = response.json()
 
        if 'Data' in data and 'Providers' in data['Data'] and data['Data']['Providers'][0]['Records']:
            records = data['Data']['Providers'][0]['Records']
            df_records = []
            
            for record in records:
                row = {
                    'Date': record.get('Date'),
                    'Hour': record.get('Hour'),
                    'StationId': record.get('Station'),
                }
                
                # Extract the Value and QC code for each requested data item
                for item in DATA_ITEMS:
                    # Convert item code to JSON key (e.g., 'hly-asce-eto' -> 'HlyAsceEto')
                    json_key = "".join(x.capitalize() for x in item.split('-'))
                    
                    if json_key in record:
                        data_field = record[json_key]
                        
                        # --- 1. Extract Value (Value column name: item) ---
                        value = data_field.get('Value')
                        # Check for missing/invalid data strings and convert to np.nan
                        if value and value.strip() not in ['M', '---', '####']:
                            try:
                                row[item] = float(value)
                            except ValueError:
                                row[item] = np.nan
                        else:
                            row[item] = np.nan
                            
                        # --- 2. Extract QC (QC column name: item + '-qc') ---
                        qc_value = data_field.get('Qc')
                        if qc_value is not None:
                            # QC codes are typically single-character strings or digits
                            row[item + '-qc'] = qc_value.strip()
                        else:
                            row[item + '-qc'] = "" # Use empty string for consistency if QC is missing
                    else:
                        # If the entire data item is missing from the record
                        row[item] = np.nan
                        row[item + '-qc'] = ""

                df_records.append(row)
            
            return pd.DataFrame(df_records)
        else:
            # print(f"-> No data records found for station {station_id} in chunk {start_date_fmt} to {end_date_fmt}.")
            return pd.DataFrame()

    except requests.exceptions.HTTPError as errh:
        print(f"-> HTTP Error for station {station_id} in chunk {start_date_fmt} to {end_date_fmt}: {errh}")
    except requests.exceptions.RequestException as err:
        print(f"-> Request Error for station {station_id} in chunk {start_date_fmt} to {end_date_fmt}: {err}")
    except json.JSONDecodeError:
        print(f"-> Failed to decode JSON for station {station_id}. Response: {response.text[:100]}...")
    except Exception as e:
        print(f"-> An unexpected error occurred for station {station_id} in chunk {start_date_fmt} to {end_date_fmt}: {e}")
        
    return pd.DataFrame()

def post_process(df_stations):
    # Remove some columns
    df_stations = df_stations.drop(columns=["Latitude", "Longitude"])
    # Rename the columns
    name2name = {"hly-asce-eto": "ETo (mm)",  
                 "hly-precip": "Precip (mm)",    
                 "hly-sol-rad": "Sol Rad (W/sq.m)",   
                 "hly-vap-pres": "Vap Pres (kPa)",  
                 "hly-air-tmp": "Air Temp (C)",   
                 "hly-rel-hum": "Rel Hum (%)",   
                 "hly-dew-pnt": "Dew Point (C)",   
                 "hly-wind-spd": "Wind Speed (m/s)",  
                 "hly-wind-dir": "Wind Dir (0-360)",  
                 "hly-soil-tmp": "Soil Temp (C)",
                 "hly-asce-eto-qc": "eto-qc",  
                 "hly-precip-qc": "precip-qc",    
                 "hly-sol-rad-qc": "solrad-qc",   
                 "hly-vap-pres-qc": "vappres-qc",  
                 "hly-air-tmp-qc": "airtmp-qc",   
                 "hly-rel-hum-qc": "relhum-qc",   
                 "hly-dew-pnt-qc": "dewpnt-qc",   
                 "hly-wind-spd-qc": "windspd-qc",  
                 "hly-wind-dir-qc": "winddir-qc",  
                 "hly-soil-tmp-qc": "soiltmp-qc",
                 "StationId": "Stn Id"   
                 }
    df_stations = df_stations.rename(columns=name2name)
    return df_stations

def main():
    if API_KEY == "YOUR-CIMIS-API-KEY":
        print("\n!!! ERROR: Please replace 'YOUR-CIMIS-API-KEY' in the script with your actual CIMIS API key. !!!\n")
        return

    print(f"--- CIMIS Data Download Script (Per Station with QC Codes) ---")
    print(f"Date Range: {START_DATE_STR} to {END_DATE_STR}")
    print("Files will be saved in a new folder named 'cimis_hourly_data'.")
    
    # 1. Read station IDs
    try:
        stations_df = pd.read_csv(os.path.join(PATH, STATIONS_INFO_FILE))
        # Convert Stn Id to integer and filter for valid IDs, and get station names for naming files
        stations_info = stations_df[['Stn Id', 'Stn Name', 'Latitude', 'Longitude']].dropna(subset=['Stn Id'])
        stations_info['Stn Id'] = stations_info['Stn Id'].astype(int)
        
        station_ids = stations_info['Stn Id'].tolist()
        
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

    # Create output directory
    output_dir = "cimis_hourly_data"
    os.makedirs(os.path.join(PATH, output_dir), exist_ok=True)
    
    successful_downloads = 0

    # 2. Loop through all stations and date chunks
    for station_id in station_ids:
        station_data = []
        current_start = start_date
        
        # Get station name for file naming
        station_name = stations_info[stations_info['Stn Id'] == station_id]['Stn Name'].iloc[0]
        # Clean the name for use in a filename
        safe_name = "".join([c for c in station_name if c.isalpha() or c.isdigit() or c==' ']).rstrip().replace(' ', '_')
        output_filename = os.path.join(PATH, output_dir, f"{station_id}-{safe_name.replace("_", "").lower()}.csv")

        print(f"\nProcessing Station ID {station_id} ({station_name})...")
        
        # --- 2.1. Loop through the date range in chunks ---
        while current_start <= end_date:
            current_end = current_start + timedelta(days=CHUNK_DAYS - 1)
            if current_end > end_date:
                current_end = end_date
            
            df_chunk = fetch_cimis_data(station_id, current_start, current_end)
            if not df_chunk.empty:
                station_data.append(df_chunk)
            
            current_start = current_end + timedelta(days=1)

        # --- 2.2. Aggregate and save data for the current station ---
        if station_data:
            df_station = pd.concat(station_data).reset_index(drop=True)
            
            # Merge station metadata
            df_station = df_station.merge(
                stations_info[['Stn Id', 'Stn Name', 'Latitude', 'Longitude']],
                left_on=df_station['StationId'].astype(int), 
                right_on='Stn Id',
                how='left'
            ).drop(columns=['Stn Id'])
            
            # Sort by Date and Hour string
            df_station = df_station.sort_values(by=['Date', 'Hour']).reset_index(drop=True)

            # Reorder columns to match the requested format (metadata first, then alternating data/qc)
            base_cols = ['Date', 'Hour', 'StationId', 'Stn Name', 'Latitude', 'Longitude']
            data_qc_cols = []
            for item in DATA_ITEMS:
                data_qc_cols.append(item)
                data_qc_cols.append(item + '-qc')
            
            final_cols = base_cols + data_qc_cols
            df_station = df_station[final_cols]
            
            # Post process
            df_station = post_process(df_station)
            
            # Save the file
            df_station.to_csv(output_filename, index=False)
            print(f"  Saved {len(df_station)} hourly records to: {output_filename}")
            successful_downloads += 1
        else:
            print(f"  No data retrieved for station {station_id}.")


    print(f"\n--- SUCCESS ---")
    print(f"Completed processing. Successfully downloaded data for {successful_downloads} stations.")

if __name__ == "__main__":
    main()
