import os
import requests
import pandas as pd
from utils.others import new_dir

# Your CIMIS API key (you can get one for free after registration)
file_path = './download/api_key.txt'

with open(file_path, 'r') as file:
    api_key = file.read().strip()

url = f"http://et.water.ca.gov/api/station"
response = requests.get(url)
data = response.json()

df_stations = pd.DataFrame([
    {
        'Stn Id': s['StationNbr'],
        'Stn Name': s['Name'],
        'Latitude': s['HmsLatitude'],
        'Longitude': s['HmsLongitude'],
        'Elevation (m)': s['Elevation'],
        'Ground Cover': s['GroundCover'],
        'County': s['County'],
        'Active': s['IsActive']
    }
    for s in data['Stations']
])

df_stations["Latitude"] = df_stations["Latitude"].str.split("/").str[-1].str.strip().astype(float)
df_stations["Longitude"] = df_stations["Longitude"].str.split("/").str[-1].str.strip().astype(float)
df_stations["Elevation (m)"] = df_stations["Elevation (m)"].astype(float)

OUT_DIR = "./Data"
new_dir(OUT_DIR)
df_stations.to_csv(os.path.join(OUT_DIR, "all_stations_info.csv"), 
                   index=False)
print(">>> Finish!")
