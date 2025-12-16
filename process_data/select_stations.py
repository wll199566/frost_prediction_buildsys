"""
Select all the stations belonging to the central valley.
"""
import os

import numpy as np
import pandas as pd

from IPython.display import display

PATH = "./Data"

# Central Valley Counties list (including Sacramento Valley and San Joaquin Valley)
# This list is used to define the area of interest.
CENTRAL_VALLEY_COUNTIES = [
    'Butte', 'Colusa', 'Glenn', 'Fresno', 'Kern', 'Kings', 'Madera', 'Merced', 'Placer',
    'San Joaquin', 'Sacramento', 'Shasta', 'Stanislaus', 'Sutter', 'Tehama', 'Tulare', 'Yolo', 'Yuba'
]

def filter_central_valley_stations(csv_info):
    """
    Reads station data, filters for Central Valley counties, and returns the result.
    """
    try:
        # Load the CSV data into a pandas DataFrame using the provided string content
        df = pd.read_csv(csv_info)
        
        # Filter the DataFrame based on the list of Central Valley counties
        central_valley_df = df[df['County'].isin(CENTRAL_VALLEY_COUNTIES)].copy()
        central_valley_df = central_valley_df[central_valley_df['Active']==True]
        
        return central_valley_df

    except Exception as e:
        print(f"An error occurred during filtering: {e}")
        return pd.DataFrame()


def main():
    df_info = pd.read_csv(os.path.join(PATH, "all_stations_info.csv"))
    print(">> df_info")
    display(df_info.head(5))
    print(f">> Num of stations before filter-out: {len(df_info)}")
    print()

    # Run the filtering function
    df_info_cv = filter_central_valley_stations(os.path.join(PATH, "all_stations_info.csv"))
    
    # There are two locations which are not in the central valley, we just remove it
    coords_rm = [(41.063767, -121.45602), (35.659128, -117.636925)]
    for lat, lon in coords_rm:
        df_info_cv = df_info_cv.loc[(df_info_cv["Latitude"]!=lat) & (df_info_cv["Longitude"]!=lon)]
    print(">> df_info_cv")
    display(df_info_cv.head(5))
    print(f">> Num of stations after filter-out: {len(df_info_cv)}")

    # Write the data into csv file
    df_info_cv.to_csv(os.path.join(PATH, "cv_stations_info.csv"), index=False)
    print(f">>> Write results into {os.path.join(PATH, 'cv_stations_info.csv')}.")
    print(">>> Finish!")

if __name__ == "__main__":
    main()
