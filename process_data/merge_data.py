"""
Merge the data from the data challenge with downloaded data.
"""
import os
import pandas as pd
import glob
import shutil
from utils.others import new_dir

OUT_DIR = os.path.join("Data", "cimis_hourly_station_data")

def remove_duplicate(download_path:str, challenge_path:str):
    """
    Remove the duplicate from downloaded data and challenge data
    and merge them into a single directory.
    """
    # Get all the data filenames from downloaded data and challenge data
    download_files = glob.glob(os.path.join("Data", download_path, "**.csv"))
    challenge_files = glob.glob(os.path.join("Data", challenge_path, "**.csv"))
    # download_files = [os.path.join(os.path.dirname(file), 
    #                                os.path.basename(file).replace("_", "").lower())
    #                   for file in download_files]

    # print(challenge_files)
    # print(download_files)
    # all_files = [] # files with merged data
    # all_files.extend(challenge_files)

    challenge_sids = [os.path.basename(file).split('-')[0].strip() for file in challenge_files]
    # print(challenge_sids)
     
    for file in (download_files + challenge_files):
        if file in challenge_files:
            shutil.copy2(file, OUT_DIR)
            print(f"File '{file}' copied to '{OUT_DIR}' with metadata preserved.")
        else:
            if os.path.basename(file).split('-')[0].strip() in challenge_sids:
                continue
            else:
                shutil.copy2(file, os.path.join(OUT_DIR, os.path.basename(file).replace("_", "").lower()))
                print(f">>> File '{file}' copied to '{OUT_DIR}' with metadata preserved.")

def process_dataframe(filenames):
    '''Change the column names and drop some columns from data challenge files'''
    for file in filenames:
        df = pd.read_csv(file, low_memory=False)
        if "Hour" in df.columns:
            # from downloaded data
            df = df.rename(columns={"Hour":"Hour (PST)"})
            
        if "CIMIS Region" in df.columns:
            # from data challenge data
            df = df.drop(columns=['CIMIS Region', 'Jul'])
            df = df.rename(columns={'qc':'eto-qc', 'qc.1':'precip-qc', 'qc.2':'solrad-qc',
                               'qc.3':'vappres-qc', 'qc.4':'airtmp-qc', 'qc.5':'relhum-qc',
                               'qc.6':'dewpnt-qc', 'qc.7':'windspd-qc', 'qc.8':'winddir-qc',
                               'qc.9':'soiltmp-qc'})
            df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')

        reorder_names = ['Stn Id', 'Stn Name', 'Date', 'Hour (PST)', 'ETo (mm)', 'eto-qc',
                        'Precip (mm)', 'precip-qc', 'Sol Rad (W/sq.m)', 'solrad-qc',
                        'Vap Pres (kPa)', 'vappres-qc', 'Air Temp (C)', 'airtmp-qc',
                        'Rel Hum (%)', 'relhum-qc', 'Dew Point (C)', 'dewpnt-qc',
                        'Wind Speed (m/s)', 'windspd-qc', 'Wind Dir (0-360)', 'winddir-qc',
                        'Soil Temp (C)', 'soiltmp-qc']
        df = df[reorder_names]

        df.to_csv(file, index=False)
        print(f">>> File {file} is processed.")

def main():
    new_dir(OUT_DIR)
    download_path = "cimis_hourly_data_hui"
    challenge_path = "cimis_stations_data_challenge"
    remove_duplicate(download_path, challenge_path)
    filenames = glob.glob(os.path.join(OUT_DIR, "**.csv"))
    process_dataframe(filenames)
    print()
    print('-' * 50)
    print(">>> Finish!")
    
if __name__ == "__main__":
    main()
    