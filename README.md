# ❄️ F3 Frost Risk Forecasting Challenge (Team  PANPANPAN)

Welcome to the repository for Team PANPANPAN's submission to the F3 Frost Risk Forecasting Data Challenge hosted by the F3 Innovate and the National Data Platform. Our aim was to build a machine learning pipeline for forecasting frost (i.e., air temperature < 0°C) within next 3, 6, 12, 24 hours, using only information available up to the forecast time.

We include the code used in our experiments in this repository and show the steps to run them to get the results in this `README.md` file.

# Steps to run the code

### 1. Set the Python working path
```bash
# Export python path
vim ~/.bashrc
export PYTHONPATH=./
source ~/.bashrc
```
Before run this, please make sure to **git clone** this repository. Then make sure your current working directory is `./`, which is the project root directory.

### 2. Install the required libraries
```bash
conda create -n frost_forecast python=3.12
conda activate frost_forecast
pip install -r requirements.txt
```

### 3. Download and copy data files
```bash
# Make data folder and copy the data challenge data
mkdir Data
cp {your_path}/cimis_all_stations.csv ./Data

# Download the station information (latitude and longitude). 
# Note: you don't need to download it, the file is already provided by us in the folder ./Data.
echo {your_cimis_api_key} ./download/api_key.txt
python ./download/coords.py

# Select stations within central valley
python process_data/select_stations.py
```

### 4. Preprocess and data split
```bash
# Preprocess the data
python process_data/process_data.py

# Split the data into train and test stations by spatial locations (we have 3 splits here)
python process_data/split_data.py
```

### 5. Train deep learning models ###
```bash
# Train deep learning model for all splits
bash run_main_all_splits.sh
```

### 6. Evaluate deep learning models
```bash
# Evaluate trained models on internal and external test sets. 
# Note: please change the best model paths in run_eval_intext.sh to yours. 
# The best model is stored at ./results/models/{some_parameters}/best_model.pt 
# Uncomment lines for each split in the file to get the correponding evaluation result.
# Change PRED_LEN to 3, 6, 12, 24 to get the results for different prediction window length.
bash run_eval_intext.sh
```


# Preprocessing Steps
1. Split all the stations into train and test using **Maximum Dissimilarity Sampling (MDS)** and **K-Fold cross validation** (K=3). The ratio between train and test is 2:1.
2. Get the hour (0,...,23) and season ("spring", "summer", "fall", "winter") indicators for each hour.
3. Remove unqualified values and outliers for each. Set them as missing values indicated by `np.nan`.
4. Normalize each variable based on (mean, std) if their values are unbounded (e.g., air temperature, soil temperature, dew point, etc.) or (min, max) if their values are bounded (e.g., relative humidity (0~100%), wind direction (0-359)). 
5. Fill the missing values by linear interpolation.

# Modeling Strategy
**Baseline:** Mean Filling (fill the mean air temperature value in the context window).

**Our model:** Transformer-MLP (use Transformer as the encoder to encode the information in the context window, i.e., historical data, then use a two-layer MLP to decode and predict the air temperature in the next several hours).

# Data Challenge Report
Please refer to this [pdf file](./Data_Challenge_Report.pdf) for our report.

# Acknowledgments
This project was developed for the Frost Forecasting Data Challenge hosted by the F3 Innovate (F3i) and the National Data Platform (NDP).

Special thanks to **Mr. Ryan Dinubilo** for organizing this challenge and his helpful responses to our questions, and **Dr. Shijia Pan** for her invaluable supports to our team. Your support made a huge difference in helping our team resolve doubts and stay on track!


# Team PANPANPAN
* Hui Wei
* Dong Yoon Lee

# Contact
For questions, suggestions, or collaborations, feel free to open an issue or reach out via email **huiwei2@ucmerced.edu**.


