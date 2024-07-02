# Import packages
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import os
import requests
import zipfile
import subprocess

# Create set of test CUSIPs
IDs = pd.read_csv("IDs.csv")

# Choose random set of 1000 CUSIPs with a key and save these as a new csv file
IDs = IDs.sample(n=1000, random_state=2024)
IDs.to_csv("RandomCUSIPs.csv", index=False)

# Run MakeIntra_Daily_v2_testing.py using the new set of CUSIPs
subprocess.run(["python", "MakeIntra_Daily_v2_testing.py"])

# Run CleanEnhanced.py using the same set of CUSIPs
subprocess.run(["python", "CleanEnhanced.py", "RandomCUSIPs.csv"])

# Read in the cleaned data
daily_cleaned_original = pd.read_csv('Prices.csv.gzip', compression='gzip')
daily_cleaned_new = pd.read_csv('Prices_daily.csv.gzip', compression='gzip')

# Merge
merged = pd.merge(daily_cleaned_original, daily_cleaned_new, left_on=['cusip_id', 'trd_exctn_dt'], right_on = ['cusip_id', 'trd_exctn_dtm'], how='outer', suffixes=('_original', '_new'))

# Sort by size of absolute difference in prices
merged['diff'] = abs(merged['prc_vw_original'] - merged['prc_vw_new'])
merged = merged.sort_values(by='diff', ascending=False)
max(merged['diff'])

