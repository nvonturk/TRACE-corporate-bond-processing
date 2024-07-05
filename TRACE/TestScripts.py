# Import packages
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import os
import requests
import zipfile
import subprocess
import wrds
import CleanStandard

## Test 1: Standard Trace

db = wrds.Connection()

# Sample of data
trace_standard_raw = db.raw_sql("SELECT * FROM trace_standard.trace WHERE ((trd_exctn_dt >= '2010-01-01' AND trd_exctn_dt <= '2010-01-10') OR (trd_exctn_dt >= '2004-09-15' AND trd_exctn_dt <= '2004-09-30') OR (trd_exctn_dt >= '2015-07-10' AND trd_exctn_dt <= '2015-07-20'))")

# Clean using Python function
trace_standard_cleaned_new = CleanStandard.clean_trace_data(trace_standard_raw)

# Read in output from original SAS code
dtypes = trace_standard_cleaned_new.drop(columns="trd_exctn_dt").dtypes.to_dict()

trace_standard_cleaned_original = pd.read_csv('trace_standard_clean_sample.csv', dtype = dtypes, parse_dates=['trd_exctn_dt'])

# Convert trd_exctn_tm to datetime.time
trace_standard_cleaned_original['trd_exctn_tm'] = pd.to_datetime(trace_standard_cleaned_original['trd_exctn_tm'], format='%H:%M:%S').dt.time

# Convert orig_dis_dt to datetime.date
trace_standard_cleaned_original['orig_dis_dt'] = pd.to_datetime(trace_standard_cleaned_original['orig_dis_dt'], format='%Y%m%d').dt.date

# Merge the two dataframes
merged = pd.merge(trace_standard_cleaned_original, trace_standard_cleaned_new, on=['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb'], how='outer', suffixes=('_original', '_new'))

# Check all columns are equal except the merged columnns
check_cols = trace_standard_cleaned_original.columns.drop(['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb'])

for cname in check_cols:
    mask = ~((merged[cname + '_original'].isna() | merged[cname + '_original'].apply(lambda x: x == "None")) & (merged[cname + '_new'].isna() | merged[cname + '_new'].apply(lambda x: x == "None")))
    if cname in ["entrd_vol_qt"]:
        max_error = merged.loc[mask, cname + '_new'] - merged.loc[mask, cname + '_original']
        # Check if the maximum error is greater than 1 as the SAS to CSV output seemed to not preserve after the decimal values (though not clear why the entrd_vol_qt values have fractions)
        if max_error.max() > 1:
            raise ValueError(f"Column {cname} is not equal")
    elif not merged.loc[mask, cname + '_original'].equals(merged.loc[mask, cname + '_new']):
        raise ValueError(f"Column {cname} is not equal")


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


