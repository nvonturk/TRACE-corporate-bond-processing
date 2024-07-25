
# Import packages
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import wrds
import matplotlib.pyplot as plt

# Connect to WRDS
db = wrds.Connection()

# Load CUSIPs for analysis
IDs = pd.read_csv('IDs.csv')

# Load FISD metadata for the CUSIPs
fisd_issue = db.raw_sql("""SELECT complete_cusip, issue_id,
                  issuer_id, foreign_currency,
                  coupon_type,coupon,convertible,
                  asset_backed,rule_144a,
                  bond_type,private_placement,
                  interest_frequency,dated_date,
                  day_count_basis,offering_date                 
                  FROM fisd.fisd_mergedissue  
                  """)

# Convert offering_date to datetime
fisd_issue['offering_date'] = pd.to_datetime(fisd_issue['offering_date'])

# Merge FISD metadata into the CUSIP sample
sample = pd.merge(IDs, fisd_issue, on = ['complete_cusip'], how = "left")

# Load traded price data after cleaning
trace_daily = pd.read_csv('Prices_daily.csv.gzip', compression='gzip')

# For each CUSIP, compute the first day and last day where it was traded
cusip_dates = trace_daily.groupby('cusip_id').agg({'trd_exctn_dtm': ['min', 'max']})
cusip_dates.columns = ['_'.join(col).strip() for col in cusip_dates.columns.values]

# Merge these dates into the sample
sample = pd.merge(sample, cusip_dates, left_on = 'complete_cusip', right_on = 'cusip_id', how = "left")

# Flag if traded after offering date
sample['traded_after_offering'] = sample['trd_exctn_dtm_min'] >= sample['offering_date']

# Create year-month datetime variable of offering date
sample['offering_ym'] = pd.to_datetime(sample['offering_date']).apply(lambda x: x.replace(day=1))

# Group by offering_ym and count the share of CUSIPs that have a non-null trd_exctn_dtm_min
sample = sample.groupby('offering_ym').agg({'traded_after_offering': 'sum', 'complete_cusip' : 'count'}).reset_index()

# Rolling sums of traded_after_offering and complete cusip
sample['traded_after_offering_rolling'] = sample['traded_after_offering'].expanding().sum()
sample['complete_cusip_rolling'] = sample['complete_cusip'].expanding().sum()


