from multiprocessing.pool import ThreadPool
from polygon.rest.client import RESTClient

import pandas as pd
from datetime import timedelta, datetime

import os

# Get the number of CPU cores
num_cores = os.cpu_count()

# Use 75% of the CPU cores
num_threads = int(num_cores * 0.75)

polyKey = "FILL IN YOUR KEY HERE"

# function to fetch data from polygon
def download_aggregates(current_date):
    try:
        resp = RESTClient(polyKey).stocks_equities_grouped_daily(locale='us', market='stocks', date=current_date.strftime("%Y-%m-%d"))
        return pd.DataFrame(resp.results)
        print('Processing ', current_date.strftime("%Y-%m-%d"))
    except:
        print('Error processing ', current_date.strftime("%Y-%m-%d"))

# define the date range for which you would like to pull the data
start_date = pd.to_datetime('2023-02-01')
end_date = pd.to_datetime('2023-02-28')

args = [(start_date + timedelta(days=x),) for x in range(0, (end_date-start_date).days + 1)]

# time stamp the start of the process
print("Current Time =", datetime.now().strftime("%H:%M:%S"))

with ThreadPool(200) as pool:
    data = pool.starmap(download_aggregates, args)

# time stamp when the process ends
print("Current Time =", datetime.now().strftime("%H:%M:%S"))

# concat all data into one single dataframe
df = pd.concat([x for x in data if x is not None])
