'''
Created on May 4, 2020

@author: aac75
'''

import alpaca_trade_api as tradeapi
import alpaca_conf_paper
import pandas as pd
import asyncio
import datetime
from alpaca_trade_api import StreamConn
import os
import logging
import sys
import json
import csv
import numpy
from tqdm import tqdm

epoch = datetime.datetime.utcfromtimestamp(0)
# Convert datetime object to unix epoch nanoseconds
# @param dt        The datetime object to convert
# @return          Unix epoch nanoseconds form (as float)
def unix_time_nanos(dt):
    return (dt-epoch).total_seconds() * 1000000000.0

api = tradeapi.REST();

FROM_DATE = '2019-04-01'
TO_DATE = '2020-05-01'

date_range = pd.date_range(FROM_DATE, TO_DATE)

fromdate = date_range[0]

_from = fromdate.strftime('%Y-%m-%d')
_to = (fromdate + pd.Timedelta(days=1)).strftime('%Y-%m-%d')

symbol = 'SPXL'
DATADIR = os.path.join('..', 'historical-market-data') # download directory for the data
            
# Trades csv file
fname = f'T-{symbol}-{_from}.csv'  # for example, T-AAPL-2016-01-04.csv
full_name = os.path.join(DATADIR, fname)

if os.path.exists(full_name):
    # data file already exists, not necessary to download
    print('Already have file')
    sys.exit()
            
# 9:30 am market open
start_date_of_interest = datetime.datetime(
    year=getattr(fromdate, 'year'),
    month=getattr(fromdate, 'month'),
    day=getattr(fromdate, 'day'), 
    hour=13, 
    minute=30)

start_timestamp_of_interest = int(unix_time_nanos(start_date_of_interest))

# 4:00 pm market close
end_date_of_interest = datetime.datetime(
    year=getattr(fromdate, 'year'),
    month=getattr(fromdate, 'month'),
    day=getattr(fromdate, 'day'), 
    hour=20, 
    minute=0)

end_timestamp_of_interest = int(unix_time_nanos(end_date_of_interest))

# 3:59 pm (used to make sure last pulled piece of data is greater than this)
endcompare_date_of_interest = datetime.datetime(
    year=getattr(fromdate, 'year'),
    month=getattr(fromdate, 'month'),
    day=getattr(fromdate, 'day'), 
    hour=19, 
    minute=59)

endcompare_timestamp_of_interest = int(unix_time_nanos(endcompare_date_of_interest))


trades = api.polygon.historic_trades_v2(
        symbol=symbol,
        date=_from,
        timestamp=start_timestamp_of_interest,
        timestamp_limit=end_timestamp_of_interest).df

while True:        
    lastTimestamp = trades.iloc[len(trades)-1,0]     
    if (lastTimestamp > endcompare_timestamp_of_interest):
        # If the last timestamp we just got is past 3:59 pm, we are done
        break
    
    # If we couldn't get data for the entire day (50,000 limit), reset the start timestamp variable 
    # to just past thelast one we were able to get in preparation for another pull of data
    start_timestamp_of_interest = lastTimestamp + 1000 
    
    tradesappend = api.polygon.historic_trades_v2(
        symbol=symbol,
        date=_from,
        timestamp=start_timestamp_of_interest,
        timestamp_limit=end_timestamp_of_interest).df
        
    trades = trades.append(tradesappend) 
  
    
trades.to_csv(full_name)  