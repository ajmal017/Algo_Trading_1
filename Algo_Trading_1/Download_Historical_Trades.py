'''
Created on May 2, 2020

@author: aac75
'''
import alpaca_trade_api as tradeapi
import alpaca_conf_paper#
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
from USTradingCalendar import USTradingCalendar
    
# unix time 0    
epoch = datetime.datetime.utcfromtimestamp(0)

# Market open and close times in GMT (EST = GMT-4), not daylight savings time
OPEN_HR = 13
OPEN_MIN = 30
CLOSE_HR = 20
CLOSE_MIN = 0

# Limit on how many data points can be pulled from polygon in one go
PULL_LIMIT = 50000
    
if __name__ == '__main__':
    
    api = tradeapi.REST()
    
    DATADIR = os.path.join('..', 'historical-market-data') # download directory for the data
    SYMBOLS = ['SGMS']  # list of symbols we're interested
    FROM_DATE = '2019-01-01'
    TO_DATE = '2019-05-05'
    
    holidays = USTradingCalendar.get_trading_close_holidays(FROM_DATE,TO_DATE)
    

        
    date_range = pd.date_range(FROM_DATE, TO_DATE)
    
    for symbol in SYMBOLS:
        # create data directory if it doesn't exist
        SYM_DATADIR = os.path.join(DATADIR, symbol)
        if not os.path.exists(SYM_DATADIR):
            os.mkdir(SYM_DATADIR)
            
        for fromdate in date_range:
            
            
            _from = fromdate.strftime('%Y-%m-%d')
            _to = (fromdate + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            
            if (fromdate.dayofweek > 4):
                # it's a weekend
                #print('{} is on the weekend'.format(_from))
                continue
            elif (fromdate in holidays):
                # it's a market holiday
                #print('{} is a holiday'.format(_from))
                continue
            
            
            
            # Trades csv file
            fname = f'T-{symbol}-{_from}.csv'  # for example, T-AAPL-2016-01-04.csv
            full_name = os.path.join(SYM_DATADIR, fname)
           
            if os.path.exists(full_name):
                # data file already exists, not necessary to download
                continue
            
            open_hr = OPEN_HR
            open_min = OPEN_MIN
            close_hr = CLOSE_HR
            close_min = CLOSE_MIN
            
            if (not USTradingCalendar.is_dst(_from)):
                # If it is not daylight savings time, add an hour to all the times
                open_hr += 1
                close_hr += 1
            
            # MARKET OPEN
            start_date_of_interest = datetime.datetime(
                year=getattr(fromdate, 'year'),
                month=getattr(fromdate, 'month'),
                day=getattr(fromdate, 'day'), 
                hour=open_hr, 
                minute=open_min)
            
            start_timestamp_of_interest = int(USTradingCalendar.unix_time_nanos(start_date_of_interest))
            
            # 4:00 pm market close
            end_date_of_interest = datetime.datetime(
                year=getattr(fromdate, 'year'),
                month=getattr(fromdate, 'month'),
                day=getattr(fromdate, 'day'), 
                hour=close_hr, 
                minute=close_min)
            
            end_timestamp_of_interest = int(USTradingCalendar.unix_time_nanos(end_date_of_interest))            
            
            trades = api.polygon.historic_trades_v2(
                symbol=symbol,
                date=_from,
                timestamp=start_timestamp_of_interest,
                timestamp_limit=end_timestamp_of_interest).df
                
            pull_length = len(trades)   # The number of data points we got

            while True:        
                lastTimestamp = trades.iloc[len(trades)-1,0]     
                if (pull_length < PULL_LIMIT):
                    # If we pulled less than the limit, we have probably got all we can
                    break
    
                # If we couldn't get data for the entire day (50,000 limit), reset the start timestamp variable 
                # to just past the last one we were able to get in preparation for another pull of data
                start_timestamp_of_interest = lastTimestamp + 1000 
    
                tradesappend = api.polygon.historic_trades_v2(
                    symbol=symbol,
                    date=_from,
                    timestamp=start_timestamp_of_interest,
                    timestamp_limit=end_timestamp_of_interest).df
                
                pull_length = len(tradesappend)
                
                trades = trades.append(tradesappend) 
                
            trades.to_csv(full_name)  