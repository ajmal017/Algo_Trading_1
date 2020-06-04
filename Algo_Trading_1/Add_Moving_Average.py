'''
Created on May 6, 2020

@author: aac75
'''
import pandas as pd
import json
import csv
import logging
import datetime
import os
from USTradingCalendar import USTradingCalendar
import pytz
import glob

def get_trade_day_data(csv_fname):
  with open(csv_fname, "r") as trade_records:
    for trade_record in csv.reader(trade_records):
      yield trade_record

      
LOCALTIME = pytz.timezone('US/Eastern')
 #     
symbol = 'TSLA'
    
DATADIR = os.path.join('..', 'historical-market-data', symbol, 'test1')

data_file_name_list = glob.glob((DATADIR + '\\Q-' + symbol + '-*.csv'))

for data_file_name in data_file_name_list:    
    
    test_data_file_name = data_file_name.replace('Q-', 'ST-')
    
    date = data_file_name.replace(DATADIR + '\\Q-' + symbol + '-','')
    date = date.replace('.csv','')
    
    if os.path.exists(test_data_file_name):
        # data file already exists, not necessary to download
        continue

    trade_day_data = get_trade_day_data(data_file_name)
    
    with open(test_data_file_name, 'w', newline='') as outFile:
        writer = csv.writer(outFile)
        
        header = next(trade_day_data)
        header.append('SECOND_AVERAGES')
        
        writer.writerow(header)
        
        
        average_ask_second = 0
        sum_asks = 0
        num_quotes = 0
        last_second = 0
        
        first = True
        
        for trade in trade_day_data:
            timestamp = int(trade[1])
            
            cur_time = USTradingCalendar.unix_time_nanos_to_datetime(timestamp)

            seconds = getattr(cur_time, 'second')
            if (seconds == last_second or first):
                num_quotes += 1
                sum_asks += float(trade[9])
                first = False
            else:
                average_ask_second = sum_asks / num_quotes
                num_quotes = 1
                sum_asks = float(trade[9])
                last_second = seconds
                trade.append(average_ask_second) 
                writer.writerow(trade)
            
               
            
                
                

        
            
        
        #for trade in trade_day_data:
            
    
 
    
    
    
    
    
    
    
    