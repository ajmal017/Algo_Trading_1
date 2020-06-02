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
from queue import Queue

def get_trade_day_data(csv_fname):
  with open(csv_fname, "r") as trade_records:
    for trade_record in csv.reader(trade_records):
      yield trade_record
      
LOCALTIME = pytz.timezone('US/Eastern')
      #
MA_seconds = 60 * 1
D_MA_seconds = 12
investment_amount = 5000
symbol = 'SPXL'

logname = 'LOGMA-MA{}-DMA{}.csv'.format(MA_seconds, D_MA_seconds, symbol) 

summary_df = pd.DataFrame(columns=['DATE', 'NUM_POSITIONS', 'RETURN', 'IND P/L', 'TOTAL P/L'])
totalProfit = 0
    
DATADIR = os.path.join('..', 'historical-market-data', symbol, 'test3')
log_file_name = os.path.join(DATADIR, logname)

data_file_name_list = glob.glob((DATADIR + '\\ST-' + symbol + '-*.csv'))

algo_start_time = '-09-31-00'        # utc
algo_end_time = '-15-56-00'        # utc

for data_file_name in data_file_name_list:
    # Dataframe to keep track of transactions
    # Creating an empty Dataframe with column names only
    trades_df = pd.DataFrame(columns=['POSITION #', 'TIME', 'ACTION', 'PRICE', 'RETURN', 'P/L'])
    
    
    date = data_file_name.replace(DATADIR + '\\ST-' + symbol + '-','')
    date = date.replace('.csv','')
    
    fname = 'DAYLOGMA-{}-{}.csv'.format(symbol, date)
    day_log_name = os.path.join(DATADIR, fname)

    trade_day_data = get_trade_day_data(data_file_name)
    next(trade_day_data)
    
    # Time to start the algorithm
    algo_start_date_str = date + algo_start_time
    algo_start_datetime = pd.to_datetime(algo_start_date_str, format='%Y-%m-%d-%H-%M-%S')
    algo_start_datetime = LOCALTIME.localize(algo_start_datetime).to_pydatetime()
    
    algo_end_date_str = date + algo_end_time
    algo_end_datetime = pd.to_datetime(algo_end_date_str, format='%Y-%m-%d-%H-%M-%S')
    algo_end_datetime = LOCALTIME.localize(algo_end_datetime).to_pydatetime()

    # Counter of what quote we are on
    quote_num = 0
    past_start_time = False
    MA_calcs_started = False
    D_MA_calcs_started = False
    MA = 0
    quote_data_queue = Queue(maxsize = MA_seconds)
    MA_queue = Queue(maxsize = D_MA_seconds)
    D_MA = 0
    
    
    # Get to the start time for the algorithm and to where the moving average calculations can take place
    for trade in trade_day_data:
      
        timestamp = int(trade[1])
        
        cur_time = USTradingCalendar.unix_time_nanos_to_datetime(timestamp)
        
        # Check if past the algorithm start time
        if (not past_start_time and cur_time > algo_start_datetime):
            past_start_time = True        
        
        # Get moving average calculations started
        if (not MA_calcs_started):
            # If we haven't started, add data to the queue
            MA = MA + float(trade[13])
            quote_data_queue.put(float(trade[13]))
            
            if (quote_data_queue.full()):
                # If we have filled the queue with data, then the moving average can be calculated
                MA_calcs_started = True
                MA = MA / MA_seconds
                MA_queue.put(MA)
        
        else:
            # The data queue is full, so we are calculating moving averages
            # The moving average is updated with every piece of data
            MA = MA + ((float(trade[13]) - quote_data_queue.get()) / MA_seconds)
            quote_data_queue.put(float(trade[13]))
            
            # If we have started calculating the derivative of the moving average, then we need to dequeue the moving average queue before we enqueue the newest moving average since the queue is full
            if (D_MA_calcs_started):
                D_MA = (MA - MA_queue.get()) / D_MA_seconds
            
            MA_queue.put(MA)
            
            if (MA_queue.full()):
                # This will be executed repetitively, but it doesn't matter
                D_MA_calcs_started = True

        
        if (past_start_time and D_MA_calcs_started):
            break
    
#     curHigh = price
#     curLow = price
#     maxDrop = 0
    profit = 0
    num_positions = 0
    buyPrice = 0
    lastTime = cur_time
      
    inPosition = False
    
    for trade in trade_day_data:
  
        timestamp = int(trade[1])
          
        cur_time = USTradingCalendar.unix_time_nanos_to_datetime(timestamp)
          
        #if (cur_time < lastTime):
            # Not sure why data goes back in time randomly, but skip it if it does
        #    print('back to the future')
        #    continue
#         
        price = float(trade[13])
#         
        # End of day
        if (cur_time > algo_end_datetime):
            if inPosition:
            # Get out of any positions we are in at the end time
                inPosition = False
                percent_return = ((price - buyPrice) / buyPrice)
                curProfit = investment_amount * percent_return   
                profit += curProfit  
                trades_df = trades_df.append({
                    'POSITION #': num_positions,
                    'TIME': cur_time.strftime('%Y-%m-%d-%H-%M-%S'),
                    'ACTION': 'SELL',
                    'PRICE': price,
                    'RETURN': percent_return,
                    'P/L': curProfit
                    }, ignore_index=True)
                break
            
        MA = MA + ((float(trade[13]) - quote_data_queue.get()) / MA_seconds)
        quote_data_queue.put(float(trade[13]))
            
        D_MA = (MA - MA_queue.get()) / D_MA_seconds
            
        MA_queue.put(MA)
        
        if (not inPosition):
            if (D_MA > 0):
                inPosition = True
                buyPrice = price
                num_positions += 1
                trades_df = trades_df.append({
                         'POSITION #': num_positions,
                         'TIME': cur_time.strftime('%Y-%m-%d-%H-%M-%S'),
                         'ACTION': 'BUY',
                         'PRICE': price,
                         'RETURN': float('nan'),
                         'P/L': float('nan')
                         }, ignore_index=True)
        else:
            if (D_MA < 0):
                inPosition = False
                percent_return = ((price - buyPrice) / buyPrice)
                curProfit = investment_amount * percent_return   
                profit += curProfit  
                trades_df = trades_df.append({
                    'POSITION #': num_positions,
                    'TIME': cur_time.strftime('%Y-%m-%d-%H-%M-%S'),
                    'ACTION': 'SELL',
                    'PRICE': price,
                    'RETURN': percent_return,
                    'P/L': curProfit
                    }, ignore_index=True)
         
    trades_df = trades_df.append({
        'POSITION #': '',
        'TIME': '',
        'ACTION': 'FINAL',
        'PRICE': '',
        'RETURN': profit / investment_amount,
        'P/L': profit
        }, ignore_index=True)    
    trades_df.to_csv(day_log_name)
     
    totalProfit += profit
    summary_df = summary_df.append({
        'DATE': date,
        'NUM_POSITIONS': num_positions, 
        'RETURN': profit / investment_amount,
        'IND P/L': profit,
        'TOTAL P/L': totalProfit
        }, ignore_index=True)
     
    print('Date = {}, Profit = {}, Num Positions = {}'.format(date, profit, num_positions))
     
print('Total Profit = {}'.format(totalProfit)) 
summary_df.to_csv(log_file_name)   
    
    
    
    
    
    
    
    