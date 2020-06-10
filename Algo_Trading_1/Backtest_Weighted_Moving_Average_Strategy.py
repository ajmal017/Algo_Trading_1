'''
Created on June 3, 2020

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
from WeightedDeque import WeightedDeque

def get_trade_day_data(csv_fname):
  with open(csv_fname, "r") as trade_records:
    for trade_record in csv.reader(trade_records):
      yield trade_record

      
LOCALTIME = pytz.timezone('US/Eastern')
      #
WMA_seconds = 60 * 2
D_MA_seconds = 12
weight_spread = 3
investment_amount = 5000
investment_increment = .05
investment_time = 4 * 60        # Time to fully invest/divest (seconds)
investment_time_inc = (investment_increment / investment_amount) * (investment_time)
symbol = 'SPXL'

logname = 'LOGWMA{}-INC{}-T{}.csv'.format(WMA_seconds, investment_increment, investment_time) 

summary_df = pd.DataFrame(columns=['DATE', 'NUM_POSITIONS', 'RETURN', 'IND P/L', 'TOTAL P/L'])
totalProfit = 0
    
DATADIR = os.path.join('..', 'historical-market-data', symbol, 'test3')
log_file_name = os.path.join(DATADIR, logname)

data_file_name_list = glob.glob((DATADIR + '\\ST-' + symbol + '-*.csv'))

algo_start_time = '-09-40-00'        # utc
algo_end_time = '-15-56-00'        # utc

weight_list = []

# The weights to apply to the previous quotes
for i in range(WMA_seconds + 1):
    weight_list.append((1 - weight_spread + ((i * weight_spread) / (WMA_seconds / 2))))
    

for data_file_name in data_file_name_list:
    # Dataframe to keep track of transactions
    # Creating an empty Dataframe with column names only
    trades_df = pd.DataFrame(columns=['POSITION #', 'TIME', 'ACTION', 'PRICE', 'AMOUNT INVESTED', 'SHARES', 'VALUE', 'P/L'])
    
    
    date = data_file_name.replace(DATADIR + '\\ST-' + symbol + '-','')
    date = date.replace('.csv','')
    
    fname = 'LOGDAY-{}-{}-WMA{}-INC{}-T{}.csv'.format(symbol, date, WMA_seconds, investment_increment, investment_time)
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
    WMA_calcs_started = False
    D_MA_calcs_started = False
    WMA = 0
    quote_data_queue = WeightedDeque(WMA_seconds + 1, weight_spread)
    quote_data_queue.assignWeights(weight_list)
    WMA_queue = Queue(maxsize = D_MA_seconds)
    D_MA = 0

     
    # Get to the start time for the algorithm and to where the moving average calculations can take place
    for trade in trade_day_data:
       
        timestamp = int(trade[1])
         
        cur_time = USTradingCalendar.unix_time_nanos_to_datetime(timestamp)
         
        # Check if past the algorithm start time
        if (not past_start_time and cur_time > algo_start_datetime):
            past_start_time = True        
         
        # Get weighted moving average calculations started
        if (not WMA_calcs_started):
            # If we haven't started, add data to the queue
            quote_data_queue.push(float(trade[13]))
             
            if (quote_data_queue.full()):
                # If we have filled the queue with data, then the moving average can be calculated
                WMA_calcs_started = True
                WMA = quote_data_queue.getWeightedAverage()
                WMA_queue.put(WMA)
         
        else:
            # The data queue is full, so we are calculating weighted moving averages
            # The weighted moving average is updated with every piece of data
            quote_data_queue.pop()                      # Remove old quote data
            quote_data_queue.push(float(trade[13]))     # Add new quote data
            WMA = quote_data_queue.getWeightedAverage() # Get new moving average
            
            WMA_queue.put(WMA)
                        
            if (WMA_queue.full()):
                D_MA_calcs_started = True
 
         
        if (past_start_time and D_MA_calcs_started):
            break
     
    profit = 0
    num_positions = 0
    cash = investment_amount      
    shares = 0
    lastTime = cur_time
    lastBuyTime = algo_start_datetime
    
    time_in_trend = 0       # The time (in seconds) that the derivative of WMA has been + or - consistently
    positiveTrend = False
     
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
        value = cash + (shares * price)
        
        # End of day
        if (cur_time > algo_end_datetime):
            if (shares > 0):
            # Get out of any positions we are in at the end time
                cash += shares * price
                shares = 0
                trades_df = trades_df.append({
                    'POSITION #': num_positions,
                    'TIME': cur_time.strftime('%Y-%m-%d-%H-%M-%S'),
                    'ACTION': 'SELL',
                    'PRICE': price,
                    'AMOUNT INVESTED': 0,
                    'SHARES': 0,
                    'VALUE': cash,
                    'P/L': value - (investment_amount)
                    }, ignore_index=True)
                break
             
        quote_data_queue.pop()                      # Remove old quote data
        quote_data_queue.push(price)                # Add new quote data
        WMA = quote_data_queue.getWeightedAverage()  # Get new moving average
        
        lastWMA = WMA_queue.get()
        D_MA = (WMA - lastWMA) / D_MA_seconds
         
        WMA_queue.put(WMA)
        
        if (D_MA >= 0):
            if (positiveTrend):
                time_in_trend += (cur_time - lastTime).total_seconds()
            else:
                positiveTrend = True
                time_in_trend = (cur_time - lastTime).total_seconds()
        else:
            if (not positiveTrend):
                time_in_trend += (cur_time - lastTime).total_seconds()
            else:
                positiveTrend = False
                time_in_trend = (cur_time - lastTime).total_seconds()
        

        increment = investment_increment * value       
        
        
        if (time_in_trend >= investment_time_inc):
            
            if (positiveTrend):
                time_in_trend = 0
                
                if (cash > 0):
                    
                    if (increment > cash):
                        increment = cash
                        
                    shares += increment / price
                    cash -= increment
                    num_positions += 1
                

                    trades_df = trades_df.append({
                        'POSITION #': num_positions,
                        'TIME': cur_time.strftime('%Y-%m-%d-%H-%M-%S'),
                        'ACTION': 'BUY',
                        'PRICE': price,
                        'AMOUNT INVESTED': (shares * price),
                        'SHARES': shares,
                        'VALUE': value,
                        'P/L': (value - investment_amount)
                        }, ignore_index=True)
            
            else:
                
                time_in_trend = 0
                
                if (shares > 0):
                    
                    if (increment > (shares * price)):
                        increment = shares * price
                        cash += increment
                        shares = 0
                    else:
                        cash += increment
                        shares -= increment / price
                    
                    num_positions += 1
                

                    trades_df = trades_df.append({
                         'POSITION #': num_positions,
                         'TIME': cur_time.strftime('%Y-%m-%d-%H-%M-%S'),
                         'ACTION': 'BUY',
                         'PRICE': price,
                         'AMOUNT INVESTED': (shares * price),
                         'SHARES': shares,
                         'VALUE': value,
                         'P/L': (value - investment_amount)
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
     
     
     
     
     
     
     
     
