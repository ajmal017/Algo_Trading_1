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
buyTrigger = 0.004
sellTrigger = 0.004
takeProfit = 0.004
minDrop = 0.002
investment_amount = 5000
symbol = 'SPXL'

#logname = 'LOGQ-B{}-S{}-TP{}-{}-{}.csv'.format(int(buyTrigger * 10000), int(sellTrigger * 10000), int(takeProfit * 10000), symbol, date)
logname = 'LOGQ-B{}-S{}-TP{}-MD{}-{}.csv'.format(int(buyTrigger * 10000), int(sellTrigger * 10000), int(takeProfit * 10000), int(minDrop * 10000), symbol) 
#logname = 'LOGQ-B{}-S{}-MD{}-{}.csv'.format(int(buyTrigger * 10000), int(sellTrigger * 10000), int(minDrop * 10000), symbol) 

summary_df = pd.DataFrame(columns=['DATE', 'NUM_POSITIONS', 'RETURN', 'IND P/L', 'TOTAL P/L'])
totalProfit = 0
    
DATADIR = os.path.join('..', 'historical-market-data', symbol, 'test3')
log_file_name = os.path.join(DATADIR, logname)

data_file_name_list = glob.glob((DATADIR + '\\Q-' + symbol + '-*.csv'))

algo_start_time = '-10-01-00'        # utc
algo_end_time = '-15-56-00'        # utc

for data_file_name in data_file_name_list:
    # Dataframe to keep track of transactions
    # Creating an empty Dataframe with column names only
    trades_df = pd.DataFrame(columns=['POSITION #', 'TIME', 'ACTION', 'PRICE', 'RETURN', 'P/L'])
    
    
    date = data_file_name.replace(DATADIR + '\\Q-' + symbol + '-','')
    date = date.replace('.csv','')
    
    fname = 'DAYLOGQ-{}-{}.csv'.format(symbol, date)
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

    for trade in trade_day_data:

        timestamp = int(trade[1])
        
        cur_time = USTradingCalendar.unix_time_nanos_to_datetime(timestamp)
        
        if (algo_start_datetime > cur_time):
            #Continue til we reach the algo start time
            continue
        else:
            price = float(trade[9])
            break
    
    curHigh = price
    curLow = price
    maxDrop = 0
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
        
        price = float(trade[9])
        
        # End of day
        if (cur_time > algo_end_datetime):
            if inPosition:
            # Get out of any positions we are in at the end time
                inPosition = False
                percent_return = ((price - buyPrice) / buyPrice)
                curProfit = investment_amount * percent_return   
                profit += curProfit  
                curHigh = price
                curLow = price
                trades_df = trades_df.append({
                    'POSITION #': num_positions,
                    'TIME': cur_time.strftime('%Y-%m-%d-%H-%M-%S'),
                    'ACTION': 'SELL',
                    'PRICE': price,
                    'RETURN': percent_return,
                    'P/L': curProfit
                    }, ignore_index=True)
                break
        # Buy trigger
        if ((price - curLow) >= (curLow * buyTrigger)):
            # Check if the price actually dropped before hitting the current low
            if maxDrop >= (minDrop * curHigh):
                if not inPosition:
                    inPosition = True
                    buyPrice = price
                    num_positions += 1
                    curHigh = price
                    curLow = price
                    trades_df = trades_df.append({
                        'POSITION #': num_positions,
                        'TIME': cur_time.strftime('%Y-%m-%d-%H-%M-%S'),
                        'ACTION': 'BUY',
                        'PRICE': price,
                        'RETURN': float('nan'),
                        'P/L': float('nan')
                        }, ignore_index=True)
                
        #Take profit
        if (price - buyPrice) >= (buyPrice * takeProfit):
            if inPosition:
                inPosition = False
                percent_return = ((price - buyPrice) / buyPrice)
                curProfit = investment_amount * percent_return   
                profit += curProfit  
                curHigh = price
                curLow = price
                maxDrop = 0
                trades_df = trades_df.append({
                    'POSITION #': num_positions,
                    'TIME': cur_time.strftime('%Y-%m-%d-%H-%M-%S'),
                    'ACTION': 'SELL - TAKE PROFIT',
                    'PRICE': price,
                    'RETURN': percent_return,
                    'P/L': curProfit
                    }, ignore_index=True)
        
        # Stop loss/sell trigger
        if (curHigh - price) >= (curHigh * sellTrigger):
            if inPosition:
                inPosition = False
                percent_return = ((price - buyPrice) / buyPrice)
                curProfit = investment_amount * percent_return   
                profit += curProfit  
                curHigh = price
                curLow = price
                maxDrop = 0
                trades_df = trades_df.append({
                    'POSITION #': num_positions,
                    'TIME': cur_time.strftime('%Y-%m-%d-%H-%M-%S'),
                    'ACTION': 'SELL - STOP LOSS',
                    'PRICE': price,
                    'RETURN': percent_return,
                    'P/L': curProfit
                    }, ignore_index=True)
                    
        
        if price > curHigh:
            curHigh = price
            
        if (curHigh - price) > maxDrop:
            maxDrop = curHigh - price
            curLow = price
        
        if price < curLow:
            curLow = price
            
        lastTime = cur_time
        
    trades_df = trades_df.append({
        'POSITION #': '',
        'TIME': '',
        'ACTION': 'FINAL',
        'PRICE': '',
        'RETURN': profit / investment_amount,
        'P/L': profit
        }, ignore_index=True)    
    #trades_df.to_csv(log_name)
    
    totalProfit += profit
    summary_df = summary_df.append({
        'DATE': date,
        'NUM_POSITIONS': num_positions, 
        'RETURN': profit / investment_amount,
        'IND P/L': profit,
        'TOTAL P/L': totalProfit
        }, ignore_index=True)
    
    print('Date = {}, Profit = {}, Num Positions = {}'.format(date, profit, num_positions))
    #trades_df.to_csv(day_log_name)
    
print('Total Profit = {}'.format(totalProfit)) 
#summary_df.to_csv(log_file_name)   
    
    
    
    
    
    
    
    