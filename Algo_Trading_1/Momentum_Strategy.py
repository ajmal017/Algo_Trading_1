'''
Created on May 2, 2020

@author: aac75
'''
import alpaca_trade_api as tradeapi
import alpaca_conf_paper
import pandas as pd
import asyncio#
import datetime
from alpaca_trade_api import StreamConn
import os
import logging
import sys
import json
import csv
import numpy

def ts():
    return pd.Timestamp.now()


def log(*args, **kwargs):
    print(ts(), " ", *args, **kwargs)
    
def debug(*args, **kwargs):
    print(ts(), " ", *args, file=sys.stderr, **kwargs)
    
def ms2date(ms, fmt='%Y-%m-%d'):
    if isinstance(ms, pd.Timestamp):
        return ms.strftime(fmt)
    else:
        return datetime.datetime.fromtimestamp(ms/1000).strftime(fmt)


async def on_quote(conn, channel, quote):
    
    ask = getattr(quote, "askprice")
    print(f'Quote ask: {ask}')
    time = getattr(quote, 'timestamp').strftime('%Y-%m-%d-%H-%M-%S')
    print(f'Timestamp: {time}')
    
if __name__ == '__main__':
    
    api = tradeapi.REST()
    
    
    ms_symbol = 'SPXS'
    ms_date = '2020-05-01'
    
    start_date_of_interest = datetime.datetime(year=2020, month=5, day=1, hour=13, minute=30)
    
    #1588339800134870000.00
    end_date_of_interest = datetime.datetime(year=2020, month=5, day=1, hour=20, minute=0)
    epoch = datetime.datetime.utcfromtimestamp(0)
    
    def unix_time_nanos(dt):
        return (dt-epoch).total_seconds() * 1000000000.0
    
    start_timestamp_of_interest = int(unix_time_nanos(start_date_of_interest))
    end_timestamp_of_interest = int(unix_time_nanos(end_date_of_interest))
    
    # Check if SPXS is tradable on the Alpaca platform.
    asset = api.get_asset(ms_symbol)
    if asset.tradable:
        print(f'We can trade {ms_symbol}.')
    
    DATADIR = os.path.join('..', 'historical-market-data') # download directory for the data
    fname_q = f'Q{ms_symbol}-{ms_date}.csv'  # for example, AAPL-2016-01-04.csv
    full_name_q = os.path.join(DATADIR, fname_q)
    fname_t = f'T{ms_symbol}-{ms_date}.csv'  # for example, AAPL-2016-01-04.csv
    full_name_t = os.path.join(DATADIR, fname_t)
    
    # create data directory if it doesn't exist
    if not os.path.exists(DATADIR):
        os.mkdir(DATADIR)
    
    if not os.path.exists(full_name_q):
        # data file does not exist, download
        quotes = api.polygon.historic_quotes_v2(symbol=ms_symbol, date=ms_date, timestamp=start_timestamp_of_interest, timestamp_limit=end_timestamp_of_interest, limit=10).df
        print (f'Number of quotes: {len(quotes)}')
        quotes.to_csv(full_name_q)

        
    if not os.path.exists(full_name_t):
        # data file does not exist, download
        trades = api.polygon.historic_trades_v2(symbol=ms_symbol, date=ms_date, timestamp=start_timestamp_of_interest, timestamp_limit=end_timestamp_of_interest, limit=10).df
        print (f'Number of trades: {len(trades)}')
        trades.to_csv(full_name_t)
            
    
    clock = api.get_clock()
    print('The market is {}'.format('open.' if clock.is_open else 'closed.'))
    
    #conn = StreamConn(
    #    key_id=os.environ.get("APCA_API_KEY_ID"),
    #    secret_key=os.environ.get("APCA_API_SECRET_KEY"),
    #    base_url=None,
    #    data_url=None,
    #    data_stream='polygon')
    
    #on_quote = conn.on(r'Q.*', {'SPY'})
    
    #try:
    #    conn.run(['Q.*'])
    #except Exception as e:
    #    print(e)