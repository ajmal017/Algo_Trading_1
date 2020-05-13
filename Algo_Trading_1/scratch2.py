'''
Created on May 5, 2020

@author: aac75
'''


from USTradingCalendar import USTradingCalendar
import pandas as pd
import pytz

import alpaca_trade_api as tradeapi
import alpaca_conf_paper

print(USTradingCalendar.get_trading_close_holidays('2020-01-01', '2020-12-31'))

FROM_DATE = '2019-11-02'
TO_DATE = '2019-11-04'

fromdate = pd.to_datetime(FROM_DATE,format='%Y-%m-%d')
todate = pd.to_datetime(TO_DATE,format='%Y-%m-%d')

localtime = pytz.timezone('US/Eastern')

fromdate = localtime.localize(fromdate)
todate = localtime.localize(todate)

print(bool(fromdate.dst()))
print(bool(todate.dst()))

print(USTradingCalendar.is_dst(FROM_DATE))
print(USTradingCalendar.is_dst(TO_DATE))

start_timestamp_of_interest = 1572382352781675000
end_timestamp_of_interest = 1572382800000000000
symbol = 'SPXL'
_from = '2019-10-29'

print((USTradingCalendar.unix_time_nanos_to_datetime(end_timestamp_of_interest)).strftime('%Y-%m-%d %H:%M:%S'))