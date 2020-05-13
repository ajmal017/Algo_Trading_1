import pandas as pd
import pytz
import datetime

from pandas.tseries.holiday import (AbstractHolidayCalendar, Holiday, nearest_workday, 
    USMartinLutherKingJr, USPresidentsDay, GoodFriday, USMemorialDay,
    USLaborDay, USThanksgivingDay)


class USTradingCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('NewYearsDay', month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday('USIndependenceDay', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday)
    ]
    
    LOCALTIME = pytz.timezone('US/Eastern')
    # unix time 0    
    epoch = datetime.datetime.utcfromtimestamp(0)

    # startdate, enddate format is 'YYYY-MM-DD'
    @staticmethod
    def get_trading_close_holidays(startdate: str, enddate: str):
        inst = USTradingCalendar()
    
        return inst.holidays(
            pd.to_datetime(startdate,format='%Y-%m-%d'),
            pd.to_datetime(enddate,format='%Y-%m-%d'))
        
    # date format is 'YYYY-MM-DD'
    @staticmethod
    def is_dst(date: str):
    
        _date = pd.to_datetime(date,format='%Y-%m-%d')
        
        _date = USTradingCalendar.LOCALTIME.localize(_date)
        
        return bool(_date.dst())
    
    # Convert datetime object to unix epoch nanoseconds
    # @param dt        The datetime object to convert
    # @return          Unix epoch nanoseconds form (as float)
    @staticmethod
    def unix_time_nanos(dt):
        return (dt-USTradingCalendar.epoch).total_seconds() * 1000000000.0
    
    # Convert unix epoch nanoseconds to datetime object
    # @param timestamp     The unix epoch nanoseconds to convert
    # @return              Equivalent datetime object
    @staticmethod
    def unix_time_nanos_to_datetime(unix_time):

        return datetime.datetime.fromtimestamp((unix_time / 1000000000.0), USTradingCalendar.LOCALTIME)
