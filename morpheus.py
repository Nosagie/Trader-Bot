#Morpheus watches over the earth, from the matrix 
#In charge of Populating Database for analytics

from databaseWrapper import DatabaseWrapper
from binanceApiWrapper import BinanceApiWrapper
import datetime,time
from functools import reduce
import multiprocessing

DEFAULT_PAIRS = [('BTC','USDT'),('ETH','USDT'),('ADA','USDT'),('LTC','USDT')]
DEFAULT_INTERVALS = ['1m','3m','5m','15m','30m','1h']
INTERVAL_SECONDS = {'1m':60,'3m':180,'5m':300,'15m':900,'30m':1800,'1h':3600}

class Morpheus:
    def __init__(self,pairs_to_trade=DEFAULT_PAIRS,intervals=DEFAULT_INTERVALS,interval_seconds=INTERVAL_SECONDS):
        self.__database = DatabaseWrapper()
        self.__binance  = BinanceApiWrapper() 
        self.INTERVALS = intervals
        self.INTERVAL_SECONDS = interval_seconds 
        self.TRADING_PAIRS = pairs_to_trade #list of tuples,base,quote 

    def get_interval_seconds(self,interval):
        if interval not in INTERVAL_SECONDS.keys():
            return None
        else:
            return INTERVAL_SECONDS[interval]

    def get_database(self):
        return self.__database
    
    def get_binance(self):
        return self.__binance

    def get_historical_prices(self,most_recent,end_msecs,base_asset,quote_asset,period_interval,interval_msecs):
        count_api_calls = 0
        while (int(end_msecs)-int(most_recent)) >= interval_msecs:
            hist_data = [self.__binance.get_kline_data(base_asset,
                        quote_asset,period_interval,most_recent,end_msecs)]
            for d in hist_data:
                self.__database.insert_market_data(base_asset,quote_asset,d)
            most_recent = self.__database.get_most_recent_pair_period_close(base_asset,quote_asset,period_interval)
            count_api_calls += 1
        return 


    # gets kline data from Binance
    def get_historical_data(self,start_date=None,end_date=datetime.datetime.now(),live_feed=False):
        for base_asset,quote_asset in self.TRADING_PAIRS:
            #convert datetime to msecs
            most_recent = self.__database.get_most_recent_pair_period_close(base_asset,quote_asset) 
            if most_recent is None:
                print ("Pair does not exist")
                import sys;sys.exit(1)
            if most_recent is None:
                most_recent = int(start_date.timestamp() * 1000)
            api_start_track = int(datetime.datetime.now().timestamp() * 1000)
            end_msecs = api_start_track if end_date is None else int(end_date.timestamp() * 1000)
            count_api_calls = 0 #LIMIT IS 20 per second
            while int(most_recent) < (int(end_msecs)):
                hist_data = [self.__binance.get_kline_data(base_asset,
                        quote_asset,interv,most_recent,end_msecs) 
                        for interv in self.INTERVALS]
                write_status = [self.__database.insert_market_data(base_asset,quote_asset,d)
                        for d in hist_data]
                count_api_calls += 1
                print (str(count_api_calls) + " calls made")
                print (base_asset+quote_asset + " write: " + str(reduce(lambda s,y:s and y,write_status)))
                now_msecs = int(datetime.datetime.now().timestamp()*1000)
                if count_api_calls >= 1100 and (now_msecs-api_start_track) <= 60000: #1minute check
                    print ("Sleeping for 1 minute, api limits")
                    time.sleep(60)
                    count_api_calls = 0
                api_start_track = int(datetime.datetime.now().timestamp()*1000) 
                most_recent = self.__database.get_most_recent_pair_period_close(base_asset,quote_asset) 
                time_ = most_recent / 1000
                time_ = datetime.datetime.fromtimestamp(time_).strftime('%Y-%m-%d %H:%M:%S')
                end_time_ = end_msecs / 1000
                end_time_ = datetime.datetime.fromtimestamp(end_time_).strftime('%Y-%m-%d %H:%M:%S')
                print (time_)
                print (end_time_)
            print ("written for " + base_asset + " " + quote_asset + 
                    " end: " + str(most_recent))
        print ("Data Fetched and Stored")
        return 

    # def update_prices()
                


    
    
            