from multiprocessing import Pool 
from databaseWrapper import DatabaseWrapper
from morpheus import Morpheus
import time,datetime


#initialize
morph_client = Morpheus()

def f(x):
    return x*x

def get_prices(base_asset,quote_asset,interval):
    global morph_client
    d = DatabaseWrapper()
    #get most recent period for interval
    most_recent = d.get_most_recent_pair_period_close(base_asset,quote_asset,interval) 
    # most_recent = int(most_recent)/1000
    # most_recent = datetime.datetime.fromtimestamp(most_recent).strftime('%Y-%m-%d %H:%M:%S')
    to_sleep = morph_client.get_interval_seconds(interval)
    time_diff_msecs = int(to_sleep)*1000
    current_time_msecs = int(datetime.datetime.now().timestamp() * 1000)
    d.close_connection()

    count_api_calls = 0
    while True:
        if (int(current_time_msecs)-int(most_recent)) >= time_diff_msecs:
            hist_data = [morph_client.get_binance().get_kline_data(base_asset,
            quote_asset,interval,most_recent,current_time_msecs)]
            d = DatabaseWrapper()
            for r in hist_data:
                d.insert_market_data(base_asset,quote_asset,r)
                most_recent = d.get_most_recent_pair_period_close(base_asset,quote_asset,interval)
                count_api_calls += 1
            d.close_connection()
            print ("---------------------------------------------------------------------------------")
            print ("Updated for " + str(to_sleep) + " seconds" + " " + base_asset + " " + quote_asset)
            print ("---------------------------------------------------------------------------------")
            print ("\n")
        else:
            print ("Sleeping for " + str(to_sleep) + " seconds" + " " + base_asset + " " + quote_asset)
            time.sleep(to_sleep)
            current_time_msecs = int(datetime.datetime.now().timestamp() * 1000)

    print ("Interval is " + interval + " most recent is " + str(most_recent) + " " + base_asset + " " + quote_asset)
    print ("\n")

    return True

def run_it():
    assets_ = morph_client.TRADING_PAIRS
    intervals = morph_client.INTERVALS

    arg_list = []
    for inter in intervals:
        for base,quote in assets_:
            to_add = (base,quote,inter)
            arg_list.append(to_add)
    
    with Pool(24) as p:
        print (p.starmap(get_prices,arg_list))

if __name__ == "__main__":
    run_it()
