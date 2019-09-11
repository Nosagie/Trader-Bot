from morpheus import Morpheus
from databaseWrapper import DatabaseWrapper
import datetime
import matplotlib.pyplot as plt
import numpy as np 
import scipy.stats as stats
import pandas as pd
import seaborn as sns 

import time 
from binance.client import Client 
from binance.websockets import BinanceSocketManager 

pairs = [('BTC','USDT'),('ETH','USDT'),('ADA','USDT'),('LTC','USDT')]
t = Morpheus(pairs)
e = DatabaseWrapper()

def investigate_range():
    fifteen_minute_data  = e.get_market_data(base_asset='BTC',quote_asset='USDT',interval='1h',num_periods=2000)
    final_data_set = []
    #format data_set
    for i in fifteen_minute_data:
        closing_timestamp = i.close_timestamp
        close_time = closing_timestamp
        # close_time = closing_timestamp / 1000
        # close_time = datetime.datetime.fromtimestamp(close_time).strftime('%Y-%m-%d %H:%M:%S')
        closing_px = float(i.quote_close_px)
        high_px = float(i.quote_high_px)
        low_px = float(i.quote_low_px)
        num_trades = i.number_of_trades
        final_data_set.append(["BTC","USDT",close_time,high_px,low_px,closing_px,num_trades])

    final_data_set = final_data_set[::-1]
    columns_ = ["base_asset","quote_asset","close_time","high_px","low_px","close_px","num_trades"]
    dataframe_ = pd.DataFrame(final_data_set,columns=columns_)

    close_px_perc_change = dataframe_[['close_px']].pct_change()
    high_px_perc_change = dataframe_[['high_px']].pct_change()
    low_px_perc_change = dataframe_[['low_px']].pct_change()
    num_trades_perc_change = dataframe_[['num_trades']].pct_change()

    dataframe_["close_px_change"] = close_px_perc_change
    dataframe_["high_px_change"] = high_px_perc_change
    dataframe_["low_px_change"] = low_px_perc_change
    dataframe_["num_trades_perc_change"] = num_trades_perc_change

    #statistical analyses
    t = close_px_perc_change.dropna().values.tolist()
    t = [r[0] for r in t]
    y = dataframe_[['close_time']].values.tolist()
    y =[y[0] for r in y]

    # print (t)
    sns.set(color_codes=True)
    data = dataframe_[["close_time","close_px_change"]]
    sns.relplot(x="close_time",y="close_px_change",data=data,kind="line")
    plt.show()

investigate_range()

#queries the market for prices
def range_bot(buy_price=9900,sell_price=10000):
    range_trader = Morpheus(pairs_to_trade=[('BTC','USDT')],intervals=["15m"])
    #check current position 


    #check minimum lot size

    #if price is less than or equal to buy price, and current position less than min lot size, and we have USDT, then buy max

    #if current position is greater than/equal to min lot size and price is greater than or equal to sell price, then sell all
    

