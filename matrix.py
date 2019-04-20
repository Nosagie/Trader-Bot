from morpheus import Morpheus
import datetime

def get_hist():
    pairs = [('BTC','USDT'),('ETH','USDT'),('ADA','USDT'),('LTC','USDT')]
    t = Morpheus(pairs)
    start = datetime.datetime(2018,1,1,00,00,00)
    end = datetime.datetime.now()
    t.get_historical_data(start,end)


