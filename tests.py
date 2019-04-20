from morpheus import Morpheus
from databaseWrapper import DatabaseWrapper
import datetime
import matplotlib.pyplot as plt

pairs = [('BTC','USDT'),('ETH','USDT'),('ADA','USDT'),('LTC','USDT')]
t = Morpheus(pairs)
e = DatabaseWrapper()

#t.get_historical_data()
hit = e.get_market_data(pairs[3][0],pairs[3][1],"3m",80)
msecs = [datetime.datetime.fromtimestamp(d.open_timestamp//1000).strftime('%H:%M:%S')for d in hit]
y = list(reversed([float(d.quote_close_px) for d in hit]))
x = list(reversed([d[:3] for d in msecs]))
plt.plot(x,y)
plt.show()
