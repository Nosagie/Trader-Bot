from morpheus import Morpheus
from databaseWrapper import DatabaseWrapper
import datetime
import matplotlib.pyplot as plt

pairs = [('BTC','USDT'),('ETH','USDT'),('ADA','USDT'),('LTC','USDT')]
t = Morpheus(pairs)
e = DatabaseWrapper()


def run():
    hit = e.get_market_data(pairs[0][0],pairs[0][1],"5m",13)
    x_values = []
    y_values = []
    period_index = 0
    for i in hit:
        x_values.append(period_index)
        y_values.append(i.quote_close_px)
        period_index += 1
    
    plt.plot(x_values,y_values)

    plt.xlim(1,period_index)

    plt.show()


#queries the market for prices
def range_bot(buy_price=9900,sell_price=10000):
    range_trader = Morpheus([('BTC','USDT')])

    #check current position 

    #check minimum lot size

    #if price is less than or equal to buy price, and current position less than min lot size, and we have USDT, then buy max

    #if current position is greater than/equal to min lot size and price is greater than or equal to sell price, then sell all
    

