import time
from datetime import datetime
from binance.client import Client 
from binance.websockets import BinanceSocketManager
from morpheus import Morpheus
from binanceApiWrapper import BinanceApiWrapper
from databaseWrapper import DatabaseWrapper


def handle_message(msg):
    # If the message is an error, print the error
    if msg['e'] == 'error':    
        print(msg['m'])
    
    # If the message is a trade: print time, symbol, price, and quantity
    else:
        timestamp = msg['T'] / 1000
        timestamp = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

        # Buy or sell?
        if msg['m'] == True:
            event_side = 'SELL'
        else:
            event_side = 'BUY '
        
        #Price
        price = float(msg['p'])

        #trading logic
    

        print("Time: {} Side: {} Symbol: {} Price: {} Quantity: {} ".format(timestamp,
                                                                   event_side,
                                                                   msg['s'],
                                                                   msg['p'],
                                                                   msg['q']))

def run_trader(bm):
    bm.start()
    #how long to let data flow for
    time.sleep(5)

morpheus_client = Morpheus([('BTC','USDT')],intervals=["15m"])
binance_client = BinanceApiWrapper()
client = Client(api_key=binance_client.get_apiKey(),api_secret=binance_client.get_secret_key())
bm = BinanceSocketManager(client)
conn_key = bm.start_trade_socket('BTCUSDT',handle_message)
while True:
    run_trader(bm)
bm.stop_socket(conn_key)