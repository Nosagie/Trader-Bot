import psycopg2,time
from functools import reduce

class DatabaseWrapper:
    def __init__(self,databaseName="morpheus",username="zaghie",
                    password="zaghie",pairs_table="TRADEDPAIRS",
                    orders_table="ORDERS",portfolio_table="PORTFOLIO",
                    marketdata_table="MARKETDATA"):
        self.__username = username
        self.__password = password
        self.DATABASE_NAME = databaseName
        self.__PAIRS_TABLE = pairs_table
        self.__ORDERS_TABLE = orders_table
        self.__PORTFOLIO_TABLE = portfolio_table
        self.__MARKETDATA_TABLE = marketdata_table 
        self.MARKETDATA_COLUMNS = ('base_asset','quote_asset','open_timestamp','close_timestamp',
              'quote_open_px','quote_high_px','quote_low_px','quote_close_px','base_volume','quote_volume',
              'number_of_trades','taker_base_volume','taker_quote_volume','interval')
        self.__checkBaseQuote = lambda base,quote: (base==None) and (quote==None) 
        try:
            self.__conn = psycopg2.connect("dbname=%s user=%s password=%s"%(self.DATABASE_NAME,self.__username,self.__password))
        except Exception:
            self.__conn = None

    def __baseQuoteGetQueryBuilder__(self,base_asset,quote_asset,table_name):
        q1 = """SELECT * FROM %s """ % (table_name)
        q2 = """WHERE BASE_ASSET=%s AND QUOTE_ASSET=%s"""
        query = q1 if not self.__checkBaseQuote(base_asset,quote_asset) else q1 + q2
        return query
    
    def __insertQueryBuilder__(self,table_name,column_names_tuple):
        values_arg_str = ""
        column_names_str = ""
        for i in column_names_tuple:
            values_arg_str = values_arg_str + "%s,"
            column_names_str = column_names_str + i + ","
        values_arg_str = values_arg_str[0:-1]
        column_names_str = column_names_str[0:-1]
        q1= """INSERT INTO %s (%s) """ % (table_name,column_names_str)
        q2 = """ VALUES (%s)"""%(values_arg_str)
        query = q1 + q2
        return query
    
    def is_connected(self):
        if self.__conn == None:
            return False
        else: 
            return True
        
    def get_traded_pairs(self):
        if not self.is_connected():
            return None 
        cursor = self.__conn.cursor() 
        cursor.execute("""SELECT * FROM %s
                          ORDER BY (period_timestamp,quote_asset) DESC""" % (self.__PAIRS_TABLE))
        results = cursor.fetchall()
        cursor.close()
        return results
    
    def get_portfolio_position(self,base_asset=None,quote_asset=None):
        if not self.is_connected():
            return None 
        cursor = self.__conn.cursor() 
        query = self.__baseQuoteGetQueryBuilder__(base_asset,quote_asset,self.__PORTFOLIO_TABLE)
        cursor.execute(query,(base_asset,quote_asset))
        results = cursor.fetchall()
        cursor.close()
        return results

    def get_executed_orders(self,base_asset=None,quote_asset=None):
        if not self.is_connected():
            return None 
        cursor = self.__conn.cursor() 
        query = self.__baseQuoteGetQueryBuilder__(base_asset,quote_asset,self.__ORDERS_TABLE)
        cursor.execute(query,(base_asset,quote_asset))
        results = cursor.fetchall()
        cursor.close()
        return results

    def get_market_data(self,base_asset=None,quote_asset=None,interval="5m"):
        if not self.is_connected():
            return None 
        cursor = self.__conn.cursor() 
        query = self.__baseQuoteGetQueryBuilder__(base_asset,quote_asset,self.__MARKETDATA_TABLE)
        query = (query + " AND " + ("INTERVAL='%s'"%interval))
        cursor.execute(query,(base_asset,quote_asset))
        results = cursor.fetchall()
        cursor.close()
        return results
    
    def insert_market_data(self,base_asset,quote_asset,data_named_tuple):
        if not self.is_connected():
            return None 
        cursor = self.__conn.cursor() 
        query = self.__insertQueryBuilder__(self.__MARKETDATA_TABLE,self.MARKETDATA_COLUMNS)
        values = ((base_asset,quote_asset) + 
                  tuple(getattr(data_named_tuple,col_name) 
                  for col_name in self.MARKETDATA_COLUMNS[2:]))
        cursor.execute(query,(values))
        self.__conn.commit()
        cursor.close()
        return

    def close_connection(self):
        self.__conn.close()


