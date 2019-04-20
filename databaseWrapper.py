import psycopg2,time
from functools import reduce
from collections import namedtuple
from ast import literal_eval

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
        self.MARKETDATA_COLUMNS = ('id','base_asset','quote_asset','open_timestamp','close_timestamp',
              'quote_open_px','quote_high_px','quote_low_px','quote_close_px','base_volume','quote_volume',
              'number_of_trades','taker_base_volume','taker_quote_volume','interval')
        self.TRADEDPAIRS_COLUMNS = ("quote_asset","base_asset","min_lot_qty","max_lot_qty","step_size_qty",
                "period_timestamp","bot_is_trading")
        try:
            self.__conn = psycopg2.connect("dbname=%s user=%s password=%s"%(self.DATABASE_NAME,self.__username,self.__password))
        except Exception:
            self.__conn = None

    def __baseQuoteGetQueryBuilder__(self,base_asset,quote_asset,table_name,column_names=None):
        q1 = """SELECT * FROM %s """ % (table_name) 
        q2 = """WHERE BASE_ASSET=%s AND QUOTE_ASSET=%s"""
        query = q1 if base_asset is None and quote_asset is None else q1 + q2
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
        q2 = """ VALUES (%s) ON CONFLICT (ID) DO NOTHING"""%(values_arg_str)
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

    def get_market_data(self,base_asset=None,quote_asset=None,interval="5m",num_periods=None):
        if not self.is_connected():
            return None 
        cursor = self.__conn.cursor() 
        #build query
        columns_str = reduce(lambda x,y:x+","+y,self.MARKETDATA_COLUMNS)
        query = """SELECT %s FROM %s """ % (columns_str,self.__MARKETDATA_TABLE) 
        query = query + """WHERE BASE_ASSET=%s AND QUOTE_ASSET=%s """
        if num_periods is None:
            query = (query + " AND " + ("INTERVAL='%s'"%interval))
        else:
            query = (query + " AND " + ("INTERVAL='%s'"%interval) +
                    " ORDER BY (OPEN_TIMESTAMP) DESC" + (" LIMIT %s "%num_periods))
        cursor.execute(query,(base_asset,quote_asset))
        results = cursor.fetchall()
        cursor.close()
        TableData=namedtuple("TableData",self.MARKETDATA_COLUMNS)
        table_results = [TableData._make(t) for t in results]
        return table_results
    
    def insert_market_data(self,base_asset,quote_asset,data_named_tuples):
        rows_written = 0
        if not self.is_connected():
            return None 
        cursor = self.__conn.cursor() 
        query = self.__insertQueryBuilder__(self.__MARKETDATA_TABLE,self.MARKETDATA_COLUMNS)
        for data_tuple in data_named_tuples:
            open_time = data_tuple.open_timestamp
            id_ = str(data_tuple.interval) + base_asset + quote_asset + str(open_time)
            values = ((id_,base_asset,quote_asset) + 
                  tuple(getattr(data_tuple,col_name) 
                  for col_name in self.MARKETDATA_COLUMNS[3:]))
            cursor.execute(query,(values))
            rows_written = rows_written + 1
        self.__conn.commit()
        cursor.close()
        return True
    
    def get_most_recent_period_close(self,base_asset,quote_asset):
        if not self.is_connected():
            return None 
        cursor = self.__conn.cursor() 
        query = """SELECT MAX(CLOSE_TIMESTAMP) FROM MARKETDATA WHERE BASE_ASSET=%s
                 AND QUOTE_ASSET=%s"""
        cursor.execute(query,(base_asset,quote_asset))
        result = cursor.fetchall()[0][0]
        return result


    def close_connection(self):
        self.__conn.close()


