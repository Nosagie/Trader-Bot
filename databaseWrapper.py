import psycopg2,time

class DatabaseWrapper:
    def __init__(self,databaseName="morpheus",username="zaghie",
                    password="zaghie",pairs_table="TRADEDPAIRS",
                    orders_table="ORDERS",portfolio_table="PORTFOLIO"):
        self.__username = username
        self.__password = password
        self.DATABASE_NAME = databaseName
        self.__PAIRS_TABLE_NAME = pairs_table
        self.__ORDERS_TABLE_NAME = orders_table
        self.__PORTFOLIO_TABLE_NAME = portfolio_table
        self.__checkBaseQuote = lambda base,quote: (base==None) and (quote==None) 
        try:
            self.__conn = psycopg2.connect("dbname=%s user=%s password=%s"%(self.DATABASE_NAME,self.__username,self.__password))
        except Exception:
            self.__conn = None

    def __baseQuoteQueryBuilder__(self,base_asset,quote_asset,table_name):
        q1 = """SELECT * FROM %s """ % (table_name)
        q2 = """WHERE BASE_ASSET=%s AND QUOTE_ASSET=%s"""
        query = q1 if self.__checkBaseQuote(base_asset,quote_asset) else q1 + q2
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
                          ORDER BY (period_timestamp,quote_asset) DESC""" % (self.__PAIRS_TABLE_NAME))
        results = cursor.fetchall()
        cursor.close()
        return results
    
    def get_portfolio_position(self,base_asset=None,quote_asset=None):
        if not self.is_connected():
            return None 
        cursor = self.__conn.cursor() 
        query = self.__baseQuoteQueryBuilder__(base_asset,quote_asset,self.__PORTFOLIO_TABLE_NAME)
        cursor.execute(query,(base_asset,quote_asset))
        results = cursor.fetchall()
        cursor.close()
        return results

    def get_executed_orders(self,base_asset=None,quote_asset=None):
        if not self.is_connected():
            return None 
        cursor = self.__conn.cursor() 
        query = self.__baseQuoteQueryBuilder__(base_asset,quote_asset,self.__ORDERS_TABLE_NAME)
        cursor.execute(query,(base_asset,quote_asset))
        results = cursor.fetchall()
        cursor.close()
        return results
    
    def close_connection(self):
        self.__conn.close()


