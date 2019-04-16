import psycopg2,time

class DatabaseWrapper:
    def __init__(self,databaseName="morpheus"):
        self.__username = "zaghie"
        self.__password = "zaghie"
        self.databaseName = "morpheus"

        try:
            self.__conn = psycopg2.connect("dbname=%s user=%s password=%s"%(self.databaseName,self.__username,self.__password))
        except Exception as e:
            print (str(e))
            self.__conn = None
    
    def get_traded_pairs(self):
        if self.__conn == None:
            return None 
    
        cursor = self.__conn.cursor() 
        cursor.execute("""SELECT * FROM TRADEDPAIRS
                          ORDER BY (period_timestamp,quote_asset) DESC""")
        results = cursor.fetchall()
        cursor.close()
        return results

    
    def close_connection(self):
        self.__conn.close()


