import requests
import hmac
import hashlib
import ujson

class BinanceApiWrapper:
    def __init__(self,apiPublicKey="hDkS3KUmSpa8hIXnzxqbiHugKc51ZRTHgkfCJ0vrCEy96rsX9zffoDNOjAmpJ4Uh"
                ,secretKey="7ADP6JjsWAm0Kc8ZFWJq9genQbXnNbnejSEsqlOWnl42a77XZ5cMPaRomME0TfZw"):
        self.__publicKey = apiPublicKey
        self.__secretKey = secretKey
    
    def generate_signature(self,query_str):
        m = hmac.new(self.__secretKey.encode('utf-8'),query_str.encode('utf-8'),hashlib.sha256).hexdigest()
        return m
    
    def get_apiKey(self):
        return self.__publicKey

    def get_traded_pairs(self):
        URL = "https://api.binance.com/api/v1/exchangeInfo"
        raw_response = requests.get(URL)
        parsed_response = ujson.loads(raw_response.text) if raw_response is not None else None 

        if parsed_response is None:
            return None 
        
        return parsed_response