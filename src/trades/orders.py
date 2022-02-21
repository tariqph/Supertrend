import os
import sys
import logging
from kiteconnect import KiteTicker
from kiteconnect import KiteConnect
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","access_config"))
sys.path.append(fpath)
from access_token import access_token
from configparser import ConfigParser
import yaml

config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","config.yml"))
try: 
    with open (config_path, 'r') as file:
        config = yaml.safe_load(file)
except Exception as e:
    print('Error reading the config file')


lot_size = 1

def place_order(symbols, strike, buy_sell, tradefile):
    
    # Initialise

    tradefile_parser = ConfigParser()
    tradefile_parser.read(tradefile)
    
    filename = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..",
                                            config['access_files']['api_three']))


    kite = access_token(filename = filename, type= 'kiteconnect')
    
    for symbol,bs in zip(symbols, buy_sell):
        if(bs == 'sell'):
            order_id = kite.place_order(tradingsymbol=symbol,
                                        # price = 1,
                                            exchange=kite.EXCHANGE_NFO,
                                            transaction_type=kite.TRANSACTION_TYPE_SELL,
                                            quantity=lot_size * 25,
                                            variety=kite.VARIETY_REGULAR,
                                            order_type=kite.ORDER_TYPE_MARKET,
                                            product=kite.PRODUCT_NRML)
        else:
            order_id = kite.place_order(tradingsymbol=symbol,
                                        # price = 1,
                                            exchange=kite.EXCHANGE_NFO,
                                            transaction_type=kite.TRANSACTION_TYPE_BUY,
                                            quantity=lot_size * 25,
                                            variety=kite.VARIETY_REGULAR,
                                            order_type=kite.ORDER_TYPE_MARKET,
                                            product=kite.PRODUCT_NRML)
        
        
    
        order = kite.order_history(order_id = order_id )
        # print("next")
        # print(order[-1])
        
        order_details = order[-1]
        
        status = order_details['status']
        
        if status == 'COMPLETE':
            if bs == 'sell':
                if 'PE' in symbol:
                    tradefile_parser.set('trades','put_id', str(order_details['order_id']))
                    tradefile_parser.set('trades','put_price',str(order_details['average_price']))
                    tradefile_parser.set('trades','put_token',str(order_details['instrument_token']))
                    tradefile_parser.set('trades','put_symbol',str(order_details['tradingsymbol']))
                if 'CE' in symbol:
                    tradefile_parser.set('trades','call_id',str(order_details['order_id']))
                    tradefile_parser.set('trades','call_price',str(order_details['average_price']))
                    tradefile_parser.set('trades','call_token',str(order_details['instrument_token']))
                    tradefile_parser.set('trades','call_symbol',str(order_details['tradingsymbol']))
            
            if bs == 'buy':
                if 'PE' in symbol:
                    tradefile_parser.set('trades','put_id', str(order_details['order_id']))
                    tradefile_parser.set('trades','put_price',str(order_details['average_price']))
                    tradefile_parser.set('trades','put_token',"closed")
                    tradefile_parser.set('trades','put_symbol',"closed")
                if 'CE' in symbol:
                    tradefile_parser.set('trades','call_id',str(order_details['order_id']))
                    tradefile_parser.set('trades','call_price',str(order_details['average_price']))
                    tradefile_parser.set('trades','call_token',"closed")
                    tradefile_parser.set('trades','call_symbol',"closed")
                    
                     
                
            
    if buy_sell[0] == 'sell':
        tradefile_parser.set('trades','position','yes')     
        tradefile_parser.set('trades','strike',str(strike))     
        count = tradefile_parser.get('trades','count')
        tradefile_parser.set('trades','count',str(int(count)+1))
    
    if buy_sell[0] == 'buy':
        tradefile_parser.set('trades','position','no')
    
    with open(tradefile, 'w') as configfile:
        tradefile_parser.write(configfile)
            
        # print(order_id)
    
    return


if __name__ == "__main__":
    symbols = ['BANKNIFTY22FEB38600PE','BANKNIFTY22FEB38600CE']
    buy_sell = ['sell','sell']
    tradefile = '/home/narayana_tariq/Zerodha/Supertrend/trades.ini'
    strike = 38600
    place_order(symbols,strike,buy_sell,tradefile)