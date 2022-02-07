import os
import sys
import logging
from kiteconnect import KiteTicker
from kiteconnect import KiteConnect
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","access_config"))
sys.path.append(fpath)
from access_token import access_token
from configparser import ConfigParser

def place_order(symbols, strike, buy_sell, tradefile):
    
    # Initialise

    tradefile_parser = ConfigParser()
    tradefile_parser.read(tradefile)
    
    filename = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","database_pettem.ini"))

    kite = access_token(filename = filename, type= 'kiteconnect')
    
    for symbol,bs in zip(symbols, buy_sell):
        if(bs == 'sell'):
            order_id = kite.place_order(tradingsymbol=symbol,
                                        # price = 1,
                                            exchange=kite.EXCHANGE_NFO,
                                            transaction_type=kite.TRANSACTION_TYPE_SELL,
                                            quantity=25,
                                            variety=kite.VARIETY_REGULAR,
                                            order_type=kite.ORDER_TYPE_MARKET,
                                            product=kite.PRODUCT_NRML)
        else:
            order_id = kite.place_order(tradingsymbol=symbol,
                                        # price = 1,
                                            exchange=kite.EXCHANGE_NFO,
                                            transaction_type=kite.TRANSACTION_TYPE_BUY,
                                            quantity=25,
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
    symbols = ['BANKNIFTY2221038600PE','BANKNIFTY2221038600CE']
    buy_sell = ['sell','sell']
    tradefile = '/home/narayana_tariq/Zerodha/Supertrend_strat/trades.ini'
    strike = 38600
    place_order(symbols,strike,buy_sell,tradefile)