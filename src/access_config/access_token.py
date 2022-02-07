from kiteconnect import KiteConnect
from configparser import ConfigParser
from kiteconnect import KiteTicker

parser = ConfigParser()
import os



def access_token(filename = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","database.ini")),
                 type = 'kiteconnect'):
    '''
    This function sets the access token generated for the day and returns a kiteconnect object 
    '''
    # filename = 'database.ini'
    # filename = "G:\DS - Competitions and projects\Zerodha\database.ini"
    # filename = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","database.ini"))
    
    
    section = 'zerodha'
    # read config file
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    if(type == 'kiteconnect'):
        kite = KiteConnect(api_key=db['api_key'])

        # data = kite.generate_session(request_token, api_secret=api_secret)
        kite.set_access_token(db['access_token'])
        
        return kite
    else:
        kws = KiteTicker(db['api_key'], db['access_token'])
        return kws
             
    
