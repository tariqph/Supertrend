import os
import pandas as pd
import sys
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","trades"))
sys.path.append(fpath)
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","publish_database"))
sys.path.append(fpath)
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","access_config"))
sys.path.append(fpath)
from supertrend import supertrend
from access_token import access_token
from orders import place_order
from celery import Celery
import datetime
from config import config
import psycopg2
import json
from json.decoder import JSONDecodeError
from configparser import ConfigParser


def trade_strategy(tradefile,tradefile_parser, parser, next, query_cursor, options_data):
    
    put_token = tradefile_parser.get('trades','put_token')
    call_token = tradefile_parser.get('trades','call_token')
    
    if(put_token == 'closed'):
        option_token = tradefile_parser.get('trades','call_token')
        option_symbol = tradefile_parser.get('trades','call_symbol')
    if(call_token == 'closed'):
        option_token = tradefile_parser.get('trades','put_token')
        option_symbol = tradefile_parser.get('trades','put_symbol')
    
    option_price = float(next[str(option_token)]['last_price'])
    
    query = f"""SELECT * FROM banknifty_option_data WHERE instrument_token = \'{str(option_token)}\' 
    order by date_time desc LIMIT 20
    """
    query_cursor.execute(query)
    columns = ['instrument_token','date_time','Close','High','Low']
    df = pd.DataFrame(query_cursor.fetchall(), columns = columns)
    
    # Convert to list and reverse so the latest time is later
    close = df['Close'].tolist()[::-1]
    high = df['High'].tolist()[::-1]
    low = df['Low'].tolist()[::-1]
    date_time = df['date_time'].tolist()[::-1]
    
    # Add the latest price, high, low data
    high.append(float(parser.get(str(option_token),'high')))
    low.append(float(parser.get(str(option_token),'low')))
    close.append(option_price)
    date_time.append(next[str(option_token)][timestamp_string])
    
    df = pd.DataFrame({'High':high,'Low':low,'Close':close,'Datetime':date_time})
    
    # Get the supertrend Indicator
    sti = supertrend(df,10,1)
    
    trending = sti['Supertrend'].tolist()[-1]
    
    if(trending == True):
        supertrend_value = sti['Final Lowerband'].tolist()[-1]
    else:
        supertrend_value = sti['Final Upperband'].tolist()[-1]
    
    if(option_price > supertrend_value):
        
        # Sqaure off the option when candle closes above supertrend
        tokens = [option_symbol]
        buy_sell = ['buy']
        strike = tradefile_parser.get('trades','strike')
        
        place_order(tokens, buy_sell, strike, tradefile)
        
        # place a new straddle
        options_df, strike_new = get_tradingsymbol(options_data,next)
        # options_symbols = options_df['tradingsymbol'].tolist()
        place_straddle(options_df,strike_new, "entry",tradefile)
    
    
    return

def check_stoploss(tradefile_parser, next, tradefile):
    
    pe_token = tradefile_parser.get('trades','put_token')
    ce_token = tradefile_parser.get('trades','call_token')
    
    pe_symbol = tradefile_parser.get('trades','put_symbol')
    ce_symbol = tradefile_parser.get('trades','call_symbol')
    
    pe_ltp = next[str(pe_token)]['last_price']
    ce_ltp = next[str(ce_token)]['last_price']
    
    put_price = tradefile_parser.get('trades','put_price')
    call_price = tradefile_parser.get('trades','call_price')
    
    strike = float(tradefile_parser.get('trades','strike'))
    print(pe_ltp,ce_ltp)
    time_now = datetime.datetime.now()
    weekday = time_now.weekday()
    
    banknifty_token = tradefile_parser.get('trades','banknifty_token')
    # print(next.keys())
    banknifty_ltp = float(next[str(banknifty_token)]['last_price'])
    
    if(weekday == 0 or weekday == 4):
        
        if(pe_ltp >= (1.245 * float(put_price))):
            print("buy put")
            place_order([pe_symbol],strike,['buy'],tradefile)
        if(ce_ltp >= (1.245 * float(call_price))):
            print("buy call")
            place_order([ce_symbol],strike,['buy'],tradefile)
    
    if(weekday >=1 and weekday <= 3):
        if((banknifty_ltp - strike) > 200):
            print("buy call")
            place_order([ce_symbol],strike,['buy'],tradefile)
               
        if((strike - banknifty_ltp) > 200):
            print("buy put")
            place_order([pe_symbol],strike,['buy'],tradefile)
            
        print("strategy")
    
    
    return

def get_tradingsymbol(options_data,next):
    """" returns a dataframe with the instruments for which 
    straddle is to be place based on current
    underlying price"""
    banknifty_token = options_data[options_data['tradingsymbol'] == 'NIFTY BANK']['instrument_token'].values[0]
    print(banknifty_token)

    banknifty_ltp = next[str(banknifty_token)]['last_price']
    
    # Round to the nearest 100
    strike = round(banknifty_ltp,-2)
    
    all_expiries = options_data['expiry'].dropna().unique()
    
    '''
    # In case the run daate is on expiry then data is collected for the nearest
    # and next expiry.
    # But the trades are only run for the nearest expiry
    
    '''
    all_expiries = sorted(all_expiries)
    
    options_symbol = options_data[(options_data['strike'] == strike) & 
                                  (options_data['expiry'] == all_expiries[0])]
    
    return options_symbol , strike

def place_straddle(options_symbols,strike, entry_exit,tradefile):
    
    if(entry_exit == 'entry'):
        buy_sell = ['sell','sell']
    else:
        buy_sell = ['buy', 'buy']
        
    symbols = options_symbols['tradingsymbol'].tolist()
    place_order(symbols,strike,buy_sell,tradefile)
    return

def exit_position(tradefile_parser,tradefile):
    
    put_symbol = tradefile_parser.get('trades','put_symbol')
    call_symbol = tradefile_parser.get('trades','call_symbol')
    
    option_symbols = []
    buy_sell = []
    if(put_symbol != 'closed'):
        option_symbols.append(put_symbol)
        buy_sell.append("buy")
    if(call_symbol != 'closed'):
        option_symbols.append(call_symbol)
        buy_sell.append("buy")
    strike = tradefile_parser.get('trades','strike')
    # print(option_symbols,strike, buy_sell)
    place_order(option_symbols,strike,buy_sell,tradefile)
    
    
    return 

def read_write_temp(filename_data, filename_timestamp,ticks,
                    next_data, timestamp, read_write):
    
    """ Reading and writing ticker data from the temp file for rollforwarding"""
    prev = {}
    next = {}
    prev_timestamp = '0'
    with open(filename_data, read_write) as file:
        try:
            if read_write == 'r':
                prev = json.load(file)
            else:
                json.dump(next_data, file)
        except JSONDecodeError:
            pass
        
    with open(filename_timestamp, read_write) as file:
        try:
            if read_write == 'r':
                prev_timestamp = json.load(file)
            else:
                json.dump(str(timestamp), file)
        except JSONDecodeError:
            pass
     
    # next_timestamp = ticks[0][timestamp_string]
    # print(prev_timestamp)
    for tick in ticks:
        next[str(tick['instrument_token'])] = tick
    
    if read_write == 'r':
        return prev,next,prev_timestamp
    

def insert_query(cursor,next_timestamp, tick,postgres_insert_query,high,low):
    record_to_insert = (tick['instrument_token'],next_timestamp, tick['last_price'],high, low)
                                    
    cursor.execute(postgres_insert_query, record_to_insert)


def union(prev,next,timestamp):
    for token, tick in prev.items():
        if str(token) not in next:
            next[str(token)] = tick
            next[str(token)][timestamp_string] = str(timestamp)
    return next


app = Celery('tasks', broker = 'amqp://guest:guest@localhost:5672//', backend='rpc:// ')

timestamp_string = 'exchange_timestamp'

@app.task
def insert_db(ticks, tablename, filename_data,
              filename_timestamp, tradefile,options_data_filename):
    ''' 
    Function to insert tick data into the database every 5 minutes. Also has the logic for the trading
    '''
    options_data = pd.read_csv(options_data_filename)
    parser = ConfigParser()
    # read config file
     
    next_timestamp = ticks[0][timestamp_string]
    
    prev,next,prev_timestamp = read_write_temp(filename_data, filename_timestamp,
                                               ticks,{},next_timestamp, 'r')
    # filename = 'G:\DS - Competitions and projects\Zerodha\db.ini'
    filename = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","high_low.ini"))
    print(filename)
    parser.read(filename)
    
    for token, tick in next.items():
        if( tick['last_price'] > float(parser.get(str(token),"high"))):
            parser.set(str(token),"high",str(tick['last_price']))
        if( tick['last_price'] < float(parser.get(str(token),"low"))):
            parser.set(str(token),"low",str(tick['last_price']))
            

    
    with open(filename, 'w') as configfile:
        parser.write(configfile)
    # Check if the insert requirements are met before proceeding
    if(prev == {}):
        print('returning')
        read_write_temp(filename_data, filename_timestamp,ticks,next, next_timestamp, 'w')
        
        return
    
    # Convert string to datetime object for easy manipulation
    print(next_timestamp)
    next_timestamp = datetime.datetime.strptime(next_timestamp,'%Y-%m-%dT%H:%M:%S')
    if(prev_timestamp !=  '0'):
        if('T' in prev_timestamp):
            prev_timestamp = datetime.datetime.strptime(prev_timestamp,'%Y-%m-%dT%H:%M:%S')
        else:
            prev_timestamp = datetime.datetime.strptime(prev_timestamp,'%Y-%m-%d %H:%M:%S')
            
    next = union(prev,next,next_timestamp)
    print(len(next))
    # print(next)
  
    # Establishing connection with database
    params = config()
    # connect to the PostgreSQL serve
    connection = psycopg2.connect(**params)
    connection.autocommit = True

    # cursor object for database
    cursor = connection.cursor()
    
    # SQL to insert for instruments and indices
    postgres_insert_query = f""" INSERT INTO {tablename} (instrument_token,date_time, 
                            ltp, high, low) 
                            VALUES (%s,%s,%s,%s,%s)"""
                            
    
    print(prev_timestamp, next_timestamp)
    
    tradefile_parser = ConfigParser()
    tradefile_parser.read(tradefile)
    
    if(tradefile_parser.get('trades','position') == 'yes'):
        
        check_stoploss(tradefile_parser, next, tradefile)
            
    
    if(next_timestamp.hour == 9 and next_timestamp.minute == 25 and next_timestamp.second == 0):
        if(tradefile_parser.get('trades','position') == 'no'):
            
            options_symbols, strike = get_tradingsymbol(options_data,next)
            
            place_straddle(options_symbols,strike, "entry",tradefile)
    
    # Exit all the active positions before 3 pm 
    exit_time_string = "14:59:55"
    exit_time = datetime.datetime.strptime(exit_time_string,"%H:%M:%S").time()    
    
    if(next_timestamp.time() == exit_time):
        
        exit_position(tradefile_parser,tradefile)


    if(next_timestamp.hour == 9 and next_timestamp.minute < 20):
        return
    
    # Insert data at the 5 minute mark
    if(next_timestamp.minute % 5 == 0):
        if(next_timestamp.second == 0):
            
            trade_count = int(tradefile_parser.get('trades','count'))
            position = tradefile_parser.get('trades','position')
            
            
            if(prev_timestamp == next_timestamp):
                # Conditional block if there are more than one tick recieved at 5 minute mark
                for token,tick in next.items():
                    parser.set(str(token),"high",'0')
                    parser.set(str(token),"low",'100000')
                    
                return                        
            print('here1')
            # logging.info('here 1')
            
            # If only one leg of straddle is in position and time is before exit then check for supertrend
            if(trade_count > 0 and position == 'no' and next_timestamp.time() < exit_time):
                query_cursor = connection.cursor()
                trade_strategy(tradefile,tradefile_parser, parser, next, query_cursor,options_data)
            
            for token,tick in next.items():
                
                insert_query(cursor,next_timestamp, tick,postgres_insert_query,
                             parser.get(str(token),"high"), parser.get(str(token),"low"))
                parser.set(str(token),"high",'0')
                parser.set(str(token),"low",'100000')
        
        # If the minute mark is missed in websocket response then the ticker 
        # did not change from the previous so used that for minute mark data
        elif((prev_timestamp != '0') and (next_timestamp.minute - prev_timestamp.minute)!=0 and
            (prev_timestamp.second)!=0):
            # if(prev_timestamp == next_timestamp):
            #     return 
            print('here2')
            
            next_timestamp_insert = next_timestamp - datetime.timedelta(seconds = next_timestamp.second)
            for token,tick in prev.items():
                insert_query(cursor,next_timestamp_insert, tick,postgres_insert_query,
                             parser.get(str(token),"high"), parser.get(str(token),"low"))
                
                parser.set(str(token),"high",'0')
                parser.set(str(token),"low",'100000')
        
    read_write_temp(filename_data, filename_timestamp, ticks,next,next_timestamp, 'w')
    
    
    # Commiting the high low of instruments to the file
    with open(filename, 'w') as configfile:
        parser.write(configfile)    
        
    return


# if __name__ == "__main__":
    
    
    
#     parser = ConfigParser()
#     filename = 'G:\DS - Competitions and projects\Supertrend_strategy\\temp_cache\prev_1.txt'
   
#     with open(filename, 'r') as file:
#         try:
#             next = json.load(file)
#         except JSONDecodeError:
#             pass
#     filename = 'G:\DS - Competitions and projects\Zerodha\db.ini'
#     parser.read(filename)
#     options_data = pd.read_csv("G:\DS - Competitions and projects\Supertrend_strategy\src\\access_config\\banknifty_instruments.csv")
#     tradefile_parser = ConfigParser()
#     tradefile_parser.read('G:\DS - Competitions and projects\Zerodha\\trades.ini')
#     options_symbol, strike = get_tradingsymbol(options_data,next)
    # print(options_symbol, strike)
    # tradefile = "G:\DS - Competitions and projects\Zerodha\\trades.ini"
    
    # place_straddle(options_symbol,strike, "entry",tradefile)
    # check_stoploss(tradefile_parser, next)
    # # print(options_symbol)
    # params = config()
    # # connect to the PostgreSQL serve
    # connection = psycopg2.connect(**params)
    # connection.autocommit = True

    # # cursor object for database
    # cursor = connection.cursor()
    
    # trade_strategy(tradefile_parser, parser, next, cursor)
# if __name__ == "__main__":
def test(ticks):
    # ticks = ''
    tablename = "banknifty_option_data"
    filename_data = "/home/narayana_tariq/Zerodha/Supertrend_strat/temp_cache/prev_1.txt"
    filename_timestamp = "/home/narayana_tariq/Zerodha/Supertrend_strat/temp_cache/prev_timestamp_1.txt"
    tradefile = "/home/narayana_tariq/Zerodha/Supertrend_strat/trades.ini"
    options_data_filename = "/home/narayana_tariq/Zerodha/Supertrend_strat/banknifty_instruments.csv"
    insert_db(ticks, tablename, filename_data,
              filename_timestamp, tradefile,options_data_filename)

if __name__ == "__main__":
    tradefile = "/home/narayana_tariq/Zerodha/Supertrend_strat/trades.ini"
    parser = ConfigParser()
    parser.read(tradefile)
    exit_position(parser,tradefile)