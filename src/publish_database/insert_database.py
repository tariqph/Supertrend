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
from strategy import trade_strategy, check_stoploss, get_tradingsymbol, place_straddle, exit_position

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
        
        check_stoploss(tradefile_parser, next, tradefile, next_timestamp)
            
    
    if(next_timestamp.hour == 9 and next_timestamp.minute == 25 and next_timestamp.second == 0):
        if(tradefile_parser.get('trades','position') == 'no'):
            
            options_symbols, strike = get_tradingsymbol(options_data,next)
            
            place_straddle(options_symbols,strike, "entry",tradefile)
    
    # Exit all the active positions before 3 pm 
    exit_time_string_1 = "14:59:55"
    exit_time_string_2 = "14:59:56"
    exit_time_string_3 = "14:59:57"
    
    exit_time_1 = datetime.datetime.strptime(exit_time_string_1,"%H:%M:%S").time()
    exit_time_2 = datetime.datetime.strptime(exit_time_string_2,"%H:%M:%S").time()    
    exit_time_3 = datetime.datetime.strptime(exit_time_string_3,"%H:%M:%S").time()    
    
        
    
    if(next_timestamp.time() == exit_time_1 
       or next_timestamp.time() == exit_time_2
       or next_timestamp.time() == exit_time_3):
        
        exit_position(tradefile_parser,tradefile)
    
    trade_exit_filename = os.path.abspath(os.path.join(os.path.dirname(__file__),"..",
                                 "..","trade_exit.txt"))
    
    with open(trade_exit_filename, 'r') as file_reader:
        file_reader.seek(0)
        trade_exit = file_reader.readline()
    
    # For manual exit through flask file update
    if(trade_exit == 'exit' and next_timestamp.time() < exit_time_1):
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
            if(trade_count > 0 and position == 'no' and next_timestamp.time() < exit_time_1):
                query_cursor = connection.cursor()
                trade_strategy(tradefile,tradefile_parser, parser,
                               next, query_cursor,options_data,tablename)
            
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


def test(ticks):
    # ticks = ''
    tablename = "banknifty_option_data"
    filename_data = "/home/narayana_tariq/Zerodha/Supertrend_strat/temp_cache/prev_1.txt"
    filename_timestamp = "/home/narayana_tariq/Zerodha/Supertrend_strat/temp_cache/prev_timestamp_1.txt"
    tradefile = "/home/narayana_tariq/Zerodha/Supertrend_strat/trades.ini"
    options_data_filename = "/home/narayana_tariq/Zerodha/Supertrend_strat/banknifty_instruments.csv"
    insert_db(ticks, tablename, filename_data,
              filename_timestamp, tradefile,options_data_filename)

# if __name__ == "__main__":
#     tradefile = "/home/narayana_tariq/Zerodha/Supertrend_strat/trades.ini"
#     parser = ConfigParser()
#     parser.read(tradefile)
#     exit_position(parser,tradefile)