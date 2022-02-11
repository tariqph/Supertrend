# Adding paths to module used

from configparser import ConfigParser
from json.decoder import JSONDecodeError
import datetime
import os
import sys
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","trades"))
sys.path.append(fpath)
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","publish_database"))
sys.path.append(fpath)
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","access_config"))
sys.path.append(fpath)
from access_token import access_token

import logging
from kiteconnect import KiteTicker
from insert_database import insert_db
logging.basicConfig(level=logging.DEBUG)
import pandas as pd
import yaml

config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","config.yml"))
try: 
    with open (config_path, 'r') as file:
        config = yaml.safe_load(file)
except Exception as e:
    print('Error reading the config file')
    
# Paths for cache for prev ticker and timestamp

filename_data = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..",
                                             config['cache_files']['prev_1']))
filename_timestamp = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..",
                                             config['cache_files']['prev_timestamp_1']))


print(filename_data)


# data = pd.read_csv(path)
options_token_filename = os.path.abspath(os.path.join(os.path.dirname(__file__),"..",
                                 "..","banknifty_instruments.csv"))
data = pd.read_csv(options_token_filename)
instrument_list = data['instrument_token'].tolist()


table_name = 'banknifty_option_data'

# instrument_list = instrument_list[0:10]
print(len(instrument_list))


# Filename for the connection details
filename = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..",
                                        config['access_files']['api_three']))

kws = access_token(filename = filename, type = 'kws')

# kws = KiteTicker(db['api_key'], db['access_token'])

# Message broker queue name
count = 0
queue = config['rabbitmq']['queues']['banknifty']

parser = ConfigParser()
filename_highlow = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","high_low.ini"))
# filename = 'G:\DS - Competitions and projects\Zerodha\db.ini'
parser.read(filename_highlow)

    
# Writing the changes to the instrument file 

    
exec_date_filename = os.path.abspath(os.path.join(os.path.dirname(__file__),"..",
                                 "..","last_execution_date.txt"))
trade_exit_filename = os.path.abspath(os.path.join(os.path.dirname(__file__),"..",
                                 "..","trade_exit.txt"))
# print(exec_date_filename)
with open(exec_date_filename, 'a+') as infile:
	try:
		# prev = json.load(infile)
		infile.seek(0)
		a = infile.readline()
		print(a)
	except JSONDecodeError:
		print("exception")
		pass
# print(a)
date = datetime.date.today()
date_exec = datetime.datetime.strptime(a, "%Y-%m-%d")


print(date,date_exec.date())
print(date == date_exec.date())

tradefile = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","trades.ini"))

if(date != date_exec.date()):
    
    open(filename_highlow, 'w').close()
    
    tradefile_parser = ConfigParser()

    tradefile_parser.read(tradefile)
    # if(date_exec.date() != date):
        # Only make initial changes to the disk files once when the run starts
    tradefile_parser.set("trades","count", "0")
    tradefile_parser.set("trades","position","no")
    banknifty_token = data[data['tradingsymbol'] == 'NIFTY BANK']['instrument_token'].values[0]
    tradefile_parser.set("trades","banknifty_token",str(banknifty_token))

    for instrument in instrument_list:
        # db[instrument] = {'high' : 0, 'low': 100000}
        if str(instrument) not in parser.sections():
            parser.add_section(str(instrument))
        parser.set(str(instrument),'high', '0')
        parser.set(str(instrument),'low', '100000')



    # Writing the changes to trades file
    with open(tradefile, 'w') as configfile:
            tradefile_parser.write(configfile)  

    with open(filename_highlow, 'w') as configfile:
        parser.write(configfile)
    
    with open(trade_exit_filename, 'w') as f:
            f.write(str('entry'))

    # Clear the previous ticks file
    open(filename_data, 'w').close()
    open(filename_timestamp, 'w').close()

    # Update the file with execution date
    with open(exec_date_filename, 'w+') as infile:
        try:
            # prev = json.load(infile)
            infile.write(str(date))
        except JSONDecodeError:
            print("exception")
            pass
        
timestamp = 'exchange_timestamp'

def on_ticks(ws, ticks):
    # print(ticks[0])
    print(ticks[0][timestamp])
    if(ticks[0][timestamp].hour >= 9):
        if(ticks[0][timestamp].hour == 9 and ticks[0][timestamp].minute >= 15):
            insert_db.apply_async(args = (ticks,table_name, filename_data,
                                        filename_timestamp, tradefile, options_token_filename),
                                queue = queue)
        elif(ticks[0][timestamp].hour > 9):
            insert_db.apply_async(args = (ticks,table_name, filename_data,
                                        filename_timestamp, tradefile, options_token_filename),
                                queue = queue)
    
    if(ticks[0][timestamp].hour == 15 and 
       ticks[0][timestamp].minute == 30 and ticks[0][timestamp].second == 0):
        on_close(ws,"100", "time-out closed - exchange closed")
    
    
    
def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens 
    ws.subscribe(instrument_list)

    # Set to tick in `full` mode.
    ws.set_mode(ws.MODE_FULL,instrument_list)

def on_close(ws, code, reason):
    # On connection close stop the main loop
    # Reconnection will not happen after executing `ws.stop()`
    ws.stop()

kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect()