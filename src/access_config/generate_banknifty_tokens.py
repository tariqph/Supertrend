import pandas as pd
from access_token import access_token
import os
import yaml
import datetime

config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","config.yml"))
try: 
    with open (config_path, 'r') as file:
    	config = yaml.safe_load(file)
except Exception as e:
    print('Error reading the config file')
    
filename = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..",
                                                config['access_files']['api_three']))

kite = access_token(filename = filename)

data = kite.instruments()
data = pd.DataFrame(data)
banknifty_underlying = data[data['tradingsymbol'] == 'NIFTY BANK']
banknifty_token = banknifty_underlying['instrument_token'].values[0]

banknifty_ltp = kite.ltp(banknifty_token)
banknifty_ltp = banknifty_ltp[str(banknifty_token)]['last_price']
middle_strike = int(banknifty_ltp - (banknifty_ltp%100))

today_date = datetime.date.today()
# today_date = today_date + datetime.timedelta(days=2)
expiries_location = os.path.abspath(os.path.join(os.path.dirname(__file__),"..",
                                 "..","expiry_index.csv"))
expiries = pd.read_csv(expiries_location)
print(today_date)
expiries = expiries['Dates'].tolist()
# expiries = sorted(expiries)
expiry = []

for i in range(len(expiries)):
    date = expiries[i]
    date  = datetime.datetime.strptime(date, "%d-%b-%y").date()
    if(date == today_date):
        expiry.append(date)
        date = expiries[i+1]
        date  = datetime.datetime.strptime(date, "%d-%b-%y").date()
        expiry.append(date)
        break
    if(date > today_date):
        expiry.append(date)
        break

print(expiry)
required_instruments = data[(data['name'] == 'BANKNIFTY') & (data['expiry'].isin(expiry)) 
                            & (data['strike'] >= (middle_strike - 2500)) & 
                            (data['strike'] <= (middle_strike + 2500))]

required_instruments = pd.concat([required_instruments,banknifty_underlying])


instruments_filename = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..",
                                               'banknifty_instruments.csv'))
required_instruments.to_csv(instruments_filename)
