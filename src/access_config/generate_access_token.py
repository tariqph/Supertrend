import logging
from kiteconnect import KiteConnect
from configparser import ConfigParser
from requests.api import request
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
import time
import pyotp
import os
import yaml

# fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

'''
The script generates the day access token after logging into kiteconnect using selenium webdriver
'''

logging.basicConfig(level=logging.DEBUG)

config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","config.yml"))
try: 
    with open (config_path, 'r') as file:
        config = yaml.safe_load(file)
except Exception as e:
    print('Error reading the config file')

# filenames = [os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","database.ini")),
# os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","database_hemant.ini"))]

filenames = [os.path.abspath(os.path.join(os.path.dirname(__file__),"..",
                                 "..",config['access_files']['api_three']))]
auth_type = ['totp']
section = 'zerodha'
print("generating tokens")
for filename , type in zip(filenames,auth_type):
   
    url = config['zerodha']['kite_trade_url'] 
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]


    # print(url + db['api_key'])
    url = url + db['api_key']

    options = Options()
    options.headless = True
    # options.headless = False
    
    driver = webdriver.Chrome(options=options)
    driver.get(url) 
    action = ActionChains(driver)

    time.sleep(2)

    user_id = driver.find_element_by_id('userid')
    print('here')
    password= driver.find_element_by_id('password')
    login = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[4]/button')

    user_id.send_keys(db['user_id'])
    password.send_keys(db['password'])
    login.click()
    time.sleep(2)

    # print(db['totp_key'])
    
    totp = pyotp.TOTP(db['totp_key'])

    pin_user = db['pin']
    if(type == 'totp'):
        pin = driver.find_element_by_id('totp')
        pin.send_keys(totp.now())
        
    else:
        pin = driver.find_element_by_id('pin')
        pin.send_keys(pin_user)
        
    cont = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[3]/button')
    print(totp.now())
    print(pin_user)
    
    pin.send_keys(pin_user)
    cont.click()

    time.sleep(2)

    curr_url = driver.current_url
    curr_url = curr_url.split('&')
    print('url', curr_url)

    token = ''
    for string in curr_url:
        if 'request_token' in string:
            token = string

    token = token.split('=')
    request_token = token[1]
    print(request_token)

    api_key = db['api_key']
    api_secret = db['api_secret']

    kite = KiteConnect(api_key=api_key)

    data = kite.generate_session(request_token, api_secret=api_secret)
    print(data['access_token'])
    parser.set('zerodha','access_token', data['access_token'])

    with open(filename, 'w') as configfile:
        parser.write(configfile)
        

    driver.quit()
    time.sleep(2)
