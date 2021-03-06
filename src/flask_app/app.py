from flask import Flask, request, render_template
from os import listdir
import datetime
import os, sys
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","access_config"))
sys.path.append(fpath)
from access_token import access_token
import time
app = Flask(__name__)
import yaml


config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","config.yml"))
try: 
    with open (config_path, 'r') as file:
        config = yaml.safe_load(file)
except Exception as e:
    print('Error reading the config file')

filename = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..",
                                        config['access_files']['api_three']))
trade_exit_filename = os.path.abspath(os.path.join(os.path.dirname(__file__),"..",
                                 "..","trade_exit.txt"))
date_today = datetime.date.today()

@app.route('/')
def my_form():

    kite = access_token(filename = filename,type = "kiteconnect")

    trades = kite.trades()
    with open(trade_exit_filename, 'r') as f:
        f.seek(0)
        a = f.readline()
    return render_template('index.html', trades=trades, date = date_today, exit = a)

@app.route('/', methods=['POST'])
def my_form_post():
    
    # input_nopol = request.form['text_box']
    if request.method == 'POST':
        with open(trade_exit_filename, 'w') as f:
            f.write(str('exit'))

    kite = access_token(filename = filename,type = "kiteconnect")
    

    # time.sleep(2)
    with open(trade_exit_filename, 'r') as f:
        f.seek(0)
        a = f.readline()
    trades = kite.trades()
    
    return render_template('index.html', trades = trades,date = date_today, exit = a)


if __name__ == '__main__':
    app.debug = True
    app.run('0.0.0.0')