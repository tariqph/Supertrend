import os
import threading
import datetime 
from json.decoder import JSONDecodeError
import logging
import datetime 
import yaml
import time
import os, sys
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__),"access_config"))
sys.path.append(fpath)
import psycopg2
from config import config
import pandas as pd 
# Reading config from yaml file

def database_cleanup():
    logging.info("Cleaning the database with older data")
    
    params = config()
    connection = psycopg2.connect(**params)
    connection.autocommit = True

    cursor = connection.cursor()
    date_time_now = datetime.datetime.now()
    date_time_del = date_time_now - datetime.timedelta(days=4)
    date_time_del = date_time_del.strftime("%Y-%m-%d %H:%M:%S")
    query = f"DELETE FROM banknifty_option_data where date_time <= \'{(date_time_del)}\'"
    cursor.execute(query)
    
def gen_tokens():
    
# Generating Access Tokens
    logging.info("Generating day account access and instrument tokens")
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), "access_config"))

    os.system('cd "'+ path +'" && python generate_access_token.py')
    # # Generating instrument tokens
    print("Generating Instrument tokens")
    os.system('cd "'+ path +'" && python generate_banknifty_tokens.py')


def celery_websocket():

    # Reading config from file
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","config.yml"))
    try: 
        with open (config_path, 'r') as file:
            config = yaml.safe_load(file)
    except Exception as e:
        print('Error reading the config file')

    logging.info('Deleting queues from rabbitmq')
    rabbitmq_del_queue = './rabbitmqadmin -f tsv -q list queues name | while read queue; do ./rabbitmqadmin -q delete queue name=${queue}; done'
    # rabbitmq_del_queue = 'python.exe rabbitmqadmin -f tsv -q list queues name | while read queue; do python.exe rabbitmqadmin -q delete queue name=banknifty; done'
    
    os.system(rabbitmq_del_queue)

    logging.info("Collecting Data")
# Class mythread inheriting thread class
    class myThread (threading.Thread):
        def __init__(self, command):
            threading.Thread.__init__(self)
            self.cmd = command

        def run(self):
            print ("Starting " + self.cmd)
            os.system(self.cmd)
            print ("Exiting " + self.cmd)

    worker = os.path.abspath(os.path.join(os.path.dirname(__file__), "publish_database"))
    ticker_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "ticker"))
    flask_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "flask_app"))
    
    
    queue_1 = config['rabbitmq']['queues']['banknifty']
    
    # Commands to run parallely
    lstCmd=['cd "' + worker + f'" && celery -A insert_database  worker -Q {queue_1} --concurrency=1  --loglevel=info -P  eventlet -n worker1@%h', 
            'cd "' + ticker_path +'" && python banknifty_conn.py',
            'cd "' + flask_path +'" && python app.py']   

    # Create new threads
    thread1 = myThread(lstCmd[0])
    thread2 = myThread(lstCmd[1])
    thread3 = myThread(lstCmd[2])
    
    

    threads = [thread1,thread2, thread3]

    # Start new Threads
    thread1.start()
    thread2.start()
    thread3.start()
    
    while True:
        time.sleep(2)
        time_string = "15:30:00"
        time_now = datetime.datetime.now().time()
        exit_time = datetime.datetime.strptime(time_string,"%H:%M:%S").time()
        for i in range(len(threads)):
            if threads[i].is_alive():
                # print('is alive')
                # logging.info('is alive')
                
                continue
                
            else:
                # The currency data is collected using conn_1 so it runs till 17:00
                if(time_now < exit_time ):

                    logging.info('Time:%s',time_now)
                    logging.info('Thread died: %s',lstCmd[i])
                    new_thread = myThread(lstCmd[i])
                    new_thread.start()
                    threads[i] = new_thread
                    logging.info('Thread restarted')
                    
        if(time_now.hour == 15 and time_now.minute == 30):
            logging.info('Run over')
            break
  

if __name__ == "__main__":
    

    date = datetime.date.today()
    log_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "..",f"logs/run_{date}.log"))
    logging.basicConfig(filename=log_file, level=logging.INFO)

    time_now = datetime.datetime.now()
    logging.info('Today date is %s',date)
    logging.info('Started run at %s',time_now)
    
    exec_date_file = "/home/narayana_tariq/Zerodha/Supertrend/last_execution_date.txt"

    print(date)
    with open(exec_date_file, 'a+') as infile:
        try:
            # prev = json.load(infile)
           infile.seek(0)
           a = infile.readline()
           print(a)
        except JSONDecodeError:
            print("exception")
            pass
    
       
    
    if(a == ''):
        database_cleanup()
        
        gen_tokens()
        celery_websocket()
        
    else:
        date_exec = datetime.datetime.strptime(a, "%Y-%m-%d")
        
        if(date_exec.date() == date):
            celery_websocket()

        else:
            gen_tokens()
            celery_websocket()
    
    