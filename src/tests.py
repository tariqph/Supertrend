import os, sys
fpath = os.path.abspath(os.path.join(os.path.dirname(__file__),"access_config"))
sys.path.append(fpath)
import psycopg2
from config import config
import pandas as pd 
params = config()
    # connect to the PostgreSQL serve
connection = psycopg2.connect(**params)
connection.autocommit = True

cursor = connection.cursor()

# query = """ SELECT * FROM banknifty_option_data; """

# cursor.execute(query)
columns = ['index','instrument_token','date_time','Close','High','Low']
# df = pd.DataFrame(cursor.fetchall())

# df.to_csv("test.csv")

df = pd.read_csv("/home/narayana_tariq/test.csv")
df.columns = columns
# print(df)
tablename  = 'banknifty_option_data'
postgres_insert_query = f""" INSERT INTO {tablename} (instrument_token,date_time, 
                            ltp, high, low) 
                            VALUES (%s,%s,%s,%s,%s)"""

# import pandas as pd 
# data = pd.read_csv("/home/narayana_tariq/Zerodha/Supertrend_strat/test.csv")

for index, row in df.iterrows():
    record_to_insert = (row['instrument_token'],row['date_time'], row['Close'],
                        row['High'], row["Low"])
                                    
    cursor.execute(postgres_insert_query, record_to_insert)