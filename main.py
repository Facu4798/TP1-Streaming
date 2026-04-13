# Here goes the main code for the binance streaming project
# Github copilot should be able to edit this file and add the necessary code 
# to connect to the binance websocket and stream the data to a spark cluster

#################
##### setup #####
#################

import os
import time
os.system("clear")
# install pyspark
os.system("pip install -q pyspark")

# install mysql driver for python
os.system("pip install -q sqlalchemy pymysql")

# install jdk 17
os.system("sudo apt-get update -qq")
os.system("sudo apt-get install openjdk-17-jdk-headless -qq")
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"



# start spark session
from pyspark.sql import functions as sf
from pyspark.sql import types as st
from pyspark.sql import SparkSession
spark = SparkSession.builder\
    .appName("Streaming")\
    .getOrCreate()



# imports
import threading
import time
import queue
import socket
import json
import pandas as pd


#######################
##### MySQL setup #####
#######################


# Install MySQL server
import os

# Install MySQL server
os.system("sudo apt-get update -y")
os.system("sudo apt-get install -y mysql-server")
os.system("sudo systemctl start mysql")

# Install Python libraries
os.system("pip install mysql-connector-python pandas sqlalchemy pymysql")


# Remove --skip-grant-tables from MySQL config if present
os.system("sudo sed -i 's/^skip-grant-tables//' /etc/mysql/mysql.conf.d/mysqld.cnf")
os.system("sudo sed -i 's/^skip_grant_tables//' /etc/mysql/mysql.conf.d/mysqld.cnf")

# Stop any running instance, then start clean
os.system("sudo service mysql stop")
os.system("sudo service mysql start")

# Give MySQL a moment to finish starting
import time
import mysql.connector
time.sleep(3)

# Create DB, user, grant privileges
sql = """
CREATE DATABASE IF NOT EXISTS crypto;
CREATE USER IF NOT EXISTS 'admin_user'@'localhost' IDENTIFIED BY 'StrongPassword123!';
GRANT ALL PRIVILEGES ON crypto.* TO 'admin_user'@'localhost';
FLUSH PRIVILEGES;
"""

with open("/tmp/setup.sql", "w") as f:
    f.write(sql)

os.system("sudo mysql < /tmp/setup.sql")
os.system("rm /tmp/setup.sql")
print("Database and user created.")







######################
##### TCP Server #####
######################

# Queue to pass data from WebSocket to TCP server
data_queue = queue.Queue()

def start_tcp_server():
    import socket
    import time
    import json
    import random

    global conn

    # liberate the port 9999 for streaming data
    os.system("fuser -k 9999/tcp")

    # create a socket object
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # bind the socket to a local address and port
    s.bind(('localhost', 9999))

    # listen for incoming connections
    s.listen(10)
    print("TCP server is listening on port 9999...")
    print("\n\n")

    # accept a connection
    conn, addr = s.accept()
    print(f"Connection from {addr} has been established.")
    
    while True:
        try:
            data = data_queue.get(timeout=1)
            conn.sendall(data.encode('utf-8') + b'\n')
        except queue.Empty:
            pass


def crypto_websocket():
    import websocket, json
    global data_queue

    def on_message(ws, message):
        data = json.loads(message)   
        if data.get("channel") == "ticker":
            data = data.get("data")[0]
            data_queue.put(json.dumps(data))

    def on_open(ws):
        ws.send(json.dumps({
            "method": "subscribe",
            "params": {
                "channel": "ticker",
                "symbol": ["BTC/USD"]
            }
        }))

    ws = websocket.WebSocketApp(
        "wss://ws.kraken.com/v2",
        on_message=on_message,
        on_open=on_open
    )
    ws.run_forever()




##############################
##### start reading data #####
##############################



# start the tcp server in a separate thread
tcp_thread = threading.Thread(target=start_tcp_server, daemon=True)
tcp_thread.start()

time.sleep(5) # wait for the tcp server to start before starting the websocket connection

# start the websocket connection in a separate thread
websocket_thread = threading.Thread(target=crypto_websocket, daemon=True)
websocket_thread.start()
print("TCP server and websocket connection started in separate threads.")
print("\n\n")


# read the data from the tcp server and print it to the console
query = spark.readStream.format("socket")\
    .option("host", "localhost")\
    .option("port", 9999)\
    .load()



def process_batch(df, epoch_id):
    df = df.withColumn("json", sf.from_json("value", st.StructType([
        st.StructField("symbol", st.StringType()),
        st.StructField("bid", st.DoubleType()),
        st.StructField("bid_qty", st.DoubleType()),
        st.StructField("ask", st.DoubleType()),
        st.StructField("ask_qty", st.DoubleType()),
        st.StructField("last", st.DoubleType()),
        st.StructField("volume", st.DoubleType()),
        st.StructField("vwap", st.DoubleType()),
        st.StructField("low", st.DoubleType()),
        st.StructField("high", st.DoubleType()),
        st.StructField("change", st.DoubleType()),
        st.StructField("change_pct", st.DoubleType()),
        st.StructField("timestamp", st.StringType())
    ]))).select("json.*")

    pandas_df = df.toPandas()
    
    from sqlalchemy import create_engine
    engine = create_engine("mysql+pymysql://admin_user:StrongPassword123!@127.0.0.1:3306/crypto")
    pandas_df.to_sql(
        "ticker", 
        con=engine, 
        if_exists="append", 
        index=False
    )
    
    print(f"Batch {epoch_id} written to MySQL.")

query.writeStream.format("console")\
    .option("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")\
    .foreachBatch(process_batch)\
    .trigger(processingTime="5 seconds")\
    .start().awaitTermination()


