# Here goes the main code for the binance streaming project
# Github copilot should be able to edit this file and add the necessary code 
# to connect to the binance websocket and stream the data to a spark cluster

#################
##### setup #####
#################

import os
# install pyspark
os.system("pip install -q pyspark")

# install jdk 17
os.system("sudo apt-get update -qq")
os.system("sudo apt-get install openjdk-17-jdk-headless -qq")
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# start spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Streaming").getOrCreate()


######################
##### TCP Server #####
######################



# start tcp server for streaming binance data

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

    # bind the socket to a local address and port
    s.bind(('localhost', 9999))

    # listen for incoming connections
    s.listen(1)
    print("TCP server is listening on port 9999...")

    # accept a connection
    conn, addr = s.accept()
    print(f"Connection from {addr} has been established.")

# start the websocket connection to binance and stream the data to the tcp server
def crypto_websocket():
    import websocket, json
    global conn


    def on_message(ws, message):
        global conn
        data = json.loads(message)   
        if data.get("channel") == "ticker":
            try:
                data = data.get("data")[0]
                conn.sendall(json.dumps(data).encode('utf-8'))
            except:
                pass

    def on_open(ws):
        ws.send(json.dumps({
            "method": "subscribe",
            "params": {
                "channel": "ticker",   # <-- ticker data
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

import threading

# start the tcp server in a separate thread
tcp_thread = threading.Thread(target=start_tcp_server, daemon=True)
tcp_thread.start()
# start the websocket connection in a separate thread
websocket_thread = threading.Thread(target=crypto_websocket, daemon=True)
websocket_thread.start()

# read the data from the tcp server and print it to the console
query = spark.readStream.format("socket")\
    .option("host", "localhost")\
    .option("port", 9999)\
    .load()

def process_batch(df, epoch_id):
    df.show()

query.writeStream.format("console").foreachBatch(process_batch).start()