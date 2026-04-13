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
os.system("pip install -q sqlalchemy pymysql flask flask-cors")

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
import sqlite3

# initiate the sqlite database and delete the data if exists
connection = sqlite3.connect("data.db")
cursor = connection.cursor()
cursor.execute("DROP TABLE IF EXISTS crypto_data;")
connection.commit()
cursor.close()
connection.close()



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








# start the tcp server in a separate thread
tcp_thread = threading.Thread(target=start_tcp_server, daemon=True)
tcp_thread.start()

time.sleep(5) # wait for the tcp server to start before starting the websocket connection

# start the websocket connection in a separate thread
websocket_thread = threading.Thread(target=crypto_websocket, daemon=True)
websocket_thread.start()
print("TCP server and websocket connection started in separate threads.")
print("\n\n")


#####################
##### Start API #####
#####################

import sqlite3
import threading
from flask import Flask, jsonify, send_from_directory

app = Flask(__name__)


# ── Routes ──────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory(".", "main.html")


@app.route("/api/data")
def get_data():
    conn = sqlite3.connect("data.db")
    conn.row_factory = sqlite3.Row  # makes rows behave like dicts
    rows = conn.execute(
        "SELECT * FROM crypto_data ORDER BY timestamp DESC LIMIT 100"
    ).fetchall()
    conn.close()
    return jsonify([dict(row) for row in rows])

@app.route("/api/latest")
def get_latest():
    conn = sqlite3.connect("data.db")
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM crypto_data ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()
    conn.close()
    return jsonify(dict(row) if row else {})

# ── Thread launcher ──────────────────────────────────────

def run_api():
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)

def start_api_thread():
    t = threading.Thread(target=run_api, daemon=True)
    t.start()
    print("API running on http://localhost:5000")

start_api_thread()


##############################
##### start reading data #####
##############################


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

    # # write the batch to a csv file
    # df.toPandas().to_csv(
    #     "data.csv",
    #     index=False,
    #     mode="a",
    #     header=not os.path.exists("data.csv")
    # )

    # print(f"Written batch {epoch_id} to CSV file.")

    # write to sqlite
    conn = sqlite3.connect("data.db")
    df.toPandas().to_sql(
        "crypto_data",
        conn,
        if_exists="append",
        index=False
    )
    conn.close()
    print(f"Written batch {epoch_id} to SQLite database.")
    
    

query.writeStream.format("console")\
    .option("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")\
    .foreachBatch(process_batch)\
    .trigger(processingTime="5 seconds")\
    .start()



