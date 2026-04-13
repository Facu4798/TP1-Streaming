# Kraken WebSocket (gratuito, sin API key)
import websocket, json

message_count = 0
avg_time = 0
import datetime
last_message_time = datetime.datetime.now()

def on_message(ws, message):
    global conn
    data = json.loads(message)   
    if data.get("channel") == "ticker":
        try:
            data = data.get("data")[0]
            conn.sendall(json.dumps(data).encode('utf-8'))
        except:
            pass

    # data = json.loads(message)

    # if data.get("channel") == "ticker":
    #     print(json.dumps(data, indent=4))
    #     print("\n\n")
    # if data.get("channel") == "ticker":

    #     for item in data.get("data", []):
    #         print(f"{item['symbol']}: Ask={item['ask']} | Bid={item['bid']}")

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