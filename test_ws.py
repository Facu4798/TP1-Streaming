# Kraken WebSocket (gratuito, sin API key)
import websocket, json

def on_message(ws, message):
    data = json.loads(message)

    if data.get("channel") == "trade":
        print(json.dumps(data, indent=4))
        print("\n\n")
    # if data.get("channel") == "ticker":

    #     for item in data.get("data", []):
    #         print(f"{item['symbol']}: Ask={item['ask']} | Bid={item['bid']}")

def on_open(ws):
    ws.send(json.dumps({
        "method": "subscribe",
        "params": {
            "channel": "trade",   # <-- trades individuales
            "symbol": ["BTC/USD"]
        }
    }))

ws = websocket.WebSocketApp(
    "wss://ws.kraken.com/v2",
    on_message=on_message,
    on_open=on_open
)
ws.run_forever()