
def start_api_thread():
    import threading
    from flask import Flask, jsonify, send_from_directory

    global data_ph
    global current_batch

    data_ph = {}
    current_batch = 0

    app = Flask(__name__)

    @app.route("/", methods=["GET"])
    def index():
        return "", 200


  
    @app.route("/api/get-batch-number", methods=["GET"])
    def get_batch_number():
        return jsonify({"batch_number": current_batch})
    
    @app.route("/api/set-batch-number", methods=["POST"])
    def set_batch_number():
        from flask import request
        data = request.get_json()
        global current_batch
        current_batch = data.get("batch_number", current_batch)
        return jsonify({"status": "success", "current_batch": current_batch})



    @app.route("/api/data", methods=["POST"])
    def post_data():
        from flask import request
        data = request.get_json()
        global data_ph
        data_ph[data.get("symbol")] = data
        
        return jsonify({"status": "success"})


   

    @app.route("/api/timedashboard")
    def timedashboard():
        return send_from_directory(".", "main2.html")

    @app.route("/api/summarydashboard")
    def summarydashboard():
        return send_from_directory(".", "main.html")




    # ── Thread launcher ──────────────────────────────────────

    def run_api():
        app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)


    t = threading.Thread(target=run_api, daemon=True)
    t.start()
    print("API running on http://localhost:5000")