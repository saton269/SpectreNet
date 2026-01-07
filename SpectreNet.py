from flask import Flask, request, jsonify
import time

app = Flask(__name__)

CHANNEL = 16

# In-memory message store
messages = []  # each item: {"time": float, "text": str}

@app.route("/")
def index():
    return jsonify({"status": "online", "channel": CHANNEL})

@app.route("/send", methods=["POST"])
def send_message():
    data = request.get_json(force=True)
    text = data.get("text", "").strip()

    if not text:
        return jsonify({"error": "empty message"}), 400

    messages.append({
        "time": time.time(),
        "text": text
    })

    # keep list small
    if len(messages) > 100:
        messages.pop(0)

    return jsonify({"status": "sent"})

@app.route("/fetch", methods=["GET"])
def fetch_messages():
    since = float(request.args.get("since", 0))
    timeout = 20  # seconds
    start = time.time()

    while time.time() - start < timeout:
        new_msgs = [m for m in messages if m["time"] > since]
        if new_msgs:
            return jsonify(new_msgs)
        time.sleep(0.25)

    return jsonify([])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
