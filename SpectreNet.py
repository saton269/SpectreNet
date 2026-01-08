from flask import Flask, request, jsonify
import time

app = Flask(__name__)

CHANNEL = 16

# In-memory message store
messages = []  # each item: {"time": float, "text": str}
next_id = 1    # incremental message ID

MAX_MESSAGES = 100  # keep list small

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
        "id": next_id,
        "text": text
    })

    next_id += 1

    # keep list small
    if len(messages) > MAX_MESSAGES:
        messages.pop(0)

    return jsonify({"status": "sent", "id": next_id - 1})

@app.route("/fetch", methods=["GET"])
def fetch_messages():
    since_id = int(request.args.get("since_id", 0))
    new_msgs = [m for m in messages if m["id"] > since_id]
    return jsonify(new_msgs)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
