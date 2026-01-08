from flask import Flask, request, jsonify
import time

app = Flask(__name__)

# Per-frequency storage
channels = {}


MAX_MESSAGES = 100  # keep list small
FREQUENCY_EXPIRE_SECONDS = 30 * 60  # 30 minutes

def get_channel(freq):
    now = time.time()

    if freq not in channels:
        channels[freq] = {
            "next_id": 1,
            "messages": [],
            "last_active": now
        }

    channels[freq]["last_active"] = now
    return channels[freq]

def cleanup_expired_frequencies():
    now = time.time()
    expired = []

    for freq, data in channels.items():
        if now - data["last_active"] > FREQUENCY_EXPIRE_SECONDS:
            expired.append(freq)

    for freq in expired:
        del channels[freq]

@app.route("/")
def index():
    cleanup_expired_frequencies()
    return jsonify({
        "status": "online",
        "active_frequencies": len(channels)
    })


@app.route("/state", methods=["GET"])
def get_state():
    cleanup_expired_frequencies()

    freq = int(request.args.get("frequency", 16))

    if freq not in channels:
        return jsonify({
            "frequency": freq,
            "last_id": 0
        })

    channel = channels[freq]

    return jsonify({
        "frequency": freq,
        "last_id": channel["next_id"] - 1
    })


@app.route("/send", methods=["POST"])
def send_message():
    cleanup_expired_frequencies()

    data = request.get_json(force=True)

    freq = int(data.get("frequency", 16))
    text = data.get("text", "").strip()
    sender = data.get("sender", "UNKNOWN")

    if not text:
        return jsonify({"error": "empty message"}), 400

    channel = get_channel(freq)

    msg = {
        "id": channel["next_id"],
        "text": text,
        "sender": sender
    }

    channel["messages"].append(msg)
    channel["next_id"] += 1

    # message cap
    if len(channel["messages"]) > MAX_MESSAGES_PER_FREQ:
        channel["messages"].pop(0)

    return jsonify({
        "status": "sent",
        "id": msg["id"]
    })


@app.route("/fetch", methods=["GET"])
def fetch_messages():
    cleanup_expired_frequencies()

    freq = int(request.args.get("frequency", 16))
    since_id = int(request.args.get("since_id", 0))

    if freq not in channels:
        return jsonify([])

    channel = get_channel(freq)

    msgs = [
        m for m in channel["messages"]
        if m["id"] > since_id
    ]

    return jsonify(msgs)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
