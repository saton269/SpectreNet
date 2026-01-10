from flask import Flask, request, jsonify
import time
import json
import random


app = Flask(__name__)

# Load airports and triggers from JSON
with open("atc_config.json", "r") as f:
    atc_config = json.load(f)

ATC_TOWERS = atc_config["airports"]
ATC_TRIGGERS = atc_config["triggers"]
TRIGGER_PHRASES = atc_config["trigger_phrases"]

# Per-frequency storage
channels = {}

DEFAULT_FREQUENCY = 16


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


# ---------------------------
# ATC Bot Logic
# ---------------------------
def handle_atc(message_text, channel):
    """
    Process ATC bot responses.
    Message format: AIRPORT_CODE, CALLSIGN, request ...
    """
    parts = [x.strip() for x in message_text.split(",")]
    if len(parts) < 3:
        return None  # Not enough parts for ATC

    airport_code, callsign, request_text = parts[0].upper(), parts[1], parts[2].lower()
    tower = ATC_TOWERS.get(airport_code)
    if not tower:
        return None  # Unknown airport

    # Only respond if message is on the airport's frequency
    freq = tower.get("frequency", DEFAULT_FREQUENCY)
    if channel != freq:
        return None

    # Check triggers
    for action, phrases in TRIGGER_PHRASES.items():
        for phrase in phrases:
            if phrase in request_text:

                # Pick a random response template
                template = random.choice(ATC_TRIGGERS[action])

                # Select runway based on action
                if action == "landing":
                    runway = random.choice(tower["landings"])

                if action in("takeoff", "taxi"):
                    runway = random.choice(tower["departures"])


                # Taxiway logic (only if template needs it)
                if "{taxiway}" in template and "taxiways" in tower:
                    taxiway = tower["taxiways"][0]
                    response_text = template.format(
                        landings=runway,
                        departures=runway,
                        taxiway=taxiway
                    )
                else:
                    response_text = template.format(
                        landings=runway,
                        departures=runway
                    )

                response = f"{callsign}, {response_text}"

                return response, tower.get("sender", f"{airport_code} ATC")

    return None  # No trigger matched

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

    atc_response = handle_atc(text, freq)
    if atc_response:
        atc_text, atc_sender = atc_response
        atc_msg = {
            "id": channel["next_id"],
            "text": atc_text,
            "sender": atc_sender
        }
        channel["messages"].append(atc_msg)
        channel["next_id"] += 1

    # message cap
    if len(channel["messages"]) > MAX_MESSAGES:
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
