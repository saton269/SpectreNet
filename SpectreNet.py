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
HANDOFF_MESSAGES = atc_config.get("handoff_messages", {})
ROLE_MAP = atc_config.get("role_map", {})

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

    # --- Parse message ---
    parts = [x.strip() for x in message_text.split(",")]
    if len(parts) < 3:
        return None

    airport_code = parts[0].upper()
    callsign = parts[1]
    request_text = parts[2].lower()

    tower = ATC_TOWERS.get(airport_code)
    if not tower:
        return None

    # --- Determine role ---
    # Ground ONLY handles taxi / pushback
    is_ground_request = any(
        phrase in request_text
        for phrase in TRIGGER_PHRASES.get("taxi", [])
    )

    if is_ground_request:
        role = "ground"
        freq_to_check = tower.get(
            "ground_frequency",
            tower.get("frequency", DEFAULT_FREQUENCY)
        )
        sender_name = tower.get(
            "ground_sender",
            f"{airport_code} Ground"
        )
    else:
        role = "tower"
        freq_to_check = tower.get(
            "tower_frequency",
            tower.get("frequency", DEFAULT_FREQUENCY)
        )
        sender_name = tower.get(
            "tower_sender",
            f"{airport_code} Tower"
        )

    # --- Frequency must match ---
    if channel != freq_to_check:
        return None

    # --- Match triggers ---
    for action, phrases in TRIGGER_PHRASES.items():
        for phrase in phrases:
            if phrase in request_text:

                template = random.choice(ATC_TRIGGERS[action])

                # --- Runway selection ---
                if action == "landing":
                    runway = random.choice(
                        tower.get("landings", tower.get("runways", []))
                    )
                elif action in ("takeoff", "taxi"):
                    runway = random.choice(
                        tower.get("departures", tower.get("runways", []))
                    )
                else:
                    runway = random.choice(tower.get("runways", []))

                # --- Build response ---
                if "{taxiway}" in template and "taxiways" in tower:
                    taxiway = random.choice(tower["taxiways"])
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

                # --- Ground → Tower handoff ---
                if role == "ground" and action == "taxi":
                    handoffs = HANDOFF_MESSAGES.get("ground_to_tower", [])

                    if handoffs:
                        handoff_template = random.choice(handoffs)

                        tower_freq = tower.get(
                            "tower_frequency",
                            tower.get("frequency", DEFAULT_FREQUENCY)
                        )

                        handoff_text = handoff_template.format(
                            airport=airport_code,
                            frequency=tower_freq
                        )

                        response_text = f"{response_text} {handoff_text}"


                # --- Final ATC message ---
                return response_text, tower.get("sender", f"{airport_code} ATC")

    return None


@app.route("/")
def index():
    cleanup_expired_frequencies()
    return jsonify({
        "status": "online",
        "active_frequencies": len(channels)
    })

@app.route("/atc/lookup", methods=["GET"])
def atc_lookup():
    airport = request.args.get("airport", "").upper()
    role = request.args.get("role", "tower").lower()  # "tower" or "ground"

    tower = ATC_TOWERS.get(airport)
    if not tower:
        return jsonify({"error": "unknown airport"}), 404

    # Determine frequency based on role
    if role == "ground":
        freq = tower.get("ground_frequency", tower.get("frequency", DEFAULT_FREQUENCY))
        sender = tower.get("ground_sender", f"{airport} Ground")
    else:
        freq = tower.get("tower_frequency", tower.get("frequency", DEFAULT_FREQUENCY))
        sender = tower.get("tower_sender", f"{airport} Tower")

    return jsonify({
        "airport": airport,
        "frequency": freq,
        "sender": sender
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
