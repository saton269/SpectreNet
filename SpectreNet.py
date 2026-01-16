from flask import Flask, request, jsonify # pyright: ignore[reportMissingImports]
import time
import json
import random


app = Flask(__name__)

# Load airports and triggers from JSON
with open("atc_config.json", "r") as f:
    atc_config = json.load(f)

ATC_TOWERS = atc_config["airports"]
ATC_RESPONSES = atc_config["responses"]
TRIGGER_PHRASES = atc_config["trigger_phrases"]
HANDOFF_MESSAGES = atc_config.get("handoff_messages", {})
ROLE_MAP = atc_config.get("role_map", {})
REDIRECT_MESSAGES = atc_config.get("redirects", {})
UNKNOWN_MESSAGES = atc_config.get("unknown", {})

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

def format_freq(freq):
    if freq < 1000:
        return f"CH {freq}"
    mhz = freq // 1000
    khz = freq % 1000
    khz_str = f"{khz:03d}"
    return f"{mhz}.{khz_str} MHz"

def normalize_atc_message(message_text: str, sender_name: str):
    """
    Supports:
      AIRPORT, CALLSIGN, request ...
      AIRPORT, request ...

    Returns: (airport_code, callsign, request_text) or (None, None, None)
    """
    # Limit splits so commas in the request are preserved
    parts = [x.strip() for x in message_text.split(",", 2)]

    if len(parts) < 2:
        return None, None, None

    airport_code = parts[0].upper()

    if len(parts) == 2:
        # Example: "SLHA, request takeoff."
        callsign = sender_name
        request_text = parts[1]
        return airport_code, callsign, request_text

    # Example: "SLHA, N463R6, request takeoff."
    callsign = parts[1].strip() or sender_name
    request_text = parts[2]
    return airport_code, callsign, request_text


# ---------------------------
# ATC Bot Logic
# ---------------------------
def handle_atc(message_text, channel, sender_name):
    """
    Process ATC bot responses.
    Message format: AIRPORT_CODE, CALLSIGN, request ...
    """

    # --- Parse message ---
        # --- Parse & normalize (fills callsign if user omitted it) ---
    airport_code, callsign, request_text = normalize_atc_message(
        message_text,
        sender_name,
    )

    if not airport_code or not request_text:
        return None

    request_text = request_text.lower()


    tower = ATC_TOWERS.get(airport_code)
    if not tower:
        return None

    # --- Base frequencies for this airport ---
    tower_freq = tower.get("tower_frequency", tower.get("frequency", DEFAULT_FREQUENCY))
    ground_freq = tower.get("ground_frequency", tower_freq)  # same as tower if no ground freq

    # --- Classify the request intent ---
    # Ground ONLY handles taxi / pushback
    is_ground_request = any(
        phrase in request_text
        for phrase in TRIGGER_PHRASES.get("taxi", [])
    )

    # Tower-style requests (takeoff / landing, you can add more actions)
    is_tower_request = any(
        phrase in request_text
        for action in ("takeoff", "landing")
        for phrase in TRIGGER_PHRASES.get(action, [])
    )

    # =========================================================
    # 1) Redirects: real ground/tower requests on the *wrong* freq
    # =========================================================

    # Taxi/pushback (ground) on Tower frequency -> redirect to Ground
    if (
        tower_freq != ground_freq
        and is_ground_request
        and channel == tower_freq
        and channel != ground_freq
    ):
        templates = REDIRECT_MESSAGES.get("tower_to_ground", [])
        if templates:
            template = random.choice(templates)
            text = template.format(
                callsign=callsign,
                airport=airport_code,
                frequency=format_freq(ground_freq)
            )
            text = text[0].upper() + text[1:]

            tower_sender = tower.get("tower_sender", f"{airport_code} Tower")
            return text, tower_sender

        # No templates? just ignore like before
        return None

    # Takeoff/landing (tower) on Ground frequency -> redirect to Tower
    if (
        tower_freq != ground_freq
        and is_tower_request
        and channel == ground_freq
        and channel != tower_freq
    ):
        templates = REDIRECT_MESSAGES.get("ground_to_tower", [])
        if templates:
            template = random.choice(templates)
            text = template.format(
                callsign=callsign,
                airport=airport_code,
                frequency=format_freq(tower_freq)
            )
            text = text[0].upper() + text[1:]

            ground_sender = tower.get("ground_sender", f"{airport_code} Ground")
            return text, ground_sender

        return None

    # =========================================================
    # 2) If the tuned frequency doesn't belong to this airport, ignore
    #    (For single-frequency airports, tower_freq == ground_freq)
    # =========================================================
    if channel not in (tower_freq, ground_freq):
        return None

    # =========================================================
    # 3) Determine role based on the frequency we are actually tuned to
    # =========================================================
    if channel == ground_freq and ground_freq != tower_freq:
        role = "ground"
        sender_name = tower.get("ground_sender", f"{airport_code} Ground")
    else:
        # Default: tower (covers both tower freq and single-frequency airports)
        role = "tower"
        sender_name = tower.get("tower_sender", f"{airport_code} Tower")

    # =========================================================
    # 4) Normal ATC trigger matching
    # =========================================================
    for action, phrases in TRIGGER_PHRASES.items():
        for phrase in phrases:
            if phrase in request_text:

                template = random.choice(ATC_RESPONSES[action])

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

                # --- Ground → Tower handoff (only when actually on Ground) ---
                if role == "ground" and action == "taxi":
                    if tower_freq != ground_freq:
                        if random.random() < 0.8:  # 80% chance
                            handoffs = HANDOFF_MESSAGES.get("ground_to_tower", [])
                            if handoffs:
                                handoff_template = random.choice(handoffs)
                                formatted_freq = format_freq(tower_freq)
                                handoff_text = handoff_template.format(
                                    airport=airport_code,
                                    frequency=formatted_freq
                                )
                                response_text = f"{response_text}, {handoff_text}"

                response = f"{callsign}, {response_text}"
                capitalized = response[0].upper() + response[1:]

                # Use per-role sender_name (Tower / Ground)
                return capitalized, sender_name

    # =========================================================
    # 5) Fallback: unknown / unrecognized request on a valid freq
    # =========================================================
    templates = UNKNOWN_MESSAGES.get(role) or UNKNOWN_MESSAGES.get("default", [])
    if templates:
        template = random.choice(templates)
        unknown_text = template.format(
            callsign=callsign,
            airport=airport_code
        )
        unknown_text = unknown_text[0].upper() + unknown_text[1:]

        return unknown_text, sender_name

    # No unknown templates defined, behave like original: silent
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

    atc_response = handle_atc(text, freq, sender)
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
