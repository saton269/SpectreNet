from flask import Flask, request, jsonify # pyright: ignore[reportMissingImports]
import time
import json
import random
import re
from collections import deque


app = Flask(__name__)

# Load airports and triggers from JSON
with open("atc_config.json", "r") as f:
    atc_config = json.load(f)

with open("channels.json") as f:
    CHANNELS_CONFIG = json.load(f)["channels"]

with open("weather.json") as f:
    WEATHER_CONFIG = json.load(f)

CHANNELS_BY_FREQ = {}
for channel_id, cfg in CHANNELS_CONFIG.items():
    freq = cfg["frequency"]

    tx_policy = cfg.get("tx_policy", {})
    if tx_policy.get("mode") == "whitelist_uuid":
        tx_policy["allowed_uuids_set"] = set(tx_policy.get("allowed_uuids", []))

    CHANNELS_BY_FREQ[freq] = {
        "id": channel_id,
        **cfg,
        "tx_policy": tx_policy,
    }

ATC_TOWERS = atc_config["airports"]
ATC_RESPONSES = atc_config["responses"]
TRIGGER_PHRASES = atc_config["trigger_phrases"]
HANDOFF_MESSAGES = atc_config.get("handoff_messages", {})
ROLE_MAP = atc_config.get("role_map", {})
REDIRECT_MESSAGES = atc_config.get("redirects", {})
UNKNOWN_MESSAGES = atc_config.get("unknown", {})
AUTO_CLEAR_RESPONSES = atc_config.get("auto_clear", {})
SEQUENCING = atc_config.get("sequencing", {})
OCCUPANCY = SEQUENCING.get("occupancy_seconds", {})
HOLD_MESSAGES = SEQUENCING.get("holds", {})
INVALID_RUNWAY_MESSAGES = atc_config.get("invalid_runway", {})


ZONE_DEFAULTS = WEATHER_CONFIG.get("defaults", {})
ZONE_CONFIGS = WEATHER_CONFIG.get("zones", {})
CONDITION_CONFIGS = WEATHER_CONFIG.get("conditions", {})

WEATHER_ZONES: dict[str, list[str]] = {}
WEATHER_STATE: dict[str, dict] = {}



# Per-frequency storage
channels = {}

RUNWAY_STATE = {}
RUNWAY_END_TO_PHYSICAL: dict[str, dict[str, str]] = {}   # ICAO -> { "27L": "RWY_L", ... }
VALID_ENDS_BY_ACTION: dict[str, dict[str, set[str]]] = {}

DEFAULT_FREQUENCY = 16

RUNWAY_RE = re.compile(r"\b(?:runway|rwy)\s*([0-3]?\d)\s*([LRC])?\b", re.IGNORECASE)
PILOT_ASSIGNED_RUNWAY = {}


MAX_MESSAGES = 100  # keep list small
FREQUENCY_EXPIRE_SECONDS = 30 * 60  # 30 minutes

def get_channel(freq):
    now = time.time()

    if freq not in channels:
        channels[freq] = {
            "next_id": 1,
            "messages": deque(maxlen=MAX_MESSAGES),
            "last_active": now
        }

    channels[freq]["last_active"] = now
    return channels[freq]

def can_transmit_on_frequency(freq, sender_uuid):
    channel = CHANNELS_BY_FREQ.get(freq)
    if not channel:
        # Not a dedicated channel – treat as normal ATC / regular freq
        return True

    policy = channel.get("tx_policy", {})
    mode = policy.get("mode", "open")

    if mode == "open":
        return True

    if mode == "server_only":
        # Only internal/server-injected messages allowed
        return False

    if mode == "whitelist_uuid":
        allowed = policy.get("allowed_uuids_set")
        if allowed is None:
            # Safety fallback if config was loaded before precompute
            allowed = set(policy.get("allowed_uuids", []))
            policy["allowed_uuids_set"] = allowed
        return sender_uuid in allowed

    # Future: other modes, but default to no if unknown
    return False

def build_runway_indexes():
    for icao, tower in ATC_TOWERS.items():
        icao_u = icao.upper()
        tower["_icao"] = icao_u  # tag config so helpers can find ICAO quickly

        # Map runway-end -> physical runway id
        end_map: dict[str, str] = {}
        for r in tower.get("runways", []):
            phys = (r.get("physical_id") or r.get("id") or "").strip()
            if not phys:
                continue

            for end in (r.get("landing_ends") or []):
                end_map[end.upper()] = phys
            for end in (r.get("takeoff_ends") or []):
                end_map[end.upper()] = phys

        RUNWAY_END_TO_PHYSICAL[icao_u] = end_map
        VALID_ENDS_BY_ACTION[icao_u] = {}  # filled lazily by runway_ends_for_action

build_runway_indexes()

def parse_requested_runway(request_text: str) -> str | None:
    m = RUNWAY_RE.search(request_text or "")
    if not m:
        return None
    num = int(m.group(1))
    if num < 1 or num > 36:
        return None
    side = (m.group(2) or "").upper()
    return f"{num:02d}{side}"

def runway_ends_for_action(tower: dict, action: str) -> set[str]:
    """
    Return valid runway END strings for the given action based on your schema.
    Caches per-airport per-action for speed.
    """
    icao = (tower.get("_icao") or "").upper()
    if icao:
        cached = VALID_ENDS_BY_ACTION.get(icao, {}).get(action)
        if cached is not None:
            return cached

    ends: set[str] = set()

    if action == "landing":
        if tower.get("landings"):
            ends.update(x.upper() for x in tower["landings"])
        else:
            for r in tower.get("runways", []):
                ends.update(x.upper() for x in r.get("landing_ends", []))

    elif action == "takeoff":
        if tower.get("departures"):
            ends.update(x.upper() for x in tower["departures"])
        else:
            for r in tower.get("runways", []):
                ends.update(x.upper() for x in r.get("takeoff_ends", []))

    elif action == "taxi":
        ends = runway_ends_for_action(tower, "takeoff")

    # Cache result
    if icao:
        VALID_ENDS_BY_ACTION.setdefault(icao, {})[action] = ends

    return ends


def physical_id_for_runway_end(tower: dict, runway_end: str) -> str | None:
    """
    Map runway end (e.g. '27L') to runway physical_id using cached lookup.
    """
    runway_end = (runway_end or "").upper()
    icao = (tower.get("_icao") or "").upper()

    if icao:
        hit = RUNWAY_END_TO_PHYSICAL.get(icao, {}).get(runway_end)
        if hit:
            return hit

    # Fallback (in case runways are missing or caches not built)
    for r in tower.get("runways", []):
        if runway_end in [x.upper() for x in r.get("landing_ends", [])]:
            return r.get("physical_id") or r.get("id")
        if runway_end in [x.upper() for x in r.get("takeoff_ends", [])]:
            return r.get("physical_id") or r.get("id")
    return None


def init_weather_zones():
    for icao, ap in ATC_TOWERS.items():
        zone_name = ap.get("weather_zone") or icao.upper()
        ap["weather_zone"] = zone_name  # ensure it's set

        WEATHER_ZONES.setdefault(zone_name, []).append(icao)

        if zone_name not in WEATHER_STATE:
            WEATHER_STATE[zone_name] = make_initial_weather_state(zone_name)


def get_zone_defaults(zone_name: str) -> dict:
    zone_cfg = ZONE_CONFIGS.get(zone_name, {})
    cfg = ZONE_DEFAULTS.copy()
    cfg.update(zone_cfg)
    return cfg


def make_initial_weather_state(zone_name: str) -> dict:
    cfg = get_zone_defaults(zone_name)

    base_temp = cfg.get("base_temp", 20)
    temp_var  = cfg.get("temp_variation", 5)
    wind_min  = cfg.get("wind_min", 0)
    wind_max  = cfg.get("wind_max", 20)
    qnh_mean  = cfg.get("qnh_mean", 1015)
    qnh_var   = cfg.get("qnh_variation", 8)
    favored   = cfg.get("favored_conditions", ["CLEAR", "FEW", "BKN"])

    condition = random.choice(favored)

    return {
        "condition": condition,
        "wind_dir": random.randint(0, 359),
        "wind_speed": random.randint(wind_min, wind_max),
        "visibility": CONDITION_CONFIGS.get(condition, {}).get("visibility", "GOOD"),
        "style": CONDITION_CONFIGS.get(condition, {}).get("style", "VFR"),
        "temp": base_temp + random.randint(-temp_var, temp_var),
        "qnh": qnh_mean + random.randint(-qnh_var, qnh_var),
        "last_update": time.time(),
        "zone": zone_name
    }

def step_value(value, step, min_v, max_v):
    return max(min_v, min(max_v, value + random.randint(-step, step)))


def pick_next_condition(current: str) -> str:
    cfg = CONDITION_CONFIGS.get(current, {})
    transitions = cfg.get("transition", [])

    for t in transitions:
        if random.random() < t.get("chance", 0.0):
            return t["to"]

    # If none hit, stay where we are
    return current


def update_zone_weather(state: dict):
    cfg = get_zone_defaults(state["zone"])

    wind_min = cfg.get("wind_min", 0)
    wind_max = cfg.get("wind_max", 20)
    base_temp = cfg.get("base_temp", 20)
    temp_var  = cfg.get("temp_variation", 5)
    qnh_mean  = cfg.get("qnh_mean", 1015)
    qnh_var   = cfg.get("qnh_variation", 8)

    # Wind random walk within zone range
    state["wind_dir"] = (state["wind_dir"] + random.randint(-10, 10)) % 360
    state["wind_speed"] = step_value(state["wind_speed"], 2, wind_min, wind_max)

    # Temp drifts toward base_temp-ish area
    state["temp"] = step_value(state["temp"], 1, base_temp - temp_var, base_temp + temp_var)

    # Pressure wiggle
    state["qnh"] = step_value(state["qnh"], 1, qnh_mean - qnh_var, qnh_mean + qnh_var)

    # Condition transition using config
    new_cond = pick_next_condition(state["condition"])
    state["condition"] = new_cond

    cond_cfg = CONDITION_CONFIGS.get(new_cond, {})
    state["visibility"] = cond_cfg.get("visibility", state.get("visibility", "GOOD"))
    state["style"] = cond_cfg.get("style", state.get("style", "VFR"))

    state["last_update"] = time.time()

def get_weather_for_airport(icao: str) -> dict | None:
    """
    Return current weather state for an airport's zone.
    Uses ATC_TOWERS as the source of truth (flat dict: ICAO -> config).
    Lazily initializes zone weather if missing.
    """
    icao = icao.upper()

    ap = ATC_TOWERS.get(icao)
    if not ap:
        # Truly unknown airport
        return None

    # Get zone from config or default to ICAO
    zone = ap.get("weather_zone")
    if not zone:
        zone = icao
        ap["weather_zone"] = zone

    # Lazy init weather state
    if zone not in WEATHER_STATE:
        WEATHER_STATE[zone] = make_initial_weather_state(zone)

    state = WEATHER_STATE[zone]
    state["zone"] = zone
    return state


def format_weather_report(icao: str) -> str | None:
    """
    Build a human-friendly weather string for an airport using the
    current zone state and the condition definitions from weather.json.
    """
    icao = icao.upper()
    state = get_weather_for_airport(icao)
    if not state:
        return None

    cond = state["condition"]
    cond_cfg = CONDITION_CONFIGS.get(cond, {})
    desc = cond_cfg.get("description", cond.lower())
    vis = state.get("visibility", "GOOD").lower()
    style = state.get("style", "VFR")

    return (
        f"{icao} weather: winds {state['wind_dir']:03.0f} at {state['wind_speed']} knots, "
        f"visibility {vis}, {desc}, temperature {state['temp']}C, "
        f"QNH {state['qnh']}, flight conditions {style}."
    )


WEATHER_UPDATE_INTERVAL = 10 * 60  # 10 minutes

def update_all_weather():
    now = time.time()
    for zone_name, state in WEATHER_STATE.items():
        if now - state.get("last_update", 0) >= WEATHER_UPDATE_INTERVAL:
            update_zone_weather(state)


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

def get_runway_state(airport, runway):
    airport_state = RUNWAY_STATE.setdefault(airport, {})
    state = airport_state.get(runway)
    if not state:
        state = {
            "active": None,          # dict or None
            "queue": deque(),             # waiting aircraft
            "expires_at": 0
        }
        airport_state[runway] = state
    return state

def runway_active(state):
    return state["active"] and time.time() < state["expires_at"]

def set_runway_active(state, entry, seconds):
    state["active"] = entry
    state["expires_at"] = time.time() + seconds

def clear_runway(state):
    state["active"] = None
    state["expires_at"] = 0

def choose_runway_for_action(tower_cfg, action):
    """
    Select a runway and end for the given action ("landing" or "takeoff").

    Returns:
        (logical_runway_key, runway_end)

    logical_runway_key: what we use for RUNWAY_STATE (sequencing)
    runway_end: the textual end we speak in phraseology ("36L", "18L")

    Rules:
    - If using new-style 'runways' config:
        * For landing -> only consider runways with non-empty 'landing_ends'
        * For takeoff -> only consider runways with non-empty 'takeoff_ends'
        * First matching runway wins (deterministic, no random choice)
    - If no 'runways' block exists, fall back to old 'landings' / 'departures'.
    """

    runways_cfg = tower_cfg.get("runways") or []

    # --- New-style config: list of runway dicts ---
    if runways_cfg and isinstance(runways_cfg[0], dict):
        for rwy in runways_cfg:
            if action == "landing":
                ends = rwy.get("landing_ends") or []
            elif action == "takeoff":
                ends = rwy.get("takeoff_ends") or []
            else:
                ends = []

            # This runway does NOT handle this operation at all
            if not ends:
                continue

            # This runway is valid for this operation -> use it
            runway_end = ends[0]  # first defined end; deterministic
            logical_id = rwy.get("physical_id") or rwy.get("id") or runway_end
            return logical_id, runway_end

    # --- Fallback: old-style config using plain lists ---
    if action == "landing":
        choices = tower_cfg.get("landings") or tower_cfg.get("runways", [])
    elif action == "takeoff":
        choices = tower_cfg.get("departures") or tower_cfg.get("runways", [])
    else:
        choices = tower_cfg.get("runways", [])

    if not choices:
        return "DEFAULT", ""

    # Old behaviour: first runway in the list
    runway_end = choices[0]
    logical_id = runway_end
    return logical_id, runway_end



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

def process_runway_sequencing():
    if not SEQUENCING.get("enabled", False):
        return
    if not SEQUENCING.get("auto_clear_next", False):
        return

    now = time.time()

    for airport_code, runways in RUNWAY_STATE.items():
        for runway, state in runways.items():

            # Expire active runway
            if state["active"] and now >= state["expires_at"]:
                clear_runway(state)

            # Auto-clear next
            if not state["active"] and state["queue"]:
                entry = state["queue"].popleft()

                occupy = OCCUPANCY.get(entry["action"], 30)
                set_runway_active(state, entry, occupy)

                templates = AUTO_CLEAR_RESPONSES.get(entry["action"], [])
                if templates:
                    template = random.choice(templates)
                    text = template.format(
                        callsign=entry["callsign"],
                        runway=entry["runway"],
                        airport=entry["airport"]
                    )
                else:
                    # fallback
                    if entry["action"] == "landing":
                        text = f"{entry['callsign']}, cleared to land runway {entry['runway']}."
                    else:
                        text = f"{entry['callsign']}, cleared for takeoff runway {entry['runway']}."

                freq = entry["frequency"]
                ch = get_channel(freq)
                if ch:
                    ch["messages"].append({
                        "id": ch["next_id"],
                        "text": text[0].upper() + text[1:],
                        "sender": entry["sender"],
                    })
                    ch["next_id"] += 1


# ---------------------------
# ATC Bot Logic
# ---------------------------
def handle_atc(message_text: str, channel: int, sender_name: str):
    """
    Process ATC bot responses.
    Message format: AIRPORT_CODE, CALLSIGN, request ...
    """

    # --- Parse & normalize (fills callsign if user omitted it) ---
    airport_code, callsign, request_text = normalize_atc_message(
        message_text,
        sender_name,
    )

    if not airport_code or not request_text:
        return None

    request_text = request_text.lower()

    requested_runway = parse_requested_runway(request_text)  # e.g. "27L"
    pilot_key = (airport_code, callsign)


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
    for action in ("taxi", "startup")
    for phrase in TRIGGER_PHRASES.get(action, [])
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

    # A) Ground-style requests (taxi / startup) on TOWER frequency -> redirect to GROUND
    if (
        tower_freq != ground_freq
        and channel == tower_freq
        and channel != ground_freq
    ):
        is_ground_request = any(
            phrase in request_text
            for action in ("taxi", "startup")
            for phrase in TRIGGER_PHRASES.get(action, [])
        )

        if is_ground_request:
            # Special-case startup redirect if desired
            is_startup_request = any(
                phrase in request_text
                for phrase in TRIGGER_PHRASES.get("startup", [])
            )

            if is_startup_request:
                templates = REDIRECT_MESSAGES.get("startup_tower_to_ground", [])
                # Fall back to generic tower_to_ground if startup-specific empty
                if not templates:
                    templates = REDIRECT_MESSAGES.get("tower_to_ground", [])
            else:
                templates = REDIRECT_MESSAGES.get("tower_to_ground", [])

            if templates:
                template = random.choice(templates)
                text = template.format(
                    callsign=callsign,
                    airport=airport_code,
                    # These messages talk about CONTACT GROUND on {frequency}
                    frequency=format_freq(ground_freq),
                )
                text = text[0].upper() + text[1:]

                tower_sender = tower.get("tower_sender", f"{airport_code} Tower")
                return text, tower_sender

            # No templates? just ignore like before
            return None

    # B) Tower-style requests (takeoff / landing) on GROUND frequency -> redirect to TOWER
    if (
        tower_freq != ground_freq
        and channel == ground_freq
        and channel != tower_freq
    ):
        is_tower_request = any(
            phrase in request_text
            for action in ("takeoff", "landing")
            for phrase in TRIGGER_PHRASES.get(action, [])
        )

        if is_tower_request:
            templates = REDIRECT_MESSAGES.get("ground_to_tower", [])
            if templates:
                template = random.choice(templates)
                text = template.format(
                    callsign=callsign,
                    airport=airport_code,
                    # These messages talk about CONTACT TOWER on {frequency}
                    frequency=format_freq(tower_freq),
                )
                text = text[0].upper() + text[1:]

                ground_sender = tower.get("ground_sender", f"{airport_code} Ground")
                return text, ground_sender

            return None


    # =========================================================
    # 2) If the tuned frequency doesn't belong to this airport, ignore
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

                # --------------------------------------------------
                # Runway selection (now using JSON runway config)
                # --------------------------------------------------
                logical_runway_id = None
                runway = ""

                if action in ("landing", "takeoff"):
                    if action == "takeoff":
                        valid = runway_ends_for_action(tower, "takeoff")

                        # 1) Honor explicit requested runway if valid
                        if requested_runway and requested_runway.upper() in valid:
                            runway = requested_runway.upper()
                            logical_runway_id = physical_id_for_runway_end(tower, runway)
                            PILOT_ASSIGNED_RUNWAY[pilot_key] = runway

                        else:
                            # 2) Reuse taxi-assigned runway if valid
                            assigned = PILOT_ASSIGNED_RUNWAY.get(pilot_key)
                            if assigned and assigned in valid:
                                runway = assigned
                                logical_runway_id = physical_id_for_runway_end(tower, runway)
                            else:
                                # 3) Fall back to existing chooser
                                logical_runway_id, runway = choose_runway_for_action(tower, action)
                                if runway:
                                    PILOT_ASSIGNED_RUNWAY[pilot_key] = runway

                    else:
                        # landing:
                        valid = runway_ends_for_action(tower, "landing")

                        if requested_runway and requested_runway.upper() in valid:
                            runway = requested_runway.upper()
                            logical_runway_id = physical_id_for_runway_end(tower, runway)
                            PILOT_ASSIGNED_RUNWAY[pilot_key] = runway
                        else:
                            logical_runway_id, runway = choose_runway_for_action(tower, action)
                            if runway:
                                PILOT_ASSIGNED_RUNWAY[pilot_key] = runway


                elif action == "taxi":
                    valid = runway_ends_for_action(tower, "taxi")

                    # 1) If pilot explicitly requested a runway and it's valid for taxi → honor it
                    if requested_runway and requested_runway.upper() in valid:
                        runway = requested_runway.upper()
                        PILOT_ASSIGNED_RUNWAY[pilot_key] = runway

                    else:
                        # 2) Reuse previously assigned runway (keeps taxi->takeoff consistent)
                        assigned = PILOT_ASSIGNED_RUNWAY.get(pilot_key)
                        if assigned and assigned in valid:
                            runway = assigned
                        else:
                            # 3) Otherwise pick a runway (random or your own strategy)
                            runway = random.choice(sorted(valid)) if valid else ""
                            if runway:
                                PILOT_ASSIGNED_RUNWAY[pilot_key] = runway


                elif action == "startup":
                    # startup does not need a runway
                    logical_runway_id = None
                    runway = ""

                else:
                    # Other actions (non-runway-specific)
                    base_choices = (
                        tower.get("runways")
                        or tower.get("landings")
                        or tower.get("departures")
                        or []
                    )
                    runway = base_choices[0] if base_choices else ""

                # --------------------------------------------------
                # Runway sequencing (landing / takeoff only)
                # --------------------------------------------------
                if (
                    SEQUENCING.get("enabled", True)
                    and role == "tower"
                    and action in ("landing", "takeoff")
                ):
                    # Group by physical runway when using new config;
                    # fall back to using the runway end string otherwise.
                    runway_key = logical_runway_id or runway or "DEFAULT"
                    state = get_runway_state(airport_code, runway_key)

                    # Runway currently occupied → HOLD and queue
                    if runway_active(state):
                        entry = {
                            "airport": airport_code,
                            "runway": runway,    # end used in messages
                            "callsign": callsign,
                            "action": action,
                            "frequency": channel,
                            "sender": sender_name,
                        }
                        state["queue"].append(entry)

                        position = len(state["queue"]) + 1
                        hold_templates = HOLD_MESSAGES.get(action, [])
                        if hold_templates:
                            hold_template = random.choice(hold_templates)
                            hold_text = hold_template.format(
                                callsign=callsign,
                                runway=runway,
                                position=position,
                            )
                        else:
                            hold_text = f"{callsign}, hold, traffic in sequence."

                        hold_text = hold_text[0].upper() + hold_text[1:]
                        return hold_text, sender_name

                    # Runway free → mark active
                    occupy = OCCUPANCY.get(action, 30)
                    set_runway_active(
                        state,
                        {
                            "airport": airport_code,
                            "runway": runway,
                            "callsign": callsign,
                            "action": action,
                            "frequency": channel,
                            "sender": sender_name,
                        },
                        occupy,
                    )

                        # --------------------------------------------------
                        # If pilot requested an invalid runway, override with
                        # a friendly "unable, use {runway}" style message
                        # --------------------------------------------------
                # --------------------------------------------------
                # Invalid runway request handling (JSON-driven)
                # --------------------------------------------------
                if action in ("landing", "takeoff") and requested_runway:
                    requested_norm = requested_runway.upper()
                    valid_for_action = runway_ends_for_action(tower, action)

                    if requested_norm not in valid_for_action and runway:
                        templates = INVALID_RUNWAY_MESSAGES.get(action, [])
                        if templates:
                            template = random.choice(templates)
                            invalid_text = template.format(
                                callsign=callsign,
                                requested=requested_norm,
                                runway=runway,
                            )
                            invalid_text = invalid_text[0].upper() + invalid_text[1:]
                            return invalid_text, sender_name



                # --- Build response text with runway/taxiway placeholders ---
                if "{taxiway}" in template and "taxiways" in tower:
                    taxiway = random.choice(tower["taxiways"])
                    response_text = template.format(
                        landings=runway,
                        departures=runway,
                        taxiway=taxiway,
                    )
                else:
                    response_text = template.format(
                        landings=runway,
                        departures=runway,
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
                                    frequency=formatted_freq,
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
            airport=airport_code,
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
    process_runway_sequencing()
    update_all_weather()

    data = request.get_json(force=True)

    freq = int(data.get("frequency", 16))
    text = data.get("text", "").strip()
    sender = data.get("sender", "UNKNOWN")
    sender_uuid = data.get("sender_uuid")

    if not text:
        return jsonify({"error": "empty message"}), 400
    
     # --- Dedicated-channel TX permission check (GNN, etc.) ---
    if not can_transmit_on_frequency(freq, sender_uuid):
        return jsonify({
            "status": "blocked",
            "error": "TX_NOT_ALLOWED",
            "reason": "CHANNEL_RECV_ONLY"
        }), 403

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
    process_runway_sequencing()

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

@app.route("/weather", methods=["POST"])
def get_weather():
    """
    Return simulated weather for a given airport.

    Request JSON:
      { "airport": "SLHA" }

    Response JSON (200):
      {
        "ok": true,
        "airport": "SLHA",
        "zone": "NORTH_COAST",
        "condition": "FEW",
        "visibility": "GOOD",
        "style": "VFR",
        "wind_dir": 190,
        "wind_speed": 8,
        "temp": 18,
        "qnh": 1015,
        "report": "SLHA weather: winds 190 at 8 knots, visibility good, few clouds, temperature 18°C, QNH 1015, flight conditions VFR."
      }
    """
    data = request.get_json(force=True, silent=True) or {}
    icao = data.get("airport", "").upper().strip()

    if not icao:
        return jsonify({"ok": False, "error": "Missing 'airport' field"}), 400

    # Advance weather sim so it stays alive even if nobody's sending on /send
    update_all_weather()

    state = get_weather_for_airport(icao)
    if not state:
        return jsonify({"ok": False, "error": f"Unknown airport '{icao}'"}), 404

    report = format_weather_report(icao)

    return jsonify({
        "ok": True,
        "airport": icao,
        "zone": state.get("zone"),
        "condition": state.get("condition"),
        "visibility": state.get("visibility"),
        "style": state.get("style"),
        "wind_dir": state.get("wind_dir"),
        "wind_speed": state.get("wind_speed"),
        "temp": state.get("temp"),
        "qnh": state.get("qnh"),
        "report": report
    })



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)