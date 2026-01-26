from flask import Flask, request, jsonify # pyright: ignore[reportMissingImports]
import time
import json
import random
import re
import uuid
from collections import deque
from datetime import datetime
import logging


app = Flask(__name__)

# Make sure Python's root logging is at least INFO
logging.basicConfig(level=logging.INFO)

# Make sure Flask's logger is also INFO
app.logger.setLevel(logging.INFO)

app.logger.info("🔥 ATC server starting up, custom logging should be visible")
print("🔥 ATC server print() starting up")  # this *definitely* goes to Render logs

# Load airports and triggers from JSON
with open("airports.json", "r") as f:
    airport_data = json.load(f)

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

    sender = cfg.get("sender") or cfg.get("name") or channel_id

    CHANNELS_BY_FREQ[freq] = {
        "id": channel_id,
        **cfg,
        "sender": sender,
        "tx_policy": tx_policy,
    }

ATC_TOWERS = airport_data["airports"]
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
EMERGENCY_TRIGGERS = atc_config.get("emergency_triggers", {})
POSSIBLE_EMERGENCY_TRIGGERS = EMERGENCY_TRIGGERS.get("possible_emergency_triggers", [])

GROUND_TRIGGER_PHRASES = tuple(TRIGGER_PHRASES.get("taxi", []) + TRIGGER_PHRASES.get("startup", []))
TOWER_TRIGGER_PHRASES = tuple(TRIGGER_PHRASES.get("takeoff", []) + TRIGGER_PHRASES.get("landing", []))
STARTUP_TRIGGER_PHRASES = tuple(TRIGGER_PHRASES.get("startup", []))
EMERGENCY_TRIGGER_PHRASES = tuple(EMERGENCY_TRIGGERS.get("mayday", []) + EMERGENCY_TRIGGERS.get("pan", []) + EMERGENCY_TRIGGERS.get("generic", []))

FLIGHT_PLAN_CONFIG = atc_config.get("flight_plan", {})
FP_TRIGGERS = [t.lower() for t in FLIGHT_PLAN_CONFIG.get("triggers", [])]
FP_RESPONSES = FLIGHT_PLAN_CONFIG.get("responses", [])

FP_HANDOFF_CONFIG = atc_config.get("flight_plan_departure_handoff", {})
FP_HANDOFF_RESPONSES = FP_HANDOFF_CONFIG.get("responses", [])
FP_HANDOFF_CHANCE = float(FP_HANDOFF_CONFIG.get("chance", 0.0))

ZONE_DEFAULTS = WEATHER_CONFIG.get("defaults", {})
ZONE_CONFIGS = WEATHER_CONFIG.get("zones", {})
CONDITION_CONFIGS = WEATHER_CONFIG.get("conditions", {})
WEATHER_ZONES: dict[str, list[str]] = {}
WEATHER_STATE: dict[str, dict] = {}

SERVER_INSTANCE_ID = str(uuid.uuid4())

# Per-frequency storage
channels = {}

RUNWAY_STATE = {}
RUNWAY_END_TO_PHYSICAL: dict[str, dict[str, str]] = {}   # ICAO -> { "27L": "RWY_L", ... }
VALID_ENDS_BY_ACTION: dict[str, dict[str, set[str]]] = {}

HELIPADS_BY_AIRPORT: dict[str, dict[str, dict]] = {}     # ICAO -> { "H1": {...}, "HOSP": {...} }
HELIPAD_OCCUPANCY: dict[str, dict[str, int]] = {}        # ICAO -> { "H1": 0, "HOSP": 0, ... }


# Airport+callsign -> timestamp (or just flag) for active flight plans
ACTIVE_FLIGHT_PLANS: dict[tuple[str, str], float] = {}
# (airport_code, CALLSIGN) -> {"origin": ..., "destination": ...}
FLIGHT_PLAN_ROUTES: dict[tuple[str, str], dict] = {}
FLIGHT_PLAN_TTL_SECONDS = 60 * 60

ACTIVE_EMERGENCIES: dict[tuple[str, str], dict] = {}
EMERGENCY_TTL_SECONDS = 5 * 60  # auto-expire after 5 minutes

DEFAULT_FREQUENCY = 16

RUNWAY_RE = re.compile(r"\b(?:runway|rwy)\s*([0-3]?\d)\s*([LRC])?\b", re.IGNORECASE)
PILOT_ASSIGNED_RUNWAY = {}

ROUTE_PATTERN = re.compile(
    r"\b([A-Z0-9]{3,4})\s*(?:>|to|-|–)\s*([A-Z0-9]{3,4})\b",
    re.IGNORECASE
)
DEST_ONLY_PATTERN = re.compile(
    r"\bto\s+([A-Z0-9]{3,4})\b",
    re.IGNORECASE,
)

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

# -------------------------------------------------------------------
# Helicopter detection (JSON-driven)
# -------------------------------------------------------------------

def is_helicopter_request(request_text: str, callsign: str) -> bool:
    text = (request_text or "").lower()
    cs = (callsign or "").lower()

    heli_cfg = TRIGGER_PHRASES.get("helicopter", {})

    if isinstance(heli_cfg, list):
        for kw in heli_cfg:
            if isinstance(kw, str) and kw.lower() in text:
                return True

    # Optional callsign-based detection
    if cs.startswith(("heli", "helo", "h-")):
        return True

    return False

def choose_helicopter_response(airport_code: str, action: str, callsign: str, helipad: str | None = None) -> str:
    airport_cfg = ATC_TOWERS.get(airport_code, {})
    resp_cfg = ATC_RESPONSES.get("responses", {})

    key = f"helicopter_{action}"
    candidates = ATC_RESPONSES.get(key, [])

    def _format(template: str) -> str:
        return template.format(
            CALLSIGN=callsign,
            AIRPORT=airport_code,
            HELIPAD=helipad or "",
        )

    if candidates:
        template = random.choice(candidates)
        return _format(template)

    # Fallback: generic non-runway phrasing
    generic_key = f"{action}"
    fallback = ATC_RESPONSES.get(generic_key, [])

    if fallback:
        template = random.choice(fallback)
        return _format(template)

    # Absolute fallback (never mentions runway)
    if action == "takeoff":
        return f"{callsign}, cleared for departure."
    if action == "landing":
        return f"{callsign}, cleared to land."

    return None


#------------------------------------
# EMERGENCY HELPERS
#------------------------------------
EMERGENCY_TYPE_NONE = "none"
EMERGENCY_TYPE_MAYDAY = "mayday"
EMERGENCY_TYPE_PAN = "pan"
EMERGENCY_TYPE_GENERIC = "generic"

def _contains_phrase(text: str, phrase: str) -> bool:
    """
    Case-insensitive substring match with whitespace normalization.
    """
    return phrase in text


def detect_emergency_type(text: str) -> str:
    """
    Detect emergency type based purely on JSON-defined trigger phrases.
    Priority order:
      MAYDAY > PAN > GENERIC
    """
    if not text:
        return EMERGENCY_TYPE_NONE

    t = text.lower()

    # MAYDAY has highest priority
    for phrase in EMERGENCY_TRIGGERS.get("mayday", []):
        if _contains_phrase(t, phrase.lower()):
            return EMERGENCY_TYPE_MAYDAY

    # PAN PAN next
    for phrase in EMERGENCY_TRIGGERS.get("pan", []):
        if _contains_phrase(t, phrase.lower()):
            return EMERGENCY_TYPE_PAN

    # Generic emergency last
    for phrase in EMERGENCY_TRIGGERS.get("generic", []):
        if _contains_phrase(t, phrase.lower()):
            return EMERGENCY_TYPE_GENERIC

    return EMERGENCY_TYPE_NONE

def sounds_like_possible_emergency(text: str) -> bool:
    if not text:
        return False

    t = text.lower()
    for phrase in POSSIBLE_EMERGENCY_TRIGGERS:
        if phrase.lower() in t:
            return True
    return False


#------------------------------------
# RUNWAY HELPERS
#------------------------------------
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

def build_helipad_indexes():
    """
    Build ICAO -> helipad config + occupancy maps from airport data.
    """
    HELIPADS_BY_AIRPORT.clear()
    HELIPAD_OCCUPANCY.clear()

    for icao, tower in ATC_TOWERS.items():
        icao_u = icao.upper()
        pads = tower.get("helipads", [])
        if not pads:
            continue

        pad_map: dict[str, dict] = {}
        occ_map: dict[str, int] = {}

        for pad in pads:
            pid = (pad.get("id") or "").upper().strip()
            if not pid:
                continue

            pad_map[pid] = pad
            occ_map[pid] = 0  # start empty

        if pad_map:
            HELIPADS_BY_AIRPORT[icao_u] = pad_map
            HELIPAD_OCCUPANCY[icao_u] = occ_map

build_runway_indexes()
build_helipad_indexes()

def parse_requested_runway(request_text: str) -> str | None:
    m = RUNWAY_RE.search(request_text or "")
    if not m:
        return None
    num = int(m.group(1))
    if num < 1 or num > 36:
        return None
    side = (m.group(2) or "").upper()
    return f"{num:02d}{side}"

def find_requested_helipad(airport_code: str, request_text: str) -> str | None:
    """
    Look for a helipad id (e.g. 'H1', 'HOSP') in the request text
    that matches any configured helipad for this airport.
    """
    pads = HELIPADS_BY_AIRPORT.get(airport_code, {})
    if not pads or not request_text:
        return None

    t = request_text.upper()
    for pid in pads.keys():
        if pid in t:
            return pid

    return None

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

def format_metar_from_state(icao: str, state: dict | None) -> str | None:
    """
    Build a pseudo-METAR string from your simulated weather state.

    Uses keys:
      condition (e.g. "FEW")
      visibility ("GOOD", "MODERATE", "POOR"...)
      style ("VFR", "IFR"...)
      wind_dir (degrees)
      wind_speed (knots)
      temp (°C)
      qnh (hPa)
    """
    if not state:
        return None

    # Time: DDHHMMZ in UTC
    now = datetime.utcnow()
    time_str = now.strftime("%d%H%MZ")

    # Wind
    try:
        wind_dir = int(state.get("wind_dir") or 0)
    except (TypeError, ValueError):
        wind_dir = 0

    try:
        wind_speed = int(state.get("wind_speed") or 0)
    except (TypeError, ValueError):
        wind_speed = 0

    if wind_speed <= 1:
        wind_str = "00000KT"
    else:
        wind_str = f"{wind_dir:03d}{wind_speed:02d}KT"

    # Visibility buckets from your "visibility" string
    vis_code = (state.get("visibility") or "").upper()
    if vis_code == "GOOD":
        vis_str = "10SM"
    elif vis_code in ("MODERATE", "MOD"):
        vis_str = "6SM"
    elif vis_code in ("POOR", "LOW"):
        vis_str = "3SM"
    else:
        vis_str = "10SM"

    # Clouds from your "condition" string
    cond = (state.get("condition") or "").upper()
    if cond == "FEW":
        clouds_str = "FEW020"
    elif cond in ("SCT", "SCATTERED"):
        clouds_str = "SCT025"
    elif cond in ("BKN", "BROKEN"):
        clouds_str = "BKN030"
    elif cond in ("OVC", "OVERCAST"):
        clouds_str = "OVC015"
    elif cond in ("CLR", "CLEAR", "SKC"):
        clouds_str = "SKC"
    else:
        clouds_str = "NSC"

    # Temperature / (fake) dewpoint
    try:
        temp = int(state.get("temp"))
    except (TypeError, ValueError):
        temp = 18  # safe default

    dew = temp - 6  # simple fake dewpoint; adjust if you like

    def fmt_t(t: int) -> str:
        if t < 0:
            return f"M{abs(t):02d}"
        return f"{t:02d}"

    temp_str = fmt_t(temp)
    dew_str = fmt_t(int(dew))

    # QNH
    try:
        qnh = int(state.get("qnh"))
    except (TypeError, ValueError):
        qnh = 1015
    qnh_str = f"Q{qnh:04d}"

    # Flight rules
    style = (state.get("style") or "").upper()
    if style not in ("VFR", "MVFR", "IFR", "LIFR"):
        style = "VFR"

    # Final body: ICAO DDHHMMZ ...
    return f"{icao} {time_str} {wind_str} {vis_str} {clouds_str} {temp_str}/{dew_str} {qnh_str} {style}"


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

def record_emergency(airport_code: str, callsign: str, emergency_type: str, runway: str | None = None):
    """
    Store an active emergency for this airport + callsign.
    """
    key = (airport_code.upper(), callsign.upper())
    ACTIVE_EMERGENCIES[key] = {
        "type": emergency_type,
        "runway": runway,
        "started_at": time.time(),
    }


def get_active_emergency(airport_code: str, callsign: str) -> dict | None:
    return ACTIVE_EMERGENCIES.get((airport_code.upper(), callsign.upper()))


def clear_emergency(airport_code: str, callsign: str):
    ACTIVE_EMERGENCIES.pop((airport_code.upper(), callsign.upper()), None)

HOUSEKEEP_MIN_INTERVAL = 15  # seconds
_NEXT_HOUSEKEEP = 0.0

def cleanup_stale_emergencies(now: float | None = None):
    """
    Auto-expire emergencies that have been around longer than EMERGENCY_TTL_SECONDS.
    """
    if now is None:
        now = time.time()
    if not ACTIVE_EMERGENCIES:
        return

    for key, info in list(ACTIVE_EMERGENCIES.items()):
        started = info.get("started_at", now)
        if now - started > EMERGENCY_TTL_SECONDS:
            ACTIVE_EMERGENCIES.pop(key, None)

def cleanup_stale_flight_plans(now: float | None = None):
    """
    Remove flight plans that are older than FLIGHT_PLAN_TTL_SECONDS.
    Uses ACTIVE_FLIGHT_PLANS as the timestamp source and also clears
    matching entries in FLIGHT_PLAN_ROUTES.
    """
    if now is None:
        now = time.time()

    if not ACTIVE_FLIGHT_PLANS:
        return

    # We need list(...) so we can modify the dict while iterating
    for key, ts in list(ACTIVE_FLIGHT_PLANS.items()):
        if now - ts > FLIGHT_PLAN_TTL_SECONDS:
            # key is (airport_code, CALLSIGN)
            ACTIVE_FLIGHT_PLANS.pop(key, None)
            FLIGHT_PLAN_ROUTES.pop(key, None)

def cleanup_expired_frequencies(now: float | None = None):
    """Expire inactive frequency buffers to keep memory bounded."""
    if now is None:
        now = time.time()
    if not channels:
        return

    for freq, data in list(channels.items()):
        if now - data.get("last_active", now) > FREQUENCY_EXPIRE_SECONDS:
            channels.pop(freq, None)

def housekeeping(force: bool = False):
    """
    Throttled cleanup to keep request handlers light.
    """
    global _NEXT_HOUSEKEEP
    now = time.time()

    if not force and now < _NEXT_HOUSEKEEP:
        return
    _NEXT_HOUSEKEEP = now + HOUSEKEEP_MIN_INTERVAL

    cleanup_expired_frequencies(now)
    cleanup_stale_emergencies(now)
    cleanup_stale_flight_plans(now)

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

def is_flight_plan_request(request_text: str) -> bool:
    """
    Minimal flight plan detector based on JSON-configured triggers.
    """
    t = (request_text or "").lower()
    for phrase in FP_TRIGGERS:
        if phrase and phrase in t:
            return True
    return False

def extract_route(text: str, fallback_origin: str):
    """
    Returns (origin, destination)
    """
    m = DEST_ONLY_PATTERN.search(text)
    if m:
        return fallback_origin.upper(), m.group(1).upper()

    m = ROUTE_PATTERN.search(text)
    if m:
        return m.group(1).upper(), m.group(2).upper()

    # 3) Nothing found
    return fallback_origin.upper(), None

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
                    
                # Flight plan departure handoff for auto-cleared takeoffs
                if entry["action"] == "takeoff":
                    key = (entry["airport"], entry["callsign"].upper())
                    if key in ACTIVE_FLIGHT_PLANS:
                        ACTIVE_FLIGHT_PLANS.pop(key, None)

                        if FP_HANDOFF_RESPONSES and FP_HANDOFF_CHANCE > 0.0:
                            if random.random() < FP_HANDOFF_CHANCE:
                                handoff_template = random.choice(FP_HANDOFF_RESPONSES)
                                tower_cfg = ATC_TOWERS.get(entry["airport"], {})
                                tower_freq_for_handoff = tower_cfg.get(
                                    "tower_frequency",
                                    tower_cfg.get("frequency", DEFAULT_FREQUENCY)
                                )
                                freq_str = format_freq(tower_freq_for_handoff)
                                handoff_text = handoff_template.format(
                                    AIRPORT=entry["airport"],
                                    FREQUENCY=freq_str,
                                )
                                text = f"{text} {handoff_text}"

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

    original_request_text = request_text
    request_text = request_text.lower()

    # --- Emergency detection ---
    # 1) Type from JSON-defined triggers (mayday / pan / generic)
    emergency_type = detect_emergency_type(original_request_text)
    has_emergency = emergency_type != EMERGENCY_TYPE_NONE

    # 2) Extra safety pass using flattened trigger list
    if not has_emergency and EMERGENCY_TRIGGER_PHRASES:
        if any(p.lower() in request_text for p in EMERGENCY_TRIGGER_PHRASES):
            has_emergency = True
            if emergency_type == EMERGENCY_TYPE_NONE:
                emergency_type = EMERGENCY_TYPE_GENERIC

    # 3) Optional "sounds like" fuzziness
    if not has_emergency and sounds_like_possible_emergency(original_request_text):
        has_emergency = True
        if emergency_type == EMERGENCY_TYPE_NONE:
            emergency_type = EMERGENCY_TYPE_GENERIC

    is_helicopter = is_helicopter_request(original_request_text, callsign)

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
    is_ground_request = any(p in request_text for p in GROUND_TRIGGER_PHRASES)

    # Tower-style requests (takeoff / landing, you can add more actions)
    is_tower_request = any(p in request_text for p in TOWER_TRIGGER_PHRASES)

    # =========================================================
    # 1) Redirects: real ground/tower requests on the *wrong* freq
    # =========================================================

    # A) Ground-style requests (taxi / startup) on TOWER frequency -> redirect to GROUND
    if (is_ground_request
        and tower_freq != ground_freq
        and channel == tower_freq
    ):

        # Special-case startup redirect if desired
        is_startup_request = any(p in request_text for p in STARTUP_TRIGGER_PHRASES)

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
        is_tower_request
        and tower_freq != ground_freq
        and channel == ground_freq
    ):

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
    # 2) If the tuned frequency doesn't belong to this airport
    #    (Emergencies are allowed to bypass this)
    # =========================================================
    if channel not in (tower_freq, ground_freq):
        # For emergencies, treat as if they reached this airport's ATC anyway
        if has_emergency:
            # We just continue without redirecting; role will default to Tower.
            pass
        else:
            responses = REDIRECT_MESSAGES.get("wrong_airport_frequency", [])
            if not responses:
                return None

            template = random.choice(responses)

            # Prefer tower if this handler has tower_freq, otherwise ground
            if (is_tower_request and not is_flight_plan_request(original_request_text)):
                correct_freq = tower_freq
                sender_role = tower.get("tower_sender", f"{airport_code} Tower")
            elif (is_ground_request and not is_flight_plan_request(original_request_text)):
                correct_freq = ground_freq
                sender_role = tower.get("ground_sender", f"{airport_code} Ground")
            else:
                return None

            freq_str = format_freq(correct_freq)

            response_text = template.format(
                CALLSIGN=callsign,
                REQUESTED_AIRPORT=airport_code,
                FREQUENCY=freq_str
            )
            full_text = f"{callsign}, {response_text}"

            full_text = full_text[0].upper() + full_text[1:]
            return full_text, sender_role


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
    # 4) Flight plan handling (simple: store flag, send canned reply)
    # =========================================================
    if is_flight_plan_request(request_text):
        # Mark this callsign as having a flight plan at this airport
        ACTIVE_FLIGHT_PLANS[(airport_code, callsign.upper())] = time.time()

        origin, destination = extract_route(original_request_text, airport_code)

        FLIGHT_PLAN_ROUTES[(airport_code, callsign.upper())] = {
        "origin": origin,
        "destination": destination,
        }

        usable_templates = []

        for t in FP_RESPONSES:
            if "{DESTINATION}" in t:
                if destination:
                    usable_templates.append(t)
            else:
                usable_templates.append(t)

        # Fallback safety
        if not usable_templates:
            usable_templates = FP_RESPONSES

        template = random.choice(usable_templates)

        fp_text = template.format(
            CALLSIGN=callsign,
            AIRPORT=airport_code,
            ORIGIN=origin or airport_code,
            DESTINATION=destination or "",
        )

        fp_text = fp_text[0].upper() + fp_text[1:]

        # Always respond as Tower for flight plans
        sender_name = tower.get("sender", f"{airport_code} ATC")
        return fp_text, sender_name

    # =========================================================
    # 5) Normal ATC trigger matching
    # =========================================================

    for action, phrases in TRIGGER_PHRASES.items():
        for phrase in phrases:
            if phrase in request_text:

                helicopter_full_text = False

                effective_action = action

                # If an emergency was declared but we matched some generic/emergency action
                # (e.g. action == "emergency"), treat it as a landing by default.
                if has_emergency and effective_action not in ("landing", "takeoff", "taxi"):
                    effective_action = "landing"

                if has_emergency and effective_action == "landing":
                # Use emergency landing templates from auto_clear if present; fall back to normal landing
                    templates_pool = (
                        AUTO_CLEAR_RESPONSES.get("emergency_landing_clearance")
                        or ATC_RESPONSES.get("landing", [])
                    )
                else:
                    templates_pool = ATC_RESPONSES.get(effective_action, [])

                if not templates_pool:
                    # No templates for this action; move on to next match
                    continue

                template = random.choice(templates_pool)

                # --------------------------------------------------
                # Runway selection (now using JSON runway config)
                # --------------------------------------------------
                logical_runway_id = None
                runway = ""
                helipad_id = None

                # Helicopter → prefer helipads if defined for this airport
                if is_helicopter and action in ("landing", "takeoff"):
                    pad_map = HELIPADS_BY_AIRPORT.get(airport_code, {})
                    occ_map = HELIPAD_OCCUPANCY.get(airport_code, {})

                    if pad_map:
                        pad_count = len(pad_map)
                        requested_helipad = find_requested_helipad(airport_code, original_request_text)

                        if requested_helipad and requested_helipad in pad_map:
                            requested_helipad = requested_helipad.upper()

                            max_sim = int(pad_map[requested_helipad].get("max_simultaneous", 1))
                            current = int(occ_map.get(requested_helipad, 0))

                            if current < max_sim:
                                # Requested pad has room
                                helipad_id = requested_helipad
                            else:
                                # Requested pad is full
                                if pad_count > 1:
                                    # Divert to next open helipad
                                    for pid, pad_cfg in pad_map.items():
                                        if pid == requested_helipad:
                                            continue
                                        max_sim_alt = int(pad_cfg.get("max_simultaneous", 1))
                                        current_alt = int(occ_map.get(pid, 0))
                                        if current_alt < max_sim_alt:
                                            helipad_id = pid
                                            break

                                    if not helipad_id:
                                        # All helipads are full at a multi-pad airport → hold
                                        hold_text = (
                                            f"{callsign}, all helipads are currently occupied, standby."
                                        )
                                        hold_text = hold_text[0].upper() + hold_text[1:]
                                        return hold_text, sender_name
                                else:
                                    # Only 1 helipad and it's full → allow landing anywhere
                                    anywhere_text = (
                                        f"{callsign}, helipad {requested_helipad} is occupied, "
                                        f"cleared to land anywhere on the field."
                                    )
                                    anywhere_text = anywhere_text[0].upper() + anywhere_text[1:]
                                    return anywhere_text, sender_name

                        else:
                            # No specific helipad requested: auto-pick first with space
                            for pid, pad_cfg in pad_map.items():
                                max_sim = int(pad_cfg.get("max_simultaneous", 1))
                                current = int(occ_map.get(pid, 0))
                                if current < max_sim:
                                    helipad_id = pid
                                    break

                            if not helipad_id:
                                # No pad requested and all pads are full
                                if pad_count == 1:
                                    # Single-pad airport: let them land anywhere
                                    only_id = next(iter(pad_map.keys()))
                                    anywhere_text = (
                                        f"{callsign}, helipad {only_id} is occupied, "
                                        f"cleared to land anywhere on the field."
                                    )
                                    anywhere_text = anywhere_text[0].upper() + anywhere_text[1:]
                                    return anywhere_text, sender_name
                                else:
                                    # Multi-pad airport: hold for space
                                    hold_text = (
                                        f"{callsign}, all helipads are currently occupied, standby."
                                    )
                                    hold_text = hold_text[0].upper() + hold_text[1:]
                                    return hold_text, sender_name

                        # If we got here with a helipad_id, we intentionally do NOT pick a runway
                        if helipad_id:
                            logical_runway_id = None
                            runway = ""
                    # If no pad_map: fall through into normal runway logic below

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
                # Emergency bookkeeping: record which runway we gave them
                # --------------------------------------------------
                if has_emergency and action == "landing" and runway:
                    record_emergency(airport_code, callsign, emergency_type, runway)

                                # --------------------------------------------------
                # Runway sequencing (landing / takeoff only)
                # Emergencies are allowed to override existing occupancy.
                # --------------------------------------------------
                if (
                    SEQUENCING.get("enabled", True)
                    and role == "tower"
                    and action in ("landing", "takeoff")
                    and not is_helicopter
                ):
                    # Group by physical runway when using new config;
                    # fall back to using the runway end string otherwise.
                    runway_key = logical_runway_id or runway or "DEFAULT"
                    state = get_runway_state(airport_code, runway_key)

                    if has_emergency and action == "landing":
                        occupy = OCCUPANCY.get("emergency_landing", OCCUPANCY.get(action, 60))
                    else:
                        occupy = OCCUPANCY.get(action, 30)

                    entry = {
                        "airport": airport_code,
                        "runway": runway,    # end used in messages
                        "callsign": callsign,
                        "action": action,
                        "frequency": channel,
                        "sender": sender_name,
                        "emergency": has_emergency,
                    }

                    # Check if there's already an active aircraft and whether it's an emergency
                    active = state.get("active") or state.get("current")  # depends on your structure
                    active_is_emergency = bool(active and active.get("emergency"))

                    if runway_active(state):

                        if not has_emergency:
                            # ---- NORMAL TRAFFIC WHILE RUNWAY IS BUSY ----
                            # Always queue normal traffic so process_runway_sequencing()
                            # can auto-clear it later.
                            state["queue"].append(entry)

                            position = len(state["queue"]) + 1

                            # If the *current* active aircraft is an emergency, prefer the
                            # spial emergency-hold messages, otherwise normal hold text.
                            if active_is_emergency:
                                # --- NEW: hold normal traffic due to active emergency ---
                                hold_templates = HOLD_MESSAGES.get("emergency_hold_traffic", []) or HOLD_MESSAGES.get(action, [])
                            
                            else:
                                hold_templates = HOLD_MESSAGES.get(action, [])

                            if hold_templates:
                                hold_template = random.choice(hold_templates)
                                # You can include emergency runway / callsign in the message later
                                hold_text = hold_template.format(
                                    callsign=callsign,
                                    runway=runway,
                                    position=position,
                                )
                            else:
                                if active_is_emergency:
                                    hold_text = (
                                        f"{callsign}, hold, runway blocked due to "
                                        f"emergency traffic."
                                    )
                                else:
                                    hold_text = f"{callsign}, hold, traffic in sequence."

                            hold_text = hold_text[0].upper() + hold_text[1:]
                            return hold_text, sender_name
                        
                        if active and not active_is_emergency:
                            state["queue"].append(active)

                    # Either runway is free OR this is an emergency:
                    # mark it active for this aircraft (emergency overrides whoever was there).
                    set_runway_active(state, entry, occupy)


                # --------------------------------------------------
                # If pilot requested an invalid runway, override with
                # a friendly "unable, use {runway}" style message
                # --------------------------------------------------
                # --------------------------------------------------
                # Invalid runway request handling (JSON-driven)
                # --------------------------------------------------
                if action in ("landing", "takeoff") and requested_runway and not is_helicopter:
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
                        callsign=callsign,
                        landings=runway,
                        departures=runway,
                    )

                # --- Helicopter-specific phrasing (JSON-driven) ---
                # For helicopters requesting takeoff/landing, switch to helicopter_* responses.
                if is_helicopter and effective_action in ("takeoff", "landing"):
                    heli_text = choose_helicopter_response(airport_code, effective_action, callsign, helipad=helipad_id)
                    if heli_text:
                        response_text = heli_text
                        helicopter_full_text = True

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

                # --- Flight plan departure handoff (Tower, takeoff only) ---
                if action == "takeoff" and role == "tower":
                    key = (airport_code, callsign.upper())
                    route_info = FLIGHT_PLAN_ROUTES.pop(key, None)

                    if key in ACTIVE_FLIGHT_PLANS:
                        # Drop the plan as soon as we issue a takeoff clearance
                        ACTIVE_FLIGHT_PLANS.pop(key, None)

                        if FP_HANDOFF_RESPONSES and FP_HANDOFF_CHANCE > 0.0:
                            if random.random() < FP_HANDOFF_CHANCE:
                                handoff_template = random.choice(FP_HANDOFF_RESPONSES)
                                # Default: handoff is back to the *current* airport tower
                                handoff_airport = airport_code
                                handoff_freq = tower.get(
                                    "tower_frequency",
                                    tower.get("frequency", DEFAULT_FREQUENCY)
                                )

                                # If we have a destination from the flight plan, try to hand off there instead
                                dest_icao = None
                                if route_info:
                                    dest_icao = route_info.get("destination")

                                if dest_icao:
                                    dest_tower = ATC_TOWERS.get(dest_icao.upper())
                                    if dest_tower:
                                        dest_freq = dest_tower.get(
                                            "tower_frequency",
                                            dest_tower.get("frequency", DEFAULT_FREQUENCY)
                                        )
                                        if dest_freq:
                                            handoff_airport = dest_icao.upper()
                                            handoff_freq = dest_freq

                                freq_str = format_freq(handoff_freq)

                                # Allow templates to use AIRPORT and/or DESTINATION for the handoff airport
                                handoff_text = handoff_template.format(
                                    AIRPORT=handoff_airport,
                                    DESTINATION=handoff_airport,
                                    FREQUENCY=freq_str,
                                )
                                response_text = f"{response_text}, {handoff_text}"

                # --- Emergency acknowledgements and traffic hold calls ---
                if has_emergency and role == "tower" and action == "landing":
                    # 1) Pick the right ack family
                    if emergency_type == EMERGENCY_TYPE_MAYDAY:
                        ack_pool = ATC_RESPONSES.get("emergency_ack_mayday", [])
                    elif emergency_type == EMERGENCY_TYPE_PAN:
                        ack_pool = ATC_RESPONSES.get("emergency_ack_pan", [])
                    else:
                        ack_pool = ATC_RESPONSES.get("emergency_ack_generic", [])

                    if ack_pool:
                        ack_template = random.choice(ack_pool)
                        ack_text = ack_template.format(
                            CALLSIGN=callsign,
                            AIRPORT=airport_code,
                        )
                    else:
                        ack_text = f"{callsign}, roger, emergency acknowledged."

                    # 2) Optional broadcast-style traffic hold message
                    emergency_hold_pool = HOLD_MESSAGES.get("emergency_hold_traffic", [])
                    hold_broadcast = ""
                    if emergency_hold_pool and random.random() < 0.6:
                        hold_broadcast = " " + random.choice(emergency_hold_pool)

                    # Stick ack in front, broadcast at the end
                    response_text = f"{ack_text} {response_text}{hold_broadcast}".strip()
                    

                # --- Helipad occupancy bookkeeping ---
                if is_helicopter and helipad_id and action == "landing":
                    occ_map = HELIPAD_OCCUPANCY.get(airport_code, {})
                    occ_map[helipad_id] = occ_map.get(helipad_id, 0) + 1

                if is_helicopter and helipad_id and action == "takeoff":
                    occ_map = HELIPAD_OCCUPANCY.get(airport_code, {})
                    occ_map[helipad_id] = max(0, occ_map.get(helipad_id, 0) - 1)


                if helicopter_full_text:
                    # Helicopter templates already include the callsign
                    full_text = response_text
                else:
                    full_text = f"{callsign}, {response_text}"

                capitalized = full_text[0].upper() + full_text[1:]

                # Use per-role sender_name (Tower / Ground)
                return capitalized, sender_name
            
    # =========================================================
    # 5b) Emergency fallback: emergency but no action matched
    # =========================================================
    if has_emergency:
        if emergency_type == EMERGENCY_TYPE_MAYDAY:
            ack_pool = ATC_RESPONSES.get("emergency_ack_mayday", [])
        elif emergency_type == EMERGENCY_TYPE_PAN:
            ack_pool = ATC_RESPONSES.get("emergency_ack_pan", [])
        else:
            ack_pool = ATC_RESPONSES.get("emergency_ack_generic", [])

        if ack_pool:
            ack_template = random.choice(ack_pool)
            ack_text = ack_template.format(
                CALLSIGN=callsign,
                AIRPORT=airport_code,
            )
        else:
            ack_text = f"{callsign}, roger, emergency acknowledged."

        ack_text = ack_text[0].upper() + ack_text[1:]
        return ack_text, sender_name

    # =========================================================
    # 5c) Fallback: unknown / unrecognized request on a valid freq
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
    housekeeping()
    return jsonify({
        "status": "online",
        "instance_id": SERVER_INSTANCE_ID,
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

    update_all_weather()
    state = get_weather_for_airport(airport)
    metar = None
    if state:
        body = format_metar_from_state(airport, state)
        if body:
            metar = f"METAR {body}"

    return jsonify({
        "airport": airport,
        "frequency": freq,
        "sender": sender,
        "metar": metar
    })

@app.route("/state", methods=["GET"])
def get_state():
    housekeeping()

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
    housekeeping()
    process_runway_sequencing()
    update_all_weather()

    data = request.get_json(force=True)

    freq = int(data.get("frequency", 16))
    text = data.get("text", "").strip()
    sender = data.get("sender", "UNKNOWN")
    sender_uuid = data.get("sender_uuid")

    app.logger.info(
        "[RX /send] freq=%r sender=%r text=%r full_json=%s",
        freq,
        sender,
        text,
        json.dumps(data, ensure_ascii=False)
    )

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

    return jsonify({
        "status": "sent",
        "id": msg["id"]
    })


@app.route("/fetch", methods=["GET"])
def fetch_messages():
    housekeeping()
    process_runway_sequencing()

    freq = int(request.args.get("frequency", 16))
    since_id = int(request.args.get("since_id", 0))

    app.logger.info(
        "[RX /fetch] freq=%r since=%r query=%r",
        freq,
        since_id,
        dict(request.args)
    )

    if freq not in channels:
        return jsonify({
            "instance_id": SERVER_INSTANCE_ID,
            "messages": []
        })

    channel = get_channel(freq)

    msgs = [
        m for m in channel["messages"]
        if m["id"] > since_id
    ]

    return jsonify({
        "instance_id": SERVER_INSTANCE_ID,
        "messages": msgs
    })

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