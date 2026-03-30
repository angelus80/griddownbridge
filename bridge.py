import os
import time
import logging
import threading
from collections import deque
from typing import Any, Optional

from flask import Flask, jsonify, request
from pubsub import pub

import meshtastic.serial_interface

try:
    from meshtastic import __version__ as MESHTASTIC_VERSION
except Exception:
    MESHTASTIC_VERSION = "unknown"


BRIDGE_VERSION = "1.3.1"

HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "5001"))
SERIAL_PORT = os.getenv("MESHTASTIC_PORT", "COM3")

MESSAGE_LIMIT = max(10, min(int(os.getenv("MESSAGE_LIMIT", "200")), 1000))
DEFAULT_MESSAGES_LIMIT = 50
MAX_MESSAGES_LIMIT = 200
NODE_ACTIVE_WINDOW_SEC = int(os.getenv("NODE_ACTIVE_WINDOW_SEC", "900"))

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

start_ts = int(time.time())
state_lock = threading.RLock()

iface = None
recent_messages = deque(maxlen=MESSAGE_LIMIT)
last_error: Optional[str] = None
last_connect_ts: Optional[int] = None


def now_ts() -> int:
    return int(time.time())


def set_error(message: Optional[str]) -> None:
    global last_error
    with state_lock:
        last_error = message


def safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def get_interface():
    with state_lock:
        return iface


def is_connected() -> bool:
    return get_interface() is not None


def connect_interface() -> bool:
    global iface, last_connect_ts

    with state_lock:
        if iface is not None:
            return True

        try:
            logging.info("Connecting to Meshtastic on %s", SERIAL_PORT)
            iface = meshtastic.serial_interface.SerialInterface(devPath=SERIAL_PORT)
            last_connect_ts = now_ts()
            set_error(None)
            logging.info("Connected to Meshtastic on %s", SERIAL_PORT)
            return True
        except Exception as exc:
            iface = None
            set_error(str(exc))
            logging.exception("Meshtastic connect failed: %s", exc)
            return False


def disconnect_interface() -> None:
    global iface

    with state_lock:
        current = iface
        iface = None

    if current is not None:
        try:
            current.close()
        except Exception:
            pass


def reconnect_interface() -> bool:
    disconnect_interface()
    time.sleep(0.5)
    return connect_interface()


def ensure_connected() -> bool:
    if is_connected():
        return True
    return connect_interface()


def get_node_name(node_num: Any, nodes: dict) -> str:
    try:
        raw = nodes.get(node_num, {}) or {}
        user = raw.get("user", {}) or {}
        return user.get("longName") or user.get("shortName") or f"Node {node_num}"
    except Exception:
        return f"Node {node_num}"


def normalize_node(node_num: Any, raw: dict) -> dict:
    user = raw.get("user", {}) or {}
    position = raw.get("position", {}) or {}
    device_metrics = raw.get("deviceMetrics", {}) or {}

    long_name = user.get("longName")
    short_name = user.get("shortName")
    name = long_name or short_name or f"Node {node_num}"

    last_heard = safe_int(raw.get("lastHeard"))
    online = bool(last_heard and (now_ts() - last_heard) <= NODE_ACTIVE_WINDOW_SEC)

    return {
        "id": str(node_num),
        "node_num": node_num,
        "name": name,
        "longName": long_name,
        "shortName": short_name,
        "lat": safe_float(position.get("latitude")),
        "lon": safe_float(position.get("longitude")),
        "altitude": safe_float(position.get("altitude")),
        "lastHeard": last_heard,
        "snr": raw.get("snr"),
        "rxSnr": raw.get("rxSnr"),
        "signal": raw.get("rxRssi") if raw.get("rxRssi") is not None else raw.get("snr"),
        "hopsAway": raw.get("hopsAway"),
        "batteryLevel": device_metrics.get("batteryLevel"),
        "online": online,
    }


def get_nodes_payload() -> list:
    current = get_interface()
    if current is None:
        return []

    try:
        raw_nodes = getattr(current, "nodes", {}) or {}
        nodes = [normalize_node(node_num, raw) for node_num, raw in raw_nodes.items()]
        nodes.sort(key=lambda n: (not n["online"], (n["name"] or "").lower()))
        return nodes
    except Exception as exc:
        logging.exception("Failed to load nodes: %s", exc)
        set_error(f"nodes error: {exc}")
        return []


def on_receive(packet, **kwargs) -> None:
    try:
        if not isinstance(packet, dict):
            return

        decoded = packet.get("decoded", {}) or {}
        text = decoded.get("text")
        if not text:
            return

        current = get_interface()
        nodes = getattr(current, "nodes", {}) if current is not None else {}

        from_id = packet.get("fromId") or packet.get("from")
        to_id = packet.get("toId") or packet.get("to")
        channel = packet.get("channel") or 0
        rx_time = safe_int(packet.get("rxTime"), now_ts())

        msg = {
            "id": f"rx-{from_id}-{rx_time}-{abs(hash(text)) % 100000}",
            "ts": rx_time,
            "timestamp": rx_time,
            "from": get_node_name(from_id, nodes) if from_id is not None else "Unknown",
            "from_num": from_id,
            "to": (
                get_node_name(to_id, nodes)
                if to_id not in (None, 0xFFFFFFFF)
                else "Broadcast"
            ),
            "to_num": to_id,
            "channel": channel,
            "text": text,
            "type": "received",
            "rxSnr": packet.get("rxSnr"),
            "rxRssi": packet.get("rxRssi"),
        }

        with state_lock:
            recent_messages.appendleft(msg)

        logging.info("RX from=%s to=%s text=%r", from_id, to_id, text)

    except Exception as exc:
        logging.exception("on_receive error: %s", exc)
        set_error(f"receive error: {exc}")


def normalize_destination(destination: Any):
    if destination in (None, "", "Broadcast", "broadcast"):
        return None

    dest_int = safe_int(destination, None)
    if dest_int is not None:
        return dest_int

    return destination


def send_text(current_iface, text: str, destination: Any = None):
    errors = []
    normalized_destination = normalize_destination(destination)

    candidates = []

    if normalized_destination is not None:
        candidates.extend([
            lambda: current_iface.sendText(text=text, destinationId=normalized_destination),
            lambda: current_iface.sendText(text, destinationId=normalized_destination),
        ])

    candidates.extend([
        lambda: current_iface.sendText(text=text),
        lambda: current_iface.sendText(text),
    ])

    for fn in candidates:
        try:
            return fn()
        except Exception as exc:
            errors.append(str(exc))

    raise RuntimeError("All sendText attempts failed: " + " | ".join(errors))


@app.get("/status")
def status():
    connected = ensure_connected()
    current = get_interface()
    nodes = get_nodes_payload()

    device = None
    if current is not None:
        try:
            my_info = getattr(current, "myInfo", None)
            my_node_num = getattr(my_info, "my_node_num", None)
            if my_node_num is not None:
                device = {"node_num": my_node_num}
        except Exception:
            pass

    with state_lock:
        payload = {
            "ok": True,
            "connected": connected,
            "device": device,
            "last_connect_ts": last_connect_ts,
            "last_error": last_error,
            "message_count": len(recent_messages),
            "node_count": len(nodes),
            "port": SERIAL_PORT,
            "bridge_version": BRIDGE_VERSION,
            "meshtastic_version": MESHTASTIC_VERSION,
            "uptime_sec": now_ts() - start_ts,
        }

    return jsonify(payload)


@app.get("/nodes")
def nodes():
    if not ensure_connected():
        return jsonify({
            "ok": False,
            "error": last_error or "Bridge not connected",
            "nodes": [],
        }), 502

    return jsonify({
        "ok": True,
        "nodes": get_nodes_payload(),
    })


@app.get("/messages")
def messages():
    since = safe_int(request.args.get("since"), 0) or 0
    limit = safe_int(request.args.get("limit"), DEFAULT_MESSAGES_LIMIT) or DEFAULT_MESSAGES_LIMIT
    limit = max(1, min(limit, MAX_MESSAGES_LIMIT))

    with state_lock:
        filtered = [m for m in recent_messages if safe_int(m.get("ts"), 0) > since]
        payload = filtered[:limit]

    return jsonify({
        "ok": True,
        "messages": payload,
        "count": len(payload),
    })


@app.post("/send")
def send():
    if not ensure_connected():
        return jsonify({
            "ok": False,
            "error": last_error or "Bridge not connected",
        }), 502

    payload = request.get_json(silent=True) or {}
    text = str(payload.get("text") or "").strip()
    destination = payload.get("destination")

    if not text:
        return jsonify({
            "ok": False,
            "error": "Missing text",
        }), 400

    current = get_interface()
    if current is None:
        return jsonify({
            "ok": False,
            "error": "Bridge not connected",
        }), 502

    try:
        result = send_text(current, text, destination)
        ts = now_ts()
        normalized_destination = normalize_destination(destination)

        msg = {
            "id": f"tx-{ts}-{abs(hash(text)) % 100000}",
            "ts": ts,
            "timestamp": ts,
            "from": "You",
            "from_num": None,
            "to": "Broadcast" if normalized_destination is None else str(normalized_destination),
            "to_num": normalized_destination,
            "channel": 0,
            "text": text,
            "type": "sent",
        }

        with state_lock:
            recent_messages.appendleft(msg)

        return jsonify({
            "ok": True,
            "message": msg,
            "result": str(result) if result is not None else None,
        })

    except Exception as exc:
        logging.exception("Send failed: %s", exc)
        set_error(str(exc))
        return jsonify({
            "ok": False,
            "error": str(exc),
        }), 500


@app.post("/reconnect")
def reconnect():
    ok = reconnect_interface()
    return jsonify({
        "ok": ok,
        "connected": ok,
        "last_error": last_error,
        "last_connect_ts": last_connect_ts,
        "port": SERIAL_PORT,
    }), (200 if ok else 502)


@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "connected": is_connected(),
        "bridge_version": BRIDGE_VERSION,
        "meshtastic_version": MESHTASTIC_VERSION,
        "uptime_sec": now_ts() - start_ts,
    })


@app.get("/")
def root():
    return jsonify({
        "ok": True,
        "service": "GridDown Meshtastic Bridge",
        "bridge_version": BRIDGE_VERSION,
        "connected": is_connected(),
        "port": SERIAL_PORT,
        "endpoints": ["/status", "/nodes", "/messages", "/send", "/reconnect", "/health"],
    })


def bootstrap() -> None:
    pub.subscribe(on_receive, "meshtastic.receive")
    connect_interface()


if __name__ == "__main__":
    bootstrap()
    app.run(host=HOST, port=PORT, debug=False, threaded=True)