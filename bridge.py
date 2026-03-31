import os
import time
import logging
import threading
from collections import deque
from typing import Any, Optional

from flask import Flask, jsonify, request
from pubsub import pub

import meshtastic.serial_interface
from serial.tools import list_ports

try:
    from meshtastic import __version__ as MESHTASTIC_VERSION
except Exception:
    MESHTASTIC_VERSION = "unknown"


BRIDGE_VERSION = "1.7.2"

HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "5001"))
SERIAL_PORT = os.getenv("MESHTASTIC_PORT", "COM5")

MESSAGE_LIMIT = max(10, min(int(os.getenv("MESSAGE_LIMIT", "200")), 1000))
DEFAULT_MESSAGES_LIMIT = 50
MAX_MESSAGES_LIMIT = 200
NODE_ACTIVE_WINDOW_SEC = int(os.getenv("NODE_ACTIVE_WINDOW_SEC", "900"))
DELIVERY_CONFIRM_WINDOW_SEC = int(os.getenv("DELIVERY_CONFIRM_WINDOW_SEC", "180"))
DELIVERY_RETENTION_SEC = int(os.getenv("DELIVERY_RETENTION_SEC", "900"))
WATCHDOG_INTERVAL_SEC = max(5, int(os.getenv("WATCHDOG_INTERVAL_SEC", "15")))

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
last_send_ts: Optional[int] = None
last_send_error: Optional[str] = None
current_serial_port: Optional[str] = None
last_good_port: Optional[str] = os.getenv("MESHTASTIC_LAST_PORT") or None

# Runtime telemetry learned from packets. This helps when iface.nodes lacks
# fresh lastHeard / signal values.
node_runtime: dict[str, dict[str, Any]] = {}
pending_outbound: dict[str, dict[str, Any]] = {}
watchdog_last_run_ts: Optional[int] = None
watchdog_last_reconnect_ts: Optional[int] = None
watchdog_reconnect_count = 0
watchdog_state = "idle"


def now_ts() -> int:
    return int(time.time())


def set_error(message: Optional[str]) -> None:
    global last_error
    with state_lock:
        last_error = message


def set_last_send_error(message: Optional[str]) -> None:
    global last_send_error
    with state_lock:
        last_send_error = message


def mark_send_success() -> None:
    global last_send_ts, last_send_error
    with state_lock:
        last_send_ts = now_ts()
        last_send_error = None


def set_watchdog_state(state: str) -> None:
    global watchdog_state
    with state_lock:
        watchdog_state = str(state or "idle")


def mark_watchdog_run() -> None:
    global watchdog_last_run_ts
    with state_lock:
        watchdog_last_run_ts = now_ts()


def mark_watchdog_reconnect() -> None:
    global watchdog_last_reconnect_ts, watchdog_reconnect_count
    with state_lock:
        watchdog_last_reconnect_ts = now_ts()
        watchdog_reconnect_count += 1


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



def update_recent_message_locked(message_id: str, **fields: Any) -> Optional[dict]:
    target = str(message_id or "")
    if not target:
        return None

    for message in recent_messages:
        if str(message.get("id") or "") == target:
            message.update(fields)
            if "updated_ts" not in message or not message.get("updated_ts"):
                message["updated_ts"] = now_ts()
            return message
    return None


def sync_pending_outbound() -> None:
    now = now_ts()
    stale_ids = []
    with state_lock:
        for message_id, entry in list(pending_outbound.items()):
            sent_ts = safe_int(entry.get("sent_ts"), 0) or safe_int(entry.get("ts"), 0) or now
            age = max(0, now - sent_ts)
            status = str(entry.get("delivery_status") or "")

            if status == "sent" and age >= DELIVERY_CONFIRM_WINDOW_SEC:
                entry["delivery_status"] = "unconfirmed"
                entry["delivery_detail"] = "No return traffic seen from destination yet"
                entry["updated_ts"] = now
                update_recent_message_locked(
                    message_id,
                    delivery_status="unconfirmed",
                    delivery_detail="No return traffic seen from destination yet",
                    updated_ts=now,
                )

            if age >= DELIVERY_RETENTION_SEC:
                stale_ids.append(message_id)

        for message_id in stale_ids:
            pending_outbound.pop(message_id, None)


def confirm_delivery_for_sender(sender_id: Any, packet: Optional[dict] = None) -> None:
    normalized_sender = normalize_destination(sender_id)
    if normalized_sender is None:
        return

    packet_ts = now_ts()
    if isinstance(packet, dict):
        packet_ts = safe_int(packet.get("rxTime"), packet_ts) or packet_ts

    best_message_id = None
    best_sent_ts = -1

    with state_lock:
        sync_pending_outbound()
        sender_key = str(normalized_sender)
        for message_id, entry in pending_outbound.items():
            if str(entry.get("destination") or "") != sender_key:
                continue
            status = str(entry.get("delivery_status") or "")
            if status not in ("sent", "unconfirmed"):
                continue
            sent_ts = safe_int(entry.get("sent_ts"), 0) or safe_int(entry.get("ts"), 0) or 0
            if sent_ts > packet_ts:
                continue
            if sent_ts > best_sent_ts:
                best_sent_ts = sent_ts
                best_message_id = message_id

        if not best_message_id:
            return

        entry = pending_outbound.get(best_message_id) or {}
        detail = "Confirmed by return traffic from destination"
        entry["delivery_status"] = "delivered"
        entry["delivery_detail"] = detail
        entry["confirmed_ts"] = packet_ts
        entry["updated_ts"] = packet_ts
        update_recent_message_locked(
            best_message_id,
            delivery_status="delivered",
            delivery_detail=detail,
            confirmed_ts=packet_ts,
            updated_ts=packet_ts,
        )


def get_interface():
    with state_lock:
        return iface


def is_connected() -> bool:
    return get_interface() is not None


def scan_serial_ports() -> list[str]:
    ports = []
    try:
        for p in list_ports.comports():
            dev = getattr(p, "device", None)
            if dev:
                ports.append(str(dev))
    except Exception:
        pass

    seen = set()
    ordered = []
    for p in ports:
        key = p.upper()
        if key not in seen:
            seen.add(key)
            ordered.append(p)
    return ordered


def _set_current_port(port_name: Optional[str]) -> None:
    global current_serial_port, last_good_port
    with state_lock:
        current_serial_port = port_name
        if port_name:
            last_good_port = port_name


def _candidate_ports() -> list[str]:
    preferred = []
    if SERIAL_PORT:
        preferred.append(SERIAL_PORT)
    if last_good_port and last_good_port not in preferred:
        preferred.append(last_good_port)

    for p in scan_serial_ports():
        if p not in preferred:
            preferred.append(p)
    return preferred


def _touch_runtime_node(node_id: Any, packet: Optional[dict] = None) -> None:
    if node_id in (None, ""):
        return

    node_key = str(node_id)
    heard = now_ts()
    rx_snr = None
    rx_rssi = None

    if isinstance(packet, dict):
        heard = safe_int(packet.get("rxTime"), heard)
        rx_snr = packet.get("rxSnr")
        rx_rssi = packet.get("rxRssi")

    with state_lock:
        runtime = node_runtime.setdefault(node_key, {})
        runtime["last_seen"] = heard
        if rx_snr is not None:
            runtime["rxSnr"] = rx_snr
        if rx_rssi is not None:
            runtime["rxRssi"] = rx_rssi


def try_connect_port(port_name: str) -> bool:
    global iface, last_connect_ts
    logging.info("Connecting to Meshtastic on %s", port_name)
    new_iface = meshtastic.serial_interface.SerialInterface(devPath=port_name)
    with state_lock:
        iface = new_iface
        last_connect_ts = now_ts()
    _set_current_port(port_name)
    set_error(None)
    logging.info("Connected to Meshtastic on %s", port_name)
    return True


def auto_connect_interface() -> bool:
    global iface
    attempts = []

    for port_name in _candidate_ports():
        try:
            return try_connect_port(port_name)
        except Exception as exc:
            attempts.append(f"{port_name}: {exc}")
            logging.warning("Meshtastic connect failed on %s: %s", port_name, exc)
            with state_lock:
                iface = None

    _set_current_port(None)
    msg = "No usable Meshtastic serial port found"
    if attempts:
        msg += " | " + " | ".join(attempts)
    set_error(msg)
    logging.error(msg)
    return False


def connect_interface() -> bool:
    global iface

    with state_lock:
        if iface is not None:
            return True

    return auto_connect_interface()


def disconnect_interface() -> None:
    global iface

    with state_lock:
        current = iface
        iface = None
    _set_current_port(None)

    if current is not None:
        try:
            current.close()
        except Exception:
            pass


def reconnect_interface() -> bool:
    disconnect_interface()
    time.sleep(0.5)
    return auto_connect_interface()


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

    runtime = node_runtime.get(str(node_num), {})
    raw_last_heard = safe_int(raw.get("lastHeard"))
    runtime_last_seen = safe_int(runtime.get("last_seen"))
    last_heard = raw_last_heard or runtime_last_seen

    signal = raw.get("rxRssi")
    if signal is None:
        signal = runtime.get("rxRssi")
    if signal is None:
        signal = raw.get("snr")
    if signal is None:
        signal = runtime.get("rxSnr")

    rx_snr = raw.get("rxSnr")
    if rx_snr is None:
        rx_snr = runtime.get("rxSnr")

    # Presence must be time-bound. Signal/SNR alone can be stale telemetry and
    # should not keep a node online forever when no fresh packet has been heard.
    online = bool(
        last_heard and (now_ts() - last_heard) <= NODE_ACTIVE_WINDOW_SEC
    )

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
        "rxSnr": rx_snr,
        "signal": signal,
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

        # Update runtime cache from node table too, if available
        for node_num, raw in raw_nodes.items():
            heard = safe_int(raw.get("lastHeard"))
            if heard:
                with state_lock:
                    runtime = node_runtime.setdefault(str(node_num), {})
                    runtime["last_seen"] = heard
                    if raw.get("rxSnr") is not None:
                        runtime["rxSnr"] = raw.get("rxSnr")
                    if raw.get("rxRssi") is not None:
                        runtime["rxRssi"] = raw.get("rxRssi")

        nodes = [normalize_node(node_num, raw) for node_num, raw in raw_nodes.items()]
        nodes.sort(key=lambda n: (not n["online"], (n["name"] or "").lower()))
        return nodes
    except Exception as exc:
        logging.exception("Failed to load nodes: %s", exc)
        set_error(f"nodes error: {exc}")
        return []


def summarize_mesh(nodes: list[dict]) -> dict:
    sync_pending_outbound()
    now = now_ts()
    online_nodes = [n for n in nodes if n.get("online")]
    recent_message_ts = 0
    with state_lock:
        if recent_messages:
            recent_message_ts = max(safe_int(m.get("ts"), 0) or 0 for m in recent_messages)

    recent_inbound = bool(recent_message_ts and (now - recent_message_ts) <= 300)
    freshest_node_ts = 0
    snr_values: list[float] = []
    signal_values: list[float] = []

    for node in nodes:
        ts = safe_int(node.get("lastHeard"), 0) or 0
        if ts > freshest_node_ts:
            freshest_node_ts = ts

        rx_snr = safe_float(node.get("rxSnr"))
        if rx_snr is None:
            rx_snr = safe_float(node.get("snr"))
        if rx_snr is not None:
            snr_values.append(rx_snr)

        signal = safe_float(node.get("signal"))
        if signal is not None:
            signal_values.append(signal)

    avg_rx_snr = round(sum(snr_values) / len(snr_values), 2) if snr_values else None
    avg_signal = round(sum(signal_values) / len(signal_values), 2) if signal_values else None

    if not is_connected():
        route_status = "bridge_down"
    elif not nodes:
        route_status = "no_nodes"
    elif not online_nodes:
        route_status = "stale_mesh"
    elif recent_inbound and len(online_nodes) >= 3:
        route_status = "mesh_stable"
    elif recent_inbound:
        route_status = "mesh_active"
    elif len(online_nodes) >= 3:
        route_status = "mesh_degraded"
    else:
        route_status = "limited_route"

    network_score = 0
    if is_connected():
        network_score += 25
    if nodes:
        network_score += 10
    network_score += min(len(online_nodes), 5) * 8
    if recent_inbound:
        network_score += 20
    if freshest_node_ts:
        age = max(0, now - freshest_node_ts)
        if age <= 120:
            network_score += 10
        elif age <= 300:
            network_score += 5

    if avg_rx_snr is not None:
        if avg_rx_snr >= 8:
            network_score += 15
        elif avg_rx_snr >= 3:
            network_score += 10
        elif avg_rx_snr >= 0:
            network_score += 5
        elif avg_rx_snr <= -8:
            network_score -= 5

    if avg_signal is not None:
        if avg_signal >= -85:
            network_score += 10
        elif avg_signal >= -95:
            network_score += 6
        elif avg_signal >= -105:
            network_score += 2
        else:
            network_score -= 5

    network_score = max(0, min(100, int(round(network_score))))

    if route_status == "bridge_down":
        network_quality = "offline"
        mesh_banner = "BRIDGE OFFLINE"
    elif route_status == "no_nodes":
        network_quality = "poor"
        mesh_banner = "DEVICE ONLY"
    elif route_status == "stale_mesh":
        network_quality = "poor"
        mesh_banner = "STALE MESH"
    elif network_score >= 85:
        network_quality = "excellent"
        mesh_banner = "MESH STABLE"
    elif network_score >= 65:
        network_quality = "good"
        mesh_banner = "MESH ACTIVE"
    elif network_score >= 40:
        network_quality = "degraded"
        mesh_banner = "MESH DEGRADED"
    else:
        network_quality = "poor"
        mesh_banner = "LIMITED ROUTE"

    return {
        "node_count": len(nodes),
        "online_node_count": len(online_nodes),
        "recent_message_ts": recent_message_ts or None,
        "recent_inbound": recent_inbound,
        "freshest_node_ts": freshest_node_ts or None,
        "route_status": route_status,
        "avg_rx_snr": avg_rx_snr,
        "avg_signal": avg_signal,
        "network_score": network_score,
        "network_quality": network_quality,
        "mesh_banner": mesh_banner,
    }


def estimate_delivery_confidence(mesh: dict, destination: Any = None) -> str:
    if normalize_destination(destination) is None:
        return "low"

    route_status = str(mesh.get("route_status") or "")
    network_score = safe_int(mesh.get("network_score"), 0) or 0
    online_node_count = safe_int(mesh.get("online_node_count"), 0) or 0

    if route_status == "mesh_stable" and network_score >= 80 and online_node_count >= 3:
        return "high"
    if route_status in ("mesh_stable", "mesh_active", "mesh_degraded") and network_score >= 45:
        return "medium"
    return "low"


def on_receive(packet, interface=None, **kwargs) -> None:
    try:
        if not isinstance(packet, dict):
            return

        decoded = packet.get("decoded", {}) or {}
        text = decoded.get("text")

        from_id = packet.get("fromId") or packet.get("from")
        to_id = packet.get("toId") or packet.get("to")

        confirm_delivery_for_sender(from_id, packet)

        logging.info("PKT from=%s to=%s portnum=%s has_text=%s",
            from_id, to_id,
            decoded.get("portnum", "?"),
            bool(text)
        )

        _touch_runtime_node(from_id, packet)
        _touch_runtime_node(to_id, packet)

        if not text:
            return

        current = get_interface()
        nodes = getattr(current, "nodes", {}) if current is not None else {}

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

    if normalized_destination is not None:
        candidates = [
            lambda: current_iface.sendText(text=text, destinationId=normalized_destination),
            lambda: current_iface.sendText(text, destinationId=normalized_destination),
        ]
        failure_label = f"direct send to {normalized_destination}"
    else:
        candidates = [
            lambda: current_iface.sendText(text=text),
            lambda: current_iface.sendText(text),
        ]
        failure_label = "broadcast send"

    for fn in candidates:
        try:
            return fn()
        except Exception as exc:
            errors.append(str(exc))

    raise RuntimeError(f"All {failure_label} attempts failed: " + " | ".join(errors))


@app.get("/status")
def status():
    connected = ensure_connected()
    current = get_interface()
    nodes = get_nodes_payload()
    mesh = summarize_mesh(nodes)

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
            "last_send_ts": last_send_ts,
            "last_send_error": last_send_error,
            "message_count": len(recent_messages),
            "pending_direct_count": len([p for p in pending_outbound.values() if str(p.get("delivery_status") or "") in ("sent", "unconfirmed")]),
            "node_count": mesh["node_count"],
            "online_node_count": mesh["online_node_count"],
            "recent_message_ts": mesh["recent_message_ts"],
            "recent_inbound": mesh["recent_inbound"],
            "route_status": mesh["route_status"],
            "freshest_node_ts": mesh["freshest_node_ts"],
            "avg_rx_snr": mesh["avg_rx_snr"],
            "avg_signal": mesh["avg_signal"],
            "network_score": mesh["network_score"],
            "network_quality": mesh["network_quality"],
            "mesh_banner": mesh["mesh_banner"],
            "port": current_serial_port or SERIAL_PORT,
            "serial_candidates": scan_serial_ports(),
            "bridge_version": BRIDGE_VERSION,
            "meshtastic_version": MESHTASTIC_VERSION,
            "uptime_sec": now_ts() - start_ts,
            "watchdog_interval_sec": WATCHDOG_INTERVAL_SEC,
            "watchdog_last_run_ts": watchdog_last_run_ts,
            "watchdog_last_reconnect_ts": watchdog_last_reconnect_ts,
            "watchdog_reconnect_count": watchdog_reconnect_count,
            "watchdog_state": watchdog_state,
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

    sync_pending_outbound()

    with state_lock:
        filtered = [
            m for m in recent_messages
            if max(safe_int(m.get("ts"), 0) or 0, safe_int(m.get("updated_ts"), 0) or 0) > since
        ]
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
    client_message_id = str(payload.get("client_message_id") or "").strip()

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
        mark_send_success()
        ts = now_ts()
        normalized_destination = normalize_destination(destination)
        mesh = summarize_mesh(get_nodes_payload())
        network_score = mesh.get("network_score")
        network_quality = mesh.get("network_quality")
        delivery_confidence = estimate_delivery_confidence(mesh, normalized_destination)
        message_id = client_message_id or f"tx-{ts}-{abs(hash((text, normalized_destination))) % 100000}"
        delivery_status = "broadcast_sent" if normalized_destination is None else "sent"
        delivery_detail = (
            "Broadcast sent; delivery is not individually confirmable"
            if normalized_destination is None
            else "Awaiting return traffic from destination"
        )

        msg = {
            "id": message_id,
            "client_message_id": client_message_id or None,
            "ts": ts,
            "timestamp": ts,
            "updated_ts": ts,
            "from": "You",
            "from_num": None,
            "to": "Broadcast" if normalized_destination is None else str(normalized_destination),
            "to_num": normalized_destination,
            "channel": 0,
            "text": text,
            "type": "sent",
            "delivery_status": delivery_status,
            "delivery_detail": delivery_detail,
            "delivery_confidence": delivery_confidence,
            "network_quality": network_quality,
            "network_score": network_score,
        }

        with state_lock:
            recent_messages.appendleft(msg)
            if normalized_destination is not None:
                pending_outbound[message_id] = {
                    "id": message_id,
                    "destination": str(normalized_destination),
                    "text": text,
                    "ts": ts,
                    "sent_ts": ts,
                    "delivery_status": delivery_status,
                    "delivery_detail": delivery_detail,
                    "delivery_confidence": delivery_confidence,
                    "network_quality": network_quality,
                    "network_score": network_score,
                    "updated_ts": ts,
                }

        return jsonify({
            "ok": True,
            "message": msg,
            "result": str(result) if result is not None else None,
            "delivery_status": delivery_status,
            "delivery_detail": delivery_detail,
            "delivery_confidence": delivery_confidence,
            "network_quality": network_quality,
            "network_score": network_score,
        })

    except Exception as exc:
        logging.exception("Send failed: %s", exc)
        set_error(str(exc))
        set_last_send_error(str(exc))
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
        "port": current_serial_port or SERIAL_PORT,
        "serial_candidates": scan_serial_ports(),
    }), (200 if ok else 502)


@app.post("/rescan")
def rescan():
    ok = reconnect_interface()
    return jsonify({
        "ok": ok,
        "connected": ok,
        "last_error": last_error,
        "last_connect_ts": last_connect_ts,
        "port": current_serial_port or SERIAL_PORT,
        "serial_candidates": scan_serial_ports(),
    }), (200 if ok else 502)


@app.get("/health")
def health():
    mesh = summarize_mesh(get_nodes_payload())
    return jsonify({
        "ok": True,
        "connected": is_connected(),
        "bridge_version": BRIDGE_VERSION,
        "meshtastic_version": MESHTASTIC_VERSION,
        "uptime_sec": now_ts() - start_ts,
        "watchdog_state": watchdog_state,
        "watchdog_last_run_ts": watchdog_last_run_ts,
        "watchdog_reconnect_count": watchdog_reconnect_count,
        "route_status": mesh["route_status"],
        "online_node_count": mesh["online_node_count"],
        "recent_inbound": mesh["recent_inbound"],
        "freshest_node_ts": mesh["freshest_node_ts"],
        "avg_rx_snr": mesh["avg_rx_snr"],
        "avg_signal": mesh["avg_signal"],
        "network_score": mesh["network_score"],
        "network_quality": mesh["network_quality"],
        "mesh_banner": mesh["mesh_banner"],
    })


@app.get("/")
def root():
    return jsonify({
        "ok": True,
        "service": "GridDown Meshtastic Bridge",
        "bridge_version": BRIDGE_VERSION,
        "connected": is_connected(),
        "port": current_serial_port or SERIAL_PORT,
        "serial_candidates": scan_serial_ports(),
        "endpoints": ["/status", "/nodes", "/messages", "/send", "/reconnect", "/rescan", "/health"],
    })




def watchdog_loop() -> None:
    while True:
        try:
            mark_watchdog_run()
            sync_pending_outbound()

            current = get_interface()
            if current is None:
                set_watchdog_state("reconnecting")
                mark_watchdog_reconnect()
                auto_connect_interface()
                if is_connected():
                    set_watchdog_state("connected")
                else:
                    set_watchdog_state("waiting")
                time.sleep(WATCHDOG_INTERVAL_SEC)
                continue

            try:
                getattr(current, "myInfo", None)
                raw_nodes = getattr(current, "nodes", {}) or {}
                if isinstance(raw_nodes, dict):
                    for node_num, raw in raw_nodes.items():
                        heard = safe_int((raw or {}).get("lastHeard"))
                        if heard:
                            with state_lock:
                                runtime = node_runtime.setdefault(str(node_num), {})
                                runtime["last_seen"] = heard
                set_watchdog_state("connected")
            except Exception as exc:
                logging.warning("Watchdog detected unhealthy Meshtastic interface: %s", exc)
                set_error(f"watchdog: {exc}")
                set_watchdog_state("reconnecting")
                mark_watchdog_reconnect()
                reconnect_interface()
                if is_connected():
                    set_watchdog_state("connected")
                else:
                    set_watchdog_state("waiting")
        except Exception as exc:
            logging.exception("watchdog loop error: %s", exc)
            set_watchdog_state("error")
        time.sleep(WATCHDOG_INTERVAL_SEC)


def bootstrap() -> None:
    # Subscribe to both generic and text-specific topics
    # meshtastic.receive.text is more reliable on Windows with threaded Flask
    try:
        pub.subscribe(on_receive, "meshtastic.receive")
    except Exception:
        pass
    try:
        pub.subscribe(on_receive, "meshtastic.receive.text")
    except Exception:
        pass
    connect_interface()
    threading.Thread(target=watchdog_loop, name="bridge-watchdog", daemon=True).start()


if __name__ == "__main__":
    bootstrap()
    app.run(host=HOST, port=PORT, debug=False, threaded=True)
