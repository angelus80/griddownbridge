from __future__ import annotations

import threading
import time
from collections import deque
from typing import Any

from flask import Flask, jsonify, request
from pubsub import pub
import meshtastic.serial_interface

HOST = "0.0.0.0"
PORT = 5001
SERIAL_PORT = "COM3"
MAX_MESSAGES = 200

app = Flask(__name__)

state_lock = threading.Lock()
interface = None
bridge_state: dict[str, Any] = {
    "connected": False,
    "port": SERIAL_PORT,
    "device": None,
    "last_error": None,
    "last_connect_ts": None,
}
recent_messages: deque[dict[str, Any]] = deque(maxlen=MAX_MESSAGES)


def now_ts() -> int:
    return int(time.time())


def on_receive(packet, **kwargs) -> None:
    decoded = packet.get("decoded", {}) if isinstance(packet, dict) else {}
    text = decoded.get("text")
    from_id = packet.get("fromId") or packet.get("from")
    to_id = packet.get("toId") or packet.get("to")
    channel = packet.get("channel") or 0

    msg = {
        "ts": now_ts(),
        "from": from_id,
        "to": to_id,
        "channel": channel,
        "text": text,
    }

    with state_lock:
        recent_messages.appendleft(msg)


def connect_loop() -> None:
    global interface

    pub.subscribe(on_receive, "meshtastic.receive")

    while True:
        try:
            if interface is None:
                interface = meshtastic.serial_interface.SerialInterface(devPath=SERIAL_PORT)
                with state_lock:
                    bridge_state["connected"] = True
                    bridge_state["last_error"] = None
                    bridge_state["last_connect_ts"] = now_ts()

                    my_info = getattr(interface, "myInfo", None)
                    if my_info:
                        bridge_state["device"] = {
                            "node_num": getattr(my_info, "my_node_num", None),
                        }

            time.sleep(2)

        except Exception as exc:
            with state_lock:
                bridge_state["connected"] = False
                bridge_state["last_error"] = str(exc)

            try:
                if interface:
                    interface.close()
            except Exception:
                pass

            interface = None
            time.sleep(5)


@app.get("/status")
def status():
    with state_lock:
        return jsonify({
            "ok": True,
            **bridge_state,
            "message_count": len(recent_messages),
        })


@app.get("/messages")
def messages():
    limit = max(1, min(int(request.args.get("limit", 50)), 200))
    with state_lock:
        payload = list(list(recent_messages)[:limit])
    return jsonify({"ok": True, "messages": payload})


@app.get("/nodes")
def nodes():
    global interface
    if interface is None:
        return jsonify({"ok": False, "error": "Not connected", "nodes": []}), 503

    try:
        nodes_out = []
        for node_id, node_data in (interface.nodes or {}).items():
            user = node_data.get("user", {})
            position = node_data.get("position", {})
            last_heard = node_data.get("lastHeard")

            nodes_out.append({
                "id": node_id,
                "long_name": user.get("longName"),
                "short_name": user.get("shortName"),
                "hw_model": user.get("hwModel"),
                "lat": position.get("latitude"),
                "lon": position.get("longitude"),
                "last_heard": last_heard,
                "raw": node_data,
            })

        return jsonify({"ok": True, "nodes": nodes_out})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "nodes": []}), 500


@app.post("/send")
def send():
    global interface

    data = request.get_json(silent=True) or {}
    text = (data.get("text") or "").strip()
    destination = data.get("destination")
    want_ack = bool(data.get("want_ack", True))

    if not text:
        return jsonify({"ok": False, "error": "Text is required"}), 400

    if not interface:
        return jsonify({"ok": False, "error": "No connection"}), 503

    try:
        if destination:
            interface.sendText(text=text, destinationId=destination, wantAck=want_ack)
        else:
            interface.sendText(text=text, wantAck=want_ack)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def main():
    t = threading.Thread(target=connect_loop, daemon=True)
    t.start()
    app.run(host=HOST, port=PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()