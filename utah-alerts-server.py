#!/usr/bin/env python3
"""Utah Live Alerts board server.

Polls the National Weather Service active-alerts feed for Utah once a minute,
normalizes it into the board's alert shape, and serves the board page plus a
JSON API. Standard library only, so it runs on Render's free tier as-is.

Endpoints:
  /              -> the board page (utah-alerts-board.html, same directory)
  /api/alerts    -> JSON: { alerts, updated, sources, count, stale, error }

Future sources (PBS WARN WEA feed, IPAWS All-Hazards feed) plug into
fetch_all() and are merged into the same normalized shape.
"""

import json
import os
import threading
import time
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
BOARD_FILE = os.path.join(HERE, "utah-alerts-board.html")

NWS_URL = "https://api.weather.gov/alerts/active?area=UT"
USER_AGENT = "UtahAlertsBoard/1.0 (state alerting group dashboard)"
POLL_INTERVAL = 60  # seconds
STALE_AFTER = 180   # seconds; API reports stale=true past this

COUNTY_NAMES = {
    "001": "Beaver", "003": "Box Elder", "005": "Cache", "007": "Carbon",
    "009": "Daggett", "011": "Davis", "013": "Duchesne", "015": "Emery",
    "017": "Garfield", "019": "Grand", "021": "Iron", "023": "Juab",
    "025": "Kane", "027": "Millard", "029": "Morgan", "031": "Piute",
    "033": "Rich", "035": "Salt Lake", "037": "San Juan", "039": "Sanpete",
    "041": "Sevier", "043": "Summit", "045": "Tooele", "047": "Uintah",
    "049": "Utah", "051": "Wasatch", "053": "Washington", "055": "Wayne",
    "057": "Weber",
}
SEV_ORDER = {"Extreme": 0, "Severe": 1, "Moderate": 2, "Minor": 3, "Unknown": 4}

CACHE = {"alerts": [], "updated": None, "error": None, "sources": ["NWS"]}
CACHE_LOCK = threading.Lock()


def utah_counties(props):
    out = []
    for code in (props.get("geocode") or {}).get("SAME") or []:
        code = str(code)
        if len(code) == 6 and code.startswith("049"):
            out.append(COUNTY_NAMES.get(code[3:], "FIPS " + code))
    return out


def polygon_ring(geometry):
    if not geometry or not geometry.get("coordinates"):
        return None
    coords = geometry["coordinates"]
    if geometry.get("type") == "Polygon":
        return coords[0] if coords else None
    if geometry.get("type") == "MultiPolygon" and coords and coords[0]:
        return coords[0][0]
    return None


def normalize_nws(feature):
    p = feature.get("properties") or {}
    same_codes = (p.get("eventCode") or {}).get("SAME") or []
    return {
        "id": p.get("id") or feature.get("id") or "",
        "event": p.get("event") or "Alert",
        "code": same_codes[0] if same_codes else "—",
        "category": p.get("category") or "—",
        "severity": p.get("severity") or "Unknown",
        "certainty": p.get("certainty") or "—",
        "urgency": p.get("urgency") or "—",
        "status": p.get("status") or "—",
        "messageType": p.get("messageType") or "—",
        "headline": p.get("headline") or p.get("event") or "Alert",
        "description": p.get("description") or "",
        "instruction": p.get("instruction") or "",
        "areaDesc": p.get("areaDesc") or "—",
        "counties": utah_counties(p),
        "sent": p.get("sent") or "",
        "expires": p.get("expires") or p.get("ends") or "",
        "sender": p.get("senderName") or p.get("sender") or "—",
        "ring": polygon_ring(feature.get("geometry")),
        "source": "NWS",
    }


def fetch_nws():
    req = urllib.request.Request(
        NWS_URL, headers={"Accept": "application/geo+json", "User-Agent": USER_AGENT}
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.load(resp)
    alerts = [normalize_nws(f) for f in (data.get("features") or [])]
    # Most recently sent first within each severity (stable sorts).
    alerts.sort(key=lambda a: a["sent"] or "", reverse=True)
    alerts.sort(key=lambda a: SEV_ORDER.get(a["severity"], 4))
    return alerts


def fetch_all():
    """Aggregate every configured source into one alert list (NWS today)."""
    alerts = fetch_nws()
    return alerts, ["NWS"]


def poll_loop():
    while True:
        try:
            alerts, sources = fetch_all()
            with CACHE_LOCK:
                CACHE["alerts"] = alerts
                CACHE["updated"] = datetime.now(timezone.utc).isoformat()
                CACHE["error"] = None
                CACHE["sources"] = sources
            print(f"[poll] {len(alerts)} active Utah alerts", flush=True)
        except Exception as exc:  # keep serving last good data
            with CACHE_LOCK:
                CACHE["error"] = str(exc)
            print(f"[poll] FAILED, keeping last good data: {exc}", flush=True)
        time.sleep(POLL_INTERVAL)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass  # keep logs to our own poll lines

    def _send(self, status, body, content_type):
        raw = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path in ("/", "/index.html", "/utah-alerts-board.html"):
            try:
                with open(BOARD_FILE, "r", encoding="utf-8") as fh:
                    self._send(200, fh.read(), "text/html; charset=utf-8")
            except FileNotFoundError:
                self._send(500, "Board page not found on server.", "text/plain")
            return
        if self.path == "/api/alerts":
            with CACHE_LOCK:
                alerts = list(CACHE["alerts"])
                updated = CACHE["updated"]
                error = CACHE["error"]
                sources = list(CACHE["sources"])
            stale = False
            if updated:
                age = (datetime.now(timezone.utc)
                       - datetime.fromisoformat(updated)).total_seconds()
                stale = age > STALE_AFTER
            payload = {
                "alerts": alerts,
                "updated": updated,
                "sources": sources,
                "count": len(alerts),
                "stale": stale,
                "error": error,
            }
            self._send(200, json.dumps(payload), "application/json")
            return
        self._send(404, "Not found.", "text/plain")


def main():
    port = int(os.environ.get("PORT", "8000"))
    # Prime the cache before serving so the first page load has data.
    try:
        alerts, sources = fetch_all()
        with CACHE_LOCK:
            CACHE["alerts"] = alerts
            CACHE["updated"] = datetime.now(timezone.utc).isoformat()
            CACHE["sources"] = sources
        print(f"[startup] primed with {len(alerts)} alerts", flush=True)
    except Exception as exc:
        print(f"[startup] prime failed (will retry in background): {exc}", flush=True)
    threading.Thread(target=poll_loop, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"[startup] serving on port {port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
