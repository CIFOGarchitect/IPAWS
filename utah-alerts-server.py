#!/usr/bin/env python3
"""Utah Live Alerts board server.

Polls two production feeds once a minute and merges them into one board:
  * National Weather Service active-alerts feed for Utah (GeoJSON CAP)
  * FEMA IPAWS All-Hazards PUBLIC feed, filtered to Utah (CAP XML)

The IPAWS feed is the production feed at apps.fema.gov (NOT the staging /
test environment at tdl.apps.fema.gov, whose traffic must never mix with the
live board). Per FEMA's Oct 27, 2026 guidance, polling is no more than once
a minute and results are cached server-side. The IPAWS /recent/{timestamp}
endpoint returns alerts sent since that timestamp, so the poller keeps a
watermark and accumulates active IPAWS alerts in memory, pruning expired
ones each poll. Observed behavior in testing (Oct 27, 2026): the endpoint
returned the same single most-recent alert regardless of the timestamp
passed, so treat it as a rolling recent window, not a full history — on
restart the poller recovers only what is currently in that window, and
continuous running is what makes the IPAWS layer complete. The NWS feed
remains the reliable backbone; IPAWS is a live supplement while the
server runs.

IPAWS alerts are filtered to Utah by SAME geocodes starting with 049
(Utah FIPS state code). An IPAWS alert that duplicates an NWS alert (same
event, same counties, sent within 15 minutes) is shown once, labelled
"NWS + IPAWS", with the NWS copy kept as the fuller text.

Endpoints:
  /              -> the board page (utah-alerts-board.html, same directory)
  /api/alerts    -> JSON: { alerts, updated, sources, count, stale, error,
                            source_errors }

Standard library only, so it runs on Render's free tier as-is.
"""

import json
import os
import re
import threading
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
BOARD_FILE = os.path.join(HERE, "utah-alerts-board.html")

NWS_URL = "https://api.weather.gov/alerts/active?area=UT"
IPAWS_PUBLIC_BASE = "https://apps.fema.gov/IPAWSOPEN_EAS_SERVICE/rest/public/recent/"
USER_AGENT = "UtahAlertsBoard/1.0 (state alerting group dashboard)"
POLL_INTERVAL = 60  # seconds; FEMA asks for no more than one IPAWS poll/min
STALE_AFTER = 180   # seconds; API reports stale=true past this
IPAWS_BACKFILL = timedelta(hours=24)
IPAWS_OVERLAP = timedelta(minutes=5)

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

CACHE = {"alerts": [], "updated": None, "error": None,
         "sources": ["NWS"], "source_errors": {}}
CACHE_LOCK = threading.Lock()

# IPAWS accumulation state: id -> normalized alert, plus poll watermark.
IPAWS_STATE = {"alerts": {}, "watermark": None, "last_ok": False}


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
    alerts.sort(key=lambda a: a["sent"] or "", reverse=True)
    alerts.sort(key=lambda a: SEV_ORDER.get(a["severity"], 4))
    return alerts


# ---------------------------------------------------------------- IPAWS

def _local(tag):
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _find_text(elem, name):
    for child in elem:
        if _local(child.tag) == name:
            return (child.text or "").strip()
    return ""


def _info_text(info, name):
    return _find_text(info, name)


def _parse_iso(text):
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _ipaws_iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _cap_alerts_from_bytes(raw):
    """Parse CAP XML into (alert, info) pairs, tolerating the feed's shapes:
    a single <alert> document, a wrapper containing several <alert>
    children, or several concatenated <alert> documents."""
    pairs = []
    try:
        root = ET.fromstring(raw)
        candidates = []
        if _local(root.tag) == "alert":
            candidates = [root]
        else:
            candidates = [el for el in root.iter() if _local(el.tag) == "alert"]
        for alert in candidates:
            for info in alert:
                if _local(info.tag) == "info":
                    pairs.append((alert, info))
        if pairs:
            return pairs
    except ET.ParseError:
        pass
    # Fallback: split concatenated documents on <alert ...> boundaries.
    text = raw.decode("utf-8", errors="replace")
    chunks = re.split(r"(?=<alert[\s>])", text)
    for chunk in chunks:
        chunk = chunk.strip()
        if not chunk.startswith("<alert"):
            continue
        try:
            alert = ET.fromstring(chunk)
        except ET.ParseError:
            continue
        for info in alert:
            if _local(info.tag) == "info":
                pairs.append((alert, info))
    return pairs


def _normalize_ipaws(alert, info):
    # CAP puts identifier, sender, sent, status, and msgType at alert level;
    # event, severity, headline, description, expires, and area live in info.
    identifier = _find_text(alert, "identifier")
    sender = _info_text(info, "senderName") or _find_text(alert, "sender")
    same_codes = []
    event_code = "—"
    area_desc = ""
    ring = None
    for elem in info:
        name = _local(elem.tag)
        if name == "eventCode":
            vname = _find_text(elem, "valueName")
            value = _find_text(elem, "value")
            if vname == "SAME" and value:
                event_code = value
        elif name == "area":
            area_desc = _find_text(elem, "areaDesc")
            for sub in elem:
                if _local(sub.tag) == "geocode":
                    vname = _find_text(sub, "valueName")
                    if vname == "SAME":
                        for val in sub:
                            if _local(val.tag) == "value" and val.text:
                                same_codes.append(val.text.strip())
                elif _local(sub.tag) == "polygon" and sub.text:
                    pts = []
                    for pair in sub.text.split():
                        try:
                            lat, lon = pair.split(",", 1)
                            pts.append([float(lon), float(lat)])
                        except ValueError:
                            continue
                    if len(pts) >= 3:
                        ring = pts
    utah = [c for c in same_codes if len(c) == 6 and c.startswith("049")]
    if not utah:
        return None  # not a Utah alert; the feed is national
    counties = [COUNTY_NAMES.get(c[3:], "FIPS " + c) for c in utah]
    return {
        "id": "IPAWS:" + identifier if identifier else "",
        "event": _info_text(info, "event") or "Alert",
        "code": event_code,
        "category": _info_text(info, "category") or "—",
        "severity": _info_text(info, "severity") or "Unknown",
        "certainty": _info_text(info, "certainty") or "—",
        "urgency": _info_text(info, "urgency") or "—",
        "status": _find_text(alert, "status") or "—",
        "messageType": _find_text(alert, "msgType") or "—",
        "headline": _info_text(info, "headline") or _info_text(info, "event") or "Alert",
        "description": _info_text(info, "description") or "",
        "instruction": _info_text(info, "instruction") or "",
        "areaDesc": area_desc or "—",
        "counties": counties,
        "sent": _find_text(alert, "sent"),
        "expires": _info_text(info, "expires"),
        "sender": sender or "IPAWS",
        "ring": ring,
        "source": "IPAWS",
    }


def _prune_ipaws(now):
    keep = {}
    for alert_id, alert in IPAWS_STATE["alerts"].items():
        exp = _parse_iso(alert.get("expires") or "")
        if exp is None or exp > now:
            keep[alert_id] = alert
    IPAWS_STATE["alerts"] = keep


def fetch_ipaws():
    """Poll the production PUBLIC feed once and return active Utah alerts."""
    now = datetime.now(timezone.utc)
    watermark = IPAWS_STATE["watermark"]
    since = (watermark - IPAWS_OVERLAP) if watermark else (now - IPAWS_BACKFILL)
    url = IPAWS_PUBLIC_BASE + _ipaws_iso(since)
    req = urllib.request.Request(
        url, headers={"Accept": "application/xml", "User-Agent": USER_AGENT}
    )
    with urllib.request.urlopen(req, timeout=25) as resp:
        raw = resp.read()
    for alert, info in _cap_alerts_from_bytes(raw):
        alert = _normalize_ipaws(alert, info)
        if alert and alert["id"]:
            IPAWS_STATE["alerts"][alert["id"]] = alert
    IPAWS_STATE["watermark"] = now
    IPAWS_STATE["last_ok"] = True
    _prune_ipaws(now)
    alerts = sorted(IPAWS_STATE["alerts"].values(),
                    key=lambda a: a["sent"] or "", reverse=True)
    alerts.sort(key=lambda a: SEV_ORDER.get(a["severity"], 4))
    return alerts


def _same_alert(a, b):
    """Near-duplicate test for NWS/IPAWS copies of one alert."""
    if a["event"] != b["event"]:
        return False
    if sorted(a["counties"]) != sorted(b["counties"]):
        return False
    ta, tb = _parse_iso(a.get("sent") or ""), _parse_iso(b.get("sent") or "")
    if ta is None or tb is None:
        return a["headline"] == b["headline"]
    return abs((ta - tb).total_seconds()) <= 900


def fetch_all():
    """Aggregate NWS + IPAWS into one deduplicated list and source list."""
    alerts, sources, errors = [], [], {}
    nws, ipaws = [], []
    try:
        nws = fetch_nws()
        sources.append("NWS")
    except Exception as exc:
        errors["NWS"] = str(exc)
    try:
        ipaws = fetch_ipaws()
        sources.append("IPAWS")
    except Exception as exc:
        errors["IPAWS"] = str(exc)
        # Keep last good IPAWS data (pruned of expired) rather than dropping it.
        _prune_ipaws(datetime.now(timezone.utc))
        ipaws = sorted(IPAWS_STATE["alerts"].values(),
                       key=lambda a: a["sent"] or "", reverse=True)
        if ipaws:
            sources.append("IPAWS (cached)")
    merged = list(nws)
    for alert in ipaws:
        dup = None
        for existing in merged:
            if _same_alert(existing, alert):
                dup = existing
                break
        if dup is not None:
            if dup["source"] == "NWS":
                dup["source"] = "NWS + IPAWS"
        else:
            merged.append(alert)
    merged.sort(key=lambda a: a["sent"] or "", reverse=True)
    merged.sort(key=lambda a: SEV_ORDER.get(a["severity"], 4))
    return merged, sources, errors


def poll_loop():
    while True:
        try:
            alerts, sources, errors = fetch_all()
            with CACHE_LOCK:
                CACHE["alerts"] = alerts
                CACHE["updated"] = datetime.now(timezone.utc).isoformat()
                CACHE["error"] = "; ".join(f"{k}: {v}" for k, v in errors.items()) or None
                CACHE["sources"] = sources
                CACHE["source_errors"] = errors
            print(f"[poll] {len(alerts)} active Utah alerts via {sources}", flush=True)
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
                source_errors = dict(CACHE["source_errors"])
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
                "source_errors": source_errors,
            }
            self._send(200, json.dumps(payload), "application/json")
            return
        self._send(404, "Not found.", "text/plain")


def main():
    port = int(os.environ.get("PORT", "8000"))
    # Prime the cache before serving so the first page load has data.
    try:
        alerts, sources, errors = fetch_all()
        with CACHE_LOCK:
            CACHE["alerts"] = alerts
            CACHE["updated"] = datetime.now(timezone.utc).isoformat()
            CACHE["sources"] = sources
            CACHE["source_errors"] = errors
            CACHE["error"] = "; ".join(f"{k}: {v}" for k, v in errors.items()) or None
        print(f"[startup] primed with {len(alerts)} alerts via {sources}", flush=True)
    except Exception as exc:
        print(f"[startup] prime failed (will retry in background): {exc}", flush=True)
    threading.Thread(target=poll_loop, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"[startup] serving on port {port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
