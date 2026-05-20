"""SQLite persistence layer for Crop Sentry.

Replaces JSONL flat-file storage with a single SQLite database.
Thread-safe: uses check_same_thread=False and a module-level lock
for writes (reads are inherently safe in WAL mode).
"""

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "cropsentry.db"

_write_lock = threading.Lock()


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


_conn = _get_conn()


def init():
    """Create tables if they don't exist."""
    with _write_lock:
        _conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                name TEXT DEFAULT '',
                password_hash TEXT NOT NULL,
                salt TEXT NOT NULL,
                saved_fields TEXT DEFAULT '[]',
                created TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                created TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS subscribers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                ip_hash TEXT DEFAULT '',
                created TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS analytics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL,
                ip_hash TEXT DEFAULT '',
                ua TEXT DEFAULT '',
                created TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS consent_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ip_hash TEXT DEFAULT '',
                ua TEXT DEFAULT '',
                terms_version TEXT DEFAULT '',
                privacy_version TEXT DEFAULT '',
                created TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS webhooks (
                id TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                events TEXT NOT NULL,
                secret TEXT NOT NULL,
                zip_code TEXT,
                lat REAL,
                lon REAL,
                ip_hash TEXT DEFAULT '',
                created TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS push_subscriptions (
                id TEXT PRIMARY KEY,
                endpoint TEXT NOT NULL,
                keys TEXT NOT NULL,
                lat REAL,
                lon REAL,
                place TEXT,
                crop TEXT DEFAULT 'corn',
                alerts TEXT NOT NULL,
                ip_hash TEXT DEFAULT '',
                created TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS eval_counter (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                count INTEGER DEFAULT 0
            );

            INSERT OR IGNORE INTO eval_counter (id, count) VALUES (1, 0);

            CREATE TABLE IF NOT EXISTS prediction_snapshots (
                id TEXT PRIMARY KEY,
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                place TEXT DEFAULT '',
                crop TEXT NOT NULL,
                survival_pct REAL,
                survival_low REAL,
                survival_high REAL,
                recommendation TEXT DEFAULT '',
                factor_scores TEXT DEFAULT '{}',
                soil_temp_f REAL,
                air_temp_f REAL,
                precip_mm REAL,
                eval_date TEXT NOT NULL,
                created TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS field_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id TEXT,
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                crop TEXT NOT NULL,
                planting_date TEXT NOT NULL,
                emergence_pct REAL,
                days_to_emerge INTEGER,
                stand_quality TEXT DEFAULT '',
                frost_damage INTEGER DEFAULT 0,
                disease_issues TEXT DEFAULT '',
                pest_issues TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                reporter_hash TEXT DEFAULT '',
                created TEXT NOT NULL,
                FOREIGN KEY (snapshot_id) REFERENCES prediction_snapshots(id)
            );

            CREATE TABLE IF NOT EXISTS nass_benchmarks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state TEXT NOT NULL,
                crop TEXT NOT NULL,
                week_ending TEXT NOT NULL,
                pct_planted REAL,
                pct_emerged REAL,
                condition_good_excellent REAL,
                fetched TEXT NOT NULL,
                UNIQUE(state, crop, week_ending)
            );
        """)


# ---- Eval counter --------------------------------------------------------

def get_eval_count() -> int:
    row = _conn.execute("SELECT count FROM eval_counter WHERE id=1").fetchone()
    return row["count"] if row else 0


def increment_eval_count() -> int:
    with _write_lock:
        _conn.execute("UPDATE eval_counter SET count = count + 1 WHERE id=1")
        _conn.commit()
    return get_eval_count()


# ---- Analytics -----------------------------------------------------------

def log_pageview(path: str, ip_hash: str, ua: str):
    with _write_lock:
        _conn.execute(
            "INSERT INTO analytics (path, ip_hash, ua, created) VALUES (?, ?, ?, ?)",
            (path, ip_hash, ua[:200], datetime.now(timezone.utc).isoformat()),
        )
        _conn.commit()


# ---- Consent -------------------------------------------------------------

def log_consent(ip_hash: str, ua: str, terms_v: str, privacy_v: str):
    with _write_lock:
        _conn.execute(
            "INSERT INTO consent_log (ip_hash, ua, terms_version, privacy_version, created) VALUES (?, ?, ?, ?, ?)",
            (ip_hash, ua[:200], terms_v, privacy_v, datetime.now(timezone.utc).isoformat()),
        )
        _conn.commit()


# ---- Subscribers ---------------------------------------------------------

def add_subscriber(email: str, ip_hash: str):
    with _write_lock:
        _conn.execute(
            "INSERT INTO subscribers (email, ip_hash, created) VALUES (?, ?, ?)",
            (email, ip_hash, datetime.now(timezone.utc).isoformat()),
        )
        _conn.commit()


# ---- Users ---------------------------------------------------------------

def find_user_by_email(email: str) -> dict | None:
    row = _conn.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
    if not row:
        return None
    u = dict(row)
    u["saved_fields"] = json.loads(u.get("saved_fields") or "[]")
    return u


def find_user_by_id(user_id: str) -> dict | None:
    row = _conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    if not row:
        return None
    u = dict(row)
    u["saved_fields"] = json.loads(u.get("saved_fields") or "[]")
    return u


def create_user(user_id: str, email: str, name: str, password_hash: str, salt: str) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    with _write_lock:
        _conn.execute(
            "INSERT INTO users (id, email, name, password_hash, salt, saved_fields, created) VALUES (?, ?, ?, ?, ?, '[]', ?)",
            (user_id, email, name, password_hash, salt, now),
        )
        _conn.commit()
    return {"id": user_id, "email": email, "name": name, "saved_fields": [], "created": now}


def update_user_fields(user_id: str, saved_fields: list):
    with _write_lock:
        _conn.execute(
            "UPDATE users SET saved_fields=? WHERE id=?",
            (json.dumps(saved_fields), user_id),
        )
        _conn.commit()


# ---- Sessions ------------------------------------------------------------

def create_session(token: str, user_id: str):
    with _write_lock:
        _conn.execute(
            "INSERT INTO sessions (token, user_id, created) VALUES (?, ?, ?)",
            (token, user_id, datetime.now(timezone.utc).isoformat()),
        )
        _conn.commit()


def get_session(token: str) -> dict | None:
    row = _conn.execute("SELECT * FROM sessions WHERE token=?", (token,)).fetchone()
    return dict(row) if row else None


def delete_expired_sessions(max_age_seconds: int):
    """Purge sessions older than max_age_seconds."""
    cutoff = datetime.now(timezone.utc).timestamp() - max_age_seconds
    with _write_lock:
        _conn.execute(
            "DELETE FROM sessions WHERE created < ?",
            (datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat(),),
        )
        _conn.commit()


# ---- Webhooks ------------------------------------------------------------

def add_webhook(hook_id: str, url: str, events: list, secret: str, zip_code: str | None, lat: float | None, lon: float | None, ip_hash: str):
    with _write_lock:
        _conn.execute(
            "INSERT INTO webhooks (id, url, events, secret, zip_code, lat, lon, ip_hash, created) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (hook_id, url, json.dumps(events), secret, zip_code, lat, lon, ip_hash, datetime.now(timezone.utc).isoformat()),
        )
        _conn.commit()


def list_webhooks() -> list[dict]:
    rows = _conn.execute("SELECT id, url, events, created FROM webhooks").fetchall()
    return [{"id": r["id"], "url": r["url"], "events": json.loads(r["events"]), "created": r["created"]} for r in rows]


def get_webhooks_for_event(event: str) -> list[dict]:
    rows = _conn.execute("SELECT * FROM webhooks").fetchall()
    result = []
    for r in rows:
        events = json.loads(r["events"])
        if event in events:
            d = dict(r)
            d["events"] = events
            result.append(d)
    return result


def delete_webhook(hook_id: str) -> bool:
    with _write_lock:
        cur = _conn.execute("DELETE FROM webhooks WHERE id=?", (hook_id,))
        _conn.commit()
        return cur.rowcount > 0


# ---- Push subscriptions --------------------------------------------------

def add_push_sub(sub_id: str, endpoint: str, keys: dict, lat: float | None, lon: float | None, place: str | None, crop: str, alerts: list, ip_hash: str):
    with _write_lock:
        _conn.execute(
            "INSERT INTO push_subscriptions (id, endpoint, keys, lat, lon, place, crop, alerts, ip_hash, created) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (sub_id, endpoint, json.dumps(keys), lat, lon, place, crop, json.dumps(alerts), ip_hash, datetime.now(timezone.utc).isoformat()),
        )
        _conn.commit()


def list_push_subs_by_ip(ip_hash: str) -> list[dict]:
    rows = _conn.execute("SELECT id, place, crop, alerts, created FROM push_subscriptions WHERE ip_hash=?", (ip_hash,)).fetchall()
    return [{"id": r["id"], "place": r["place"], "crop": r["crop"], "alerts": json.loads(r["alerts"]), "created": r["created"]} for r in rows]


def get_push_subs_for_alert(crop: str, lat: float, lon: float, alert_type: str) -> list[dict]:
    rows = _conn.execute("SELECT * FROM push_subscriptions WHERE crop=?", (crop,)).fetchall()
    result = []
    for r in rows:
        s_lat, s_lon = r["lat"], r["lon"]
        if s_lat is not None and s_lon is not None:
            if abs(s_lat - lat) > 0.5 or abs(s_lon - lon) > 0.5:
                continue
        alerts = json.loads(r["alerts"])
        if alert_type in alerts:
            d = dict(r)
            d["keys"] = json.loads(d["keys"])
            d["alerts"] = alerts
            result.append(d)
    return result


def delete_push_sub(sub_id: str) -> bool:
    with _write_lock:
        cur = _conn.execute("DELETE FROM push_subscriptions WHERE id=?", (sub_id,))
        _conn.commit()
        return cur.rowcount > 0


# ---- Prediction snapshots ------------------------------------------------

def save_prediction_snapshot(
    snap_id: str, lat: float, lon: float, place: str, crop: str,
    survival_pct: float | None, survival_low: float | None, survival_high: float | None,
    recommendation: str, factor_scores: dict,
    soil_temp_f: float | None, air_temp_f: float | None, precip_mm: float | None,
    eval_date: str,
):
    with _write_lock:
        _conn.execute(
            """INSERT OR REPLACE INTO prediction_snapshots
            (id, lat, lon, place, crop, survival_pct, survival_low, survival_high,
             recommendation, factor_scores, soil_temp_f, air_temp_f, precip_mm,
             eval_date, created)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (snap_id, lat, lon, place, crop, survival_pct, survival_low, survival_high,
             recommendation, json.dumps(factor_scores), soil_temp_f, air_temp_f, precip_mm,
             eval_date, datetime.now(timezone.utc).isoformat()),
        )
        _conn.commit()


def get_prediction_snapshot(snap_id: str) -> dict | None:
    row = _conn.execute("SELECT * FROM prediction_snapshots WHERE id=?", (snap_id,)).fetchone()
    if not row:
        return None
    d = dict(row)
    d["factor_scores"] = json.loads(d.get("factor_scores") or "{}")
    return d


def get_snapshots_near(lat: float, lon: float, crop: str, radius_deg: float = 0.1) -> list[dict]:
    rows = _conn.execute(
        """SELECT * FROM prediction_snapshots
           WHERE crop=? AND abs(lat-?) < ? AND abs(lon-?) < ?
           ORDER BY created DESC LIMIT 50""",
        (crop, lat, radius_deg, lon, radius_deg),
    ).fetchall()
    return [dict(r) for r in rows]


# ---- Field outcomes ------------------------------------------------------

def save_field_outcome(
    snapshot_id: str | None, lat: float, lon: float, crop: str,
    planting_date: str, emergence_pct: float | None, days_to_emerge: int | None,
    stand_quality: str, frost_damage: bool, disease_issues: str, pest_issues: str,
    notes: str, reporter_hash: str,
) -> int:
    with _write_lock:
        cur = _conn.execute(
            """INSERT INTO field_outcomes
            (snapshot_id, lat, lon, crop, planting_date, emergence_pct, days_to_emerge,
             stand_quality, frost_damage, disease_issues, pest_issues, notes,
             reporter_hash, created)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (snapshot_id, lat, lon, crop, planting_date, emergence_pct, days_to_emerge,
             stand_quality, 1 if frost_damage else 0, disease_issues, pest_issues,
             notes, reporter_hash, datetime.now(timezone.utc).isoformat()),
        )
        _conn.commit()
        return cur.lastrowid


def get_outcomes_with_predictions(crop: str | None = None, limit: int = 200) -> list[dict]:
    """Join outcomes with their prediction snapshots for accuracy analysis."""
    query = """
        SELECT o.*, s.survival_pct as predicted_pct, s.recommendation as predicted_rec,
               s.survival_low as predicted_low, s.survival_high as predicted_high,
               s.soil_temp_f, s.air_temp_f, s.factor_scores
        FROM field_outcomes o
        LEFT JOIN prediction_snapshots s ON o.snapshot_id = s.id
    """
    params = []
    if crop:
        query += " WHERE o.crop = ?"
        params.append(crop)
    query += " ORDER BY o.created DESC LIMIT ?"
    params.append(limit)
    rows = _conn.execute(query, params).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["factor_scores"] = json.loads(d.get("factor_scores") or "{}") if d.get("factor_scores") else {}
        result.append(d)
    return result


def get_validation_stats(crop: str | None = None) -> dict:
    """Aggregate prediction accuracy stats."""
    rows = get_outcomes_with_predictions(crop, limit=1000)
    if not rows:
        return {"total_reports": 0, "with_predictions": 0}

    with_pred = [r for r in rows if r.get("predicted_pct") is not None and r.get("emergence_pct") is not None]
    total = len(rows)

    if not with_pred:
        return {"total_reports": total, "with_predictions": 0}

    errors = []
    within_ci = 0
    frost_correct = 0
    frost_total = 0
    for r in with_pred:
        pred = r["predicted_pct"]
        actual = r["emergence_pct"]
        errors.append(pred - actual)
        low = r.get("predicted_low") or pred
        high = r.get("predicted_high") or pred
        if low <= actual <= high:
            within_ci += 1
        if r.get("frost_damage") is not None:
            frost_total += 1
            if (pred < 60 and r["frost_damage"]) or (pred >= 60 and not r["frost_damage"]):
                frost_correct += 1

    mean_error = sum(errors) / len(errors)
    abs_errors = [abs(e) for e in errors]
    mae = sum(abs_errors) / len(abs_errors)

    return {
        "total_reports": total,
        "with_predictions": len(with_pred),
        "mean_error": round(mean_error, 1),
        "mean_absolute_error": round(mae, 1),
        "ci_coverage_pct": round(100 * within_ci / len(with_pred), 1) if with_pred else None,
        "frost_accuracy_pct": round(100 * frost_correct / frost_total, 1) if frost_total else None,
        "bias": "optimistic" if mean_error > 2 else "pessimistic" if mean_error < -2 else "well-calibrated",
    }


# ---- NASS benchmarks -----------------------------------------------------

def save_nass_benchmark(state: str, crop: str, week_ending: str,
                        pct_planted: float | None, pct_emerged: float | None,
                        condition_good_excellent: float | None):
    with _write_lock:
        _conn.execute(
            """INSERT OR REPLACE INTO nass_benchmarks
            (state, crop, week_ending, pct_planted, pct_emerged, condition_good_excellent, fetched)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (state, crop, week_ending, pct_planted, pct_emerged, condition_good_excellent,
             datetime.now(timezone.utc).isoformat()),
        )
        _conn.commit()


def get_nass_benchmarks(state: str = "MICHIGAN", crop: str | None = None, limit: int = 20) -> list[dict]:
    query = "SELECT * FROM nass_benchmarks WHERE state=?"
    params = [state]
    if crop:
        query += " AND crop=?"
        params.append(crop)
    query += " ORDER BY week_ending DESC LIMIT ?"
    params.append(limit)
    return [dict(r) for r in _conn.execute(query, params).fetchall()]


# ---- Migration from JSONL ------------------------------------------------

def migrate_from_jsonl(base_dir: Path):
    """One-time import from JSONL flat files into SQLite."""
    import logging
    log = logging.getLogger("cropsentry.migrate")

    # Eval count
    ec_file = base_dir / "eval_count.json"
    if ec_file.exists():
        try:
            data = json.loads(ec_file.read_text("utf-8"))
            count = int(data.get("count", 0))
            if count > get_eval_count():
                with _write_lock:
                    _conn.execute("UPDATE eval_counter SET count=? WHERE id=1", (count,))
                    _conn.commit()
                log.info("migrated eval_count: %d", count)
        except (json.JSONDecodeError, OSError):
            pass

    # Users
    users_file = base_dir / "users.jsonl"
    if users_file.exists():
        migrated = 0
        try:
            for line in users_file.read_text("utf-8").splitlines():
                if not line.strip():
                    continue
                u = json.loads(line)
                if not find_user_by_id(u["id"]):
                    with _write_lock:
                        _conn.execute(
                            "INSERT OR IGNORE INTO users (id, email, name, password_hash, salt, saved_fields, created) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (u["id"], u["email"], u.get("name", ""), u["password_hash"], u["salt"], json.dumps(u.get("saved_fields", [])), u.get("created", "")),
                        )
                        _conn.commit()
                    migrated += 1
        except (OSError, json.JSONDecodeError):
            pass
        if migrated:
            log.info("migrated %d users", migrated)

    # Sessions
    sessions_file = base_dir / "sessions.jsonl"
    if sessions_file.exists():
        migrated = 0
        try:
            for line in sessions_file.read_text("utf-8").splitlines():
                if not line.strip():
                    continue
                s = json.loads(line)
                if not get_session(s["token"]):
                    with _write_lock:
                        _conn.execute(
                            "INSERT OR IGNORE INTO sessions (token, user_id, created) VALUES (?, ?, ?)",
                            (s["token"], s["user_id"], s.get("created", "")),
                        )
                        _conn.commit()
                    migrated += 1
        except (OSError, json.JSONDecodeError):
            pass
        if migrated:
            log.info("migrated %d sessions", migrated)

    # Webhooks
    wh_file = base_dir / "webhooks.jsonl"
    if wh_file.exists():
        migrated = 0
        try:
            for line in wh_file.read_text("utf-8").splitlines():
                if not line.strip():
                    continue
                h = json.loads(line)
                with _write_lock:
                    _conn.execute(
                        "INSERT OR IGNORE INTO webhooks (id, url, events, secret, zip_code, lat, lon, ip_hash, created) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (h["id"], h["url"], json.dumps(h.get("events", [])), h["secret"], h.get("zip_code"), h.get("lat"), h.get("lon"), h.get("ip_hash", ""), h.get("created", "")),
                    )
                    _conn.commit()
                migrated += 1
        except (OSError, json.JSONDecodeError):
            pass
        if migrated:
            log.info("migrated %d webhooks", migrated)

    # Push subs
    push_file = base_dir / "push_subscriptions.jsonl"
    if push_file.exists():
        migrated = 0
        try:
            for line in push_file.read_text("utf-8").splitlines():
                if not line.strip():
                    continue
                s = json.loads(line)
                with _write_lock:
                    _conn.execute(
                        "INSERT OR IGNORE INTO push_subscriptions (id, endpoint, keys, lat, lon, place, crop, alerts, ip_hash, created) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (s["id"], s["endpoint"], json.dumps(s.get("keys", {})), s.get("lat"), s.get("lon"), s.get("place"), s.get("crop", "corn"), json.dumps(s.get("alerts", [])), s.get("ip_hash", ""), s.get("created", "")),
                    )
                    _conn.commit()
                migrated += 1
        except (OSError, json.JSONDecodeError):
            pass
        if migrated:
            log.info("migrated %d push subscriptions", migrated)
