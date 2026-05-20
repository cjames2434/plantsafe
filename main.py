"""FastAPI entry point.

Two route layers:
  * HTML pages (server-rendered Jinja2 shells)
  * JSON API (consumed by the page JS and external callers)

Run: uvicorn main:app --reload
"""

import hashlib
import hmac
import io
import json
import logging
import os
import re
import secrets
import time
import threading
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Query, Request, Response as FastAPIResponse, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

import db
import services
from services import UserInputs

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("cropsentry")

# ----- Environment validation ---------------------------------------------

_NWS_EMAIL = os.getenv("NWS_CONTACT_EMAIL", "")
if not _NWS_EMAIL or _NWS_EMAIL == "you@example.com":
    log.warning(
        "NWS_CONTACT_EMAIL is not set or still default. "
        "weather.gov requires a valid contact email in User-Agent headers. "
        "Copy .env.example to .env and fill it in."
    )
if os.getenv("CROPSENTRY_SECRET_KEY") or os.getenv("PLANTSAFE_SECRET_KEY"):
    _SECRET_KEY = os.getenv("CROPSENTRY_SECRET_KEY") or os.getenv("PLANTSAFE_SECRET_KEY")
else:
    _SECRET_KEY = secrets.token_urlsafe(32)
    log.warning(
        "CROPSENTRY_SECRET_KEY not set — generated ephemeral key. "
        "Sessions and CSRF tokens will not survive restarts. "
        "Set CROPSENTRY_SECRET_KEY in .env for production."
    )

BASE_DIR = Path(__file__).resolve().parent


# ----- CSRF protection ----------------------------------------------------

def _csrf_token(session_id: str) -> str:
    """Generate an HMAC-based CSRF token tied to the session."""
    return hmac.new(
        _SECRET_KEY.encode(), session_id.encode(), "sha256"
    ).hexdigest()[:32]

def _verify_csrf(request: Request) -> bool:
    """Check the X-CSRF-Token header against the session cookie."""
    token = request.headers.get("x-csrf-token", "")
    session_id = request.cookies.get("ps_session", "")
    if not token or not session_id:
        return False
    expected = _csrf_token(session_id)
    return hmac.compare_digest(token, expected)


# Bumping these dates invalidates prior client-side acknowledgments and re-prompts users.
TOS_EFFECTIVE_DATE = "2026-04-29"
PRIVACY_EFFECTIVE_DATE = "2026-04-29"

# Short, machine-readable disclaimer attached to every API response so
# downstream/API users can't claim they never saw it.
API_DISCLAIMER = (
    "Crop Sentry output is heuristic decision-support, not professional advice or any "
    "guarantee of outcome. Third-party data may be unavailable, delayed, or inaccurate. "
    "You are solely responsible for planting decisions. See /terms and /privacy."
)

DB_FILE = BASE_DIR / "cropsentry.db"

app = FastAPI(
    title="Crop Planting Predictor",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)


# ----- Security headers middleware ----------------------------------------

@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "geolocation=(self), camera=(), microphone=()"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    csp = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://unpkg.com https://cdn.jsdelivr.net; "
        "style-src 'self' 'unsafe-inline' https://unpkg.com https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com; "
        "img-src 'self' data: blob: https://*.tile.openstreetmap.org https://mesonet.agron.iastate.edu https://unpkg.com; "
        "connect-src 'self' https://nominatim.openstreetmap.org https://gis.sc.egov.usda.gov; "
        "frame-ancestors 'none'"
    )
    response.headers["Content-Security-Policy"] = csp
    return response


# ----- Global exception handler -------------------------------------------

from starlette.exceptions import HTTPException as StarletteHTTPException

@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        accept = request.headers.get("accept", "")
        if "text/html" in accept:
            return templates.TemplateResponse(
                request, "404.html", {"active": ""}, status_code=404,
            )
    if exc.status_code >= 500:
        log.error("HTTP %s at %s: %s", exc.status_code, request.url.path, exc.detail)
    return JSONResponse(
        {"error": exc.detail or "An error occurred"},
        status_code=exc.status_code,
    )

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    log.exception("Unhandled error at %s", request.url.path)
    accept = request.headers.get("accept", "")
    if "text/html" in accept:
        return templates.TemplateResponse(
            request, "500.html", {"active": ""}, status_code=500,
        )
    return JSONResponse(
        {"error": "Internal server error. Please try again."},
        status_code=500,
    )


# ----- Rate limiter -------------------------------------------------------
# Simple sliding-window per-IP rate limit on /api/evaluate.

RATE_LIMIT_PER_MIN = int(os.getenv("CROPSENTRY_RATE_LIMIT", os.getenv("PLANTSAFE_RATE_LIMIT", "30")))

_rate_buckets: dict[str, list[float]] = {}
_rate_lock = threading.Lock()


def _is_rate_limited(ip: str) -> bool:
    now = time.time()
    with _rate_lock:
        hits = _rate_buckets.get(ip, [])
        recent = [t for t in hits if now - t < 60]
        if len(recent) >= RATE_LIMIT_PER_MIN:
            _rate_buckets[ip] = recent
            return True
        recent.append(now)
        _rate_buckets[ip] = recent
        return False


# ----- Eval counter (delegates to db) -------------------------------------

def _read_eval_count() -> int:
    return db.get_eval_count()

def _increment_eval_count() -> int:
    return db.increment_eval_count()


# ----- Simple analytics (delegates to db) ---------------------------------

def _log_pageview(path: str, ip: str, ua: str):
    ip_hash = hashlib.sha256(ip.encode()).hexdigest()[:12] if ip else ""
    db.log_pageview(path, ip_hash, ua)


db.init()
db.migrate_from_jsonl(BASE_DIR)

app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")
templates.env.globals["tos_version"] = TOS_EFFECTIVE_DATE
templates.env.globals["privacy_version"] = PRIVACY_EFFECTIVE_DATE


# ----- HTML pages --------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def page_index(request: Request):
    from datetime import date
    today = date.today()
    corn_brands = sorted(services.SEED_CATALOG.get("corn", {}).keys())
    soy_brands = sorted(services.SEED_CATALOG.get("soybeans", {}).keys())
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "active": "home",
            "corn_brands": corn_brands,
            "soy_brands": soy_brands,
            "bulletin_date": today.strftime("%a · %d %b %Y").upper(),
            "bulletin_number": today.strftime("%Y-") + str(today.timetuple().tm_yday),
            "today_display": today.strftime("%d %b %Y"),
        },
    )


@app.get("/results", response_class=HTMLResponse)
def page_results(request: Request):
    """Results page is a thin shell — it fetches its own data via the JSON API."""
    corn_brands = sorted(services.SEED_CATALOG.get("corn", {}).keys())
    soy_brands = sorted(services.SEED_CATALOG.get("soybeans", {}).keys())
    return templates.TemplateResponse(
        request,
        "results.html",
        {"active": "home", "corn_brands": corn_brands, "soy_brands": soy_brands},
    )


@app.get("/map", response_class=HTMLResponse)
def page_map(request: Request):
    return templates.TemplateResponse(request, "map.html", {"active": "map"})


@app.get("/methodology", response_class=HTMLResponse)
def page_methodology(request: Request):
    """Full reference page — explains the survival math and every risk evaluator.
    Linked from the topnav and from the info popover on each risk card."""
    return templates.TemplateResponse(
        request,
        "methodology.html",
        {"active": "methodology", "methodology": services.get_methodology()},
    )


@app.get("/terms", response_class=HTMLResponse)
def page_terms(request: Request):
    return templates.TemplateResponse(
        request,
        "terms.html",
        {"active": "terms", "effective_date": TOS_EFFECTIVE_DATE},
    )


@app.get("/privacy", response_class=HTMLResponse)
def page_privacy(request: Request):
    return templates.TemplateResponse(
        request,
        "privacy.html",
        {"active": "privacy", "effective_date": PRIVACY_EFFECTIVE_DATE},
    )


@app.get("/faq", response_class=HTMLResponse)
def page_faq(request: Request):
    return templates.TemplateResponse(request, "faq.html", {"active": "faq"})


BLOG_DIR = BASE_DIR / "blog"

def _load_blog_posts() -> list[dict]:
    """Load blog posts from markdown files in the blog/ directory."""
    posts = []
    if not BLOG_DIR.is_dir():
        return posts
    for md_file in sorted(BLOG_DIR.glob("*.md"), reverse=True):
        try:
            text = md_file.read_text("utf-8")
            if not text.startswith("---"):
                continue
            _, front, body = text.split("---", 2)
            meta = {}
            for line in front.strip().splitlines():
                if ":" in line:
                    key, val = line.split(":", 1)
                    meta[key.strip()] = val.strip().strip('"')
            html_body = _md_to_html(body.strip())
            posts.append({
                "slug": meta.get("slug", md_file.stem),
                "date": meta.get("date", ""),
                "tag": meta.get("tag", ""),
                "title": meta.get("title", md_file.stem),
                "body": html_body,
            })
        except (OSError, ValueError):
            continue
    return posts

def _md_to_html(md: str) -> str:
    """Minimal markdown to HTML (paragraphs, bold, italic, links, lists)."""
    import re as _re
    lines = md.split("\n")
    html_parts = []
    in_list = False
    for line in lines:
        stripped = line.strip()
        if not stripped:
            if in_list:
                html_parts.append("</ul>")
                in_list = False
            continue
        if stripped.startswith("- "):
            if not in_list:
                html_parts.append("<ul>")
                in_list = True
            item = stripped[2:]
            item = _md_inline(item)
            html_parts.append(f"<li>{item}</li>")
        else:
            if in_list:
                html_parts.append("</ul>")
                in_list = False
            converted = _md_inline(stripped)
            html_parts.append(f"<p>{converted}</p>")
    if in_list:
        html_parts.append("</ul>")
    return "\n".join(html_parts)

def _md_inline(text: str) -> str:
    """Convert inline markdown: **bold**, *italic*, [text](url)."""
    import re as _re
    text = _re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', text)
    text = _re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
    text = _re.sub(r'\*(.+?)\*', r'<em>\1</em>', text)
    return text


@app.get("/blog", response_class=HTMLResponse)
def page_blog(request: Request):
    posts = _load_blog_posts()
    return templates.TemplateResponse(
        request, "blog.html", {"active": "blog", "posts": posts},
    )


# ----- SEO files ----------------------------------------------------------

@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt():
    return (
        "User-agent: *\n"
        "Allow: /\n"
        "Disallow: /api/\n"
        "\n"
        "Sitemap: https://cropsentry.app/sitemap.xml\n"
    )


@app.get("/sitemap.xml")
def sitemap_xml():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    pages = [
        ("/", "daily", "1.0"),
        ("/map", "daily", "0.9"),
        ("/methodology", "weekly", "0.7"),
        ("/blog", "weekly", "0.8"),
        ("/faq", "monthly", "0.6"),
        ("/terms", "monthly", "0.3"),
        ("/privacy", "monthly", "0.3"),
    ]
    urls = "\n".join(
        f"  <url><loc>https://cropsentry.app{p}</loc>"
        f"<lastmod>{today}</lastmod>"
        f"<changefreq>{freq}</changefreq>"
        f"<priority>{pri}</priority></url>"
        for p, freq, pri in pages
    )
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f"{urls}\n"
        "</urlset>"
    )
    return Response(content=xml, media_type="application/xml")


# ----- JSON API ----------------------------------------------------------

def _build_inputs(tillage, residue, manure_recent, previous_grass, herbicide, field_tiled=None, seeds_per_acre=None) -> UserInputs:
    spa = None
    if seeds_per_acre is not None:
        try:
            spa = int(seeds_per_acre)
        except (ValueError, TypeError):
            spa = None
    return UserInputs.from_query(
        tillage=tillage,
        residue=residue,
        manure_recent=(manure_recent in ("1", "true", "yes", "on")) if manure_recent else None,
        previous_grass=(previous_grass in ("1", "true", "yes", "on")) if previous_grass else None,
        herbicide_last_season=herbicide,
        field_tiled=(field_tiled in ("1", "true", "yes", "on")) if field_tiled else None,
        seeds_per_acre=spa,
    )


@app.get("/api/evaluate")
def api_evaluate(
    request: Request,
    zip_code: str | None = Query(None, alias="zip"),
    lat: float | None = None,
    lon: float | None = None,
    crop: str = Query("corn"),
    tillage: str | None = None,
    residue: str | None = None,
    manure_recent: str | None = None,
    previous_grass: str | None = None,
    herbicide: str | None = None,
    field_tiled: str | None = None,
    seeds_per_acre: str | None = Query(None),
    seed_brand: str | None = Query(None),
    seed_cultivar: str | None = Query(None),
):
    """Evaluate planting conditions by either zip code or coordinates."""
    client_ip = (request.client.host if request.client else "") or "unknown"
    if _is_rate_limited(client_ip):
        return JSONResponse(
            {"error": "Rate limit exceeded. Try again in a minute."},
            status_code=429,
        )
    _increment_eval_count()
    log.info("evaluate zip=%s lat=%s lon=%s crop=%s", zip_code, lat, lon, crop)
    inputs = _build_inputs(tillage, residue, manure_recent, previous_grass, herbicide, field_tiled, seeds_per_acre)
    seed_kw = {"seed_brand": seed_brand, "seed_cultivar": seed_cultivar}
    country_code = ""
    if lat is not None and lon is not None:
        place, country_code = services.reverse_geocode_with_country(lat, lon)
        # If the geocoder returned no country (e.g. ocean), fall back to a
        # bounding-box check for the continental US + Alaska + Hawaii.
        is_outside_us = False
        if country_code and country_code.upper() != "US":
            is_outside_us = True
        elif not country_code:
            in_conus = 24.0 <= lat <= 50.0 and -125.5 <= lon <= -66.0
            in_alaska = 51.0 <= lat <= 72.0 and -180.0 <= lon <= -129.0
            in_hawaii = 18.5 <= lat <= 22.5 and -160.5 <= lon <= -154.5
            if not (in_conus or in_alaska or in_hawaii):
                is_outside_us = True
        if is_outside_us:
            return {
                "location": {"lat": lat, "lon": lon, "place": place},
                "crop": {"key": crop, "label": crop.title()},
                "recommendation": "TBD",
                "verdict_detail": "We are currently only calculating survival for locations within the United States of America.",
                "survival": {"point_pct": None, "low_pct": None, "high_pct": None, "tbd": True},
                "risks": [],
                "plant_days": [],
                "best_days": [],
                "outside_us": True,
                "disclaimer": API_DISCLAIMER + " International coverage is not yet available.",
            }
        result = services.evaluate(lat, lon, place, crop=crop, inputs=inputs, **seed_kw)
    elif zip_code:
        rlat, rlon, place = services.get_coordinates(zip_code)
        result = services.evaluate(rlat, rlon, place, crop=crop, inputs=inputs, **seed_kw)
    else:
        return JSONResponse({"error": "Provide ?zip=... or ?lat=&lon="}, status_code=400)
    result["disclaimer"] = API_DISCLAIMER
    result["legal"] = {
        "terms_url": "/terms",
        "privacy_url": "/privacy",
        "terms_effective": TOS_EFFECTIVE_DATE,
        "privacy_effective": PRIVACY_EFFECTIVE_DATE,
    }
    if result.get("survival") and result["survival"].get("point_pct") is not None:
        _dispatch_webhooks("condition_change", {
            "event": "condition_change",
            "location": result.get("location"),
            "crop": result.get("crop", {}).get("key", crop),
            "survival_pct": result["survival"]["point_pct"],
            "recommendation": result.get("recommendation", ""),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        snap_id = hashlib.sha256(
            f"{result['location']['lat']:.4f},{result['location']['lon']:.4f},{crop},{datetime.now(timezone.utc).date().isoformat()}".encode()
        ).hexdigest()[:16]
        survival = result["survival"]
        factor_scores = {f["key"]: f["survival_factor"] for f in survival.get("per_factor", [])}
        hourly = result.get("hourly", {})
        soil_temps = hourly.get("soil_temp_f", [])
        air_temps = hourly.get("air_temp_f", [])
        precip = hourly.get("precip_in", [])
        try:
            db.save_prediction_snapshot(
                snap_id=snap_id,
                lat=result["location"]["lat"],
                lon=result["location"]["lon"],
                place=result["location"].get("place", ""),
                crop=crop,
                survival_pct=survival.get("point_pct"),
                survival_low=survival.get("low_pct"),
                survival_high=survival.get("high_pct"),
                recommendation=result.get("recommendation", ""),
                factor_scores=factor_scores,
                soil_temp_f=round(min(soil_temps[:48]), 1) if soil_temps else None,
                air_temp_f=round(min(air_temps[:48]), 1) if air_temps else None,
                precip_mm=round(sum(p for p in precip[:48] if p), 2) if precip else None,
                eval_date=datetime.now(timezone.utc).date().isoformat(),
            )
        except Exception:
            log.debug("snapshot save failed", exc_info=True)
        result["snapshot_id"] = snap_id
    return result


@app.get("/api/methodology")
def api_methodology():
    """Methodology payload for the in-card popover. Cached client-side after first fetch."""
    return services.get_methodology()


@app.get("/api/seeds")
def api_seeds(crop: str = Query("corn")):
    """Seed-brand catalog for the picker. Returns a flat list of brand+cultivar
    entries with display labels, search keywords, and the trait fields the
    server uses to tailor risk thresholds. The frontend renders this as a
    searchable, scrollable combo on the dashboard."""
    return {"crop": crop, "items": services.list_seed_catalog(crop)}


@app.get("/api/legal")
def api_legal():
    """Returns the current effective dates so the client can decide whether to re-prompt."""
    return {
        "terms_effective": TOS_EFFECTIVE_DATE,
        "privacy_effective": PRIVACY_EFFECTIVE_DATE,
        "disclaimer": API_DISCLAIMER,
    }


class ConsentRecord(BaseModel):
    terms_version: str
    privacy_version: str
    accepted_at: str  # ISO 8601, client-supplied for UX; server timestamp wins for evidence


@app.post("/api/consent")
async def api_consent(body: ConsentRecord, request: Request):
    """Append-only click-wrap log. Records that a user accepted a specific Terms/Privacy
    version. IP is hashed (not stored in the clear) — enough for evidentiary linking
    without keeping a raw identifier list around."""
    raw_ip = (request.client.host if request.client else "") or ""
    ip_hash = hashlib.sha256(raw_ip.encode("utf-8")).hexdigest()[:16] if raw_ip else ""
    ua = request.headers.get("user-agent", "")[:300]
    try:
        db.log_consent(ip_hash, ua, body.terms_version, body.privacy_version)
    except Exception:
        return JSONResponse({"ok": False, "error": "log unavailable"}, status_code=500)
    return {"ok": True}


# ----- Stats / subscribe / analytics / cache clear ------------------------

@app.get("/api/stats")
def api_stats():
    return {"eval_count": _read_eval_count()}


class SubscribeRequest(BaseModel):
    email: str


EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@app.post("/api/subscribe")
async def api_subscribe(body: SubscribeRequest, request: Request):
    email = body.email.strip().lower()
    if not EMAIL_RE.match(email):
        return JSONResponse({"ok": False, "error": "Invalid email address."}, status_code=400)
    ip_hash = hashlib.sha256(
        ((request.client.host if request.client else "") or "").encode()
    ).hexdigest()[:16]
    try:
        db.add_subscriber(email, ip_hash)
    except Exception:
        return JSONResponse({"ok": False, "error": "Could not save subscription."}, status_code=500)
    log.info("new subscriber: %s", email)
    return {"ok": True}


@app.post("/api/pageview")
async def api_pageview(request: Request):
    body = await request.json()
    path = str(body.get("path", "/"))[:200]
    ip = (request.client.host if request.client else "") or ""
    ua = request.headers.get("user-agent", "")
    _log_pageview(path, ip, ua)
    return {"ok": True}


@app.post("/api/cache/clear")
async def api_cache_clear():
    count = services._cache.size
    services._cache.clear()
    log.info("cache cleared: %d entries", count)
    return {"cleared": count}


# ----- User accounts (delegates to db) ------------------------------------

_users_lock = threading.Lock()

SESSION_MAX_AGE_SECONDS = 30 * 86400  # 30 days

def _hash_password(password: str, salt: str | None = None) -> tuple[str, str]:
    if salt is None:
        salt = secrets.token_hex(16)
    hashed = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 200_000).hex()
    return hashed, salt

def _find_user(email: str) -> dict | None:
    return db.find_user_by_email(email)

def _create_session(user_id: str) -> str:
    token = secrets.token_urlsafe(32)
    db.create_session(token, user_id)
    return token

def _get_session_user(request: Request) -> dict | None:
    token = request.cookies.get("ps_session")
    if not token:
        return None
    sess = db.get_session(token)
    if not sess:
        return None
    created = sess.get("created", "")
    if created:
        try:
            created_dt = datetime.fromisoformat(created)
            age = (datetime.now(timezone.utc) - created_dt).total_seconds()
            if age > SESSION_MAX_AGE_SECONDS:
                return None
        except (ValueError, TypeError):
            pass
    return db.find_user_by_id(sess["user_id"])

def _find_user_by_id(user_id: str) -> dict | None:
    return db.find_user_by_id(user_id)


class RegisterRequest(BaseModel):
    email: str
    password: str
    name: str = ""

class LoginRequest(BaseModel):
    email: str
    password: str


@app.post("/api/register")
async def api_register(body: RegisterRequest):
    email = body.email.strip().lower()
    if not EMAIL_RE.match(email):
        return JSONResponse({"ok": False, "error": "Invalid email address."}, status_code=400)
    if len(body.password) < 8:
        return JSONResponse({"ok": False, "error": "Password must be at least 8 characters."}, status_code=400)
    if _find_user(email):
        return JSONResponse({"ok": False, "error": "An account with this email already exists."}, status_code=409)
    hashed, salt = _hash_password(body.password)
    user_id = secrets.token_urlsafe(16)
    try:
        user = db.create_user(user_id, email, body.name.strip()[:100], hashed, salt)
    except Exception:
        return JSONResponse({"ok": False, "error": "Could not create account."}, status_code=500)
    token = _create_session(user["id"])
    resp = JSONResponse({"ok": True, "user": {"id": user["id"], "email": email, "name": user["name"]}, "csrf_token": _csrf_token(token)})
    resp.set_cookie("ps_session", token, httponly=True, secure=True, samesite="lax", max_age=SESSION_MAX_AGE_SECONDS)
    return resp


@app.post("/api/login")
async def api_login(body: LoginRequest):
    email = body.email.strip().lower()
    user = _find_user(email)
    if not user:
        return JSONResponse({"ok": False, "error": "Invalid email or password."}, status_code=401)
    hashed, _ = _hash_password(body.password, user["salt"])
    if hashed != user["password_hash"]:
        return JSONResponse({"ok": False, "error": "Invalid email or password."}, status_code=401)
    token = _create_session(user["id"])
    resp = JSONResponse({"ok": True, "user": {"id": user["id"], "email": user["email"], "name": user.get("name", "")}, "csrf_token": _csrf_token(token)})
    resp.set_cookie("ps_session", token, httponly=True, secure=True, samesite="lax", max_age=SESSION_MAX_AGE_SECONDS)
    return resp


@app.post("/api/logout")
async def api_logout(request: Request):
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("ps_session")
    return resp


@app.get("/api/me")
async def api_me(request: Request):
    user = _get_session_user(request)
    if not user:
        return JSONResponse({"ok": False, "error": "Not logged in."}, status_code=401)
    return {
        "ok": True,
        "user": {
            "id": user["id"],
            "email": user["email"],
            "name": user.get("name", ""),
            "saved_fields": user.get("saved_fields", []),
        },
    }


@app.post("/api/me/fields")
async def api_save_field(request: Request):
    user = _get_session_user(request)
    if not user:
        return JSONResponse({"ok": False, "error": "Not logged in."}, status_code=401)
    if not _verify_csrf(request):
        return JSONResponse({"ok": False, "error": "Invalid or missing CSRF token."}, status_code=403)
    body = await request.json()
    field_data = {
        "lat": body.get("lat"),
        "lon": body.get("lon"),
        "place": str(body.get("place", ""))[:200],
        "crop": str(body.get("crop", "corn"))[:20],
        "savedAt": datetime.now(timezone.utc).isoformat(),
    }
    fields = user.get("saved_fields", [])
    fields.append(field_data)
    fields = fields[-50:]
    try:
        db.update_user_fields(user["id"], fields)
    except Exception:
        return JSONResponse({"ok": False, "error": "Could not save."}, status_code=500)
    return {"ok": True}


# ----- Field photo uploads -------------------------------------------------

PHOTOS_DIR = BASE_DIR / "uploads" / "photos"
PHOTOS_DIR.mkdir(parents=True, exist_ok=True)
MAX_PHOTO_SIZE = 5 * 1024 * 1024  # 5 MB
ALLOWED_PHOTO_TYPES = {"image/jpeg", "image/png", "image/webp"}

@app.post("/api/me/fields/photo")
async def api_upload_photo(
    request: Request,
    file: UploadFile = File(...),
    field_idx: int = Query(0),
):
    """Upload a geotagged photo for a saved field."""
    user = _get_session_user(request)
    if not user:
        return JSONResponse({"ok": False, "error": "Not logged in."}, status_code=401)
    if not _verify_csrf(request):
        return JSONResponse({"ok": False, "error": "Invalid CSRF token."}, status_code=403)
    if file.content_type not in ALLOWED_PHOTO_TYPES:
        return JSONResponse({"ok": False, "error": "Only JPEG, PNG, and WebP images allowed."}, status_code=400)

    contents = await file.read()
    if len(contents) > MAX_PHOTO_SIZE:
        return JSONResponse({"ok": False, "error": "Photo must be under 5 MB."}, status_code=400)

    photo_id = secrets.token_urlsafe(16)
    ext = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp"}.get(file.content_type, ".jpg")
    photo_path = PHOTOS_DIR / f"{user['id']}_{photo_id}{ext}"
    photo_path.write_bytes(contents)

    photo_url = f"/uploads/photos/{photo_path.name}"
    fields = user.get("saved_fields", [])
    if 0 <= field_idx < len(fields):
        photos = fields[field_idx].get("photos", [])
        photos.append({
            "id": photo_id,
            "url": photo_url,
            "uploaded": datetime.now(timezone.utc).isoformat(),
        })
        fields[field_idx]["photos"] = photos[-10:]
        try:
            db.update_user_fields(user["id"], fields)
        except Exception:
            return JSONResponse({"ok": False, "error": "Could not save."}, status_code=500)

    return {"ok": True, "photo_id": photo_id, "url": photo_url}


# Serve uploaded photos
app.mount("/uploads", StaticFiles(directory=BASE_DIR / "uploads"), name="uploads")


# ----- Webhook subscriptions -----------------------------------------------

class WebhookRegister(BaseModel):
    url: str
    events: list[str] = ["condition_change"]
    zip_code: str | None = None
    lat: float | None = None
    lon: float | None = None


@app.post("/api/webhooks")
async def api_webhook_register(body: WebhookRegister, request: Request):
    if not body.url.startswith("https://"):
        return JSONResponse({"ok": False, "error": "Webhook URL must use HTTPS."}, status_code=400)
    valid_events = {"condition_change", "frost_alert", "optimal_window", "severe_weather"}
    for ev in body.events:
        if ev not in valid_events:
            return JSONResponse(
                {"ok": False, "error": f"Unknown event '{ev}'. Valid: {', '.join(sorted(valid_events))}"},
                status_code=400,
            )
    hook_id = secrets.token_urlsafe(12)
    secret = secrets.token_urlsafe(24)
    ip_hash = hashlib.sha256(
        ((request.client.host if request.client else "") or "").encode()
    ).hexdigest()[:16]
    try:
        db.add_webhook(hook_id, body.url, body.events, secret, body.zip_code, body.lat, body.lon, ip_hash)
    except Exception:
        return JSONResponse({"ok": False, "error": "Could not register webhook."}, status_code=500)
    log.info("webhook registered: %s -> %s", hook_id, body.url)
    return {
        "ok": True,
        "id": hook_id,
        "secret": secret,
        "events": body.events,
        "note": "Include this secret to verify payloads. We sign each delivery with HMAC-SHA256 in the X-CropSentry-Signature header.",
    }


def _dispatch_webhooks(event: str, payload: dict):
    """Fire matching webhooks in a background thread. HMAC-SHA256 signed."""
    import httpx as _hx

    hooks = db.get_webhooks_for_event(event)
    if not hooks:
        return

    def _send():
        for h in hooks:
            body_bytes = json.dumps(payload).encode()
            sig = hmac.new(h["secret"].encode(), body_bytes, "sha256").hexdigest()
            try:
                _hx.post(
                    h["url"],
                    content=body_bytes,
                    headers={
                        "Content-Type": "application/json",
                        "X-CropSentry-Signature": f"sha256={sig}",
                        "X-CropSentry-Event": event,
                        "X-CropSentry-Webhook-Id": h["id"],
                    },
                    timeout=10,
                )
                log.info("webhook delivered: %s -> %s", h["id"], event)
            except Exception:
                log.warning("webhook delivery failed: %s -> %s", h["id"], h["url"])

    threading.Thread(target=_send, daemon=True).start()


@app.get("/api/webhooks")
async def api_webhook_list(request: Request):
    return {"webhooks": db.list_webhooks()}


@app.delete("/api/webhooks/{hook_id}")
async def api_webhook_delete(hook_id: str):
    if not db.delete_webhook(hook_id):
        return JSONResponse({"ok": False, "error": "Webhook not found."}, status_code=404)
    return {"ok": True}


# ----- Push notification subscriptions ------------------------------------

ALERT_TYPES = {
    "frost_alert": "Frost/freeze warning when temps drop below threshold",
    "optimal_window": "Notification when planting conditions become optimal",
    "severe_weather": "Severe weather alerts (hail, high wind, tornado)",
    "crop_stress": "NDVI drop detected — possible crop stress",
    "spray_window": "Good spray window opening in next 24h",
    "soil_temp": "Soil temperature crossing planting threshold",
}


class PushSubscription(BaseModel):
    endpoint: str
    keys: dict
    lat: float | None = None
    lon: float | None = None
    place: str | None = None
    crop: str = "corn"
    alerts: list[str] = ["frost_alert", "optimal_window", "severe_weather"]


@app.post("/api/push/subscribe")
async def api_push_subscribe(body: PushSubscription, request: Request):
    """Register a browser push subscription for field alerts."""
    for a in body.alerts:
        if a not in ALERT_TYPES:
            return JSONResponse(
                {"ok": False, "error": f"Unknown alert type '{a}'. Valid: {', '.join(sorted(ALERT_TYPES))}"},
                status_code=400,
            )
    if not body.endpoint.startswith("https://"):
        return JSONResponse({"ok": False, "error": "Invalid push endpoint."}, status_code=400)

    sub_id = secrets.token_urlsafe(12)
    ip_hash = hashlib.sha256(
        ((request.client.host if request.client else "") or "").encode()
    ).hexdigest()[:16]
    try:
        db.add_push_sub(sub_id, body.endpoint, body.keys, body.lat, body.lon, body.place, body.crop, body.alerts, ip_hash)
    except Exception:
        return JSONResponse({"ok": False, "error": "Could not save subscription."}, status_code=500)

    return {
        "ok": True,
        "id": sub_id,
        "alerts": body.alerts,
        "note": "You will receive push notifications for the selected alert types.",
    }


@app.get("/api/push/subscriptions")
async def api_push_list(request: Request):
    """List active push subscriptions (for the current session/IP)."""
    ip_hash = hashlib.sha256(
        ((request.client.host if request.client else "") or "").encode()
    ).hexdigest()[:16]
    return {"subscriptions": db.list_push_subs_by_ip(ip_hash)}


@app.delete("/api/push/subscribe/{sub_id}")
async def api_push_unsubscribe(sub_id: str):
    """Remove a push subscription."""
    if not db.delete_push_sub(sub_id):
        return JSONResponse({"ok": False, "error": "Subscription not found."}, status_code=404)
    return {"ok": True}


@app.get("/api/push/alert-types")
async def api_alert_types():
    """List available alert types and their descriptions."""
    return {"alert_types": ALERT_TYPES}


@app.get("/api/push/check")
async def api_push_check(
    lat: float | None = None,
    lon: float | None = None,
    zip_code: str | None = Query(None, alias="zip"),
    crop: str = Query("corn"),
):
    """Check current alert conditions for a location (doesn't send push, just evaluates)."""
    if lat is not None and lon is not None:
        place = services.reverse_geocode(lat, lon)
    elif zip_code:
        lat, lon, place = services.get_coordinates(zip_code)
    else:
        return JSONResponse({"error": "Provide ?zip=... or ?lat=&lon="}, status_code=400)

    alerts = services.evaluate_alerts(lat, lon, place, crop)
    return {"location": place, "alerts": alerts}


@app.post("/api/push/send")
async def api_push_send(
    lat: float | None = None,
    lon: float | None = None,
    zip_code: str | None = Query(None, alias="zip"),
    crop: str = Query("corn"),
):
    """Evaluate alerts and send push notifications to matching subscribers."""
    if lat is not None and lon is not None:
        place = services.reverse_geocode(lat, lon)
    elif zip_code:
        lat, lon, place = services.get_coordinates(zip_code)
    else:
        return JSONResponse({"error": "Provide ?zip=... or ?lat=&lon="}, status_code=400)

    alerts = services.evaluate_alerts(lat, lon, place, crop)
    triggered = [a for a in alerts if a.get("active")]
    if not triggered:
        return {"sent": 0, "reason": "no active alerts"}

    sent = _send_push_notifications(lat, lon, crop, triggered)
    return {"sent": sent, "alerts": [a["type"] for a in triggered]}


def _send_push_notifications(lat: float, lon: float, crop: str, alerts: list[dict]) -> int:
    """Deliver web push to subscribers matching location/crop/alert type."""
    vapid_private = os.getenv("VAPID_PRIVATE_KEY", "")
    vapid_contact = os.getenv("VAPID_CONTACT_EMAIL", "")
    if not vapid_private or not vapid_contact:
        log.debug("push skipped: VAPID keys not configured")
        return 0

    sent = 0
    for alert in alerts:
        matching = db.get_push_subs_for_alert(crop, lat, lon, alert["type"])
        for sub in matching:
            payload = json.dumps({
                "title": f"Crop Sentry: {alert.get('title', alert['type'])}",
                "message": alert.get("message", ""),
                "type": alert["type"],
                "lat": lat,
                "lon": lon,
                "urgency": alert.get("urgency", "normal"),
            })
            try:
                from pywebpush import webpush
                webpush(
                    subscription_info={"endpoint": sub["endpoint"], "keys": sub["keys"]},
                    data=payload,
                    vapid_private_key=vapid_private,
                    vapid_claims={"sub": f"mailto:{vapid_contact}"},
                    timeout=10,
                )
                sent += 1
            except Exception:
                log.warning("push failed for sub %s", sub.get("id", "?"))
    return sent


# ----- iCal export ---------------------------------------------------------

@app.get("/api/export/calendar.ics")
def api_export_ical(
    request: Request,
    zip_code: str | None = Query(None, alias="zip"),
    lat: float | None = None,
    lon: float | None = None,
    crop: str = Query("corn"),
    tillage: str | None = None,
    residue: str | None = None,
    manure_recent: str | None = None,
    previous_grass: str | None = None,
    herbicide: str | None = None,
    field_tiled: str | None = None,
    seed_brand: str | None = Query(None),
    seed_cultivar: str | None = Query(None),
):
    """Export the 14-day tactical calendar as an iCal (.ics) file."""
    inputs = _build_inputs(tillage, residue, manure_recent, previous_grass, herbicide, field_tiled)
    seed_kw = {"seed_brand": seed_brand, "seed_cultivar": seed_cultivar}
    if lat is not None and lon is not None:
        place = services.reverse_geocode(lat, lon)
        result = services.evaluate(lat, lon, place, crop=crop, inputs=inputs, **seed_kw)
    elif zip_code:
        rlat, rlon, place = services.get_coordinates(zip_code)
        result = services.evaluate(rlat, rlon, place, crop=crop, inputs=inputs, **seed_kw)
    else:
        return JSONResponse({"error": "Provide ?zip=... or ?lat=&lon="}, status_code=400)

    ical_str = services.generate_ical_events(
        result.get("plant_days", []),
        result.get("spray_windows", []),
        result.get("location", {}).get("place", "Unknown"),
        crop,
    )
    filename = f"cropsentry-calendar-{crop}-{datetime.now().strftime('%Y%m%d')}.ics"
    return Response(
        content=ical_str,
        media_type="text/calendar",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ----- PDF export ----------------------------------------------------------

@app.get("/api/export/pdf")
def api_export_pdf(
    request: Request,
    zip_code: str | None = Query(None, alias="zip"),
    lat: float | None = None,
    lon: float | None = None,
    crop: str = Query("corn"),
    tillage: str | None = None,
    residue: str | None = None,
    manure_recent: str | None = None,
    previous_grass: str | None = None,
    herbicide: str | None = None,
    field_tiled: str | None = None,
    seed_brand: str | None = Query(None),
    seed_cultivar: str | None = Query(None),
):
    inputs = _build_inputs(tillage, residue, manure_recent, previous_grass, herbicide, field_tiled)
    seed_kw = {"seed_brand": seed_brand, "seed_cultivar": seed_cultivar}
    if lat is not None and lon is not None:
        place = services.reverse_geocode(lat, lon)
        result = services.evaluate(lat, lon, place, crop=crop, inputs=inputs, **seed_kw)
    elif zip_code:
        rlat, rlon, place = services.get_coordinates(zip_code)
        result = services.evaluate(rlat, rlon, place, crop=crop, inputs=inputs, **seed_kw)
    else:
        return JSONResponse({"error": "Provide ?zip=... or ?lat=&lon="}, status_code=400)

    pdf_bytes = _generate_pdf(result)
    filename = f"cropsentry-{result.get('location', {}).get('place', 'report').replace(', ', '-')}-{datetime.now().strftime('%Y%m%d')}.pdf"
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _pdf_safe(text: str) -> str:
    replacements = {
        "—": "--",   # em dash
        "–": "-",    # en dash
        "‘": "'",    # left single quote
        "’": "'",    # right single quote
        "“": '"',    # left double quote
        "”": '"',    # right double quote
        "…": "...",  # ellipsis
        "°": " deg", # degree sign
        "•": "*",    # bullet
        "·": "*",    # middle dot
        "′": "'",    # prime
        "″": '"',    # double prime
        "Δ": "delta ",  # Greek capital delta
        "≥": ">=",   # greater-than-or-equal
        "≤": "<=",   # less-than-or-equal
        "±": "+/-",  # plus-minus
        "×": "x",    # multiplication sign
        "÷": "/",    # division sign
    }
    for ch, repl in replacements.items():
        text = text.replace(ch, repl)
    return text.encode("latin-1", errors="replace").decode("latin-1")


def _generate_pdf(data: dict) -> bytes:
    from fpdf import FPDF

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    pdf.set_font("Helvetica", "B", 24)
    pdf.cell(0, 14, "Crop Sentry Assessment", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    loc = data.get("location", {})
    pdf.set_font("Helvetica", "", 12)
    pdf.cell(0, 8, _pdf_safe(f"Location: {loc.get('place', 'N/A')}"), new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, _pdf_safe(f"Crop: {data.get('crop', {}).get('label', 'N/A')}"), new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 8, f"Date: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)

    pdf.set_font("Helvetica", "B", 18)
    rec = data.get("recommendation", "N/A")
    pdf.cell(0, 12, _pdf_safe(f"Recommendation: {rec}"), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    pdf.set_font("Helvetica", "", 11)
    detail = data.get("verdict_detail", "")
    if detail:
        pdf.multi_cell(0, 6, _pdf_safe(detail))
    pdf.ln(4)

    summary = data.get("summary", {})
    if summary:
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 10, "Summary", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 11)
        soil = summary.get("min_soil_temp_f")
        if soil is not None:
            pdf.cell(0, 7, f"Min soil temp (48h): {soil:.1f} F", new_x="LMARGIN", new_y="NEXT")
        precip = summary.get("total_precip_in_48h")
        if precip is not None:
            pdf.cell(0, 7, f"Total precip (48h): {precip:.2f} in", new_x="LMARGIN", new_y="NEXT")
        uv = summary.get("max_uv_48h")
        if uv is not None:
            pdf.cell(0, 7, f"Peak UV (48h): {uv:.1f}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(4)

    risks = data.get("risks", [])
    if risks:
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 10, "Risk Factors", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)
        for r in risks:
            level = r.get("level", "low").upper()
            name = r.get("name", "")
            headline = r.get("headline", "")
            metric = r.get("metric", "")
            pdf.set_font("Helvetica", "B", 11)
            pdf.cell(0, 7, _pdf_safe(f"[{level}] {name}"), new_x="LMARGIN", new_y="NEXT")
            pdf.set_font("Helvetica", "", 10)
            if headline:
                pdf.multi_cell(0, 5, _pdf_safe(f"  {headline}"))
            if metric:
                pdf.cell(0, 5, _pdf_safe(f"  Metric: {metric}"), new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)

    plant_days = data.get("plant_days", [])
    if plant_days:
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 10, "Best Days to Plant", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)
        for pd in plant_days[:14]:
            label = pd.get("label", "")
            score = pd.get("survival_pct")
            verdict = pd.get("verdict", "")
            score_str = f"{score:.0f}%" if score is not None else "N/A"
            pdf.set_font("Helvetica", "", 11)
            pdf.cell(0, 7, _pdf_safe(f"{label}: {score_str} - {verdict}"), new_x="LMARGIN", new_y="NEXT")
        pdf.ln(4)

    pdf.set_font("Helvetica", "I", 9)
    pdf.ln(8)
    pdf.multi_cell(0, 5, (
        "DISCLAIMER: Crop Sentry output is heuristic decision-support, not professional advice "
        "or any guarantee of outcome. Third-party data may be unavailable, delayed, or inaccurate. "
        "You are solely responsible for planting decisions. See cropsentry.app/terms."
    ))

    return pdf.output()


# ----- County pest pressure ------------------------------------------------

COUNTY_PEST_TIERS = {
    "black_cutworm": {
        "label": "Black Cutworm",
        "source": "ISU ICM pheromone-trap network",
        "high": ["Huron, MI", "Sanilac, MI", "Tuscola, MI", "Hancock, OH", "Seneca, OH",
                 "Champaign, IL", "Macon, IL", "Vermilion, IL", "Tippecanoe, IN"],
        "moderate": ["Lenawee, MI", "Monroe, MI", "Hillsdale, MI", "St. Joseph, MI",
                     "Defiance, OH", "Fulton, OH", "Henry, OH", "Putnam, OH",
                     "Benton, IN", "White, IN", "Warren, IN"],
        "low": [],
    },
    "wireworm": {
        "label": "Wireworm",
        "source": "Extension survey aggregates (MSU, OSU, Purdue)",
        "high": ["Huron, MI", "Sanilac, MI", "St. Joseph, MI",
                 "Defiance, OH", "Paulding, OH"],
        "moderate": ["Tuscola, MI", "Lenawee, MI", "Monroe, MI", "Hillsdale, MI",
                     "Hancock, OH", "Seneca, OH", "Fulton, OH",
                     "Tippecanoe, IN", "Benton, IN"],
        "low": [],
    },
    "slugs": {
        "label": "Slugs",
        "source": "Extension survey aggregates — high risk correlates with heavy residue + poorly drained soils",
        "high": ["Seneca, OH", "Hancock, OH", "Huron, MI"],
        "moderate": ["Sanilac, MI", "Tuscola, MI", "Lenawee, MI",
                     "Defiance, OH", "Fulton, OH", "Henry, OH",
                     "Tippecanoe, IN", "White, IN"],
        "low": [],
    },
    "seedcorn_maggot": {
        "label": "Seedcorn Maggot",
        "source": "ISU ICM GDD model + manure/residue risk overlay",
        "high": ["Huron, MI", "Sanilac, MI", "Tuscola, MI",
                 "Hancock, OH", "Seneca, OH", "Champaign, IL"],
        "moderate": ["Lenawee, MI", "Monroe, MI", "Hillsdale, MI",
                     "Defiance, OH", "Fulton, OH", "Putnam, OH",
                     "Tippecanoe, IN", "Benton, IN", "White, IN"],
        "low": [],
    },
}


@app.get("/api/pest-pressure")
def api_pest_pressure(
    lat: float | None = None,
    lon: float | None = None,
    zip_code: str | None = Query(None, alias="zip"),
):
    if lat is not None and lon is not None:
        place = services.reverse_geocode(lat, lon)
    elif zip_code:
        _, _, place = services.get_coordinates(zip_code)
    else:
        return JSONResponse({"error": "Provide ?zip=... or ?lat=&lon="}, status_code=400)

    results = {}
    for pest_key, pest in COUNTY_PEST_TIERS.items():
        tier = "low"
        for county in pest.get("high", []):
            if county.lower() in place.lower():
                tier = "high"
                break
        if tier == "low":
            for county in pest.get("moderate", []):
                if county.lower() in place.lower():
                    tier = "moderate"
                    break
        results[pest_key] = {
            "label": pest["label"],
            "tier": tier,
            "source": pest["source"],
        }

    return {"location": place, "pests": results}


# ----- Fertility & Pest Management -----------------------------------------

@app.get("/api/fertility")
def api_fertility(
    lat: float | None = None,
    lon: float | None = None,
    zip_code: str | None = Query(None, alias="zip"),
    crop: str = Query("corn"),
    yield_goal: int | None = Query(None, ge=50, le=400),
    previous_crop: str = Query("corn"),
    soil_test_p: float | None = Query(None, ge=0, le=200),
    soil_test_k: float | None = Query(None, ge=0, le=600),
    soil_ph: float | None = Query(None, ge=3.5, le=9.0),
):
    """Nutrient management recommendations based on Tri-State Fertility Guide."""
    if lat is not None and lon is not None:
        pass
    elif zip_code:
        lat, lon, _ = services.get_coordinates(zip_code)
    else:
        return JSONResponse({"error": "Provide ?zip=... or ?lat=&lon="}, status_code=400)

    soil_profile = services.fetch_ssurgo_soil(lat, lon)
    result = services.generate_fertility_recommendations(
        crop=crop,
        soil=soil_profile,
        yield_goal_bu=yield_goal,
        previous_crop=previous_crop,
        soil_test_p_ppm=soil_test_p,
        soil_test_k_ppm=soil_test_k,
        soil_ph=soil_ph,
    )
    result["disclaimer"] = API_DISCLAIMER
    return result


@app.get("/api/pest-management")
def api_pest_management(
    lat: float | None = None,
    lon: float | None = None,
    zip_code: str | None = Query(None, alias="zip"),
    crop: str = Query("corn"),
):
    """Pest management recommendations based on GDD and rotation history."""
    if lat is not None and lon is not None:
        pass
    elif zip_code:
        lat, lon, _ = services.get_coordinates(zip_code)
    else:
        return JSONResponse({"error": "Provide ?zip=... or ?lat=&lon="}, status_code=400)

    from datetime import date as _date
    gdd_lookup = services.build_base50_gdd_lookup(
        *services.fetch_scm_inputs(lat, lon)
    )
    today_gdd = gdd_lookup.get(_date.today().isoformat())

    rotation = services.fetch_cropscape_history(lat, lon, 3)
    soil_profile = services.fetch_ssurgo_soil(lat, lon)

    result = services.generate_pest_recommendations(
        crop=crop,
        cum_gdd=today_gdd,
        soil=soil_profile,
        rotation=rotation,
    )
    result["disclaimer"] = API_DISCLAIMER
    return result


# ----- Community / Cooperative Data Sharing ----------------------------------

COMMUNITY_FILE = BASE_DIR / "community_data.jsonl"


class CommunitySubmission(BaseModel):
    lat: float
    lon: float
    crop: str
    yield_bu_ac: float | None = None
    soil_texture: str | None = None
    planting_date: str | None = None
    notes: str | None = None


@app.post("/api/community/submit")
def api_community_submit(sub: CommunitySubmission, request: Request):
    """Anonymously contribute field data for community benchmarking (opt-in)."""
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "lat_bucket": round(sub.lat, 1),
        "lon_bucket": round(sub.lon, 1),
        "crop": sub.crop,
        "yield_bu_ac": sub.yield_bu_ac,
        "soil_texture": sub.soil_texture,
        "planting_date": sub.planting_date,
    }
    try:
        with open(COMMUNITY_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except OSError:
        return JSONResponse({"status": "error", "message": "Could not save submission."}, status_code=500)
    return {"status": "ok", "message": "Thank you! Your anonymous data helps the community."}


@app.get("/api/community/benchmarks")
def api_community_benchmarks(
    lat: float | None = None,
    lon: float | None = None,
    zip_code: str | None = Query(None, alias="zip"),
    crop: str = Query("corn"),
):
    """Anonymous county-level benchmarks from community submissions + NASS data."""
    if lat is not None and lon is not None:
        pass
    elif zip_code:
        lat, lon, _ = services.get_coordinates(zip_code)
    else:
        return JSONResponse({"error": "Provide ?zip=... or ?lat=&lon="}, status_code=400)

    lat_bucket = round(lat, 1)
    lon_bucket = round(lon, 1)

    # Load community submissions for this area
    community_yields = []
    if COMMUNITY_FILE.exists():
        try:
            with open(COMMUNITY_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        entry = json.loads(line.strip())
                        if (entry.get("crop") == crop and
                            entry.get("lat_bucket") == lat_bucket and
                            entry.get("lon_bucket") == lon_bucket and
                            entry.get("yield_bu_ac") is not None):
                            community_yields.append(entry["yield_bu_ac"])
                    except (json.JSONDecodeError, KeyError):
                        pass
        except OSError:
            pass

    # NASS county average (from our existing data)
    nass = services._COUNTY_YIELD_RANGES.get(crop, {})
    county_avg = nass.get("avg", 0)
    county_low = nass.get("low", 0)
    county_high = nass.get("high", 0)

    # Community stats
    comm_avg = round(sum(community_yields) / len(community_yields), 1) if community_yields else None
    comm_count = len(community_yields)

    # Peer comparison text
    peer_comparison = None
    if comm_avg and county_avg:
        diff = comm_avg - county_avg
        if diff > 5:
            peer_comparison = f"Community fields in this area average {comm_avg} bu/ac — {diff:.0f} bu above county NASS average."
        elif diff < -5:
            peer_comparison = f"Community fields in this area average {comm_avg} bu/ac — {abs(diff):.0f} bu below county NASS average."
        else:
            peer_comparison = f"Community fields in this area average {comm_avg} bu/ac — in line with county average."

    return {
        "crop": crop,
        "county_benchmarks": {
            "avg_yield": county_avg,
            "low_yield": county_low,
            "high_yield": county_high,
            "source": "USDA NASS 5-year county averages",
        },
        "community": {
            "avg_yield": comm_avg,
            "submission_count": comm_count,
            "peer_comparison": peer_comparison,
        },
        "privacy_note": "All data is anonymized to 0.1-degree grid cells (~7 mile resolution). No personal or exact-location information is stored.",
    }


# ----- Field Profitability Calculator ----------------------------------------

@app.get("/api/profitability")
def api_profitability(
    lat: float | None = None,
    lon: float | None = None,
    zip_code: str | None = Query(None, alias="zip"),
    crop: str = Query("corn"),
    acres: float = Query(80, ge=1, le=50000),
    seed_cost_ac: float = Query(0, ge=0),
    fert_cost_ac: float = Query(0, ge=0),
    chem_cost_ac: float = Query(0, ge=0),
    fuel_cost_ac: float = Query(0, ge=0),
    other_cost_ac: float = Query(0, ge=0),
    price_per_bu: float = Query(0, ge=0),
):
    """Field-level profitability estimate combining yield estimation and user costs."""
    if lat is not None and lon is not None:
        pass
    elif zip_code:
        lat, lon, _ = services.get_coordinates(zip_code)
    else:
        return JSONResponse({"error": "Provide ?zip=... or ?lat=&lon="}, status_code=400)

    yield_data = services.estimate_yield(lat, lon, crop=crop)
    y = yield_data.get("yield_estimate", {})
    expected_yield = y.get("expected", 0)
    unit = y.get("unit", "bu/ac")

    # Default market prices if not provided
    _DEFAULT_PRICES = {
        "corn": 4.50, "soybeans": 11.50, "winter_wheat": 5.80,
        "spring_wheat": 6.20, "dry_beans": 35.0, "sugar_beets": 45.0,
        "alfalfa": 180.0,
    }
    if price_per_bu <= 0:
        price_per_bu = _DEFAULT_PRICES.get(crop, 5.0)

    total_cost_ac = seed_cost_ac + fert_cost_ac + chem_cost_ac + fuel_cost_ac + other_cost_ac
    revenue_ac = expected_yield * price_per_bu
    profit_ac = revenue_ac - total_cost_ac
    total_profit = profit_ac * acres

    # Break-even yield
    break_even_yield = total_cost_ac / price_per_bu if price_per_bu > 0 else 0

    return {
        "crop": crop,
        "acres": acres,
        "yield_estimate": expected_yield,
        "yield_unit": unit,
        "price_per_unit": price_per_bu,
        "costs": {
            "seed": seed_cost_ac,
            "fertilizer": fert_cost_ac,
            "chemicals": chem_cost_ac,
            "fuel": fuel_cost_ac,
            "other": other_cost_ac,
            "total_per_acre": round(total_cost_ac, 2),
        },
        "revenue_per_acre": round(revenue_ac, 2),
        "profit_per_acre": round(profit_ac, 2),
        "total_profit": round(total_profit, 2),
        "break_even_yield": round(break_even_yield, 1),
        "margin_pct": round((profit_ac / revenue_ac * 100), 1) if revenue_ac > 0 else 0,
        "disclaimer": API_DISCLAIMER,
    }


# ----- VRA Prescription Export ----------------------------------------------

class VRARequest(BaseModel):
    crop: str = "corn"
    yield_goal: int | None = None
    previous_crop: str = "corn"
    soil_test_p: float | None = None
    soil_test_k: float | None = None
    soil_ph: float | None = None
    boundaries: list[dict] = []  # GeoJSON features with properties.name


@app.post("/api/vra/generate")
def api_vra_generate(req: VRARequest):
    """Generate a VRA prescription CSV for field boundaries.

    Takes field boundaries (GeoJSON features) and generates per-zone nutrient
    recommendations. Output is a CSV that can be imported into John Deere
    Operations Center, Ag Leader SMS, or Trimble Ag Software.
    """
    if not req.boundaries:
        return JSONResponse({"error": "No field boundaries provided."}, status_code=400)

    zones = []
    for i, feature in enumerate(req.boundaries):
        props = feature.get("properties", {})
        name = props.get("name", f"Zone {i + 1}")
        coords = feature.get("geometry", {}).get("coordinates", [[]])
        if not coords or not coords[0]:
            continue

        # Get centroid for the zone
        ring = coords[0]
        avg_lat = sum(p[1] for p in ring) / len(ring)
        avg_lon = sum(p[0] for p in ring) / len(ring)

        # Get soil data for zone centroid
        soil = services.fetch_ssurgo_soil(avg_lat, avg_lon)
        fert = services.generate_fertility_recommendations(
            crop=req.crop, soil=soil,
            yield_goal_bu=req.yield_goal,
            previous_crop=req.previous_crop,
            soil_test_p_ppm=req.soil_test_p,
            soil_test_k_ppm=req.soil_test_k,
            soil_ph=req.soil_ph,
        )

        zones.append({
            "zone_name": name,
            "lat": round(avg_lat, 6),
            "lon": round(avg_lon, 6),
            "acres": props.get("acres", 0),
            "soil_texture": soil.get("texture_class", "unknown"),
            "n_rate_lb_ac": fert["nitrogen"]["recommended_lb_ac"],
            "p2o5_rate_lb_ac": fert["phosphorus"]["recommended_lb_ac"],
            "k2o_rate_lb_ac": fert["potassium"]["recommended_lb_ac"],
            "s_rate_lb_ac": fert["sulfur"]["recommended_lb_ac"],
            "lime_tons_ac": fert["lime"]["tons_ac"] if fert.get("lime") else 0,
        })

    # Generate CSV
    import csv
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "zone_name", "lat", "lon", "acres", "soil_texture",
        "n_rate_lb_ac", "p2o5_rate_lb_ac", "k2o_rate_lb_ac",
        "s_rate_lb_ac", "lime_tons_ac",
    ])
    writer.writeheader()
    for z in zones:
        writer.writerow(z)

    csv_content = output.getvalue()

    return {
        "zones": zones,
        "csv": csv_content,
        "format_notes": "Compatible with John Deere Operations Center CSV import, Ag Leader SMS, and Trimble Ag Software.",
        "disclaimer": API_DISCLAIMER,
    }


@app.post("/api/vra/download")
def api_vra_download(req: VRARequest):
    """Download VRA prescription as a CSV file."""
    result = api_vra_generate(req)
    if isinstance(result, JSONResponse):
        return result

    csv_content = result["csv"]
    return StreamingResponse(
        io.BytesIO(csv_content.encode('utf-8')),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=cropsentry_vra_prescription.csv"},
    )


# ----- Predictive Yield Estimation -----------------------------------------

@app.get("/api/yield-estimate")
def api_yield_estimate(
    lat: float | None = None,
    lon: float | None = None,
    zip_code: str | None = Query(None, alias="zip"),
    crop: str = Query("corn"),
    rm: int | None = Query(None, ge=70, le=130, description="Relative maturity"),
    planting_date: str | None = Query(None),
    seeds_per_acre: int | None = Query(None, ge=1000, le=2000000, description="Planting population (seeds/acre)"),
):
    """Pre-harvest yield estimation using GDD pace, NDVI, drought, and population."""
    if lat is not None and lon is not None:
        pass
    elif zip_code:
        lat, lon, _ = services.get_coordinates(zip_code)
    else:
        return JSONResponse({"error": "Provide ?zip=... or ?lat=&lon="}, status_code=400)

    result = services.estimate_yield(lat, lon, crop=crop,
                                     relative_maturity=rm,
                                     planting_date_iso=planting_date,
                                     seeds_per_acre=seeds_per_acre)
    result["disclaimer"] = API_DISCLAIMER
    return result


# ----- Crop Rotation Intelligence ------------------------------------------

@app.get("/api/rotation")
def api_rotation(
    lat: float | None = None,
    lon: float | None = None,
    zip_code: str | None = Query(None, alias="zip"),
    crop: str = Query("corn"),
    cover_crop: str | None = Query(None),
):
    """Crop rotation intelligence: history, recommendations, and cover crop options."""
    if lat is not None and lon is not None:
        pass
    elif zip_code:
        lat, lon, _ = services.get_coordinates(zip_code)
    else:
        return JSONResponse({"error": "Provide ?zip=... or ?lat=&lon="}, status_code=400)

    result = services.generate_rotation_intelligence(lat, lon, crop=crop, cover_crop=cover_crop)
    result["disclaimer"] = API_DISCLAIMER
    return result


# ----- NDVI / Field Health -------------------------------------------------

@app.get("/api/ndvi")
def api_ndvi(
    lat: float | None = None,
    lon: float | None = None,
    zip_code: str | None = Query(None, alias="zip"),
    crop: str = Query("corn"),
    days: int = Query(90, ge=30, le=365),
):
    """NDVI/EVI time series from Sentinel-2 satellite imagery.

    Returns recent NDVI readings at the field location along with crop health
    classification and trend analysis. Data from Copernicus Sentinel-2 L2A
    via AWS Open Data (free, no auth).
    """
    if lat is not None and lon is not None:
        pass
    elif zip_code:
        lat, lon, _ = services.get_coordinates(zip_code)
    else:
        return JSONResponse({"error": "Provide ?zip=... or ?lat=&lon="}, status_code=400)

    result = services.fetch_ndvi_timeseries(lat, lon, days_back=days, crop=crop)
    return result


# ----- Validation loop ----------------------------------------------------


class OutcomeReport(BaseModel):
    snapshot_id: str | None = None
    lat: float
    lon: float
    crop: str
    planting_date: str
    emergence_pct: float | None = None
    days_to_emerge: int | None = None
    stand_quality: str = ""
    frost_damage: bool = False
    disease_issues: str = ""
    pest_issues: str = ""
    notes: str = ""


@app.post("/api/validation/outcome")
async def api_submit_outcome(body: OutcomeReport, request: Request):
    """Farmer-reported field outcome for model calibration."""
    raw_ip = (request.client.host if request.client else "") or ""
    reporter_hash = hashlib.sha256(raw_ip.encode()).hexdigest()[:12] if raw_ip else ""
    try:
        row_id = db.save_field_outcome(
            snapshot_id=body.snapshot_id,
            lat=body.lat, lon=body.lon, crop=body.crop,
            planting_date=body.planting_date,
            emergence_pct=body.emergence_pct,
            days_to_emerge=body.days_to_emerge,
            stand_quality=body.stand_quality,
            frost_damage=body.frost_damage,
            disease_issues=body.disease_issues,
            pest_issues=body.pest_issues,
            notes=body.notes,
            reporter_hash=reporter_hash,
        )
        return {"ok": True, "id": row_id, "message": "Outcome recorded — thank you!"}
    except Exception:
        log.exception("Failed to save field outcome")
        return JSONResponse({"error": "Failed to save outcome"}, status_code=500)


@app.get("/api/validation/stats")
def api_validation_stats(crop: str | None = None, state: str = "MICHIGAN"):
    """Aggregated prediction accuracy metrics."""
    stats = db.get_validation_stats(crop)
    benchmarks = db.get_nass_benchmarks(state=state.upper(), crop=crop.upper() if crop else None, limit=10)
    return {
        "stats": stats,
        "nass_benchmarks": benchmarks,
        "disclaimer": "Validation data is preliminary and based on voluntary farmer reports.",
    }


_NASS_STATES = [
    "ALABAMA", "ARIZONA", "ARKANSAS", "CALIFORNIA", "COLORADO", "CONNECTICUT",
    "DELAWARE", "FLORIDA", "GEORGIA", "IDAHO", "ILLINOIS", "INDIANA", "IOWA",
    "KANSAS", "KENTUCKY", "LOUISIANA", "MAINE", "MARYLAND", "MASSACHUSETTS",
    "MICHIGAN", "MINNESOTA", "MISSISSIPPI", "MISSOURI", "MONTANA", "NEBRASKA",
    "NEVADA", "NEW HAMPSHIRE", "NEW JERSEY", "NEW MEXICO", "NEW YORK",
    "NORTH CAROLINA", "NORTH DAKOTA", "OHIO", "OKLAHOMA", "OREGON",
    "PENNSYLVANIA", "RHODE ISLAND", "SOUTH CAROLINA", "SOUTH DAKOTA",
    "TENNESSEE", "TEXAS", "UTAH", "VERMONT", "VIRGINIA", "WASHINGTON",
    "WEST VIRGINIA", "WISCONSIN", "WYOMING",
]


@app.post("/api/validation/nass-sync")
async def api_nass_sync(request: Request, state: str | None = None):
    """Fetch latest NASS crop progress data and store as benchmarks."""
    states = [state.upper()] if state else _NASS_STATES
    crops = ["CORN", "SOYBEANS", "WINTER WHEAT", "SPRING WHEAT", "DRY BEANS", "SUGARBEETS", "HAY"]
    synced = 0
    for st in states:
        for nass_crop in crops:
            try:
                progress = services.fetch_nass_crop_progress(st, nass_crop)
                if not progress.get("available"):
                    continue
                week = progress.get("week_ending", datetime.now(timezone.utc).date().isoformat())
                db.save_nass_benchmark(
                    state=st,
                    crop=nass_crop,
                    week_ending=week,
                    pct_planted=progress.get("pct_planted"),
                    pct_emerged=progress.get("pct_emerged"),
                    condition_good_excellent=progress.get("condition_good_excellent"),
                )
                synced += 1
            except Exception:
                log.debug("NASS sync failed for %s/%s", st, nass_crop, exc_info=True)
    return {"ok": True, "synced": synced}


# ----- Health check -------------------------------------------------------

@app.get("/api/health")
def api_health():
    """Lightweight probe for uptime monitors and container orchestrators.

    Checks reachability of each upstream data source with a fast, minimal
    request. Returns per-source status so operators can see exactly which
    layer is degraded.
    """
    import httpx as _hx

    checks: dict[str, str] = {}
    sources = {
        "open_meteo_forecast": ("GET", services.FORECAST_URL, {"latitude": 43.52, "longitude": -83.06, "hourly": "temperature_2m", "forecast_days": 1, "temperature_unit": "fahrenheit"}),
        "open_meteo_archive": ("GET", services.ARCHIVE_URL, {"latitude": 43.52, "longitude": -83.06, "start_date": "2026-01-01", "end_date": "2026-01-02", "daily": "temperature_2m_min", "temperature_unit": "fahrenheit"}),
        "usda_ssurgo": ("POST", services.SSURGO_URL, None),
        "nws": ("GET", "https://api.weather.gov/points/43.52,-83.06", None),
        "usgs_water": ("GET", services.USGS_WATER_URL, {"format": "json", "sites": "04159492", "parameterCd": "00065", "period": "PT1H"}),
        "iem": ("GET", f"{services.IEM_API_URL}/station_list.json", {"network": "ISUSM"}),
        "scan_awdb": ("GET", services.SCAN_AWDB_URL, {"action": "getStations", "networkCds": "SCAN", "minLatitude": "43", "maxLatitude": "44", "minLongitude": "-84", "maxLongitude": "-83", "returnFields": "stationTriplet"}),
        "cropscape": ("GET", services.CROPSCAPE_URL, {"a": "GetCDLValue", "year": "2024", "x": "-83.06", "y": "43.52"}),
        "msu_enviroweather": ("GET", f"{services.MSU_ENVIRO_URL}/weather/stations.json", None),
    }
    for name, (method, url, params) in sources.items():
        try:
            if method == "POST":
                r = _hx.post(url, json={"query": "SELECT TOP 1 mukey FROM mapunit", "format": "JSON"}, timeout=5)
            else:
                r = _hx.get(url, params=params, headers={"User-Agent": services.NWS_USER_AGENT}, timeout=5, follow_redirects=True)
            checks[name] = "ok" if r.status_code == 200 else f"http_{r.status_code}"
        except _hx.HTTPError as e:
            checks[name] = f"error: {type(e).__name__}"

    all_ok = all(v == "ok" for v in checks.values())
    return JSONResponse(
        {"status": "healthy" if all_ok else "degraded", "upstreams": checks,
         "cache_entries": services._cache.size},
        status_code=200 if all_ok else 503,
    )


# Backwards-compatible legacy endpoint
@app.get("/evaluate-planting/{zip_code}")
def legacy_evaluate(zip_code: str, crop: str = "corn"):
    lat, lon, place = services.get_coordinates(zip_code)
    result = services.evaluate(lat, lon, place, crop=crop)
    result["disclaimer"] = API_DISCLAIMER
    return result
