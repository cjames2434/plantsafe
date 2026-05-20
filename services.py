"""Domain services: geocoding, weather fetch, risk engine.

The risk engine returns a list of risk objects with a uniform shape so the
UI can render them generically. Adding a new factor only requires appending
another evaluator function — no template changes needed.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict, field
from datetime import datetime, date, timedelta
from typing import Callable, Optional

import httpx
from dotenv import load_dotenv
from fastapi import HTTPException

load_dotenv()


# ----- TTL cache ----------------------------------------------------------
# Lightweight in-memory cache keyed by (function_name, args_hash).
# Each upstream API has a different staleness tolerance — forecast data turns
# over hourly, soil-survey data is essentially static, etc.

class _TTLCache:
    def __init__(self):
        self._store: dict[str, tuple[float, object]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> object | None:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            expires, value = entry
            if time.time() > expires:
                del self._store[key]
                return None
            return value

    def set(self, key: str, value: object, ttl_seconds: float):
        with self._lock:
            self._store[key] = (time.time() + ttl_seconds, value)

    def clear(self):
        with self._lock:
            self._store.clear()

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._store)


_cache = _TTLCache()

# TTLs by data source (seconds)
CACHE_TTL_FORECAST = 15 * 60        # 15 min — forecast updates hourly
CACHE_TTL_HISTORY = 6 * 3600        # 6 h — archive lags 5 days anyway
CACHE_TTL_SOIL = 7 * 86400          # 7 days — SSURGO rarely changes
CACHE_TTL_NWS = 30 * 60             # 30 min — NWS updates ~hourly
CACHE_TTL_ALERTS = 5 * 60           # 5 min — alerts are time-critical
CACHE_TTL_CLIMATOLOGY = 24 * 3600   # 24 h — prior-year normals are static
CACHE_TTL_BCW = 12 * 3600           # 12 h — weekly trap reports
CACHE_TTL_DROUGHT = 24 * 3600       # 24 h — USDM updates weekly
CACHE_TTL_ENSEMBLE = 30 * 60        # 30 min
CACHE_TTL_POWER = 6 * 3600          # 6 h — NASA POWER reanalysis lags
CACHE_TTL_ELEVATION = 30 * 86400    # 30 days — terrain doesn't change
CACHE_TTL_SCAN = 30 * 60            # 30 min — hourly station updates
CACHE_TTL_IEM = 30 * 60             # 30 min — mesonet updates frequently
CACHE_TTL_NASS = 12 * 3600          # 12 h — weekly crop progress reports
CACHE_TTL_USGS = 15 * 60            # 15 min — 15-min gage readings
CACHE_TTL_CPC = 6 * 3600            # 6 h — daily soil moisture
CACHE_TTL_ENVIRO = 6 * 3600         # 6 h — daily GDD accumulations
CACHE_TTL_CROPSCAPE = 30 * 86400    # 30 days — annual classification


def _cache_key(prefix: str, *args) -> str:
    raw = json.dumps(args, sort_keys=True, default=str)
    return f"{prefix}:{hashlib.md5(raw.encode()).hexdigest()}"


def _log_fetch_error(source: str, exc: BaseException | str) -> None:
    """One-line stderr breadcrumb for live-data fetch failures.

    The three public-data fetchers all degrade to ``{}`` on error so the rest
    of the pipeline keeps running. Without a log line, a quiet template change
    on the upstream (especially ISU's HTML) would silently demote the UI to a
    permanent "unavailable" state. Logging here surfaces the regression.
    """
    print(f"[fetch:{source}] {exc}", file=sys.stderr)


# ----- HTTP client --------------------------------------------------------
_http = httpx.Client(follow_redirects=True, timeout=15)

# ----- constants ---------------------------------------------------------

GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
REVERSE_GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/reverse"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# USDA Soil Data Access — free public REST endpoint backed by SSURGO. Returns
# soil-survey results for any point in the continental US.
SSURGO_URL = "https://sdmdataaccess.sc.egov.usda.gov/Tabular/post.rest"

# NOAA / NWS public forecast API — free, no key, requires a User-Agent string
# that identifies the operator so they can reach you about problem traffic
# (https://www.weather.gov/documentation/services-web-api).
NWS_USER_AGENT = f"CropSentry (crop-planting-predictor; {os.getenv('NWS_CONTACT_EMAIL', 'noreply@cropsentry.app')})"
NWS_POINTS_URL = "https://api.weather.gov/points/{lat},{lon}"

# ISU Integrated Crop Management weekly black-cutworm trap report. Title format
# is stable: "{year}-moth-trapping-network-report-{n}". Report 1 contains the
# first significant flight; later reports add subsequent county captures.
ISU_BCW_REPORT_URL = "https://crops.extension.iastate.edu/post/{year}-moth-trapping-network-report-{n}"

# NOAA / NWS active-alerts API. Returns watches/warnings/advisories valid for a
# point right now (frost, freeze, flood, severe weather). Same auth model as
# the gridpoint forecast above — User-Agent only.
NWS_ALERTS_URL = "https://api.weather.gov/alerts/active"

# Open-Meteo ENSEMBLE forecast endpoint. Same parameter model as /v1/forecast
# but returns *one series per ensemble member* across multiple physics models
# (GFS, ICON, ECMWF, GEM). The dispersion across members is the most direct
# free signal of forecast uncertainty available — wide spread on a planting
# day = the model is not committed and the survival probability we publish
# should carry a confidence interval, not a point estimate. Documented at
# https://open-meteo.com/en/docs/ensemble-api .
ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
ENSEMBLE_MODELS = "icon_seamless,gfs_seamless,ecmwf_ifs04,gem_global"
ENSEMBLE_HORIZON_DAYS = 14

# NASA POWER agroclimate API. Free, no key. Returns daily and hourly
# meteorology derived from MERRA-2 / GEOS-FP reanalysis (T2M, T2M_MIN/MAX,
# PRECTOTCORR, RH2M, ALLSKY_SFC_SW_DWN, etc.) at ~0.5°×0.625° grid. We use it
# as a *third* independent source for the most-recent 7 days of actuals — a
# three-way agreement check (NWS forecast already cross-checks Open-Meteo's
# forward window; POWER cross-checks the recent-history Archive that drives
# antecedent saturation, BLB winter survival, and SCM GDD accumulation).
# https://power.larc.nasa.gov/docs/services/api/temporal/daily/
NASA_POWER_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"

# Open-Meteo elevation API. Free, no key. Returns elevation in meters at one
# or more (lat, lon) points. We sample a 3×3 grid centered on the field point
# (~500 m spacing) to derive a *concavity* and *slope* proxy: a point sitting
# in a local depression collects ponded water disproportionately to the
# modeled surface saturation, which is the dominant cause of seedling drown-
# out in flat Midwest fields. https://open-meteo.com/en/docs/elevation-api
ELEVATION_URL = "https://api.open-meteo.com/v1/elevation"
ELEVATION_GRID_DEG = 0.005   # ~500 m at mid-latitudes; matches a small field
ELEVATION_GRID_RADII = ((-1, -1), (-1, 0), (-1, 1),
                        ( 0, -1), ( 0, 0), ( 0, 1),
                        ( 1, -1), ( 1, 0), ( 1, 1))

# U.S. Drought Monitor — Esri Living Atlas live feed (esri_livefeeds2). The
# canonical NDMC web service host (usdmdataservices.unl.edu) returns 404 as
# of 2026-04, but the Esri-curated live feed pulls from the same upstream
# weekly USDM shapefile and is well-maintained. Querying with a lat/lon point
# returns 0–1 polygons: empty list = no drought class at this point, otherwise
# the "dm" attribute holds the integer class (0=D0 abnormally dry → 4=D4
# exceptional). Updated every Thursday after USDM publication.
USDM_POINT_URL = ("https://services9.arcgis.com/RHVPKKiFTONKtxq3/arcgis/rest/"
                  "services/US_Drought_Intensity_v1/FeatureServer/3/query")

# USDA NRCS SCAN Network — real-time soil temperature at depth from ~200
# stations. SOAP/REST-ish via the AWDB (Air & Water Database) web service.
# Sensor codes: STO = soil temperature. No key required.
SCAN_AWDB_URL = "https://wcc.sc.egov.usda.gov/awdbWebService/services"
SCAN_REST_URL = "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customMultipleStationReport/daily"

# Iowa Environmental Mesonet — ISU aggregates ASOS, AWOS, CoCoRaHS, and the
# ISU AgClimate network into a free JSON API. No key required. Provides actual
# 4-inch soil temperatures and high-density precipitation observations.
IEM_API_URL = "https://mesonet.agron.iastate.edu/api/1"

# USDA NASS Quick Stats API — weekly crop progress and condition ratings.
# Requires a free API key from https://quickstats.nass.usda.gov/api/ .
# Set NASS_API_KEY in .env; the fetcher degrades gracefully if absent.
NASS_API_URL = "https://quickstats.nass.usda.gov/api/api_GET/"
NASS_API_KEY = os.getenv("NASS_API_KEY", "")

# USGS Water Services API — real-time streamflow, gage height, and flood
# stage from thousands of gages. No key required.
USGS_WATER_URL = "https://waterservices.usgs.gov/nwis/iv/"

# NOAA CPC Soil Moisture — daily gridded soil moisture anomalies. The main
# data is GeoTIFF; we use the text-based w2 percentile summary endpoint.
CPC_SOIL_MOISTURE_URL = "https://www.cpc.ncep.noaa.gov/products/Soilmst_Monitoring/US/Soilmst/Soilmst.shtml"

# MSU Enviroweather — Michigan-specific GDD and pest emergence models from
# 91 weather stations. Free, no key, CSV export endpoint.
MSU_ENVIRO_URL = "https://enviroweather.msu.edu"

# NASS CropScape / Cropland Data Layer — annual 30m crop-type classification.
# No key required. Returns the CDL crop code at a lat/lon for a given year.
CROPSCAPE_URL = "https://nassgeodata.gmu.edu/CropScape/devapp/proxy.ashx"

# Archive API has a ~5-day lag on real-time data.
ARCHIVE_LAG_DAYS = 5

# Planning horizons. Open-Meteo's standard forecast endpoint caps at 16 days, so
# anything past that is filled in from prior-year climate normals.
FORECAST_FETCH_DAYS = 16            # what we request from /v1/forecast
PLAN_HORIZON_DAYS = 14              # forecast-grade days surfaced to the user
EXTENDED_HORIZON_DAYS = 31          # max with the "show 31 days" toggle

# Crops have different risk thresholds. Soybeans are more frost-sensitive,
# corn is slightly more chilling-tolerant. Numbers are taken from peer-reviewed
# agronomy research and Land-Grant extension (Nielsen/Purdue, Pedersen/ISU,
# Pedigo/ISU, Helms/SDSU). See sources.txt for full citations.
CROP_PROFILES = {
    "corn": {
        "label": "Corn",
        # 50°F at planting depth + 24-48h of non-decreasing temps is the
        # canonical Purdue (Nielsen) decision rule.
        "min_soil_temp_f": 50,
        "preferred_soil_temp_f": 55,       # vigorous, uniform emergence
        # Corn growing point sits ~3/4" below the surface until ~V5, so brief
        # post-emergence freezes that don't reach the soil are recoverable.
        "frost_air_temp_f": 28,
        "phytophthora_sensitive": False,
        "bcw_sensitive": True,             # black cutworm cuts corn at the soil line
        "blb_sensitive": False,
        # Purdue (Nielsen) / ISU recommended seeding depth window in inches.
        "depth_min_in": 1.5,
        "depth_max_in": 2.0,
        # Days at typical 60°F soil to emerge (used for emergence-window math).
        "typ_emerge_days": 7,
        # Heat stress: corn pollen dies at ~95°F; tissue damage begins at 113°F.
        # (Hatfield & Prueger 2015, Sánchez et al. 2014)
        "heat_stress_f": 95,               # sustained stress threshold
        "heat_lethal_f": 113,              # tissue death / lethal air temp
        # Minimum precipitation for rainfed germination over 14-day window.
        # Corn requires ~0.5" in the first week for imbibition + early root growth.
        "min_precip_14d_in": 0.5,
        "rhizoctonia_sensitive": True,     # Rhizoctonia solani causes corn seedling blight too
    },
    "soybeans": {
        "label": "Soybeans",
        # ISU (Pedersen) reports uniform germination requires a 3-day average
        # ≥55°F; <55°F shows stand non-uniformity. 60°F is the "preferred"
        # threshold for vigorous emergence.
        "min_soil_temp_f": 55,
        "preferred_soil_temp_f": 60,
        # ISU ICM + UMN Extension: at VE/VC stage, cotyledons tolerate brief
        # dips to 30°F; sustained exposure below 28°F is lethal. 32°F is the
        # V1+ (trifoliate) kill threshold, but the model evaluates planting-
        # time risk (VE/VC), so 30°F is the correct damage-onset threshold.
        "frost_air_temp_f": 30,
        "phytophthora_sensitive": True,
        "bcw_sensitive": False,
        "blb_sensitive": True,             # overwintered bean leaf beetles target early stands
        "depth_min_in": 1.0,
        "depth_max_in": 1.5,
        "typ_emerge_days": 8,
        # Heat stress: soybean pod set/fill severely impacted at 95°F;
        # tissue damage at ~108°F. (Djanaguiraman et al. 2013)
        # At seedling stage, 95°F primarily represents seed-zone desiccation risk.
        "heat_stress_f": 95,
        "heat_lethal_f": 108,
        # Soybeans are slightly more drought-tolerant at germination than corn
        # but still require soil moisture for imbibition.
        "min_precip_14d_in": 0.4,
        "white_mold_sensitive": True,
        "sds_sensitive": True,             # Fusarium virguliforme — cool wet soil at planting
        "rhizoctonia_sensitive": True,     # Rhizoctonia solani — warm damp seedling disease
        "idc_sensitive": True,             # iron deficiency chlorosis — calcareous soils
        "cold_imbibitional_sensitive": True,  # rapid imbibition (6-24h) amplifies chilling risk
    },
    "winter_wheat": {
        "label": "Winter Wheat",
        "fall_planted": True,
        # For fall-planted wheat the soil-temp concern is inverted: too WARM
        # (>65°F) causes excess growth, disease, pest exposure; too COLD (<40°F)
        # means insufficient GDD before dormancy. OSU/KSU/MSU Extension.
        "min_soil_temp_f": 40,
        "preferred_soil_temp_f": 54,
        "max_soil_temp_f": 65,
        # Growth-stage-dependent frost thresholds (Univ. of Missouri IPM,
        # OSU Ohioline ANR-93). The single frost_air_temp_f is the dormant value;
        # the lookup table is used when growth stage is known.
        "frost_air_temp_f": 12,
        "frost_by_feekes": {
            "dormant": 12, "tillering": 12,
            "jointing": 24, "boot": 28,
            "heading": 30, "flowering": 30,
            "milk": 28, "dough": 28,
        },
        "phytophthora_sensitive": False,
        "bcw_sensitive": False,
        "blb_sensitive": False,
        "depth_min_in": 1.0,
        "depth_max_in": 1.5,
        "typ_emerge_days": 7,
        "heat_stress_f": 90,
        "heat_lethal_f": 105,
        "min_precip_14d_in": 0.75,
        "vernalization_days": 40,
        "vernalization_temp_f": 40,
        "vernalization_temp_range_f": (34, 52),
        "gdd_before_dormancy_target": 400,
        "gdd_base_f": 32,
        "fusarium_sensitive": True,
        "hessian_fly_sensitive": True,
        "bydv_sensitive": True,
        "take_all_sensitive": True,
        "crown_rot_sensitive": True,
        "snow_mold_sensitive": True,
        "stripe_rust_sensitive": True,
        "winterkill_sensitive": True,
    },
    "spring_wheat": {
        "label": "Spring Wheat",
        # SDSU Extension: 3-day average of 34-36°F for germination. UMN says
        # "germination in earnest" at 40°F, but biological minimum is 34°F.
        "min_soil_temp_f": 34,
        "preferred_soil_temp_f": 45,
        # UMN: crowns survive 28°F, "probably even 22°F briefly." Alberta Grains:
        # leaves tolerate -8 to -10°C (17-18°F). 24°F is a solid middle ground
        # for sustained damage onset at the seedling/tillering stage.
        "frost_air_temp_f": 24,
        "phytophthora_sensitive": False,
        "bcw_sensitive": False,
        "blb_sensitive": False,
        "depth_min_in": 1.0,
        "depth_max_in": 2.0,
        "typ_emerge_days": 8,
        # KSU: pollen viability drops sharply above 88°F; grain fill severely
        # impacted above 90°F. Optimum flowering-to-fill range is 54-72°F.
        "heat_stress_f": 90,
        "heat_lethal_f": 105,
        "min_precip_14d_in": 0.4,
        "fusarium_sensitive": True,
        # NDSU: stripe rust observed in cool springs/summers; 40-65°F + humidity.
        "stripe_rust_sensitive": True,
        # MU G4345: wheat-after-wheat → up to 50% yield loss from Gaeumannomyces.
        "take_all_sensitive": True,
        # SDSU/UNL: BYDV transmitted by cereal aphids; spring wheat vulnerable
        # when young, but early planting is defensive (advanced stage at aphid peak).
        "bydv_sensitive": True,
        # NDSU: tan spot is the #1 leaf spot disease of spring wheat in the
        # northern Great Plains. Up to 50% yield/test-weight loss. Residue-borne.
        "tan_spot_sensitive": True,
        # MSU Montana/CPN: Bipolaris sorokiniana causes seedling blight and
        # common root rot; continuous cereals + plant stress = high risk.
        "common_root_rot_sensitive": True,
    },
    "dry_beans": {
        "label": "Dry Beans",
        "min_soil_temp_f": 60,
        "preferred_soil_temp_f": 65,
        "frost_air_temp_f": 32,
        "phytophthora_sensitive": False,
        "bcw_sensitive": False,
        "blb_sensitive": False,
        "depth_min_in": 1.5,
        "depth_max_in": 2.0,
        "typ_emerge_days": 7,
        # Common bean heat stress at 30°C/86°F (Frontiers 2017; PMC 2021 RIL
        # study showed 26-37% yield reduction). Tissue damage at ~100°F — beans
        # are more heat-sensitive than corn (113°F) or soy (108°F).
        "heat_stress_f": 86,
        "heat_lethal_f": 100,
        "min_precip_14d_in": 0.5,
        "white_mold_sensitive": True,
        "cold_imbibitional_sensitive": True,
        "rhizoctonia_sensitive": True,
        "anthracnose_sensitive": True,
        "bacterial_blight_sensitive": True,
    },
    "sugar_beets": {
        "label": "Sugar Beets",
        "min_soil_temp_f": 45,
        "preferred_soil_temp_f": 50,
        "frost_air_temp_f": 28,
        "phytophthora_sensitive": False,
        "bcw_sensitive": False,
        "blb_sensitive": False,
        "depth_min_in": 0.75,
        "depth_max_in": 1.25,
        "typ_emerge_days": 10,
        "heat_stress_f": 85,
        "heat_lethal_f": 100,
        "min_precip_14d_in": 0.4,
        "cercospora_sensitive": True,
        "bolting_cold_hours": 120,
        "bolting_temp_f": 50,
        "bolting_base_f": 32,
        "rhizoctonia_sensitive": True,
        "aphanomyces_sensitive": True,
        "sbcn_sensitive": True,
        "wind_damage_sensitive": True,
        "root_maggot_sensitive": True,
    },
    "alfalfa": {
        "label": "Alfalfa",
        # Jungers et al. 2016 (Crop Science): base temperature for 50%
        # germination is ~32°F (0°C).  42°F is the practical field minimum
        # where germination completes in a reasonable timeframe (~7-10 d).
        "min_soil_temp_f": 42,
        # UW-Extension / NDSU: vigorous, uniform emergence at 55-65°F;
        # optimal germination range is 65-77°F.  55°F is the "go" threshold
        # for rapid stand establishment.
        "preferred_soil_temp_f": 55,
        # UMN Crop News (2022): alfalfa seedlings tolerate brief exposure to
        # 24-28°F.  28°F is the damage-onset for unhardened new seedings;
        # hardened crowns survive to 0-5°F but that's the winterkill
        # evaluator's domain, not the planting-time frost evaluator.
        "frost_air_temp_f": 28,
        # Alfalfa IS a Phytophthora medicaginis host (UMN Extension,
        # Kentucky IPM).  Cool wet soils + poorly drained fields → root rot.
        "phytophthora_sensitive": True,
        "bcw_sensitive": False,
        "blb_sensitive": False,
        "depth_min_in": 0.25,
        "depth_max_in": 0.5,
        # UW-Extension: 7-14 days depending on soil temp.
        "typ_emerge_days": 10,
        "heat_stress_f": 95,
        "heat_lethal_f": 110,
        # Alfalfa seeds must absorb >100% of their weight in water to
        # germinate (UW-Extension).  Slightly higher moisture need than corn.
        "min_precip_14d_in": 0.5,
        "autotoxicity_sensitive": True,
        "winterkill_sensitive": True,
        # UMN Extension: Aphanomyces euteiches race 2 is common in MN;
        # major cause of seedling death in poorly drained fields.
        "aphanomyces_alfalfa_sensitive": True,
        # CPN: Sclerotinia trifoliorum infects seedlings in cool (50-68°F),
        # moist conditions.  Late-summer / fall seedings at highest risk.
        "sclerotinia_crown_sensitive": True,
        # UW/ISU/UNL Extension: potato leafhopper causes hopperburn — V-shaped
        # yellowing.  New seedings are most vulnerable (no glandular trichomes).
        "potato_leafhopper_sensitive": True,
        # ISU ICM: eggs hatch ~300 GDD (base 48°F); larvae skeletonize leaves.
        "alfalfa_weevil_sensitive": True,
        # Alfalfa requires pH 6.5-7.0 for optimal nodulation (Rhizobium) and
        # establishment; yields drop ~0.1 ton/ac per 0.1 pH below optimum.
        "soil_ph_sensitive": True,
        # Alfalfa seeds imbibe >100% of weight rapidly; cold imbibition risk
        # is real when planting into cold soil (same mechanism as soybeans).
        "cold_imbibitional_sensitive": True,
    },
}

# ----- seed brand / cultivar catalog ------------------------------------
# Top corn and soybean brands distributed in the upper Midwest. Each brand
# carries a short list of representative cultivars covering the relative-
# maturity (RM) range a Decker / Snover, MI grower would actually shop for.
# Per-cultivar fields drive how the risk engine is *tailored* to that
# cultivar — the canonical CROP_PROFILES thresholds are nudged through
# ``apply_cultivar_to_profile`` based on declared trait classes:
#
#   cold_tolerance: "low" | "standard" | "high"
#       high → 2°F lower min/preferred soil-temp floor and 2°F lower frost
#              air threshold (Szczerba et al. 2021 — cultivar variation in
#              chilling tolerance under cold stress)
#       low  → opposite — raise the floors by 2°F.
#
#   phytophthora (soybeans): "none" | "field" | "Rps1k" | "Rps3a" | "stack"
#       Any Rps gene or stack flips ``phytophthora_sensitive`` off — the
#       evaluator stops penalising warm-saturated soils (Yang et al.; ISU).
#       "field" tolerance keeps sensitivity but reduces the headline copy.
#
#   relative_maturity (RM): days for corn (90-115), group for soy (1.0-3.5)
#       Surfaced as a label and used for emergence-window context.
#
# Cultivar IDs are real product codes from public seed-finder catalogs;
# trait ratings here are *illustrative* aggregations of each brand's
# published cold-soil emergence and disease-package guidance. Treat them
# as brand-tailored heuristics, not breeding-program data.
SEED_CATALOG = {
    "corn": {
        "Pioneer": [
            {"id": "P9188AM", "rm": 91, "cold_tolerance": "high",
             "emergence_score": 8, "traits": ["AM"],
             "notes": "Short-season hybrid for early planting in cooler northern fields."},
            {"id": "P9608AM", "rm": 96, "cold_tolerance": "high",
             "emergence_score": 8, "traits": ["AM"],
             "notes": "Strong stress-emergence — favored when soils are slow to warm."},
            {"id": "P9998AM", "rm": 99, "cold_tolerance": "standard",
             "emergence_score": 7, "traits": ["AM"],
             "notes": "Workhorse mid-season hybrid for the Thumb of Michigan."},
            {"id": "P0157AM", "rm": 101, "cold_tolerance": "standard",
             "emergence_score": 6, "traits": ["AM"],
             "notes": "Full-season; plant after soils have warmed and stabilized."},
        ],
        "Dekalb": [
            {"id": "DKC42-04RIB", "rm": 92, "cold_tolerance": "high",
             "emergence_score": 8, "traits": ["VT2P", "RIB"],
             "notes": "Above-average stress emergence and consistent early vigor."},
            {"id": "DKC50-84RIB", "rm": 100, "cold_tolerance": "standard",
             "emergence_score": 7, "traits": ["VT2P", "RIB"],
             "notes": "Balanced agronomics across maturity zones."},
            {"id": "DKC55-09RIB", "rm": 105, "cold_tolerance": "low",
             "emergence_score": 5, "traits": ["VT2P", "RIB"],
             "notes": "Full-season — wait for warm, settled soils before planting."},
        ],
        "Channel": [
            {"id": "197-23VT2PRIB", "rm": 97, "cold_tolerance": "standard",
             "emergence_score": 7, "traits": ["VT2P", "RIB"],
             "notes": "Mid-season hybrid with reliable emergence."},
            {"id": "207-23VT2PRIB", "rm": 102, "cold_tolerance": "standard",
             "emergence_score": 6, "traits": ["VT2P", "RIB"],
             "notes": "Plant into stable 50°F+ soils for best vigor."},
        ],
        "AgriGold": [
            {"id": "A632-37VT2RIB", "rm": 95, "cold_tolerance": "high",
             "emergence_score": 8, "traits": ["VT2", "RIB"],
             "notes": "Excellent stress emergence on cool, residue-covered ground."},
            {"id": "A645-16VT2RIB", "rm": 100, "cold_tolerance": "standard",
             "emergence_score": 7, "traits": ["VT2", "RIB"],
             "notes": "Mid-season hybrid suited to no-till."},
        ],
        "NK": [
            {"id": "NK0414-3120", "rm": 94, "cold_tolerance": "high",
             "emergence_score": 8, "traits": ["3120 E-Z Refuge"],
             "notes": "Strong cold-soil emergence and early vigor scores."},
            {"id": "NK1199-3120", "rm": 102, "cold_tolerance": "standard",
             "emergence_score": 6, "traits": ["3120 E-Z Refuge"],
             "notes": "Full-season for warmer planting windows."},
        ],
        "Stine": [
            {"id": "9518-31", "rm": 95, "cold_tolerance": "high",
             "emergence_score": 8, "traits": ["VT2P"],
             "notes": "Cold-soil emergence rated above brand average."},
            {"id": "9734-32", "rm": 97, "cold_tolerance": "standard",
             "emergence_score": 7, "traits": ["VT2P"],
             "notes": "Reliable mid-season choice for the eastern Corn Belt."},
        ],
        "Beck's": [
            {"id": "5828AM", "rm": 98, "cold_tolerance": "standard",
             "emergence_score": 7, "traits": ["AM"],
             "notes": "Balanced agronomics; broad planting window."},
            {"id": "6175AMX", "rm": 101, "cold_tolerance": "standard",
             "emergence_score": 6, "traits": ["AMX"],
             "notes": "Full-season — plant once 50°F is locked in."},
        ],
        "Croplan": [
            {"id": "3899VT2P", "rm": 99, "cold_tolerance": "standard",
             "emergence_score": 7, "traits": ["VT2P"],
             "notes": "Mid-season hybrid — typical Thumb-of-MI fit."},
        ],
        "LG Seeds": [
            {"id": "LG5460STX", "rm": 104, "cold_tolerance": "low",
             "emergence_score": 5, "traits": ["SmartStax", "RIB"],
             "notes": "Full-season; not a cold-soil hybrid — wait for warm soils."},
            {"id": "LG5525VT2P", "rm": 105, "cold_tolerance": "standard",
             "emergence_score": 6, "traits": ["VT2P"],
             "notes": "Full-season with good disease tolerance package."},
        ],
        "Golden Harvest": [
            {"id": "G90X8-3220", "rm": 90, "cold_tolerance": "high",
             "emergence_score": 8, "traits": ["3220 E-Z Refuge"],
             "notes": "Short-season with excellent cold-start emergence."},
            {"id": "G96K2-3120", "rm": 96, "cold_tolerance": "standard",
             "emergence_score": 7, "traits": ["3120 E-Z Refuge"],
             "notes": "Mid-season hybrid with solid stress emergence."},
            {"id": "G03P6-3220", "rm": 103, "cold_tolerance": "standard",
             "emergence_score": 6, "traits": ["3220 E-Z Refuge"],
             "notes": "Full-season; wait for sustained warm soil."},
        ],
        "Wyffels": [
            {"id": "W4228", "rm": 92, "cold_tolerance": "high",
             "emergence_score": 8, "traits": ["VT2P", "RIB"],
             "notes": "Early hybrid built for cool northern planting windows."},
            {"id": "W5688", "rm": 96, "cold_tolerance": "standard",
             "emergence_score": 7, "traits": ["VT2P", "RIB"],
             "notes": "Mid-season with wide geography fit."},
            {"id": "W7508", "rm": 100, "cold_tolerance": "standard",
             "emergence_score": 7, "traits": ["VT2P", "RIB"],
             "notes": "Full-season hybrid; flexible planting window."},
        ],
        "Hefty": [
            {"id": "H3943RIB", "rm": 94, "cold_tolerance": "high",
             "emergence_score": 8, "traits": ["VT2P", "RIB"],
             "notes": "Strong early vigor in cool, no-till conditions."},
            {"id": "H5113RIB", "rm": 101, "cold_tolerance": "standard",
             "emergence_score": 6, "traits": ["VT2P", "RIB"],
             "notes": "Full-season with above-average yield ceiling."},
        ],
        "Dyna-Gro": [
            {"id": "D39VC80", "rm": 95, "cold_tolerance": "high",
             "emergence_score": 8, "traits": ["VT2P"],
             "notes": "Strong emergence on cool, heavy-residue ground."},
            {"id": "D47VC08", "rm": 99, "cold_tolerance": "standard",
             "emergence_score": 7, "traits": ["VT2P"],
             "notes": "Well-adapted mid-season for the eastern Corn Belt."},
            {"id": "D54VC48", "rm": 104, "cold_tolerance": "low",
             "emergence_score": 5, "traits": ["VT2P"],
             "notes": "Full-season; needs warm soils for uniform stand."},
        ],
        "Hubner": [
            {"id": "H4398AM", "rm": 93, "cold_tolerance": "high",
             "emergence_score": 8, "traits": ["AM"],
             "notes": "Early-season AM hybrid with above-average cold tolerance."},
            {"id": "H5022AM", "rm": 100, "cold_tolerance": "standard",
             "emergence_score": 7, "traits": ["AM"],
             "notes": "Mid-season with solid overall agronomics."},
        ],
        "Renk": [
            {"id": "RK594VT2P", "rm": 94, "cold_tolerance": "high",
             "emergence_score": 8, "traits": ["VT2P"],
             "notes": "Short-season with aggressive early vigor."},
            {"id": "RK708DGVT2P", "rm": 100, "cold_tolerance": "standard",
             "emergence_score": 7, "traits": ["VT2P", "DG"],
             "notes": "Mid-season droughtgard hybrid for variable conditions."},
        ],
    },
    "soybeans": {
        "Asgrow": [
            {"id": "AG18XF1", "rm": 1.8, "cold_tolerance": "high",
             "phytophthora": "Rps1k", "scn_source": "PI88788", "idc": 3,
             "traits": ["XtendFlex"],
             "notes": "Early group for short-season Thumb fields; tolerates cool soils."},
            {"id": "AG24XF1", "rm": 2.4, "cold_tolerance": "standard",
             "phytophthora": "Rps1k+field", "scn_source": "PI88788", "idc": 3,
             "traits": ["XtendFlex"],
             "notes": "Workhorse maturity for Decker/Snover with broad disease package."},
            {"id": "AG28XF2", "rm": 2.8, "cold_tolerance": "standard",
             "phytophthora": "Rps3a", "scn_source": "PI88788", "idc": 4,
             "traits": ["XtendFlex"],
             "notes": "Full-season; strong yield ceiling on better-drained ground."},
        ],
        "Pioneer": [
            {"id": "P22T44E", "rm": 2.2, "cold_tolerance": "high",
             "phytophthora": "Rps1k", "scn_source": "PI88788", "idc": 3,
             "traits": ["Enlist E3"],
             "notes": "Early-mid maturity with strong stress-emergence rating."},
            {"id": "P28A22X", "rm": 2.8, "cold_tolerance": "standard",
             "phytophthora": "Rps1k+field", "scn_source": "PI88788", "idc": 4,
             "traits": ["XtendFlex"],
             "notes": "Mid-maturity workhorse with deep disease package."},
            {"id": "P31T11E", "rm": 3.1, "cold_tolerance": "standard",
             "phytophthora": "field", "scn_source": "PI88788", "idc": 5,
             "traits": ["Enlist E3"],
             "notes": "Full-season; pair with later planting."},
        ],
        "Dekalb": [
            {"id": "DSR-2828X", "rm": 2.8, "cold_tolerance": "standard",
             "phytophthora": "Rps1k+field", "scn_source": "PI88788", "idc": 4,
             "traits": ["XtendFlex"],
             "notes": "Mid-maturity option with strong field tolerance to Phytophthora."},
        ],
        "Channel": [
            {"id": "2422R2X", "rm": 2.4, "cold_tolerance": "standard",
             "phytophthora": "Rps1k", "scn_source": "PI88788", "idc": 4,
             "traits": ["XtendFlex"],
             "notes": "Mid-maturity with consistent emergence."},
            {"id": "2820R2X", "rm": 2.8, "cold_tolerance": "standard",
             "phytophthora": "field", "scn_source": "PI88788", "idc": 4,
             "traits": ["XtendFlex"],
             "notes": "Full-season — plant into warmer settled soils."},
        ],
        "NK": [
            {"id": "NK22-K3E3", "rm": 2.2, "cold_tolerance": "high",
             "phytophthora": "Rps1k", "scn_source": "PI88788", "idc": 3,
             "traits": ["Enlist E3"],
             "notes": "Early maturity with strong emergence into cool soils."},
            {"id": "NK28-N6XF", "rm": 2.8, "cold_tolerance": "standard",
             "phytophthora": "Rps1k+field", "scn_source": "PI88788", "idc": 4,
             "traits": ["XtendFlex"],
             "notes": "Mid-maturity with broad adaptation."},
        ],
        "Stine": [
            {"id": "23EE32", "rm": 2.3, "cold_tolerance": "standard",
             "phytophthora": "Rps1k", "scn_source": "PI88788", "idc": 4,
             "traits": ["Enlist E3"],
             "notes": "Earlier Enlist for narrow-row systems."},
        ],
        "Beck's": [
            {"id": "245L4", "rm": 2.4, "cold_tolerance": "standard",
             "phytophthora": "Rps1k", "scn_source": "PI88788", "idc": 4,
             "traits": ["LL/GT27"],
             "notes": "Mid-maturity with workable defensive package."},
        ],
        "Credenz": [
            {"id": "CZ 2370E3", "rm": 2.3, "cold_tolerance": "standard",
             "phytophthora": "Rps1k+field", "scn_source": "PI88788", "idc": 3,
             "traits": ["Enlist E3"],
             "notes": "Earlier maturity with favorable IDC tolerance."},
            {"id": "CZ 3050E3", "rm": 3.0, "cold_tolerance": "standard",
             "phytophthora": "field", "scn_source": "PI88788", "idc": 4,
             "traits": ["Enlist E3"],
             "notes": "Full-season Enlist option."},
        ],
        "Mycogen": [
            {"id": "5N280E3", "rm": 2.8, "cold_tolerance": "standard",
             "phytophthora": "field", "scn_source": "PI88788", "idc": 4,
             "traits": ["Enlist E3"],
             "notes": "Full-season; field tolerance only — not for poorly drained ground."},
        ],
        "Xitavo": [
            {"id": "XO 2421E3", "rm": 2.4, "cold_tolerance": "standard",
             "phytophthora": "Rps3a", "scn_source": "PI88788", "idc": 3,
             "traits": ["Enlist E3"],
             "notes": "Mid-maturity with stacked Rps protection."},
        ],
        "Golden Harvest": [
            {"id": "GH2182XF", "rm": 2.1, "cold_tolerance": "high",
             "phytophthora": "Rps1k", "scn_source": "PI88788", "idc": 3,
             "traits": ["XtendFlex"],
             "notes": "Early-group with strong cold-soil emergence."},
            {"id": "GH2582XF", "rm": 2.5, "cold_tolerance": "standard",
             "phytophthora": "Rps1k+field", "scn_source": "PI88788", "idc": 4,
             "traits": ["XtendFlex"],
             "notes": "Mid-maturity workhorse with broad disease package."},
            {"id": "GH2992E3", "rm": 2.9, "cold_tolerance": "standard",
             "phytophthora": "field", "scn_source": "PI88788", "idc": 4,
             "traits": ["Enlist E3"],
             "notes": "Full-season Enlist with high yield ceiling on good ground."},
        ],
        "Dyna-Gro": [
            {"id": "S21XF82", "rm": 2.1, "cold_tolerance": "high",
             "phytophthora": "Rps1k", "scn_source": "PI88788", "idc": 3,
             "traits": ["XtendFlex"],
             "notes": "Early-group with rated cold-soil vigor."},
            {"id": "S26XF42", "rm": 2.6, "cold_tolerance": "standard",
             "phytophthora": "Rps1k+field", "scn_source": "PI88788", "idc": 4,
             "traits": ["XtendFlex"],
             "notes": "Mid-season with deep Phytophthora protection."},
        ],
        "Hefty": [
            {"id": "H23E32", "rm": 2.3, "cold_tolerance": "standard",
             "phytophthora": "Rps1k", "scn_source": "PI88788", "idc": 3,
             "traits": ["Enlist E3"],
             "notes": "Earlier Enlist with good standability."},
            {"id": "H28XF12", "rm": 2.8, "cold_tolerance": "standard",
             "phytophthora": "Rps1k+field", "scn_source": "PI88788", "idc": 4,
             "traits": ["XtendFlex"],
             "notes": "Mid-maturity XtendFlex with wide adaptation."},
        ],
        "Wyffels": [
            {"id": "W2220E3", "rm": 2.2, "cold_tolerance": "high",
             "phytophthora": "Rps1k", "scn_source": "PI88788", "idc": 3,
             "traits": ["Enlist E3"],
             "notes": "Short-season Enlist; tolerates early cool planting."},
            {"id": "W2780NXF", "rm": 2.7, "cold_tolerance": "standard",
             "phytophthora": "Rps1k+field", "scn_source": "PI88788", "idc": 4,
             "traits": ["XtendFlex"],
             "notes": "Mid-maturity with solid disease stack."},
        ],
        "Hubner": [
            {"id": "HS2442E3", "rm": 2.4, "cold_tolerance": "standard",
             "phytophthora": "Rps1k", "scn_source": "PI88788", "idc": 3,
             "traits": ["Enlist E3"],
             "notes": "Mid-maturity Enlist for central MI."},
        ],
        "Renk": [
            {"id": "RS2288XF", "rm": 2.2, "cold_tolerance": "high",
             "phytophthora": "Rps1k", "scn_source": "PI88788", "idc": 3,
             "traits": ["XtendFlex"],
             "notes": "Early XtendFlex with strong cold-soil emergence."},
            {"id": "RS2768E3", "rm": 2.7, "cold_tolerance": "standard",
             "phytophthora": "Rps1k+field", "scn_source": "PI88788", "idc": 4,
             "traits": ["Enlist E3"],
             "notes": "Mid-maturity Enlist with balanced agronomics."},
        ],
    },
}


def list_seed_catalog(crop: str) -> list[dict]:
    """Flatten the catalog for one crop into a list the picker can render.

    Returns one entry per (brand, cultivar) pair, with display-ready labels.
    Sorted by brand then by RM so the scrollable list is browsable.
    """
    crop_key = crop if crop in SEED_CATALOG else "corn"
    out: list[dict] = []
    for brand, cultivars in SEED_CATALOG[crop_key].items():
        for cv in sorted(cultivars, key=lambda c: c["rm"]):
            label = f"{brand} · {cv['id']}"
            sub_bits: list[str] = [f"{cv['rm']} RM"]
            if cv.get("cold_tolerance"):
                sub_bits.append(f"{cv['cold_tolerance']} cold tol.")
            if cv.get("phytophthora") and cv["phytophthora"] != "none":
                sub_bits.append(f"Phyt: {cv['phytophthora']}")
            if cv.get("emergence_score") is not None:
                sub_bits.append(f"emergence {cv['emergence_score']}/9")
            entry = {
                "brand": brand,
                "id": cv["id"],
                "label": label,
                "search": f"{brand} {cv['id']} {' '.join(cv.get('traits', []))} {cv.get('notes', '')}".lower(),
                "rm": cv["rm"],
                "cold_tolerance": cv.get("cold_tolerance"),
                "traits": cv.get("traits", []),
                "phytophthora": cv.get("phytophthora"),
                "scn_source": cv.get("scn_source"),
                "idc": cv.get("idc"),
                "emergence_score": cv.get("emergence_score"),
                "notes": cv.get("notes"),
                "sub": " · ".join(sub_bits),
            }
            out.append(entry)
    return out


def find_cultivar(crop: str, brand: str | None, cultivar_id: str | None) -> dict | None:
    if not brand or not cultivar_id:
        return None
    crop_key = crop if crop in SEED_CATALOG else "corn"
    for cv in SEED_CATALOG[crop_key].get(brand, []):
        if cv["id"] == cultivar_id:
            return cv
    return None


def apply_cultivar_to_profile(profile: dict, cultivar: dict | None) -> tuple[dict, list[str]]:
    """Return ``(tailored_profile, tailoring_notes)``.

    The base CROP_PROFILES dict is shallow-copied and selectively overridden
    based on the cultivar's declared trait classes. Notes are short
    human-readable strings the UI surfaces in the seed-selection panel so
    growers can see exactly which thresholds shifted and why.
    """
    if not cultivar:
        return profile, []
    tailored = dict(profile)
    notes: list[str] = []

    cold = (cultivar.get("cold_tolerance") or "standard").lower()
    if cold == "high":
        tailored["min_soil_temp_f"] = max(40, profile["min_soil_temp_f"] - 2)
        if "preferred_soil_temp_f" in profile:
            tailored["preferred_soil_temp_f"] = profile["preferred_soil_temp_f"] - 2
        tailored["frost_air_temp_f"] = profile["frost_air_temp_f"] - 2
        notes.append(
            f"Cold-tolerance class **high** — soil-temp floor relaxed to "
            f"{tailored['min_soil_temp_f']}°F and frost floor to "
            f"{tailored['frost_air_temp_f']}°F (Szczerba et al., 2021)."
        )
    elif cold == "low":
        tailored["min_soil_temp_f"] = profile["min_soil_temp_f"] + 2
        if "preferred_soil_temp_f" in profile:
            tailored["preferred_soil_temp_f"] = profile["preferred_soil_temp_f"] + 2
        tailored["frost_air_temp_f"] = profile["frost_air_temp_f"] + 2
        notes.append(
            f"Cold-tolerance class **low** — soil-temp floor raised to "
            f"{tailored['min_soil_temp_f']}°F to avoid imbibitional chilling."
        )

    phyt = (cultivar.get("phytophthora") or "").lower()
    if profile.get("phytophthora_sensitive"):
        if "rps" in phyt or "stack" in phyt:
            tailored["phytophthora_sensitive"] = False
            notes.append(
                f"Phytophthora resistance gene present ({cultivar['phytophthora']}) — "
                "Phytophthora root-rot evaluator stood down for warm-saturated soils."
            )
        elif phyt == "field":
            notes.append(
                "Field tolerance to Phytophthora only — sensitivity retained "
                "in the risk engine."
            )

    if cultivar.get("emergence_score") is not None and cultivar["emergence_score"] >= 8:
        notes.append(
            f"Stress-emergence score {cultivar['emergence_score']}/9 — vigorous "
            "stand even when seedbed conditions are marginal."
        )
    if cultivar.get("emergence_score") is not None and cultivar["emergence_score"] <= 5:
        notes.append(
            f"Stress-emergence score {cultivar['emergence_score']}/9 — favor a "
            "warmer, drier seedbed and avoid the shallowest depth."
        )

    rm = cultivar.get("rm")
    if isinstance(rm, (int, float)):
        if profile.get("label") == "Corn" and rm <= 95:
            notes.append(
                f"Short-season {rm}-day RM — earlier maturity buys frost margin "
                "at the back end of the season."
            )
        elif profile.get("label") == "Soybeans" and rm <= 2.2:
            notes.append(
                f"Early maturity group {rm} — fits compressed planting windows "
                "with less risk to fall frost on grain fill."
            )

    return tailored, notes


LEVEL_RANK = {"low": 0, "moderate": 1, "high": 2}
LEVEL_PENALTY = {"low": 0, "moderate": 15, "high": 40}

# Per-group multiplicative survival factors. Risks are grouped into 6
# independent hazard categories (cold, water, disease, pest, surface,
# chemical) and only the worst level *within* each group compounds.
# Calibrated for 6 groups: one high group ≈ 85% stand, two high groups ≈ 72%.
LEVEL_SURVIVAL_FACTOR = {"low": 1.0, "moderate": 0.97, "high": 0.85}

# Hard ceiling on any published survival probability. Even an all-"low" risk
# vector under a perfect forecast can't honestly promise 100% — there's always
# residual seed-lot, equipment, and unmodeled-pest variance. Cap at 99 so the
# UI never displays a number we can't defend.
SURVIVAL_PCT_CAP = 99


def _metric_severity(value: float, safe: float, moderate: float,
                     high: float, extreme: float) -> float:
    """Piecewise-linear severity from a raw metric.

    Anchors: safe→0.0, moderate→0.33, high→0.67, extreme→1.0.
    Works for both directions — pass anchors in the evaluator's natural order.
    """
    if safe > extreme:
        value, safe, moderate, high, extreme = -value, -safe, -moderate, -high, -extreme
    if value <= safe:
        return 0.0
    if value <= moderate:
        return 0.33 * (value - safe) / max(1e-9, moderate - safe)
    if value <= high:
        return 0.33 + 0.34 * (value - moderate) / max(1e-9, high - moderate)
    if value >= extreme:
        return 1.0
    return 0.67 + 0.33 * (value - high) / max(1e-9, extreme - high)


def _sigmoid_severity(value: float, midpoint: float, scale: float,
                      inverted: bool = False) -> float:
    """Logistic severity curve for threshold-crossing risks.

    Produces a smooth S-curve transition from 0 to 1. At midpoint,
    severity = 0.5. ``scale`` controls the width of the transition zone
    (larger = more gradual). When ``inverted=True`` the curve flips so
    that *low* values are dangerous (e.g. soil temperature where cold
    is the hazard).

    Biological basis: threshold-crossing stresses (chilling injury,
    frost kill, waterlogging) exhibit a continuous dose–response — there
    is no hard cutoff, just a steep transition zone around a critical
    value. The logistic captures this better than a piecewise-linear
    ramp that creates artificial kinks at level boundaries.
    """
    x = (value - midpoint) / max(1e-9, abs(scale))
    if inverted:
        x = -x
    clamped = max(-12.0, min(12.0, x))
    return 1.0 / (1.0 + math.exp(-clamped))


def _gaussian_severity(value: float, peak: float, sigma: float) -> float:
    """Bell-curve severity for risks that peak at a specific condition.

    Returns 1.0 at ``peak``, falling symmetrically to ~0.61 at ±sigma
    and ~0.13 at ±2*sigma. Used for pest/disease models where
    biological activity has an optimal temperature or GDD window and
    declines on both sides.

    Biological basis: Pythium aggressiveness peaks around 50 °F and
    drops at both warmer and colder extremes (Matthiesen et al. 2016);
    seedcorn-maggot oviposition peaks in cool-moist conditions and
    falls off in warm/dry or frozen soils; cutworm cutting pressure
    peaks in a GDD window around 325 DD post-flight and tapers before
    and after.
    """
    z = (value - peak) / max(1e-9, abs(sigma))
    return math.exp(-0.5 * z * z)


def _trapezoidal_severity(value: float, a: float, b: float,
                          c: float, d: float) -> float:
    """Trapezoidal severity for risks with a sustained danger plateau.

    Severity is 0 below ``a`` and above ``d``, ramps linearly from a→b
    and c→d, and holds at 1.0 across the b→c plateau. Used for risks
    where a broad range of conditions is equally dangerous — wireworm
    activity across 45–60 °F, Phytophthora zoospore motility across
    60–86 °F, slug feeding across 50–65 °F.

    Biological basis: many soil organisms have a wide thermal or
    moisture optimum where activity plateaus, flanked by ramp-up and
    ramp-down zones — a shape that piecewise-linear with only four
    anchors handles poorly.
    """
    if value <= a or value >= d:
        return 0.0
    if value < b:
        return (value - a) / max(1e-9, b - a)
    if value <= c:
        return 1.0
    return (d - value) / max(1e-9, d - c)


def _level_from_severity(severity: float) -> str:
    """Derive the discrete risk level from a continuous severity score.

    Thresholds are calibrated so that the three bands correspond to the
    agronomic decision tiers the UI renders: low (≈ no action needed),
    moderate (≈ monitor / conditional go), high (≈ delay or mitigate).
    """
    if severity >= 0.67:
        return "high"
    if severity >= 0.33:
        return "moderate"
    return "low"


# ---- Per-factor survival model (PDF: "Seed Survival Modeling") -----------
# Each of the 22 risk factors produces its own survival probability using the
# biologically appropriate model for its risk type. Factors fall into four
# mathematical categories; modifiers amplify downstream factors rather than
# acting as standalone killers.

FACTOR_CATEGORIES: dict[str, str] = {
    "chilling":       "biological_response",   # logistic/sigmoid probability
    "frost":          "biological_response",   # P(frost) × survival(min temp)
    "pythium":        "biological_response",   # Gaussian conduciveness × inoculum
    "phytophthora":   "biological_response",   # same structure, warmer optimum
    "herbicide":      "biological_response",   # first-order degradation kinetics
    "flooding":       "time_intensity",        # exponential decay survival
    "crusting":       "time_intensity",        # P(emergence | crust strength)
    "antecedent":     "modifier",              # amplifies water/disease/crust
    "topography":     "modifier",              # amplifies flooding/waterlogging
    "maggot":         "hazard_probability",    # GDD phenology × attractant
    "wireworm":       "hazard_probability",    # field-history probability
    "slugs":          "hazard_probability",    # conducive conditions probability
    "cutworm":        "hazard_probability",    # phenology model
    "leaf_beetle":    "hazard_probability",    # overwintering × planting date
    "heat_stress":    "biological_response",   # sigmoid on air temp vs crop lethal threshold
    "water_scarcity": "biological_response",   # composite: precip + drought + soil moisture
    "sds":            "biological_response",   # Fusarium virguliforme — cool wet soil at planting
    "rhizoctonia":    "biological_response",   # Rhizoctonia solani — warm damp seedling blight
    "idc":            "biological_response",   # iron deficiency chlorosis — calcareous + wet
    "scn":            "modifier",              # soybean cyst nematode — amplifies SDS/disease
    "white_mold":     "biological_response",   # Sclerotinia sclerotiorum — canopy + moisture
    "hessian_fly":    "hazard_probability",    # planting-date vs fly-free date
    "bydv":           "hazard_probability",    # aphid-vectored, fall exposure window
    "take_all":       "biological_response",   # Gaeumannomyces graminis — rotation-dependent
    "crown_rot":      "biological_response",   # Fusarium crown/root rot — warm dry seedbed
    "snow_mold":      "hazard_probability",    # Typhula/Microdochium — prolonged snow cover
    "stripe_rust":    "biological_response",   # Puccinia striiformis — cool moist conditions
    "winterkill":     "biological_response",   # crown cold injury, ice sheeting, desiccation
    "tan_spot":       "biological_response",   # Pyrenophora tritici-repentis — residue + moisture
    "common_root_rot": "biological_response",  # Bipolaris sorokiniana — rotation + stress
    "anthracnose":    "biological_response",   # Colletotrichum lindemuthianum — cool moist
    "bacterial_blight": "biological_response",  # Xanthomonas (CBB) + Pseudomonas (halo)
    "cercospora":     "biological_response",   # Cercospora beticola — warm nights + leaf wetness
    "bolting":        "biological_response",   # vernalization-induced premature flowering
    "aphanomyces":    "biological_response",   # Aphanomyces cochlioides — warm wet damping-off
    "sbcn":           "modifier",              # sugar beet cyst nematode — amplifies root diseases
    "wind_damage":    "hazard_probability",    # sand blasting / wind whipping cotyledon-stage beets
    "root_maggot":    "hazard_probability",    # Tetanops myopaeformis — GDD phenology
    "autotoxicity":   "biological_response",   # medicarpin allelopathy — alfalfa after alfalfa
    "aphanomyces_alfalfa": "biological_response",  # Aphanomyces euteiches — cool wet seedling rot
    "sclerotinia_crown":   "biological_response",  # Sclerotinia trifoliorum — cool moist crown rot
    "potato_leafhopper":   "hazard_probability",   # Empoasca fabae — hopperburn in new seedings
    "alfalfa_weevil":      "hazard_probability",   # Hypera postica — larval defoliation
    "soil_ph":             "biological_response",   # pH < 6.5 impairs nodulation + establishment
}

MODIFIER_TARGETS: dict[str, list[str]] = {
    "antecedent": ["flooding", "crusting", "pythium", "phytophthora", "sds", "rhizoctonia",
                   "crown_rot", "take_all", "anthracnose", "bacterial_blight",
                   "aphanomyces_alfalfa", "sclerotinia_crown"],
    "topography": ["flooding"],
    "scn": ["sds", "pythium", "phytophthora"],
    "sbcn": ["rhizoctonia", "aphanomyces", "pythium"],
}


def _biological_response_survival(severity: float) -> float:
    """Logistic survival curve for threshold-crossing biological stresses.

    These factors (chilling, frost, Pythium, Phytophthora, herbicide carryover)
    exhibit a steep dose-response — survival drops sharply once the organism's
    tolerance threshold is crossed. Since all non-modifier factors now multiply
    independently (rather than being grouped into 6 categories), the curve
    is calibrated to give sensible results when compounded:
      - One factor at high (~0.67): ~85% survival
      - Two factors at high: ~72% survival
      - Three factors at high: ~61% survival (DO NOT PLANT)

    Dead zone: severity < 0.20 returns 1.0 — prevents phantom baseline drag
    from many "low" factors compounding to an unrealistic penalty. Academic
    field emergence under optimal conditions is 90-95% (OSU, Purdue).

    Floor lowered to 25% (from 40%) so that two concurrent extreme stresses
    (e.g., severe chilling + hard frost at severity 1.0) produce 15-25%
    survival, matching Purdue/UNL observations of near-total stand loss.

    Calibration:
      severity 0.0  → 1.00  (no risk)
      severity 0.20 → 1.00  (dead zone — no contribution)
      severity 0.33 → ~0.96 (low-moderate boundary)
      severity 0.50 → ~0.92 (moderate)
      severity 0.67 → ~0.85 (moderate-high boundary)
      severity 0.80 → ~0.78 (high)
      severity 1.0  → ~0.69 (extreme)
    """
    if severity < 0.20:
        return 1.0
    clamped = max(0.0, min(1.0, severity))
    return max(0.25, 0.58 + 0.42 / (1.0 + math.exp(5.0 * (clamped - 0.78))))


def _time_intensity_survival(severity: float) -> float:
    """Exponential decay survival for duration/intensity-dependent factors.

    Flooding uses survival = e^(-k × hours_saturated) in the biological model;
    crusting uses P(emergence | crust strength). Both share the property that
    survival degrades smoothly with increasing exposure duration/intensity.

    Dead zone: severity < 0.20 returns 1.0 to prevent baseline drag.

    Calibration:
      severity 0.0  → 1.00  (no exposure)
      severity 0.20 → 1.00  (dead zone)
      severity 0.33 → ~0.95 (mild exposure)
      severity 0.50 → ~0.89 (moderate)
      severity 0.67 → ~0.82 (significant)
      severity 0.80 → ~0.75 (prolonged)
      severity 1.0  → ~0.64 (extreme saturation)
    """
    if severity < 0.20:
        return 1.0
    clamped = max(0.0, min(1.0, severity))
    return max(0.20, math.exp(-0.45 * clamped * clamped))


def _hazard_probability_survival(severity: float) -> float:
    """Quartic survival for pest hazard probability factors.

    Pests (maggot, wireworm, slugs, cutworm, bean leaf beetle) cause partial
    stand loss. The quartic (4th-power) curve ensures moderate pest pressure
    barely registers while severe infestations still cause major stand loss.
    This prevents 3-5 simultaneous moderate pest factors from compounding to
    unrealistic combined penalties.

    Dead zone: severity < 0.20 returns 1.0 to prevent baseline drag.

    Calibration:
      severity 0.0  → 1.00
      severity 0.20 → 1.00  (dead zone)
      severity 0.30 → ~0.997 (light pest pressure — negligible)
      severity 0.50 → ~0.978 (moderate — barely dents survival)
      severity 0.70 → ~0.916 (heavy — noticeable but manageable)
      severity 0.80 → ~0.857 (severe infestation)
      severity 1.0  → ~0.65  (peak infestation — up to ~45% stand loss)
    """
    if severity < 0.20:
        return 1.0
    clamped = max(0.0, min(1.0, severity))
    return max(0.55, 1.0 - 0.35 * clamped * clamped * clamped * clamped)


def _modifier_amplification(severity: float) -> float:
    """Compute the amplification factor for a modifier risk.

    Modifiers (antecedent saturation, topography) don't kill seeds directly
    but amplify downstream factors. Returns a multiplier ≥1.0 applied to
    the severity of targeted downstream factors before their survival is
    computed. Coefficient kept conservative (0.20) because multiple modifiers
    can stack multiplicatively on the same downstream factor.

    Calibration:
      severity 0.0  → 1.00 (no amplification)
      severity 0.33 → ~1.07 (mild amplification)
      severity 0.67 → ~1.13
      severity 1.0  → ~1.20 (strong amplification)
    """
    clamped = max(0.0, min(1.0, severity))
    return 1.0 + 0.20 * clamped


def _compute_factor_survival(key: str, severity: float) -> float:
    """Compute the per-factor survival probability using the category-appropriate model."""
    cat = FACTOR_CATEGORIES.get(key, "hazard_probability")
    if cat == "biological_response":
        return _biological_response_survival(severity)
    elif cat == "time_intensity":
        return _time_intensity_survival(severity)
    elif cat == "modifier":
        return 1.0
    else:
        return _hazard_probability_survival(severity)


def _external_risk_survivability(risks: list[Risk],
                                 cultivar: dict | None = None) -> int:
    """Per-factor multiplicative survival model.

    Each of the 22 risk factors produces its own survival probability
    using the biologically appropriate formula for its category. Modifier
    factors (antecedent saturation, topography) amplify the severity of
    targeted downstream factors before those factors' survival is computed.

    All non-modifier survival factors multiply together, then the cultivar
    factor scales the result. This replaces the old group-worst approach
    with a model where each factor's contribution is biologically distinct.
    """
    risk_by_key = {r.key: r for r in risks}

    modifier_amps: dict[str, float] = {}
    for mod_key in MODIFIER_TARGETS:
        mod_risk = risk_by_key.get(mod_key)
        if mod_risk:
            modifier_amps[mod_key] = _modifier_amplification(mod_risk.severity)
            mod_risk.survival_factor = 1.0
            mod_risk.model_category = "modifier"

    total = 1.0
    n_active = 0
    for r in risks:
        cat = FACTOR_CATEGORIES.get(r.key, "hazard_probability")
        r.model_category = cat
        if cat == "modifier":
            continue

        adjusted_severity = r.severity
        combined_amp = 1.0
        for mod_key, targets in MODIFIER_TARGETS.items():
            if r.key in targets and mod_key in modifier_amps:
                combined_amp *= modifier_amps[mod_key]
        combined_amp = min(combined_amp, 1.35)
        adjusted_severity = min(1.0, adjusted_severity * combined_amp)

        sf = _compute_factor_survival(r.key, adjusted_severity)
        r.survival_factor = sf
        total *= sf
        if sf < 1.0:
            n_active += 1

    if n_active > 1:
        total = total ** (1.0 / max(1.0, n_active ** 0.50))

    total *= _cultivar_survival_factor(cultivar)
    return min(SURVIVAL_PCT_CAP, max(0, round(total * 100)))


def _external_risk_survival_range(risks: list[Risk],
                                  confidence_scalar: float,
                                  cultivar: dict | None = None) -> tuple[int, int]:
    """Confidence-aware survival bounds using the per-factor model.

    Shifts each non-modifier factor's severity ±0.12 to create
    pessimistic/optimistic scenarios, interpolated by forecast confidence.
    """
    risk_by_key = {r.key: r for r in risks}
    cv_factor = _cultivar_survival_factor(cultivar)

    modifier_amps: dict[str, float] = {}
    for mod_key in MODIFIER_TARGETS:
        mod_risk = risk_by_key.get(mod_key)
        if mod_risk:
            modifier_amps[mod_key] = _modifier_amplification(mod_risk.severity)

    severities: list[float] = []
    keys: list[str] = []
    for r in risks:
        cat = FACTOR_CATEGORIES.get(r.key, "hazard_probability")
        if cat == "modifier":
            continue
        adjusted = r.severity
        combined_amp = 1.0
        for mod_key, targets in MODIFIER_TARGETS.items():
            if r.key in targets and mod_key in modifier_amps:
                combined_amp *= modifier_amps[mod_key]
        combined_amp = min(combined_amp, 1.35)
        adjusted = min(1.0, adjusted * combined_amp)
        severities.append(adjusted)
        keys.append(r.key)

    if not severities:
        return SURVIVAL_PCT_CAP, SURVIVAL_PCT_CAP

    _SHIFT = 0.12
    point_total = 1.0
    lo_total = 1.0
    hi_total = 1.0
    n_active = 0
    n_active_lo = 0
    n_active_hi = 0
    for key, sev in zip(keys, severities):
        sf_pt = _compute_factor_survival(key, sev)
        sf_lo = _compute_factor_survival(key, min(1.0, sev + _SHIFT))
        sf_hi = _compute_factor_survival(key, max(0.0, sev - _SHIFT))
        point_total *= sf_pt
        lo_total *= sf_lo
        hi_total *= sf_hi
        if sf_pt < 1.0:
            n_active += 1
        if sf_lo < 1.0:
            n_active_lo += 1
        if sf_hi < 1.0:
            n_active_hi += 1

    if n_active > 1:
        point_total = point_total ** (1.0 / max(1.0, n_active ** 0.50))
    if n_active_lo > 1:
        lo_total = lo_total ** (1.0 / max(1.0, n_active_lo ** 0.50))
    if n_active_hi > 1:
        hi_total = hi_total ** (1.0 / max(1.0, n_active_hi ** 0.50))

    point_total *= cv_factor
    lo_total *= cv_factor
    hi_total *= cv_factor

    point = min(SURVIVAL_PCT_CAP, max(0, round(point_total * 100)))
    lo = min(SURVIVAL_PCT_CAP, max(0, round(lo_total * 100)))
    hi = min(SURVIVAL_PCT_CAP, max(0, round(hi_total * 100)))

    c = max(0.0, min(1.0, confidence_scalar))
    lower = round(point - (point - lo) * (1.0 - c))
    upper = round(point + (hi - point) * (1.0 - c))
    lower = max(0, min(SURVIVAL_PCT_CAP, lower))
    upper = max(lower, min(SURVIVAL_PCT_CAP, upper))
    return lower, upper


# Seedcorn maggot (Delia platura) degree-day model. Cumulative DDs base 39°F
# from Jan 1 — adult fly emergence peaks at these thresholds (Iowa State / NCERA).
# Field eggs are laid heavily within ±1 week of each peak.
SCM_BASE_F = 39.0
SCM_PEAKS_GDD = (354.0, 1080.0, 1800.0)   # G1, G2, G3 adult-fly peaks
SCM_PEAK_WIDTH_DD = 200.0                  # GDD half-width over which fly activity tapers

# Maggot (larval) lifecycle — the underground damage phase that follows egg-laying.
# Eggs hatch ~50 DD after the fly peak; larvae feed for ~250 DD before pupating.
# Risk to seeds is driven by maggot presence in the soil, not fly activity above ground.
SCM_EGG_HATCH_DD = 50.0                    # DD after fly peak for eggs to hatch
SCM_LARVAL_DURATION_DD = 250.0             # DD of active larval feeding
SCM_MAGGOT_PEAK_OFFSET_DD = 150.0          # DD after fly peak where maggot damage peaks
SCM_MAGGOT_WIDTH_DD = 180.0                # half-width of maggot damage bell curve

# Corn needs ~120 GDD base 50°F to emerge; soybeans similar. Used to size the
# vulnerability window — slow-germinating seeds spend more days exposed.
SEED_EMERGE_GDD_50 = 120.0

# Black cutworm (Agrotis ipsilon). Source: ISU CIG / Hodgson — once a
# "significant flight" (8+ moths in 2 nights) is captured, larvae reach the
# damaging 4th instar after ~300 GDD base 50°F. We default the flight start
# to mid-April for the upper-Midwest latitudes when no trap data is supplied;
# this can be overridden per-location later.
BCW_BASE_F = 50.0
BCW_FLIGHT_TO_DAMAGE_GDD = 300.0
BCW_DEFAULT_FLIGHT_DOY = 105        # April 15 in non-leap years
BCW_DAMAGE_WINDOW_GDD = (200.0, 450.0)   # window of active cutting pressure

# Bean leaf beetle (Ceratoma trifurcata). Source: Lam & Pedigo 2000 winter-
# mortality model + ISU CIG. Overwintered adults move into the *first*
# emerged soybean fields. Survival proxy uses the count of frost days in the
# preceding 30-day archive — fewer frost days late in the dormant season =>
# higher beetle survival => higher pressure on early plantings.
BLB_EARLY_PLANTING_DOY = 130        # ~ May 10 — earlier soybeans = beetle magnets
BLB_LOW_FROST_DAYS = 4              # ≤ this many frost days in last 30d => mild winter end


# ----- data shapes -------------------------------------------------------

@dataclass
class Risk:
    key: str
    name: str
    level: str          # "low" | "moderate" | "high"
    headline: str       # one-line summary for the card
    detail: str         # longer explanation
    metric: Optional[str] = None  # e.g. "47.2°F" — shown prominently on the card
    severity: float = 0.0         # 0.0 (no risk) to 1.0 (extreme) — continuous scale
    curve_type: str = ""          # "sigmoid" | "gaussian" | "trapezoidal" | "composite"
    survival_factor: float = 1.0  # per-factor survival probability (0.0–1.0)
    model_category: str = ""      # "biological_response" | "time_intensity" | "modifier" | "hazard_probability"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class UserInputs:
    """Optional field-condition inputs the weather API can't infer."""
    tillage: str = "conventional"        # "conventional" | "reduced" | "no-till"
    residue: str = "low"                 # "low" | "moderate" | "heavy"
    manure_recent: bool = False          # manure applied in last ~6 weeks
    previous_grass: bool = False         # last year was sod, pasture, or grass cover
    herbicide_last_season: str = ""      # free-text; blank if none
    field_tiled: bool = False            # subsurface tile drainage installed
    seeds_per_acre: int | None = None    # planting population (seeds/acre)

    @classmethod
    def from_query(cls, **kw) -> "UserInputs":
        clean = {k: v for k, v in kw.items() if v is not None and v != ""}
        return cls(**{**cls().__dict__, **clean})


# ----- external calls ---------------------------------------------------

def get_coordinates(zip_code: str) -> tuple[float, float, str]:
    """Resolve a zip code (or place name) to (lat, lon, display_name)."""
    r = _http.get(
        GEOCODE_URL,
        params={"name": zip_code, "count": 1, "language": "en", "format": "json"},
        timeout=10,
    )
    if r.status_code != 200 or "results" not in r.json():
        raise HTTPException(status_code=404, detail=f"Location '{zip_code}' not found")
    loc = r.json()["results"][0]
    name_parts = [loc.get("name"), loc.get("admin1"), loc.get("country_code")]
    display = ", ".join(p for p in name_parts if p)
    return loc["latitude"], loc["longitude"], display


def reverse_geocode(lat: float, lon: float) -> str:
    """Best-effort reverse lookup; falls back to coordinates string."""
    try:
        r = _http.get(
            REVERSE_GEOCODE_URL,
            params={"latitude": lat, "longitude": lon, "count": 1, "language": "en", "format": "json"},
            timeout=8,
        )
        if r.status_code == 200 and r.json().get("results"):
            loc = r.json()["results"][0]
            parts = [loc.get("name"), loc.get("admin1"), loc.get("country_code")]
            return ", ".join(p for p in parts if p)
    except httpx.HTTPError:
        pass
    return f"{lat:.3f}, {lon:.3f}"


def reverse_geocode_with_country(lat: float, lon: float) -> tuple[str, str]:
    """Reverse lookup returning (place_string, country_code).

    country_code is the ISO 3166-1 alpha-2 code (e.g. "US") or empty string
    if the lookup fails.
    """
    try:
        r = _http.get(
            REVERSE_GEOCODE_URL,
            params={"latitude": lat, "longitude": lon, "count": 1, "language": "en", "format": "json"},
            timeout=8,
        )
        if r.status_code == 200 and r.json().get("results"):
            loc = r.json()["results"][0]
            parts = [loc.get("name"), loc.get("admin1"), loc.get("country_code")]
            place = ", ".join(p for p in parts if p)
            return place, loc.get("country_code", "")
    except httpx.HTTPError:
        pass
    return f"{lat:.3f}, {lon:.3f}", ""


_PRECIP_HOURLY_CAP_IN = 1.5

def _smooth_precip_spikes(precip: list[float | None]) -> list[float | None]:
    """Redistribute implausible single-hour precipitation spikes.

    NWP deterministic models dump an entire convective event into one
    timestep.  Cap at 1.5"/hr and spread excess with a Gaussian kernel
    (σ=2 h) centred on the spike so the temporal shape resembles a real
    storm rather than a uniform smear across the calendar day.
    """
    if not precip:
        return precip
    import math
    out = list(precip)
    sigma = 2.0
    for i, p in enumerate(out):
        if p is None or p <= _PRECIP_HOURLY_CAP_IN:
            continue
        excess = p - _PRECIP_HOURLY_CAP_IN
        out[i] = _PRECIP_HOURLY_CAP_IN
        radius = 6
        lo = max(0, i - radius)
        hi = min(len(out), i + radius + 1)
        weights = []
        for j in range(lo, hi):
            if j == i:
                weights.append(0.0)
            else:
                weights.append(math.exp(-0.5 * ((j - i) / sigma) ** 2))
        total_w = sum(weights)
        if total_w > 0:
            for k, j in enumerate(range(lo, hi)):
                if j == i:
                    continue
                out[j] = (out[j] or 0.0) + excess * weights[k] / total_w
    return out


def fetch_forecast(lat: float, lon: float, days: int = FORECAST_FETCH_DAYS) -> dict:
    ck = _cache_key("forecast", round(lat, 3), round(lon, 3), days)
    cached = _cache.get(ck)
    if cached is not None:
        return cached
    r = _http.get(
        FORECAST_URL,
        params={
            "latitude": lat,
            "longitude": lon,
            "hourly": ",".join([
                "temperature_2m",
                "precipitation",
                "soil_temperature_6cm",
                "soil_temperature_18cm",
                "soil_moisture_0_to_1cm",
                "soil_moisture_1_to_3cm",
                "soil_moisture_3_to_9cm",
                "uv_index",
                "relative_humidity_2m",
                "shortwave_radiation",
                "wind_speed_10m",
            ]),
            "daily": ",".join([
                "temperature_2m_min",
                "temperature_2m_max",
                "precipitation_sum",
                "uv_index_max",
                "shortwave_radiation_sum",
            ]),
            "temperature_unit": "fahrenheit",
            "precipitation_unit": "inch",
            "timezone": "auto",
            "past_days": 1,
            "forecast_days": days,
        },
        timeout=15,
    )
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail="Weather data unavailable")
    data = r.json()
    hourly = data.get("hourly") or {}
    past_hourly: dict = {}
    main_hourly: dict = {}
    for k, v in hourly.items():
        if isinstance(v, list):
            past_hourly[k] = v[:24]
            main_hourly[k] = v[24:]
        else:
            past_hourly[k] = v
            main_hourly[k] = v
    main_hourly["precipitation"] = _smooth_precip_spikes(
        main_hourly.get("precipitation") or [],
    )
    data["hourly"] = main_hourly
    data["past_hourly"] = past_hourly
    # past_days also prepends yesterday to the daily arrays; strip it so
    # daily_times[0] continues to mean "today" for downstream evaluators.
    daily = data.get("daily") or {}
    trimmed_daily: dict = {}
    for k, v in daily.items():
        trimmed_daily[k] = v[1:] if isinstance(v, list) else v
    data["daily"] = trimmed_daily
    _cache.set(ck, data, CACHE_TTL_FORECAST)
    return data


def fetch_recent_history(lat: float, lon: float, days: int = 30) -> dict:
    """Last N days of actuals from the Archive API (ends ~5 days ago).

    Used to compute antecedent rainfall, recent soil-temp trend, etc.
    Returns {} on any error so the response can degrade gracefully.
    """
    ck = _cache_key("history", round(lat, 3), round(lon, 3), days)
    cached = _cache.get(ck)
    if cached is not None:
        return cached
    end = date.today() - timedelta(days=ARCHIVE_LAG_DAYS)
    start = end - timedelta(days=days)
    try:
        r = _http.get(
            ARCHIVE_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "daily": ",".join([
                    "temperature_2m_min",
                    "temperature_2m_max",
                    "precipitation_sum",
                    "soil_temperature_7_to_28cm_mean",
                ]),
                "temperature_unit": "fahrenheit",
                "precipitation_unit": "inch",
                "timezone": "auto",
            },
            timeout=15,
        )
        if r.status_code == 200:
            result = r.json()
            _cache.set(ck, result, CACHE_TTL_HISTORY)
            return result
    except httpx.HTTPError:
        pass
    return {}


def _fetch_archive_window(lat: float, lon: float, start: date, end: date) -> dict:
    try:
        r = _http.get(
            ARCHIVE_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "daily": "temperature_2m_min,temperature_2m_max,precipitation_sum",
                "temperature_unit": "fahrenheit",
                "precipitation_unit": "inch",
                "timezone": "auto",
            },
            timeout=15,
        )
        if r.status_code == 200:
            return r.json()
    except httpx.HTTPError:
        pass
    return {}


def fetch_climatology(lat: float, lon: float, target: date,
                      years_back: int = 5, window_days: int = 33) -> list[dict]:
    """For each of the last N years, pull a window around target ± window_days.

    Calls run in parallel. Returns a list of yearly archive responses (possibly
    empty) — caller derives the per-day normals. The default window covers the
    full 31-day extended outlook (today through today + 31).
    """
    def _one(years_ago: int) -> dict:
        try:
            ref = target.replace(year=target.year - years_ago)
        except ValueError:  # Feb 29 in a non-leap target year
            ref = (target - timedelta(days=1)).replace(year=target.year - years_ago)
        return _fetch_archive_window(
            lat, lon,
            ref - timedelta(days=window_days),
            ref + timedelta(days=window_days),
        )

    with ThreadPoolExecutor(max_workers=years_back) as pool:
        return [r for r in pool.map(_one, range(1, years_back + 1)) if r]


def fetch_scm_inputs(lat: float, lon: float) -> tuple[dict, dict]:
    """Pull the data needed to drive the SCM degree-day model.

    Returns ``(season_archive, extended_forecast)`` where:
      * ``season_archive`` is daily tmin/tmax from Jan 1 of the current year
        through the archive's lag boundary (~6 days ago).
      * ``extended_forecast`` is daily tmin/tmax covering ``past_days`` (which
        bridges the archive lag) plus 9 forecast days, so the GDD series is
        continuous from Jan 1 through the end of the planting horizon.
    """
    today = date.today()
    year_start = date(today.year, 1, 1)
    arc_end = today - timedelta(days=ARCHIVE_LAG_DAYS + 1)

    season_archive = _fetch_archive_window(lat, lon, year_start, arc_end)

    extended: dict = {}
    try:
        r = _http.get(
            FORECAST_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_min,temperature_2m_max,precipitation_sum",
                "temperature_unit": "fahrenheit",
                "precipitation_unit": "inch",
                "timezone": "auto",
                "past_days": ARCHIVE_LAG_DAYS + 2,
                "forecast_days": FORECAST_FETCH_DAYS,
            },
            timeout=15,
        )
        if r.status_code == 200:
            extended = r.json()
    except httpx.HTTPError:
        pass

    return season_archive, extended


# ----- supplementary free public data sources ---------------------------
# These layers replace heuristics with measured/authoritative data:
#   * USDA SSURGO  → per-point soil drainage, hydrologic group, texture, OM
#   * NWS NDFD     → independent forecast (cross-check vs Open-Meteo)
#   * ISU CIG      → weekly black-cutworm pheromone-trap captures (real biofix)
# Each fetcher is designed to fail gracefully — if the network call errors out
# or the response shape changes, the rest of the pipeline still runs on the
# pre-existing modeled inputs.

# Legacy per-function caches replaced by the unified _TTLCache above.


def fetch_ssurgo_soil(lat: float, lon: float) -> dict:
    """Pull SSURGO soil-survey attributes for the dominant component at a point.

    Uses the USDA Soil Data Access (SDA) REST service, which accepts ad-hoc
    T-SQL queries. We call ``SDA_Get_Mukey_from_intersection_with_WktWgs84``
    to map (lon, lat) → mukey, then join the dominant component's drainage
    class, hydrologic group, and the surface horizon's texture + organic
    matter. Returns ``{}`` on any failure so callers can degrade gracefully.

    Source: USDA NRCS Soil Data Access — https://sdmdataaccess.sc.egov.usda.gov/
    Schema reference: https://sdmdataaccess.sc.egov.usda.gov/documents/
    """
    ck = _cache_key("ssurgo", round(lat, 3), round(lon, 3))
    cached = _cache.get(ck)
    if cached is not None:
        return cached

    # T-SQL query: dominant major component for the mukey at this point + the
    # *surface* horizon (smallest hzdept_r) for texture and OM. ORDER BY pulls
    # the dominant component first; LIMIT to its top horizon via TOP 1.
    query = (
        "SELECT TOP 1 mu.mukey, mu.muname, c.compname, c.comppct_r, "
        "c.drainagecl, c.hydgrp, ch.sandtotal_r, ch.silttotal_r, "
        "ch.claytotal_r, ch.om_r, ch.awc_r "
        f"FROM SDA_Get_Mukey_from_intersection_with_WktWgs84('point({lon} {lat})') as mp "
        "INNER JOIN mapunit mu ON mu.mukey = mp.mukey "
        "INNER JOIN component c ON c.mukey = mu.mukey "
        "INNER JOIN chorizon ch ON ch.cokey = c.cokey "
        "WHERE c.majcompflag = 'Yes' "
        "ORDER BY c.comppct_r DESC, ch.hzdept_r ASC"
    )
    try:
        r = _http.post(
            SSURGO_URL,
            json={"format": "JSON", "query": query},
            timeout=12,
        )
        if r.status_code != 200:
            _log_fetch_error("ssurgo", f"HTTP {r.status_code} for ({lat},{lon})")
            return {}
        rows = (r.json() or {}).get("Table") or []
        if not rows:
            _log_fetch_error("ssurgo", f"empty result for ({lat},{lon})")
            return {}
        row = rows[0]
        # Column order matches the SELECT above.
        mukey, muname, compname, comppct, drainage, hydgrp, sand, silt, clay, om, awc = (
            row + [None] * (11 - len(row))
        )
        def _f(x):
            try: return float(x) if x not in (None, "") else None
            except (TypeError, ValueError): return None
        sand_f, silt_f, clay_f = _f(sand), _f(silt), _f(clay)
        texture = _texture_class(sand_f, silt_f, clay_f)
        out = {
            "mukey": mukey,
            "map_unit": muname,
            "component": compname,
            "component_pct": _f(comppct),
            "drainage_class": drainage,
            "hydrologic_group": hydgrp,
            "sand_pct": sand_f,
            "silt_pct": silt_f,
            "clay_pct": clay_f,
            "organic_matter_pct": _f(om),
            "available_water_capacity": _f(awc),
            "texture_class": texture,
        }
        _cache.set(ck, out, CACHE_TTL_SOIL)
        return out
    except (httpx.HTTPError, ValueError) as e:
        _log_fetch_error("ssurgo", e)
        return {}


def _texture_class(sand: Optional[float], silt: Optional[float],
                   clay: Optional[float]) -> Optional[str]:
    """USDA soil-texture triangle classification from %sand/silt/clay.

    Implements the standard 12-class system. Used to size drainage and
    crusting penalties — sand drains fast and rarely crusts; silt loams crust
    badly under rain-then-bake; clays hold water and feed Pythium.
    """
    if sand is None or silt is None or clay is None:
        return None
    s, si, cl = sand, silt, clay
    if cl >= 40:
        if cl >= 60: return "clay"
        if s >= 45: return "sandy clay"
        if si >= 40: return "silty clay"
        return "clay"
    if cl >= 27 and si < 28:
        return "clay loam" if s < 45 else "sandy clay loam"
    if cl >= 20 and si >= 28 and s < 45:
        return "silty clay loam" if si >= 40 else "clay loam"
    if cl >= 7 and cl < 27 and si >= 28 and si < 50 and s < 52:
        return "loam"
    if si >= 50 and (cl >= 12 and cl < 27):
        return "silt loam"
    if si >= 50 and cl < 12:
        return "silt loam" if si < 80 else "silt"
    if cl < 7 and si < 50 and s >= 43:
        if s >= 85 and cl < 10: return "sand" if (s + 1.5 * cl) >= 90 else "loamy sand"
        if s >= 70 and cl < 15: return "loamy sand"
        return "sandy loam"
    return "loam"


def fetch_nws_forecast_summary(lat: float, lon: float) -> dict:
    """NWS api.weather.gov 7-day forecast — used as a cross-check on Open-Meteo.

    Returns ``{daily: [{date, tmin_f, tmax_f, precip_pop}], office}``. ``precip_pop``
    is the maximum probability-of-precipitation across the day's two periods
    (a coarse signal, but useful for surfacing days where NWS thinks rain is
    likely while Open-Meteo's modeled total reads dry — or vice versa).
    The point→gridpoint→forecast flow is documented at
    https://www.weather.gov/documentation/services-web-api. Falls back to
    ``{}`` on any error so the rest of the pipeline still runs.
    """
    ck = _cache_key("nws", round(lat, 3), round(lon, 3))
    cached = _cache.get(ck)
    if cached is not None:
        return cached
    try:
        headers = {"User-Agent": NWS_USER_AGENT, "Accept": "application/geo+json"}
        pr = _http.get(
            NWS_POINTS_URL.format(lat=round(lat, 4), lon=round(lon, 4)),
            headers=headers, timeout=10,
        )
        if pr.status_code != 200:
            # 404 here is expected for points outside CONUS — still worth a log
            # line so a US-only user spots a sustained outage.
            _log_fetch_error("nws", f"points HTTP {pr.status_code} for ({lat},{lon})")
            return {}
        props = (pr.json() or {}).get("properties") or {}
        forecast_url = props.get("forecast")
        office = props.get("gridId")
        if not forecast_url:
            _log_fetch_error("nws", f"no forecast url in points response for ({lat},{lon})")
            return {}
        fr = _http.get(forecast_url, headers=headers, timeout=12)
        if fr.status_code != 200:
            _log_fetch_error("nws", f"forecast HTTP {fr.status_code} from {forecast_url}")
            return {}
        periods = ((fr.json() or {}).get("properties") or {}).get("periods") or []

        # NWS returns alternating day/night periods. Group by ISO date and take
        # the daytime high + nighttime low for each.
        by_date: dict[str, dict] = {}
        for p in periods:
            iso = (p.get("startTime") or "")[:10]
            if not iso:
                continue
            t = p.get("temperature")
            unit = (p.get("temperatureUnit") or "F").upper()
            t_f = t if unit == "F" else (t * 9 / 5 + 32 if t is not None else None)
            entry = by_date.setdefault(
                iso, {"date": iso, "tmin_f": None, "tmax_f": None, "precip_pop": None}
            )
            # Probability-of-precipitation is published per period as
            # {"value": int 0-100, "unitCode": "wmoUnit:percent"}. We keep the
            # day's max as the day-level POP signal.
            pop_obj = p.get("probabilityOfPrecipitation") or {}
            pop_val = pop_obj.get("value") if isinstance(pop_obj, dict) else None
            if pop_val is not None:
                entry["precip_pop"] = (pop_val if entry["precip_pop"] is None
                                       else max(entry["precip_pop"], pop_val))
            if t_f is None:
                continue
            if p.get("isDaytime"):
                entry["tmax_f"] = t_f if entry["tmax_f"] is None else max(entry["tmax_f"], t_f)
            else:
                entry["tmin_f"] = t_f if entry["tmin_f"] is None else min(entry["tmin_f"], t_f)

        daily = sorted(by_date.values(), key=lambda d: d["date"])
        out = {"daily": daily, "office": office}
        _cache.set(ck, out, CACHE_TTL_NWS)
        return out
    except (httpx.HTTPError, ValueError) as e:
        _log_fetch_error("nws", e)
        return {}


# ----- NWS active alerts ------------------------------------------------
# The alerts endpoint returns active watches/warnings/advisories at a point.
# Each alert has an ``event`` string ("Frost Advisory", "Flood Warning", etc.)
# whose text is the canonical match key. We classify the result into two
# coarse buckets — flood-class and freeze-class — that the existing flooding
# and frost evaluators consume to escalate their level when an official
# warning is in effect. Surfaced as a top-level data card too.

# Event-name regexes. NWS event vocabulary is documented at
# https://www.weather.gov/documentation/services-web-api#/default/get_alerts.
# Both watch/warning tiers count — any official issuance is a stronger signal
# than the modeled forecast alone.
_NWS_FLOOD_EVENTS = re.compile(
    r"\b(flood|flash flood|areal flood|river flood|coastal flood|hydrologic)\b",
    re.IGNORECASE,
)
_NWS_FREEZE_EVENTS = re.compile(
    r"\b(frost|freeze|hard freeze|wind chill|cold weather)\b",
    re.IGNORECASE,
)


def fetch_nws_alerts(lat: float, lon: float) -> dict:
    """Active NWS watches/warnings/advisories at a point.

    Returns ``{count, alerts: [...], any_flood, any_freeze}``. ``alerts`` is the
    distilled per-alert payload (event, severity, urgency, headline, expires).
    Falls back to ``{}`` on any error. Short 5-min cache to avoid hammering
    NWS on rapid refreshes while still keeping alerts responsive.

    Source: NOAA / NWS — https://www.weather.gov/documentation/services-web-api.
    """
    ck = _cache_key("alerts", round(lat, 3), round(lon, 3))
    cached = _cache.get(ck)
    if cached is not None:
        return cached
    try:
        headers = {"User-Agent": NWS_USER_AGENT, "Accept": "application/geo+json"}
        r = _http.get(
            NWS_ALERTS_URL,
            params={"point": f"{round(lat, 4)},{round(lon, 4)}", "status": "actual"},
            headers=headers, timeout=10,
        )
        if r.status_code != 200:
            _log_fetch_error("nws_alerts", f"HTTP {r.status_code} for ({lat},{lon})")
            return {}
        features = (r.json() or {}).get("features") or []
        alerts: list[dict] = []
        any_flood = False
        any_freeze = False
        for f in features:
            props = (f or {}).get("properties") or {}
            event = (props.get("event") or "").strip()
            if not event:
                continue
            is_flood = bool(_NWS_FLOOD_EVENTS.search(event))
            is_freeze = bool(_NWS_FREEZE_EVENTS.search(event))
            any_flood = any_flood or is_flood
            any_freeze = any_freeze or is_freeze
            alerts.append({
                "event": event,
                "severity": props.get("severity"),     # Minor / Moderate / Severe / Extreme
                "urgency": props.get("urgency"),       # Past / Future / Expected / Immediate
                "certainty": props.get("certainty"),   # Observed / Likely / Possible / Unlikely
                "headline": props.get("headline"),
                "area_desc": props.get("areaDesc"),
                "expires": props.get("expires") or props.get("ends"),
                "is_flood": is_flood,
                "is_freeze": is_freeze,
            })
        out = {
            "count": len(alerts),
            "alerts": alerts,
            "any_flood": any_flood,
            "any_freeze": any_freeze,
        }
        _cache.set(ck, out, CACHE_TTL_ALERTS)
        return out
    except (httpx.HTTPError, ValueError) as e:
        _log_fetch_error("nws_alerts", e)
        return {}


# ----- U.S. Drought Monitor --------------------------------------------
# USDM publishes a national drought-classification map every Thursday. The UNL
# web service exposes a point-query API that returns the percent area of each
# severity class (D0 abnormally dry → D4 exceptional drought) for a given
# point. For a single point one class will be 100; we use that as the dominant
# class. ``GetDroughtSeverityStatisticsByAreaPoint`` documented at
# https://droughtmonitor.unl.edu/DmData/DataDownload/Webservicesinfo.aspx.

_USDM_LABELS = {
    -1: "No drought",
    0:  "D0 — Abnormally Dry",
    1:  "D1 — Moderate Drought",
    2:  "D2 — Severe Drought",
    3:  "D3 — Extreme Drought",
    4:  "D4 — Exceptional Drought",
}


def fetch_usdm_drought(lat: float, lon: float) -> dict:
    """Pull the most recent U.S. Drought Monitor class at a point.

    The Esri Living Atlas USDM feed returns 0–1 polygons covering this point.
    An empty feature list means the point is in no published drought class
    (i.e. wetter than D0 abnormally dry) — the USDM only draws polygons for
    D0 and worse. When a polygon is present the ``dm`` attribute holds the
    integer class (0=D0 → 4=D4) and ``period`` is the publication date.

    Returns ``{class, label, map_date, source_url}`` on success and ``{}`` on
    any failure so the rest of the pipeline keeps running.
    """
    ck = _cache_key("usdm", round(lat, 2), round(lon, 2))
    cached = _cache.get(ck)
    if cached is not None:
        return cached

    params = {
        "where": "1=1",
        "geometry": f"{lon},{lat}",
        "geometryType": "esriGeometryPoint",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "period,dm,endyear,endmonth,endday",
        "returnGeometry": "false",
        "f": "json",
    }
    try:
        r = _http.get(USDM_POINT_URL, params=params, timeout=12)
        if r.status_code != 200:
            _log_fetch_error("usdm", f"HTTP {r.status_code} for ({lat},{lon})")
            return {}
        feats = (r.json() or {}).get("features") or []
        if not feats:
            # No polygon = no drought class at this point. Surface as a
            # "no drought" record (class -1) so the UI shows a clean "no
            # drought" tile rather than an "unavailable" placeholder.
            out = {
                "class": -1,
                "label": _USDM_LABELS[-1],
                "map_date": "",
                "source_url": "https://droughtmonitor.unl.edu/",
            }
            _cache.set(ck, out, CACHE_TTL_DROUGHT)
            return out
        # Pick the most-recent (largest period) and most-severe (largest dm)
        # polygon when multiple weeks overlap a point.
        def _sort_key(f: dict) -> tuple:
            a = f.get("attributes") or {}
            return (str(a.get("period") or ""), int(a.get("dm") or 0))
        latest = max(feats, key=_sort_key)
        attrs = latest.get("attributes") or {}
        try:
            cls = int(attrs.get("dm"))
        except (TypeError, ValueError):
            return {}
        period = str(attrs.get("period") or "")
        # period is YYYYMMDD; format to ISO for display.
        map_date = ""
        if len(period) == 8 and period.isdigit():
            map_date = f"{period[:4]}-{period[4:6]}-{period[6:8]}"
        out = {
            "class": cls,
            "label": _USDM_LABELS.get(cls, "Unknown"),
            "map_date": map_date,
            "source_url": "https://droughtmonitor.unl.edu/",
        }
        _cache.set(ck, out, CACHE_TTL_DROUGHT)
        return out
    except (httpx.HTTPError, ValueError) as e:
        _log_fetch_error("usdm", e)
        return {}


def fetch_isu_bcw_flight(year: Optional[int] = None) -> dict:
    """Scrape the latest ISU moth-trapping report for the first significant
    black-cutworm flight.

    Iowa State publishes a numbered weekly post during spring; report #1 is the
    earliest flight of the season for any IA county. We use the *earliest*
    confirmed flight as the upper-Midwest biofix — moths arrive in IA on the
    same southern storm fronts that carry them into MI a few days later, so it
    is a defensible (slightly conservative) seed for our GDD-since-flight
    accumulation.

    Source: ISU Integrated Crop Management — Hodgson, "Moth Trapping Network"
    series, https://crops.extension.iastate.edu/.
    """
    yr = year or date.today().year
    ck = _cache_key("bcw", yr)
    cached = _cache.get(ck)
    if cached is not None:
        return cached

    # Fan out the report fetches — sequential 10s timeouts compounded into a
    # 40s tail when early-season reports 404. Run them concurrently and pick
    # the lowest-numbered report that parsed successfully (later reports are
    # cumulative supersets, so #1 is preferred when present).
    report_nums = (1, 2, 3, 4)
    fetched: dict[int, str] = {}
    with ThreadPoolExecutor(max_workers=len(report_nums)) as pool:
        futures = {n: pool.submit(_fetch_isu_report, yr, n) for n in report_nums}
        for n, fut in futures.items():
            text = fut.result()
            if text:
                fetched[n] = text

    out: dict = {}
    for n in report_nums:
        if n not in fetched:
            continue
        flights = _parse_isu_bcw_flights(fetched[n], yr)
        if flights:
            out = flights
            break

    if out:
        _cache.set(ck, out, CACHE_TTL_BCW)
    elif fetched:
        # Reports came back but parsed empty — likely an HTML template change
        # at ISU. Surface it so we don't silently drift to the default biofix.
        _log_fetch_error("isu_bcw",
                         f"fetched reports {sorted(fetched)} for {yr} but parser found no flights")
    return out


def _fetch_isu_report(year: int, n: int) -> str:
    """Fetch one ISU moth-trapping report; return body text or '' on miss."""
    try:
        r = _http.get(
            ISU_BCW_REPORT_URL.format(year=year, n=n),
            timeout=10,
            headers={"User-Agent": NWS_USER_AGENT},
        )
        if r.status_code == 404:
            # Expected when a later-week report hasn't been published yet.
            return ""
        if r.status_code != 200:
            _log_fetch_error("isu_bcw", f"report #{n} HTTP {r.status_code}")
            return ""
        return r.text
    except httpx.HTTPError as e:
        _log_fetch_error("isu_bcw", f"report #{n}: {e}")
        return ""


_MONTHS = {m: i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}

# Tightened to the actual ISU table format: each report has an HTML table with
# columns "County (Crop Reporting District) | Significant Flight Date | Projected
# Cutting Date(s)". Parsing the table rows directly avoids the false positives
# a free-text scan picks up from prose ("Trapping update Between April 1...").
_TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
_CELL_RE = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.DOTALL | re.IGNORECASE)
_COUNTY_RE = re.compile(r"^([A-Z][A-Za-z\.\- ]{2,30}?)\s*(?:\([^)]*\))?\s*$")
_DATE_RE = re.compile(
    r"^\s*(January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+(\d{1,2})\s*$",
    re.IGNORECASE,
)


def _parse_isu_bcw_flights(html: str, year: int) -> dict:
    """Extract county/flight-date pairs from an ISU report page.

    Returns ``{counties: {county: iso_date}, earliest_iso, earliest_doy}``.
    Walks <tr><td>…</td>…</tr> rows because the report is published as a
    structured HTML table whose layout is far more stable than the prose
    surrounding it.
    """
    counties: dict[str, str] = {}
    for tr in _TR_RE.finditer(html):
        cells_html = _CELL_RE.findall(tr.group(1))
        if len(cells_html) < 2:
            continue
        cleaned = [re.sub(r"<[^>]+>", "", c) for c in cells_html]
        cleaned = [re.sub(r"&nbsp;|&amp;", " ", c) for c in cleaned]
        cleaned = [re.sub(r"\s+", " ", c).strip() for c in cleaned]

        county_match = _COUNTY_RE.match(cleaned[0])
        date_match = _DATE_RE.match(cleaned[1])
        if not county_match or not date_match:
            continue
        county = county_match.group(1).strip()
        # Skip header rows ("County (Crop Reporting District)").
        if county.lower().startswith("county"):
            continue
        try:
            month = _MONTHS[date_match.group(1).title()]
            day = int(date_match.group(2))
            iso = date(year, month, day).isoformat()
        except (KeyError, ValueError):
            continue
        # BCW first flights in the upper Midwest land Mar–Jun. Anything
        # outside that window is almost certainly a parsing error.
        if month not in (3, 4, 5, 6):
            continue
        counties.setdefault(county, iso)

    if not counties:
        return {}
    earliest_iso = min(counties.values())
    try:
        earliest_doy = (date.fromisoformat(earliest_iso)
                        - date(year, 1, 1)).days + 1
    except ValueError:
        return {}
    return {
        "counties": counties,
        "earliest_iso": earliest_iso,
        "earliest_doy": earliest_doy,
        "year": year,
        "source_url": ISU_BCW_REPORT_URL.format(year=year, n=1),
    }


# ----- Open-Meteo ensemble forecast ------------------------------------
# The ensemble endpoint returns one timeseries per member per model (typically
# 30-60 members across four global models). The *spread* across members on a
# given day is the single best free signal of forecast uncertainty — when GFS,
# ICON, ECMWF and GEM all converge on the same forecast (low spread) we can
# publish a sharp survival probability; when they fan out (high spread) we
# should widen the interval and demote the recommendation.

def fetch_openmeteo_ensemble(lat: float, lon: float,
                             days: int = ENSEMBLE_HORIZON_DAYS) -> dict:
    """Pull multi-model ensemble temp/precip/soil-temp and compute per-day spread.

    Returns ``{available, members, daily: [{date, tmin_mean_f, tmin_std_f,
    tmax_mean_f, tmax_std_f, precip_mean_in, precip_std_in, frost_prob,
    chilling_prob, models}]}``. ``frost_prob`` is the fraction of members whose
    daily tmin lands at or below 32°F; ``chilling_prob`` is the fraction whose
    daily mean soil-6cm proxy lands below 50°F (the corn imbibitional-chilling
    threshold).

    Source: Open-Meteo Ensemble API — https://open-meteo.com/en/docs/ensemble-api .
    Falls back to ``{}`` on any failure so the rest of the pipeline keeps running.
    """
    ck = _cache_key("ensemble", round(lat, 3), round(lon, 3), days)
    cached = _cache.get(ck)
    if cached is not None:
        return cached
    try:
        r = _http.get(
            ENSEMBLE_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "models": ENSEMBLE_MODELS,
                # We need hourly t2m + soil_t6cm + precipitation across all
                # members. Open-Meteo broadcasts each variable as one column
                # per (model, member) pair — column suffix "_member01" etc.
                "hourly": "temperature_2m,precipitation,soil_temperature_6cm",
                "temperature_unit": "fahrenheit",
                "precipitation_unit": "inch",
                "timezone": "auto",
                "forecast_days": days,
            },
            timeout=20,
        )
        if r.status_code != 200:
            _log_fetch_error("openmeteo_ensemble",
                             f"HTTP {r.status_code} for ({lat},{lon})")
            return {}
        payload = r.json() or {}
    except (httpx.HTTPError, ValueError) as e:
        _log_fetch_error("openmeteo_ensemble", e)
        return {}

    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    if not times:
        return {}

    # Group hourly arrays by variable name. Each member is a separate top-level
    # column whose key is ``temperature_2m_member01`` / ``..._member02`` / ...
    # plus the bare ``temperature_2m`` for the control. We accept any column
    # whose name starts with the variable name as a member series.
    def _series_for(var: str) -> list[list[float]]:
        return [v for k, v in hourly.items()
                if isinstance(v, list) and (k == var or k.startswith(var + "_"))]

    t2m_members = _series_for("temperature_2m")
    pr_members = _series_for("precipitation")
    soil_members = _series_for("soil_temperature_6cm")
    n_members = max(len(t2m_members), len(pr_members), len(soil_members))
    if n_members == 0:
        return {}

    # Group hour indices by ISO date.
    by_date: dict[str, list[int]] = {}
    for i, iso in enumerate(times):
        if isinstance(iso, str) and len(iso) >= 10:
            by_date.setdefault(iso[:10], []).append(i)

    def _stats(values: list[float]) -> tuple[Optional[float], Optional[float]]:
        cleaned = [v for v in values if isinstance(v, (int, float))]
        if not cleaned:
            return None, None
        m = sum(cleaned) / len(cleaned)
        if len(cleaned) < 2:
            return m, 0.0
        var = sum((v - m) ** 2 for v in cleaned) / (len(cleaned) - 1)
        return m, math.sqrt(var)

    daily_out: list[dict] = []
    for iso in sorted(by_date.keys())[:days]:
        idxs = by_date[iso]
        per_member_tmin: list[float] = []
        per_member_tmax: list[float] = []
        per_member_precip: list[float] = []
        per_member_min_soil: list[float] = []
        per_member_avg_soil: list[float] = []
        for member_idx in range(n_members):
            t_series = t2m_members[member_idx] if member_idx < len(t2m_members) else None
            p_series = pr_members[member_idx] if member_idx < len(pr_members) else None
            s_series = soil_members[member_idx] if member_idx < len(soil_members) else None
            if t_series:
                day_t = [t_series[i] for i in idxs
                         if i < len(t_series) and isinstance(t_series[i], (int, float))]
                if day_t:
                    per_member_tmin.append(min(day_t))
                    per_member_tmax.append(max(day_t))
            if p_series:
                day_p = [p_series[i] for i in idxs
                         if i < len(p_series) and isinstance(p_series[i], (int, float))]
                if day_p:
                    per_member_precip.append(sum(day_p))
            if s_series:
                day_s = [s_series[i] for i in idxs
                         if i < len(s_series) and isinstance(s_series[i], (int, float))]
                if day_s:
                    per_member_min_soil.append(min(day_s))
                    per_member_avg_soil.append(sum(day_s) / len(day_s))

        tmin_mean, tmin_std = _stats(per_member_tmin)
        tmax_mean, tmax_std = _stats(per_member_tmax)
        precip_mean, precip_std = _stats(per_member_precip)
        soil_min_mean, soil_min_std = _stats(per_member_min_soil)
        soil_avg_mean, _ = _stats(per_member_avg_soil)

        frost_prob = (
            sum(1 for v in per_member_tmin if v <= 32.0) / len(per_member_tmin)
            if per_member_tmin else None
        )
        chilling_prob = (
            sum(1 for v in per_member_avg_soil if v < 50.0) / len(per_member_avg_soil)
            if per_member_avg_soil else None
        )
        wet_prob = (
            sum(1 for v in per_member_precip if v >= 0.5) / len(per_member_precip)
            if per_member_precip else None
        )

        daily_out.append({
            "date": iso,
            "members_used": max(
                len(per_member_tmin), len(per_member_tmax),
                len(per_member_precip), len(per_member_avg_soil),
            ),
            "tmin_mean_f": round(tmin_mean, 1) if tmin_mean is not None else None,
            "tmin_std_f": round(tmin_std, 2) if tmin_std is not None else None,
            "tmax_mean_f": round(tmax_mean, 1) if tmax_mean is not None else None,
            "tmax_std_f": round(tmax_std, 2) if tmax_std is not None else None,
            "precip_mean_in": round(precip_mean, 2) if precip_mean is not None else None,
            "precip_std_in": round(precip_std, 2) if precip_std is not None else None,
            "soil_min_mean_f": round(soil_min_mean, 1) if soil_min_mean is not None else None,
            "soil_min_std_f": round(soil_min_std, 2) if soil_min_std is not None else None,
            "soil_avg_mean_f": round(soil_avg_mean, 1) if soil_avg_mean is not None else None,
            "frost_prob": round(frost_prob, 2) if frost_prob is not None else None,
            "chilling_prob": round(chilling_prob, 2) if chilling_prob is not None else None,
            "wet_prob": round(wet_prob, 2) if wet_prob is not None else None,
        })

    out = {
        "available": True,
        "members": n_members,
        "models": ENSEMBLE_MODELS.split(","),
        "daily": daily_out,
    }
    _cache.set(ck, out, CACHE_TTL_ENSEMBLE)
    return out


# ----- NASA POWER third-source recent actuals --------------------------
# POWER returns daily reanalysis-derived meteorology. We only ask for the last
# 7 days so the call is small and the relevance is high — those are the days
# that load the antecedent-saturation evaluator. Three-source agreement on
# recent precipitation is the strongest free check we can run on the modeled
# inputs that drive Pythium / Phytophthora / antecedent saturation.

def fetch_nasa_power_recent(lat: float, lon: float, days: int = 7) -> dict:
    """Pull last ``days`` of NASA POWER daily T2M_MIN/T2M_MAX/PRECTOTCORR.

    Returns ``{available, daily: [{date, tmin_f, tmax_f, precip_in}], source}``.
    Falls back to ``{}`` on any failure. POWER values often lag ~1 day so the
    most recent date may be missing — that's expected.
    """
    ck = _cache_key("power", round(lat, 3), round(lon, 3), days)
    cached = _cache.get(ck)
    if cached is not None:
        return cached
    end = date.today()
    start = end - timedelta(days=days + 1)
    params = {
        "parameters": "T2M_MIN,T2M_MAX,PRECTOTCORR",
        "community": "AG",
        "longitude": round(lon, 4),
        "latitude": round(lat, 4),
        "start": start.strftime("%Y%m%d"),
        "end": end.strftime("%Y%m%d"),
        "format": "JSON",
    }
    try:
        r = _http.get(NASA_POWER_URL, params=params, timeout=15)
        if r.status_code != 200:
            _log_fetch_error("nasa_power", f"HTTP {r.status_code} for ({lat},{lon})")
            return {}
        payload = (r.json() or {}).get("properties", {}).get("parameter") or {}
    except (httpx.HTTPError, ValueError) as e:
        _log_fetch_error("nasa_power", e)
        return {}

    tmin_c = payload.get("T2M_MIN") or {}
    tmax_c = payload.get("T2M_MAX") or {}
    precip_mm = payload.get("PRECTOTCORR") or {}
    if not tmin_c and not tmax_c and not precip_mm:
        return {}

    def _to_f(c):
        try:
            v = float(c)
        except (TypeError, ValueError):
            return None
        # POWER fill value for missing data is -999.
        if v <= -900:
            return None
        return v * 9 / 5 + 32

    def _mm_to_in(mm):
        try:
            v = float(mm)
        except (TypeError, ValueError):
            return None
        if v <= -900:
            return None
        return v / 25.4

    rows: list[dict] = []
    for ymd in sorted(set(list(tmin_c.keys()) + list(tmax_c.keys()) + list(precip_mm.keys()))):
        if not (len(ymd) == 8 and ymd.isdigit()):
            continue
        iso = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"
        rows.append({
            "date": iso,
            "tmin_f": round(_to_f(tmin_c.get(ymd)), 1) if _to_f(tmin_c.get(ymd)) is not None else None,
            "tmax_f": round(_to_f(tmax_c.get(ymd)), 1) if _to_f(tmax_c.get(ymd)) is not None else None,
            "precip_in": round(_mm_to_in(precip_mm.get(ymd)), 2) if _mm_to_in(precip_mm.get(ymd)) is not None else None,
        })
    if not rows:
        return {}
    out = {
        "available": True,
        "daily": rows,
        "source": "NASA POWER (MERRA-2 / GEOS-FP)",
    }
    _cache.set(ck, out, CACHE_TTL_POWER)
    return out


# ----- Topography / micro-elevation ponding risk -----------------------
# Open-Meteo's modeled soil moisture is a 9-km grid average; it cannot see
# whether a specific point sits in a local depression that collects runoff.
# We sample a 3x3 grid around the field at ~500 m spacing and compute:
#   * slope_m_per_km — magnitude of the planar gradient (m/km)
#   * concavity_m   — center elevation minus the mean of the 8 neighbours.
# A field point that sits >0.5 m below its surroundings on a near-flat
# (<5 m/km) field is a depression that ponds — exactly the failure mode the
# modeled soil moisture misses.

def fetch_elevation_grid(lat: float, lon: float) -> dict:
    """Sample 9 elevations around (lat, lon) and derive slope + concavity.

    Returns ``{available, center_elev_m, mean_neighbor_elev_m, concavity_m,
    slope_m_per_km, ponding_risk}``. ``ponding_risk`` is "low" | "moderate" |
    "high" — high if concavity >= 0.6m on a near-flat field, moderate if
    concavity >= 0.3m on gentle terrain or >= 0.15m on very flat ground.
    Falls back to ``{}`` on any failure.
    """
    ck = _cache_key("elevation", round(lat, 3), round(lon, 3))
    cached = _cache.get(ck)
    if cached is not None:
        return cached

    lats = [lat + dy * ELEVATION_GRID_DEG for (dy, _dx) in ELEVATION_GRID_RADII]
    lons = [lon + dx * ELEVATION_GRID_DEG for (_dy, dx) in ELEVATION_GRID_RADII]
    try:
        r = _http.get(
            ELEVATION_URL,
            params={
                "latitude": ",".join(f"{v:.6f}" for v in lats),
                "longitude": ",".join(f"{v:.6f}" for v in lons),
            },
            timeout=10,
        )
        if r.status_code != 200:
            _log_fetch_error("elevation", f"HTTP {r.status_code} for ({lat},{lon})")
            return {}
        elev_list = (r.json() or {}).get("elevation") or []
    except (httpx.HTTPError, ValueError) as e:
        _log_fetch_error("elevation", e)
        return {}

    if len(elev_list) != 9:
        _log_fetch_error("elevation",
                         f"expected 9 grid points, got {len(elev_list)} for ({lat},{lon})")
        return {}

    try:
        elev = [float(v) for v in elev_list]
    except (TypeError, ValueError) as e:
        _log_fetch_error("elevation", e)
        return {}

    # Center is index 4 in the 3x3 raster ordering.
    center_idx = 4
    center_elev = elev[center_idx]
    neighbors = [v for i, v in enumerate(elev) if i != center_idx]
    mean_neighbor = sum(neighbors) / len(neighbors)
    concavity_m = mean_neighbor - center_elev   # +ve => center is in a bowl

    # Planar gradient via centered differences. Convert grid spacing (degrees)
    # to km: 1 deg lat ≈ 111 km; 1 deg lon ≈ 111 * cos(lat) km.
    dy_km = ELEVATION_GRID_DEG * 111.0
    dx_km = ELEVATION_GRID_DEG * 111.0 * max(0.05, math.cos(math.radians(lat)))
    grad_y = (elev[7] - elev[1]) / (2 * dy_km)   # row 2 (south) - row 0 (north)
    grad_x = (elev[5] - elev[3]) / (2 * dx_km)   # col 2 (east) - col 0 (west)
    slope_m_per_km = math.sqrt(grad_x * grad_x + grad_y * grad_y)

    if concavity_m >= 0.6 and slope_m_per_km < 5.0:
        ponding = "high"
    elif concavity_m >= 0.3 and slope_m_per_km < 3.0:
        ponding = "moderate"
    elif concavity_m >= 0.15 and slope_m_per_km < 1.0:
        ponding = "moderate"
    else:
        ponding = "low"

    # Topographic Wetness Index (TWI = ln(a/tan(β)))
    # a = specific contributing area (m²/m), approximated from grid spacing
    # β = local slope in radians
    slope_rad = math.atan(slope_m_per_km / 1000.0)
    cell_size_m = ELEVATION_GRID_DEG * 111000.0
    specific_area = cell_size_m  # single-cell approximation
    if slope_rad > 0.001:
        twi = math.log(specific_area / math.tan(slope_rad))
    else:
        twi = math.log(specific_area / 0.001)  # cap for flat areas

    # Frost pocket detection: cold air pools in concave, low-slope positions
    # where radiative cooling is trapped. A high TWI + positive concavity
    # strongly indicates frost-prone microsites.
    frost_pocket = "none"
    frost_pocket_risk = 0.0
    if concavity_m >= 0.3 and slope_m_per_km < 2.0:
        frost_pocket = "high"
        frost_pocket_risk = 0.8
    elif concavity_m >= 0.15 and slope_m_per_km < 3.0:
        frost_pocket = "moderate"
        frost_pocket_risk = 0.5
    elif concavity_m >= 0.05 and slope_m_per_km < 1.5:
        frost_pocket = "low"
        frost_pocket_risk = 0.25

    out = {
        "available": True,
        "center_elev_m": round(center_elev, 1),
        "mean_neighbor_elev_m": round(mean_neighbor, 1),
        "concavity_m": round(concavity_m, 2),
        "slope_m_per_km": round(slope_m_per_km, 2),
        "ponding_risk": ponding,
        "twi": round(twi, 2),
        "frost_pocket": frost_pocket,
        "frost_pocket_risk": frost_pocket_risk,
        "grid_deg": ELEVATION_GRID_DEG,
    }
    _cache.set(ck, out, CACHE_TTL_ELEVATION)
    return out


# ----- New public data sources (2026-05) ---------------------------------
# Seven additional free public data layers that refine the risk model with
# ground-truth measurements, real-time hydrological data, crop rotation
# context, and regional planting benchmarks. Each fetcher follows the
# existing pattern: cache → fetch → parse → degrade to {} on failure.


def fetch_scan_soil_temps(lat: float, lon: float) -> dict:
    """USDA NRCS SCAN Network — measured soil temperature at depth.

    Finds the nearest SCAN station and returns its most recent daily soil
    temperature readings at 2", 4", 8", and 20" depths. These are ground-
    truth measurements that calibrate the modeled soil temps from Open-Meteo.
    No API key required.

    Source: https://www.wcc.nrcs.usda.gov/scan/
    """
    ck = _cache_key("scan", round(lat, 2), round(lon, 2))
    cached = _cache.get(ck)
    if cached is not None:
        return cached

    # SCAN stations are sparse; we use the reportGenerator CSV endpoint to
    # pull the nearest station's last 3 days of soil temp at multiple depths.
    # The URL format accepts station triplets like "network:station_id:state".
    # We first query for nearby stations, then pull data from the closest one.
    try:
        # Step 1: Find nearest SCAN station via the station metadata endpoint.
        stations_r = _http.get(
            f"{SCAN_REST_URL}/start_of_period/stationId,name,latitude,longitude,elevation/"
            f"-1,0/SOIL/None/",
            params={"fitToClipped": "false"},
            timeout=10,
        )
        # Fallback: use the SCAN WSDL-based CSV report for the nearest station.
        # The reportGenerator supports a lat/lon nearest-station lookup via URL.
        nearby_url = (
            f"https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/"
            f"customMultipleStationReport,metric/daily/start_of_period/"
            f"network=%22SCAN%22%7Cname/"
            f"-2,0/SOIL/"
        )
        # Simpler approach: query the SCAN data directly with AWDB REST.
        # The AWDB JSON endpoint returns station metadata for a bounding box.
        bbox_deg = 1.5  # ~100 miles search radius
        meta_r = _http.get(
            "https://wcc.sc.egov.usda.gov/awdbWebService/services",
            params={
                "action": "getStations",
                "minLatitude": round(lat - bbox_deg, 3),
                "maxLatitude": round(lat + bbox_deg, 3),
                "minLongitude": round(lon - bbox_deg, 3),
                "maxLongitude": round(lon + bbox_deg, 3),
                "networkCds": "SCAN",
                "returnFields": "stationTriplet,name,latitude,longitude,elevation",
            },
            timeout=10,
        )
        if meta_r.status_code != 200:
            _log_fetch_error("scan", f"station lookup HTTP {meta_r.status_code}")
            return {}

        # Parse response — could be JSON or CSV depending on the endpoint.
        try:
            stations = meta_r.json()
        except (ValueError, json.JSONDecodeError):
            _log_fetch_error("scan", "non-JSON station response")
            return {}

        if not stations:
            _log_fetch_error("scan", f"no SCAN stations within {bbox_deg}° of ({lat},{lon})")
            return {}

        # Find nearest station by Euclidean distance.
        best = None
        best_dist = float("inf")
        for s in stations:
            s_lat = float(s.get("latitude", 0))
            s_lon = float(s.get("longitude", 0))
            d = math.sqrt((lat - s_lat) ** 2 + (lon - s_lon) ** 2)
            if d < best_dist:
                best_dist = d
                best = s
        if not best:
            return {}

        triplet = best.get("stationTriplet", "")
        station_name = best.get("name", "")
        station_lat = float(best.get("latitude", lat))
        station_lon = float(best.get("longitude", lon))
        dist_km = best_dist * 111.0

        # Step 2: Pull recent soil temps from the nearest station.
        today = date.today()
        start_date = (today - timedelta(days=3)).strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")

        data_r = _http.get(
            "https://wcc.sc.egov.usda.gov/awdbWebService/services",
            params={
                "action": "getData",
                "stationTriplets": triplet,
                "elementCds": "STO",
                "ordinal": "1",
                "duration": "DAILY",
                "getFlags": "false",
                "beginDate": start_date,
                "endDate": end_date,
                "alwaysReturnDailyFeb29": "false",
            },
            timeout=12,
        )
        if data_r.status_code != 200:
            _log_fetch_error("scan", f"data HTTP {data_r.status_code} for {triplet}")
            return {}

        try:
            data = data_r.json()
        except (ValueError, json.JSONDecodeError):
            _log_fetch_error("scan", "non-JSON data response")
            return {}

        # Parse soil temp values. The response is a list of element records.
        soil_temps: dict[str, list[float | None]] = {}
        depth_labels = {"2": "2in", "4": "4in", "8": "8in", "20": "20in"}
        if isinstance(data, list):
            for elem in data:
                depth = str(elem.get("storedUnitCd", ""))
                values = elem.get("values", [])
                parsed = []
                for v in values:
                    try:
                        val = float(v) if v not in (None, "", -99999) else None
                        if val is not None:
                            val = val * 9 / 5 + 32  # C to F
                        parsed.append(val)
                    except (TypeError, ValueError):
                        parsed.append(None)
                if parsed:
                    label = depth_labels.get(depth, f"{depth}in")
                    soil_temps[label] = parsed

        # Get the most recent non-null reading at each depth.
        latest: dict[str, float | None] = {}
        for depth_key, vals in soil_temps.items():
            for v in reversed(vals):
                if v is not None:
                    latest[depth_key] = round(v, 1)
                    break

        out = {
            "available": bool(latest),
            "station": station_name,
            "station_triplet": triplet,
            "station_lat": station_lat,
            "station_lon": station_lon,
            "distance_km": round(dist_km, 1),
            "latest_temps_f": latest,
            "readings": soil_temps,
        }
        _cache.set(ck, out, CACHE_TTL_SCAN)
        return out
    except (httpx.HTTPError, ValueError, KeyError) as e:
        _log_fetch_error("scan", e)
        return {}


def fetch_iem_soil_data(lat: float, lon: float) -> dict:
    """Iowa Environmental Mesonet — actual 4-inch soil temps and dense precip.

    Queries the ISU mesonet API for the nearest soil-temp station's recent
    readings. Covers Iowa and neighboring states (MI, IN, OH, IL, WI, MN).
    No API key required.

    Source: https://mesonet.agron.iastate.edu/api/
    """
    ck = _cache_key("iem", round(lat, 2), round(lon, 2))
    cached = _cache.get(ck)
    if cached is not None:
        return cached

    try:
        today = date.today()
        start = today - timedelta(days=3)

        # Query the IEM for nearby stations with soil temperature data.
        # The /obhistory endpoint returns recent obs for a specific station.
        # First find the nearest ISUSM (Iowa Soil Moisture) network station.
        networks_r = _http.get(
            f"{IEM_API_URL}/station_list.json",
            params={"network": "ISUSM"},
            timeout=10,
        )
        if networks_r.status_code != 200:
            # Try the general daily summary endpoint for any ASOS station.
            daily_r = _http.get(
                f"{IEM_API_URL}/daily.geojson",
                params={
                    "lat": round(lat, 4),
                    "lon": round(lon, 4),
                    "sdate": start.isoformat(),
                    "edate": today.isoformat(),
                    "radius": 50,
                },
                timeout=12,
            )
            if daily_r.status_code != 200:
                _log_fetch_error("iem", f"HTTP {daily_r.status_code}")
                return {}
            # Parse generic daily data
            _cache.set(ck, {"available": False}, CACHE_TTL_IEM)
            return {}

        try:
            stations_data = networks_r.json()
        except (ValueError, json.JSONDecodeError):
            _log_fetch_error("iem", "non-JSON stations response")
            return {}

        stations = stations_data.get("data", [])
        if not stations:
            _log_fetch_error("iem", "no ISUSM stations found")
            # Degrade to unavailable rather than failing
            _cache.set(ck, {"available": False}, CACHE_TTL_IEM)
            return {"available": False}

        # Find nearest station.
        best = None
        best_dist = float("inf")
        for s in stations:
            s_lat = float(s.get("lat", 0))
            s_lon = float(s.get("lon", 0))
            d = math.sqrt((lat - s_lat) ** 2 + (lon - s_lon) ** 2)
            if d < best_dist:
                best_dist = d
                best = s

        if not best:
            return {}

        station_id = best.get("id", "")
        dist_km = best_dist * 111.0

        # Pull recent soil temp data from the station.
        obs_r = _http.get(
            f"{IEM_API_URL}/obhistory.json",
            params={
                "station": station_id,
                "network": "ISUSM",
                "date": today.isoformat(),
                "full": "true",
            },
            timeout=12,
        )
        soil_temp_4in_f: list[dict] = []
        soil_moisture: list[dict] = []
        if obs_r.status_code == 200:
            try:
                obs_data = obs_r.json()
                for obs in obs_data.get("data", []):
                    ts = obs.get("utc_valid", "")
                    # ISU soil temp fields: tsoil_c_avg_qc (4" avg)
                    t4 = obs.get("tsoil_c_avg_qc")
                    if t4 is not None:
                        try:
                            t4_f = round(float(t4) * 9 / 5 + 32, 1)
                            soil_temp_4in_f.append({"time": ts, "temp_f": t4_f})
                        except (TypeError, ValueError):
                            pass
                    # Soil moisture (VWC)
                    sm = obs.get("vwc12_qc") or obs.get("vwc_12_avg_qc")
                    if sm is not None:
                        try:
                            soil_moisture.append({"time": ts, "vwc": round(float(sm), 3)})
                        except (TypeError, ValueError):
                            pass
            except (ValueError, json.JSONDecodeError):
                pass

        latest_4in_f = soil_temp_4in_f[0]["temp_f"] if soil_temp_4in_f else None

        out = {
            "available": bool(soil_temp_4in_f or soil_moisture),
            "station": best.get("name", station_id),
            "station_id": station_id,
            "distance_km": round(dist_km, 1),
            "latest_soil_temp_4in_f": latest_4in_f,
            "soil_temp_series": soil_temp_4in_f[:24],
            "soil_moisture_series": soil_moisture[:24],
        }
        _cache.set(ck, out, CACHE_TTL_IEM)
        return out
    except (httpx.HTTPError, ValueError, KeyError) as e:
        _log_fetch_error("iem", e)
        return {}


def fetch_nass_crop_progress(state: str = "MICHIGAN", crop: str = "CORN") -> dict:
    """USDA NASS crop progress — weekly percent-planted and condition ratings.

    Requires a free API key (set NASS_API_KEY in .env). Returns the most
    recent weekly report for the state with percent planted, percent emerged,
    and 5-year average for comparison.

    Source: https://quickstats.nass.usda.gov/api/
    """
    if not NASS_API_KEY:
        return {}

    ck = _cache_key("nass_progress", state, crop, date.today().isocalendar()[1])
    cached = _cache.get(ck)
    if cached is not None:
        return cached

    year = date.today().year
    commodity = crop.upper()
    try:
        r = _http.get(
            NASS_API_URL,
            params={
                "key": NASS_API_KEY,
                "commodity_desc": commodity,
                "statisticcat_desc": "PROGRESS",
                "unit_desc": "PCT PLANTED",
                "agg_level_desc": "STATE",
                "state_name": state.upper(),
                "year": year,
                "format": "JSON",
            },
            timeout=15,
        )
        if r.status_code != 200:
            _log_fetch_error("nass", f"HTTP {r.status_code} for {state}/{commodity}")
            return {}

        resp = r.json()
        rows = resp.get("data", [])
        if not rows:
            return {"available": False, "state": state, "crop": commodity}

        # Sort by week ending date (most recent first).
        rows.sort(key=lambda x: x.get("week_ending", ""), reverse=True)
        latest = rows[0]
        pct_planted = None
        try:
            pct_planted = float(latest.get("Value", "").replace(",", ""))
        except (TypeError, ValueError):
            pass

        # Also fetch 5-year average if available.
        avg_rows = [r for r in rows if "AVG" in (r.get("reference_period_desc") or "").upper()]
        avg_pct = None
        if avg_rows:
            try:
                avg_pct = float(avg_rows[0].get("Value", "").replace(",", ""))
            except (TypeError, ValueError):
                pass

        # Fetch condition data separately.
        condition = {}
        try:
            cond_r = _http.get(
                NASS_API_URL,
                params={
                    "key": NASS_API_KEY,
                    "commodity_desc": commodity,
                    "statisticcat_desc": "CONDITION",
                    "agg_level_desc": "STATE",
                    "state_name": state.upper(),
                    "year": year,
                    "format": "JSON",
                },
                timeout=10,
            )
            if cond_r.status_code == 200:
                cond_rows = cond_r.json().get("data", [])
                cond_rows.sort(key=lambda x: x.get("week_ending", ""), reverse=True)
                for cr in cond_rows:
                    cat = (cr.get("unit_desc") or "").lower()
                    try:
                        condition[cat] = float(cr.get("Value", "").replace(",", ""))
                    except (TypeError, ValueError):
                        pass
        except httpx.HTTPError:
            pass

        out = {
            "available": pct_planted is not None,
            "state": state,
            "crop": commodity,
            "week_ending": latest.get("week_ending"),
            "pct_planted": pct_planted,
            "avg_pct_planted": avg_pct,
            "ahead_behind": round(pct_planted - avg_pct, 1) if pct_planted is not None and avg_pct is not None else None,
            "condition": condition if condition else None,
        }
        _cache.set(ck, out, CACHE_TTL_NASS)
        return out
    except (httpx.HTTPError, ValueError, KeyError) as e:
        _log_fetch_error("nass", e)
        return {}


def fetch_usgs_streamflow(lat: float, lon: float) -> dict:
    """USGS Water Services — real-time streamflow and flood stage nearby.

    Searches for the nearest USGS gage within ~25 km of the field point and
    returns the most recent gage height, discharge, and flood-stage status.
    No API key required.

    Source: https://waterservices.usgs.gov/
    """
    ck = _cache_key("usgs", round(lat, 2), round(lon, 2))
    cached = _cache.get(ck)
    if cached is not None:
        return cached

    try:
        # Query USGS for gages near this point. The bBox parameter defines a
        # bounding box; we use ~0.25° (~25 km) on each side.
        bbox_deg = 0.25
        r = _http.get(
            USGS_WATER_URL,
            params={
                "format": "json",
                "bBox": f"{lon - bbox_deg},{lat - bbox_deg},{lon + bbox_deg},{lat + bbox_deg}",
                "parameterCd": "00065,00060",  # gage height + discharge
                "siteStatus": "active",
                "period": "PT2H",
            },
            timeout=15,
        )
        if r.status_code != 200:
            _log_fetch_error("usgs", f"HTTP {r.status_code}")
            return {}

        data = r.json()
        ts_list = data.get("value", {}).get("timeSeries", [])
        if not ts_list:
            return {"available": False}

        # Find the nearest gage from the returned sites.
        sites: dict[str, dict] = {}
        for ts in ts_list:
            si = ts.get("sourceInfo", {})
            site_code = si.get("siteCode", [{}])[0].get("value", "")
            if site_code not in sites:
                geo = si.get("geoLocation", {}).get("geogLocation", {})
                s_lat = float(geo.get("latitude", 0))
                s_lon = float(geo.get("longitude", 0))
                sites[site_code] = {
                    "name": si.get("siteName", ""),
                    "lat": s_lat,
                    "lon": s_lon,
                    "dist_km": math.sqrt((lat - s_lat) ** 2 + (lon - s_lon) ** 2) * 111.0,
                    "gage_height_ft": None,
                    "discharge_cfs": None,
                }
            # Extract the most recent value for this parameter.
            var_code = ts.get("variable", {}).get("variableCode", [{}])[0].get("value", "")
            values = ts.get("values", [{}])[0].get("value", [])
            if values:
                latest_val = values[-1].get("value")
                try:
                    v = float(latest_val)
                    if var_code == "00065":
                        sites[site_code]["gage_height_ft"] = round(v, 2)
                    elif var_code == "00060":
                        sites[site_code]["discharge_cfs"] = round(v, 1)
                except (TypeError, ValueError):
                    pass

        if not sites:
            return {"available": False}

        # Pick the closest gage.
        nearest = min(sites.values(), key=lambda s: s["dist_km"])

        # Flood stage determination — USGS doesn't include stage thresholds in
        # the instantaneous values response, but we can flag abnormal discharge
        # heuristically. "Action stage" approximations vary by watershed; here
        # we flag if discharge or gage height is notably high for a small stream.
        flood_risk = "low"
        gh = nearest.get("gage_height_ft")
        discharge = nearest.get("discharge_cfs")
        if gh is not None and gh > 12:
            flood_risk = "high"
        elif gh is not None and gh > 8:
            flood_risk = "moderate"
        elif discharge is not None and discharge > 5000:
            flood_risk = "high"
        elif discharge is not None and discharge > 2000:
            flood_risk = "moderate"

        out = {
            "available": True,
            "site_name": nearest["name"],
            "distance_km": round(nearest["dist_km"], 1),
            "gage_height_ft": gh,
            "discharge_cfs": discharge,
            "flood_risk": flood_risk,
            "sites_found": len(sites),
        }
        _cache.set(ck, out, CACHE_TTL_USGS)
        return out
    except (httpx.HTTPError, ValueError, KeyError) as e:
        _log_fetch_error("usgs", e)
        return {}


def fetch_cpc_soil_moisture(lat: float, lon: float) -> dict:
    """NOAA CPC soil moisture anomaly — daily percentile for this region.

    The CPC produces daily gridded soil moisture at ~0.5° resolution. We
    query for the nearest grid cell's soil moisture percentile (0-100) which
    gives a finer-grained saturation signal than the weekly USDM categories.
    Falls back to {} on failure.

    Source: https://www.cpc.ncep.noaa.gov/products/Soilmst_Monitoring/
    """
    ck = _cache_key("cpc_sm", round(lat, 1), round(lon, 1))
    cached = _cache.get(ck)
    if cached is not None:
        return cached

    try:
        # CPC provides gridded data. We use the anomaly data endpoint.
        # The daily w2 anomaly files are at:
        # https://ftp.cpc.ncep.noaa.gov/GIS/USDM_Products/soil/total/
        # Alternative: use the weekly summary CSV from CPC monitoring page.
        today = date.today()
        yesterday = today - timedelta(days=1)

        # Try the CPC daily soil moisture ASCII grid. These are updated daily
        # and cover CONUS at 0.5° resolution.
        sm_url = (
            f"https://ftp.cpc.ncep.noaa.gov/GIS/USDM_Products/soil/total/"
            f"w2{yesterday.strftime('%Y%m%d')}.tif"
        )
        # GeoTIFF is hard to parse without GDAL. Instead, query the CPC WMS
        # or use the NDMC soil moisture percentile API if available.
        # Fallback: use the NLDAS soil moisture from NASA GES DISC.
        sm_r = _http.get(
            "https://nassgeodata.gmu.edu/axis2/services/CDLService/GetCDLValue",
            params={
                "year": today.year,
                "x": round(lon, 4),
                "y": round(lat, 4),
            },
            timeout=10,
        )
        # Use a simpler approach: query the NOAA CPC soil moisture summary
        # page and extract the regional percentile from the text.
        # For a more reliable approach, use the gridded NLDAS data from NASA.
        nldas_url = "https://ldas.gsfc.nasa.gov/nldas/v2/NLDAS2_daily"

        # Practical approach: derive soil moisture percentile from the point's
        # recent precipitation relative to climatological norms via the USDM
        # data we already have, plus CPC's published regional stats.
        # Query CPC's monitoring page for state-level soil moisture ranking.
        sm_page_r = _http.get(
            "https://www.cpc.ncep.noaa.gov/products/Soilmst_Monitoring/US/Soilmst/Soilmst.shtml",
            timeout=10,
        )
        if sm_page_r.status_code != 200:
            _log_fetch_error("cpc_sm", f"HTTP {sm_page_r.status_code}")
            return {}

        # Parse the page to find regional percentile values.
        # The CPC page contains an image map; for a structured approach we
        # use the CPC's GrADS data server with lat/lon subsetting.
        grads_r = _http.get(
            "https://www.cpc.ncep.noaa.gov/products/Soilmst_Monitoring/US/"
            "Soilmst/w.ftpout.curr.sm.asc",
            timeout=10,
        )
        if grads_r.status_code == 200:
            text = grads_r.text
            lines = [l.strip() for l in text.split("\n") if l.strip()]
            # ASCII grid: header lines then data. Parse grid dimensions.
            ncols = nrows = 0
            xll = yll = cellsize = nodata = 0.0
            header_lines = 0
            for i, line in enumerate(lines):
                parts = line.split()
                if len(parts) == 2:
                    key = parts[0].lower()
                    try:
                        val = float(parts[1])
                        if key == "ncols":
                            ncols = int(val)
                        elif key == "nrows":
                            nrows = int(val)
                        elif key in ("xllcorner", "xllcenter"):
                            xll = val
                        elif key in ("yllcorner", "yllcenter"):
                            yll = val
                        elif key == "cellsize":
                            cellsize = val
                        elif key == "nodata_value":
                            nodata = val
                        header_lines = i + 1
                    except ValueError:
                        break
                else:
                    break

            if cellsize > 0 and ncols > 0 and nrows > 0:
                col = int((lon - xll) / cellsize)
                row = int((yll + nrows * cellsize - lat) / cellsize)
                col = max(0, min(col, ncols - 1))
                row = max(0, min(row, nrows - 1))
                data_lines = lines[header_lines:]
                if row < len(data_lines):
                    row_vals = data_lines[row].split()
                    if col < len(row_vals):
                        try:
                            sm_val = float(row_vals[col])
                            if sm_val != nodata:
                                out = {
                                    "available": True,
                                    "soil_moisture_pctl": round(sm_val, 1),
                                    "category": (
                                        "very wet" if sm_val >= 80 else
                                        "wet" if sm_val >= 60 else
                                        "normal" if sm_val >= 40 else
                                        "dry" if sm_val >= 20 else
                                        "very dry"
                                    ),
                                    "date": yesterday.isoformat(),
                                    "grid_resolution_deg": cellsize,
                                }
                                _cache.set(ck, out, CACHE_TTL_CPC)
                                return out
                        except ValueError:
                            pass

        # If the ASCII grid approach didn't work, return unavailable.
        return {"available": False}
    except (httpx.HTTPError, ValueError, KeyError) as e:
        _log_fetch_error("cpc_sm", e)
        return {}


def fetch_msu_enviroweather(lat: float, lon: float) -> dict:
    """MSU Enviroweather — Michigan-specific GDD and pest emergence models.

    Queries the Enviroweather system for accumulated GDD at multiple bases
    (39°F for SCM, 50°F for BCW, etc.) from the nearest Michigan station.
    Also pulls the pest model status if available. Free, no key required.

    Source: https://enviroweather.msu.edu/
    """
    ck = _cache_key("enviro", round(lat, 2), round(lon, 2))
    cached = _cache.get(ck)
    if cached is not None:
        return cached

    try:
        today = date.today()
        # Enviroweather provides CSV exports for GDD data. The station finder
        # returns the nearest station based on lat/lon.
        stations_r = _http.get(
            f"{MSU_ENVIRO_URL}/weathermodels/getStations",
            params={"lat": round(lat, 4), "lng": round(lon, 4)},
            timeout=10,
        )
        if stations_r.status_code != 200:
            # Try the alternate endpoint format.
            stations_r = _http.get(
                f"{MSU_ENVIRO_URL}/weather/stations.json",
                timeout=10,
            )
            if stations_r.status_code != 200:
                _log_fetch_error("enviro", f"HTTP {stations_r.status_code}")
                return {}

        try:
            stations = stations_r.json()
        except (ValueError, json.JSONDecodeError):
            _log_fetch_error("enviro", "non-JSON stations response")
            return {}

        # Parse station list and find nearest.
        station_list = stations if isinstance(stations, list) else stations.get("stations", [])
        if not station_list:
            return {"available": False}

        best = None
        best_dist = float("inf")
        for s in station_list:
            s_lat = float(s.get("latitude", s.get("lat", 0)))
            s_lon = float(s.get("longitude", s.get("lng", 0)))
            d = math.sqrt((lat - s_lat) ** 2 + (lon - s_lon) ** 2)
            if d < best_dist:
                best_dist = d
                best = s

        if not best:
            return {"available": False}

        station_id = best.get("id", best.get("stationId", ""))
        dist_km = best_dist * 111.0

        # Pull GDD data from the nearest station.
        gdd_data = {}
        for base_temp, label in [(39, "base39"), (50, "base50")]:
            try:
                gdd_r = _http.get(
                    f"{MSU_ENVIRO_URL}/weathermodels/growingdegreedays",
                    params={
                        "station": station_id,
                        "base": base_temp,
                        "start": f"{today.year}-03-01",
                        "end": today.isoformat(),
                    },
                    timeout=10,
                )
                if gdd_r.status_code == 200:
                    try:
                        gdd_resp = gdd_r.json()
                        cum_gdd = gdd_resp.get("cumulative") or gdd_resp.get("total")
                        if cum_gdd is not None:
                            gdd_data[label] = round(float(cum_gdd), 1)
                    except (ValueError, json.JSONDecodeError):
                        # Try parsing as CSV.
                        lines = gdd_r.text.strip().split("\n")
                        if len(lines) > 1:
                            last_line = lines[-1].split(",")
                            if len(last_line) >= 2:
                                try:
                                    gdd_data[label] = round(float(last_line[-1]), 1)
                                except ValueError:
                                    pass
            except httpx.HTTPError:
                pass

        out = {
            "available": bool(gdd_data),
            "station": best.get("name", str(station_id)),
            "station_id": station_id,
            "distance_km": round(dist_km, 1),
            "gdd": gdd_data,
            "date": today.isoformat(),
        }
        _cache.set(ck, out, CACHE_TTL_ENVIRO)
        return out
    except (httpx.HTTPError, ValueError, KeyError) as e:
        _log_fetch_error("enviro", e)
        return {}


# CDL crop codes — row crops, non-row agriculture, and non-agricultural land.
CDL_CROP_NAMES = {
    1: "corn", 2: "cotton", 3: "rice", 4: "sorghum", 5: "soybeans",
    6: "sunflower", 10: "peanuts", 11: "tobacco", 12: "sweet_corn",
    13: "popcorn", 21: "barley", 22: "durum_wheat", 23: "spring_wheat",
    24: "winter_wheat", 25: "other_small_grains", 26: "winter_wheat_soybeans",
    27: "rye", 28: "oats", 29: "millet", 30: "speltz", 31: "canola",
    32: "flaxseed", 33: "safflower", 34: "rape_seed", 35: "mustard",
    36: "alfalfa", 37: "other_hay", 38: "camelina", 39: "buckwheat",
    41: "sugarbeets", 42: "dry_beans", 43: "potatoes", 44: "other_crops",
    45: "sugarcane", 46: "sweet_potatoes", 47: "misc_vegs_fruits",
    48: "watermelons", 49: "onions", 50: "cucumbers", 51: "chickpeas",
    52: "lentils", 53: "peas", 54: "tomatoes", 55: "caneberries",
    56: "hops", 57: "herbs", 58: "clover_wildflowers", 59: "sod_grass_seed",
    60: "switchgrass", 61: "fallow", 62: "pasture_grass", 63: "forest",
    64: "shrubland", 65: "barren", 66: "cherries", 67: "peaches",
    68: "apples", 69: "grapes", 70: "christmas_trees", 71: "other_tree_crops",
    72: "citrus", 74: "pecans", 75: "almonds", 76: "walnuts",
    77: "pears", 111: "open_water", 112: "perennial_ice_snow",
    121: "developed_open_space", 122: "developed_low_intensity",
    123: "developed_med_intensity", 124: "developed_high_intensity",
    131: "barren", 141: "deciduous_forest", 142: "evergreen_forest",
    143: "mixed_forest", 152: "shrubland", 171: "grassland_herbaceous",
    176: "grassland_pasture", 190: "woody_wetlands", 195: "herbaceous_wetlands",
}

# Codes that represent commercial row-crop agriculture for corn/soybeans.
CDL_COMMERCIAL_CORN_SOY = {1, 5, 12, 13, 26}

# Codes that represent ANY agricultural activity (row crops + other ag).
CDL_ANY_AGRICULTURE = {
    1, 2, 3, 4, 5, 6, 10, 11, 12, 13, 21, 22, 23, 24, 25, 26, 27, 28, 29,
    30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 41, 42, 43, 44, 45, 46, 47, 48,
    49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 66, 67, 68, 69, 70,
    71, 72, 74, 75, 76, 77,
}

# Non-agricultural land types.
CDL_NON_AGRICULTURAL = {
    63, 64, 65, 111, 112, 121, 122, 123, 124, 131, 141, 142, 143,
    152, 171, 176, 190, 195,
}


def assess_commercial_farming_region(rotation: dict, crop: str) -> dict:
    """Determine if this location is within a commercial farming region for the
    selected crop based on CropScape CDL history.

    Returns a dict with:
      - in_commercial_region: bool
      - has_any_agriculture: bool
      - disclaimer: str or None
      - land_use_history: list of recent crop names
    """
    if not rotation or not rotation.get("available"):
        return {
            "in_commercial_region": None,
            "has_any_agriculture": None,
            "disclaimer": None,
            "land_use_history": [],
        }

    history = rotation.get("years", [])
    crop_codes = [h.get("crop_code") for h in history if h.get("crop_code")]

    target_codes = CDL_COMMERCIAL_CORN_SOY if crop in ("corn", "soybeans") else CDL_ANY_AGRICULTURE
    has_target_crop = any(c in target_codes for c in crop_codes)
    has_any_ag = any(c in CDL_ANY_AGRICULTURE for c in crop_codes)
    all_non_ag = all(c in CDL_NON_AGRICULTURAL for c in crop_codes) if crop_codes else False

    land_names = [CDL_CROP_NAMES.get(c, f"code_{c}") for c in crop_codes]

    disclaimer = None
    if all_non_ag:
        disclaimer = (
            f"This location has been classified as non-agricultural land "
            f"({', '.join(land_names)}) for the past {len(history)} years. "
            f"Commercial {crop} farming is not typically conducted in this area. "
            "While it may be technically possible to grow crops here with irrigation "
            "and soil amendment, yields and conditions will differ significantly from "
            "established agricultural regions."
        )
    elif not has_target_crop and has_any_ag:
        disclaimer = (
            f"This location has recent agricultural history ({', '.join(land_names)}) "
            f"but no recent commercial {crop} production. While it is technically "
            f"possible to grow {crop} here, this area is not part of the typical "
            f"commercial {crop} belt. Conditions and infrastructure may differ from "
            "established production regions."
        )
    elif not has_target_crop and not has_any_ag:
        disclaimer = (
            f"No recent agricultural activity detected at this location "
            f"(land use: {', '.join(land_names) or 'unknown'}). "
            f"Commercial farming is not typically conducted here. While it may be "
            f"technically possible to grow {crop}, this is well outside the normal "
            "production region."
        )

    return {
        "in_commercial_region": has_target_crop,
        "has_any_agriculture": has_any_ag,
        "disclaimer": disclaimer,
        "land_use_history": land_names,
    }


def fetch_cropscape_history(lat: float, lon: float, years_back: int = 3) -> dict:
    """NASS CropScape — crop rotation history at this exact point.

    Returns the last N years of CDL crop classifications at (lat, lon),
    enabling rotation-aware risk adjustments (corn-on-corn escalates
    wireworm and seedcorn maggot; soy-on-soy escalates Phytophthora).
    No API key required.

    Source: https://nassgeodata.gmu.edu/CropScape/
    """
    ck = _cache_key("cropscape", round(lat, 4), round(lon, 4), years_back)
    cached = _cache.get(ck)
    if cached is not None:
        return cached

    current_year = date.today().year
    history: list[dict] = []

    try:
        for yr in range(current_year - 1, current_year - 1 - years_back, -1):
            try:
                r = _http.get(
                    f"{CROPSCAPE_URL}",
                    params={
                        "a": "GetCDLValue",
                        "year": yr,
                        "x": round(lon, 6),
                        "y": round(lat, 6),
                    },
                    timeout=10,
                )
                if r.status_code == 200:
                    text = r.text
                    # Response format: {"cropcode": "1", "category": "Corn", ...}
                    # or plain text like "Result: {"value":"1","category":"Corn",...}"
                    crop_code = None
                    crop_name = None
                    try:
                        parsed = r.json()
                        crop_code = int(parsed.get("value", parsed.get("cropcode", 0)))
                        crop_name = parsed.get("category", "")
                    except (ValueError, json.JSONDecodeError):
                        # Try regex extraction from text response.
                        m_code = re.search(r'"(?:value|cropcode)"\s*:\s*"?(\d+)"?', text)
                        m_name = re.search(r'"category"\s*:\s*"([^"]+)"', text)
                        if m_code:
                            crop_code = int(m_code.group(1))
                        if m_name:
                            crop_name = m_name.group(1)

                    if crop_code:
                        history.append({
                            "year": yr,
                            "crop_code": crop_code,
                            "crop_name": crop_name or CDL_CROP_NAMES.get(crop_code, f"code_{crop_code}"),
                        })
            except httpx.HTTPError:
                pass

        # Derive rotation flags.
        corn_years = sum(1 for h in history if h.get("crop_code") == 1)
        soy_years = sum(1 for h in history if h.get("crop_code") == 5)
        consecutive_corn = 0
        consecutive_soy = 0
        for h in history:
            if h.get("crop_code") == 1:
                consecutive_corn += 1
            else:
                break
        for h in history:
            if h.get("crop_code") == 5:
                consecutive_soy += 1
            else:
                break

        out = {
            "available": bool(history),
            "years": history,
            "corn_on_corn": consecutive_corn >= 2,
            "soy_on_soy": consecutive_soy >= 2,
            "prev_crop_code": history[0]["crop_code"] if history else None,
            "prev_crop_name": history[0]["crop_name"] if history else None,
            "rotation_diversity": len(set(h["crop_code"] for h in history)),
        }
        _cache.set(ck, out, CACHE_TTL_CROPSCAPE)
        return out
    except (httpx.HTTPError, ValueError, KeyError) as e:
        _log_fetch_error("cropscape", e)
        return {}


# ----- NDVI / EVI field health from Sentinel-2 --------------------------
# Element84 Earth Search STAC API: free, no auth needed. Searches the
# Sentinel-2 L2A (atmospherically corrected) archive on AWS. We compute
# NDVI = (B08 - B04) / (B08 + B04) from the COG overviews using HTTP range
# requests — no GDAL/rasterio dependency.
# https://earth-search.aws.element84.com/v1

STAC_SEARCH_URL = "https://earth-search.aws.element84.com/v1/search"
CACHE_TTL_NDVI = 6 * 3600  # 6 h — Sentinel revisit is 5 days

# Sentinel-2 L2A band central wavelengths (nm) for reference:
# B02=490 (Blue), B03=560 (Green), B04=665 (Red), B08=842 (NIR broad 10m),
# B8A=865 (NIR narrow 20m), B11=1610 (SWIR1), B12=2190 (SWIR2)

_NDVI_BBOX_DEG = 0.002  # ~200m bbox around the point for pixel sampling


def _stac_search_sentinel2(lat: float, lon: float, days_back: int = 90,
                           max_cloud: int = 25, limit: int = 12) -> list[dict]:
    """Query Element84 Earth Search for recent cloud-free Sentinel-2 scenes."""
    end = date.today()
    start = end - timedelta(days=days_back)
    bbox = [
        round(lon - _NDVI_BBOX_DEG, 5),
        round(lat - _NDVI_BBOX_DEG, 5),
        round(lon + _NDVI_BBOX_DEG, 5),
        round(lat + _NDVI_BBOX_DEG, 5),
    ]
    body = {
        "collections": ["sentinel-2-l2a"],
        "bbox": bbox,
        "datetime": f"{start.isoformat()}T00:00:00Z/{end.isoformat()}T23:59:59Z",
        "query": {"eo:cloud_cover": {"lt": max_cloud}},
        "sortby": [{"field": "properties.datetime", "direction": "desc"}],
        "limit": limit,
    }
    try:
        r = _http.post(STAC_SEARCH_URL, json=body, timeout=15)
        if r.status_code != 200:
            return []
        return r.json().get("features", [])
    except (httpx.HTTPError, ValueError) as e:
        _log_fetch_error("stac_sentinel2", e)
        return []


def _latlon_to_utm(lat: float, lon: float, zone: int) -> tuple[float, float]:
    """Convert WGS84 lat/lon to UTM easting/northing (Northern hemisphere)."""
    a = 6378137.0
    f = 1 / 298.257223563
    e2 = 2 * f - f * f
    e_prime2 = e2 / (1 - e2)
    k0 = 0.9996
    lon0 = (zone - 1) * 6 - 180 + 3

    lat_r = math.radians(lat)
    lon_r = math.radians(lon)
    lon0_r = math.radians(lon0)

    N = a / math.sqrt(1 - e2 * math.sin(lat_r) ** 2)
    T = math.tan(lat_r) ** 2
    C = e_prime2 * math.cos(lat_r) ** 2
    A = math.cos(lat_r) * (lon_r - lon0_r)

    M = a * (
        (1 - e2 / 4 - 3 * e2 ** 2 / 64 - 5 * e2 ** 3 / 256) * lat_r
        - (3 * e2 / 8 + 3 * e2 ** 2 / 32 + 45 * e2 ** 3 / 1024) * math.sin(2 * lat_r)
        + (15 * e2 ** 2 / 256 + 45 * e2 ** 3 / 1024) * math.sin(4 * lat_r)
        - (35 * e2 ** 3 / 3072) * math.sin(6 * lat_r)
    )

    easting = (
        k0 * N * (
            A + (1 - T + C) * A ** 3 / 6
            + (5 - 18 * T + T ** 2 + 72 * C - 58 * e_prime2) * A ** 5 / 120
        ) + 500000
    )
    northing = k0 * (
        M + N * math.tan(lat_r) * (
            A ** 2 / 2
            + (5 - T + 9 * C + 4 * C ** 2) * A ** 4 / 24
            + (61 - 58 * T + T ** 2 + 600 * C - 330 * e_prime2) * A ** 6 / 720
        )
    )
    return easting, northing


def _undo_horizontal_differencing_u16(data: bytes, width: int, height: int) -> bytes:
    """Undo TIFF Predictor=2 (horizontal differencing) for uint16 tile data."""
    import array as _arr
    pixels = _arr.array("H", data)
    for row in range(height):
        off = row * width
        for col in range(1, width):
            pixels[off + col] = (pixels[off + col] + pixels[off + col - 1]) & 0xFFFF
    return pixels.tobytes()


def _read_cog_pixel_value(url: str, lat: float, lon: float,
                          transform: list, epsg: int,
                          full_size: int = 10980) -> int | None:
    """Read a single pixel value from a Sentinel-2 COG at the given lat/lon.

    Uses the smallest overview (~160m/px) for speed. Parses the TIFF IFD chain,
    locates the correct tile, decompresses it (DEFLATE + horizontal differencing
    predictor), and returns the raw uint16 DN. Returns None on any error.
    """
    import struct as _s
    import zlib

    try:
        utm_zone = (epsg - 32600) if epsg > 32600 else 16
        easting, northing = _latlon_to_utm(lat, lon, utm_zone)

        origin_x = transform[2]
        origin_y = transform[5]
        scale_x = transform[0]
        scale_y = transform[4]

        px_full = (easting - origin_x) / scale_x
        py_full = (northing - origin_y) / scale_y

        if px_full < 0 or px_full >= full_size or py_full < 0 or py_full >= full_size:
            return None

        r = _http.get(url, headers={"Range": "bytes=0-32767"}, timeout=12)
        if r.status_code not in (200, 206) or len(r.content) < 8:
            return None
        data = r.content

        endian = "<" if data[:2] == b"II" else ">"
        if data[:2] not in (b"II", b"MM"):
            return None

        ifd_off = _s.unpack_from(f"{endian}I", data, 4)[0]
        ifds = [ifd_off]
        current = ifd_off
        for _ in range(8):
            if current >= len(data) - 2:
                break
            n = _s.unpack_from(f"{endian}H", data, current)[0]
            nxt_ptr = current + 2 + n * 12
            if nxt_ptr + 4 > len(data):
                break
            nxt = _s.unpack_from(f"{endian}I", data, nxt_ptr)[0]
            if nxt == 0:
                break
            ifds.append(nxt)
            current = nxt

        target_ifd = ifds[-1] if len(ifds) > 1 else ifds[0]
        if target_ifd >= len(data) - 2:
            return None

        n_tags = _s.unpack_from(f"{endian}H", data, target_ifd)[0]
        tags: dict[int, int] = {}
        multi: dict[int, tuple[int, int, int]] = {}
        for i in range(n_tags):
            toff = target_ifd + 2 + i * 12
            if toff + 12 > len(data):
                break
            tid = _s.unpack_from(f"{endian}H", data, toff)[0]
            ttype = _s.unpack_from(f"{endian}H", data, toff + 2)[0]
            tcount = _s.unpack_from(f"{endian}I", data, toff + 4)[0]
            if tcount == 1:
                if ttype == 3:
                    tags[tid] = _s.unpack_from(f"{endian}H", data, toff + 8)[0]
                elif ttype == 4:
                    tags[tid] = _s.unpack_from(f"{endian}I", data, toff + 8)[0]
            else:
                ptr = _s.unpack_from(f"{endian}I", data, toff + 8)[0]
                multi[tid] = (ptr, tcount, ttype)

        ov_w = tags.get(256, 0)
        ov_h = tags.get(257, 0)
        tw = tags.get(322, ov_w)
        tl = tags.get(323, ov_h)
        if not ov_w or not ov_h:
            return None

        # Read tile offset/bytecount arrays
        if 324 not in multi or 325 not in multi:
            return None
        off_ptr, off_cnt, _ = multi[324]
        bc_ptr, bc_cnt, _ = multi[325]
        if off_ptr + off_cnt * 4 > len(data) or bc_ptr + bc_cnt * 4 > len(data):
            return None
        tile_offsets = [_s.unpack_from(f"{endian}I", data, off_ptr + i * 4)[0] for i in range(off_cnt)]
        tile_bcs = [_s.unpack_from(f"{endian}I", data, bc_ptr + i * 4)[0] for i in range(bc_cnt)]

        ov_x = int(px_full * ov_w / full_size)
        ov_y = int(py_full * ov_h / full_size)
        if ov_x < 0 or ov_x >= ov_w or ov_y < 0 or ov_y >= ov_h:
            return None

        tiles_across = (ov_w + tw - 1) // tw
        tile_idx = (ov_y // tl) * tiles_across + (ov_x // tw)
        if tile_idx >= len(tile_offsets):
            return None

        t_off = tile_offsets[tile_idx]
        t_size = tile_bcs[tile_idx]
        r2 = _http.get(url, headers={"Range": f"bytes={t_off}-{t_off + t_size - 1}"}, timeout=12)
        if r2.status_code not in (200, 206):
            return None

        try:
            raw = zlib.decompress(r2.content)
        except zlib.error:
            try:
                raw = zlib.decompress(r2.content, -15)
            except zlib.error:
                return None

        has_predictor = tags.get(317, 1) == 2
        if has_predictor:
            raw = _undo_horizontal_differencing_u16(raw, tw, tl)

        local_x = ov_x % tw
        local_y = ov_y % tl
        pix_off = (local_y * tw + local_x) * 2
        if pix_off + 2 > len(raw):
            return None
        return _s.unpack_from(f"{endian}H", raw, pix_off)[0]

    except Exception:
        return None


def _extract_scene_ndvi(item: dict, lat: float, lon: float) -> dict | None:
    """Extract NDVI from a single STAC item's B04 and B08 band COGs."""
    assets = item.get("assets", {})
    props = item.get("properties", {})

    b04_asset = assets.get("red") or assets.get("B04") or assets.get("b04")
    b08_asset = assets.get("nir") or assets.get("B08") or assets.get("b08")
    if not b04_asset or not b08_asset:
        return None

    b04_url = b04_asset.get("href", "")
    b08_url = b08_asset.get("href", "")
    if not b04_url or not b08_url:
        return None

    epsg = props.get("proj:epsg", 32616)
    transform = b04_asset.get("proj:transform") or [10, 0, 399960, 0, -10, 4500000]
    full_shape = b04_asset.get("proj:shape", [10980, 10980])
    full_size = full_shape[0]

    red = _read_cog_pixel_value(b04_url, lat, lon, transform, epsg, full_size)
    nir = _read_cog_pixel_value(b08_url, lat, lon, transform, epsg, full_size)

    if red is None or nir is None:
        return None
    if red == 0 and nir == 0:
        return None

    # S2 L2A: surface reflectance = DN * 0.0001 - 0.1 (nodata = 0)
    scale = 0.0001
    offset = -0.1
    raster_info = (b04_asset.get("raster:bands") or [{}])[0]
    scale = raster_info.get("scale", scale)
    offset = raster_info.get("offset", offset)

    red_r = red * scale + offset
    nir_r = nir * scale + offset

    if red_r < -0.05 or nir_r < -0.05 or nir_r > 1.0 or red_r > 1.0:
        return None
    if red_r < 0.005 and nir_r > 0.05:
        return None
    red_r = max(red_r, 0.001)
    nir_r = max(nir_r, 0.0)

    denom = nir_r + red_r
    ndvi = (nir_r - red_r) / denom if denom > 0 else 0.0
    evi_denom = nir_r + 6 * red_r + 1.0
    evi = 2.5 * (nir_r - red_r) / evi_denom if evi_denom > 0 else 0.0

    if ndvi > 0.95 or ndvi < -0.5:
        return None

    dt_str = props.get("datetime", "")
    cloud = props.get("eo:cloud_cover")

    return {
        "date": dt_str[:10] if dt_str else None,
        "ndvi": round(ndvi, 4),
        "evi": round(max(-1.0, min(1.0, evi)), 4),
        "cloud_cover_pct": round(cloud, 1) if cloud is not None else None,
        "red_dn": int(red),
        "nir_dn": int(nir),
    }


def fetch_ndvi_timeseries(lat: float, lon: float, days_back: int = 90,
                          crop: str = "corn") -> dict:
    """Fetch NDVI/EVI time series from Sentinel-2 L2A imagery.

    Uses the Element84 Earth Search STAC catalog (free, no auth) to find recent
    cloud-free Sentinel-2 scenes, then reads B04/B08 pixel values from COG
    overviews on AWS using HTTP range requests. Returns a time series of NDVI/EVI
    values plus health classification.

    Source: Copernicus Sentinel-2 via AWS Open Data (element84 Earth Search)
    """
    ck = _cache_key("ndvi", round(lat, 4), round(lon, 4), days_back)
    cached = _cache.get(ck)
    if cached is not None:
        return cached

    items = _stac_search_sentinel2(lat, lon, days_back=days_back, max_cloud=25, limit=12)
    if not items:
        return {"available": False, "reason": "no_scenes"}

    readings: list[dict] = []
    for item in items:
        reading = _extract_scene_ndvi(item, lat, lon)
        if reading and reading["ndvi"] is not None:
            readings.append(reading)

    if not readings:
        return {
            "available": False,
            "reason": "pixel_read_failed",
            "scenes_found": len(items),
        }

    readings.sort(key=lambda r: r["date"] or "")

    # Classify crop health from most recent NDVI
    latest = readings[-1]
    ndvi = latest["ndvi"]

    # NDVI thresholds for crop health (growing season, after emergence)
    # <0.2 = bare soil/dead, 0.2-0.4 = stressed/early, 0.4-0.6 = moderate,
    # 0.6-0.8 = healthy, >0.8 = peak vigor
    if ndvi < 0.15:
        health = "bare_soil"
        health_label = "Bare soil / no canopy"
        health_level = "info"
    elif ndvi < 0.3:
        health = "stressed"
        health_label = "Sparse / stressed vegetation"
        health_level = "high"
    elif ndvi < 0.5:
        health = "moderate"
        health_label = "Moderate canopy development"
        health_level = "moderate"
    elif ndvi < 0.7:
        health = "healthy"
        health_label = "Healthy crop canopy"
        health_level = "low"
    else:
        health = "vigorous"
        health_label = "Peak vegetative vigor"
        health_level = "low"

    # Compute trend from the two most recent readings
    trend = None
    trend_label = None
    if len(readings) >= 2:
        delta = readings[-1]["ndvi"] - readings[-2]["ndvi"]
        if delta > 0.05:
            trend = "greening"
            trend_label = "Canopy greening up"
        elif delta < -0.05:
            trend = "browning"
            trend_label = "Canopy declining"
        else:
            trend = "stable"
            trend_label = "Stable canopy"

    # Season context
    month = date.today().month
    if crop == "corn":
        if month < 5:
            season_note = "Pre-planting — low NDVI expected (bare soil)"
        elif month < 7:
            season_note = "Vegetative growth — NDVI rising toward canopy closure"
        elif month < 9:
            season_note = "Reproductive — peak NDVI expected"
        else:
            season_note = "Senescence — NDVI declining is normal"
    else:
        if month < 5:
            season_note = "Pre-planting — low NDVI expected"
        elif month < 7:
            season_note = "Vegetative growth — NDVI building"
        elif month < 10:
            season_note = "Seed fill / maturity — peak NDVI expected"
        else:
            season_note = "Post-harvest — low NDVI expected"

    out = {
        "available": True,
        "readings": readings,
        "latest_ndvi": latest["ndvi"],
        "latest_evi": latest["evi"],
        "latest_date": latest["date"],
        "health": health,
        "health_label": health_label,
        "health_level": health_level,
        "trend": trend,
        "trend_label": trend_label,
        "season_note": season_note,
        "scenes_searched": len(items),
        "scenes_usable": len(readings),
        "resolution_m": 10,
        "source": "Copernicus Sentinel-2 L2A (AWS Open Data)",
        "revisit_days": 5,
    }
    _cache.set(ck, out, CACHE_TTL_NDVI)
    return out


# ----- Three-source recent-history agreement ---------------------------

def compare_recent_three_source(open_meteo_history: dict, power_recent: dict,
                                nws_summary: dict | None = None) -> dict:
    """Cross-check Open-Meteo Archive vs NASA POWER daily lows/highs/precip.

    Open-Meteo Archive and NASA POWER are independent reanalyses for the same
    historical days. Large disagreement on precipitation in particular means
    the antecedent-saturation evaluator is operating on uncertain inputs.
    Returns ``{available, agreement, mean_dev_tmin_f, mean_dev_tmax_f,
    mean_dev_precip_in, days, sources}``.
    """
    om_daily = (open_meteo_history or {}).get("daily") or {}
    om_times = om_daily.get("time") or []
    om_tmin = om_daily.get("temperature_2m_min") or []
    om_tmax = om_daily.get("temperature_2m_max") or []
    om_precip = om_daily.get("precipitation_sum") or []
    pw_rows = (power_recent or {}).get("daily") or []
    if not om_times or not pw_rows:
        return {"available": False}

    pw_by_date = {r["date"]: r for r in pw_rows}
    devs_tmin: list[float] = []
    devs_tmax: list[float] = []
    devs_precip: list[float] = []
    rows: list[dict] = []
    for i, iso in enumerate(om_times):
        if iso not in pw_by_date:
            continue
        pw_row = pw_by_date[iso]
        om_lo = om_tmin[i] if i < len(om_tmin) else None
        om_hi = om_tmax[i] if i < len(om_tmax) else None
        om_pr = om_precip[i] if i < len(om_precip) else None
        pw_lo = pw_row.get("tmin_f")
        pw_hi = pw_row.get("tmax_f")
        pw_pr = pw_row.get("precip_in")
        d_lo = (om_lo - pw_lo) if (om_lo is not None and pw_lo is not None) else None
        d_hi = (om_hi - pw_hi) if (om_hi is not None and pw_hi is not None) else None
        d_pr = (om_pr - pw_pr) if (om_pr is not None and pw_pr is not None) else None
        if d_lo is not None: devs_tmin.append(abs(d_lo))
        if d_hi is not None: devs_tmax.append(abs(d_hi))
        if d_pr is not None: devs_precip.append(abs(d_pr))
        rows.append({
            "date": iso,
            "open_meteo_tmin_f": om_lo, "open_meteo_tmax_f": om_hi,
            "open_meteo_precip_in": om_pr,
            "power_tmin_f": pw_lo, "power_tmax_f": pw_hi,
            "power_precip_in": pw_pr,
            "tmin_dev_f": round(d_lo, 1) if d_lo is not None else None,
            "tmax_dev_f": round(d_hi, 1) if d_hi is not None else None,
            "precip_dev_in": round(d_pr, 2) if d_pr is not None else None,
        })
    if not rows:
        return {"available": False}

    mean_temp_dev = (
        (sum(devs_tmin) + sum(devs_tmax)) / max(1, len(devs_tmin) + len(devs_tmax))
        if (devs_tmin or devs_tmax) else None
    )
    mean_precip_dev = (sum(devs_precip) / len(devs_precip)) if devs_precip else None

    if mean_temp_dev is None:
        agreement = "unknown"
    elif mean_temp_dev <= 2.5 and (mean_precip_dev or 0) <= 0.15:
        agreement = "strong"
    elif mean_temp_dev <= 5.0 and (mean_precip_dev or 0) <= 0.35:
        agreement = "fair"
    else:
        agreement = "weak"

    return {
        "available": True,
        "agreement": agreement,
        "mean_dev_tmin_f": round(sum(devs_tmin) / len(devs_tmin), 1) if devs_tmin else None,
        "mean_dev_tmax_f": round(sum(devs_tmax) / len(devs_tmax), 1) if devs_tmax else None,
        "mean_dev_precip_in": round(mean_precip_dev, 2) if mean_precip_dev is not None else None,
        "days": rows,
        "sources": ["Open-Meteo Archive", "NASA POWER"],
    }


# ----- Forecast confidence aggregator ----------------------------------
# Pull together every uncertainty signal we have and produce ONE confidence
# label (high / moderate / low) and a numeric scalar in [0,1] used to widen
# the survival-probability interval downstream. This is the "honest meter"
# the user sees on the recommendation card.

def compute_forecast_confidence(cross_check: dict, three_source: dict,
                                ensemble: dict) -> dict:
    """Aggregate forecast-uncertainty signals into a single confidence label.

    Inputs:
      cross_check   — Open-Meteo vs NWS (forward window).
      three_source — Open-Meteo Archive vs NASA POWER (backward window).
      ensemble      — multi-model spread (forward window, days 1–14).

    Output:
      {label, scalar, drivers, ensemble_spread_f, agreement_forward,
       agreement_recent, weak_signals: [...]}.
    The scalar runs from 0 (unusable) to 1 (high confidence). It feeds into
    the survival-probability interval — a confidence of 0.6 on a published
    point estimate of 80% means the published interval becomes 80% ± 8%.
    """
    weak_signals: list[str] = []
    drivers: list[str] = []

    # Forward agreement (Open-Meteo vs NWS, next 5 days).
    fwd_agreement = (cross_check or {}).get("agreement") if (cross_check or {}).get("available") else None
    if fwd_agreement == "strong":
        fwd_score = 1.0; drivers.append("Open-Meteo and NWS agree on the next 5 days.")
    elif fwd_agreement == "fair":
        fwd_score = 0.7; drivers.append("Open-Meteo and NWS broadly agree, with some daily drift.")
    elif fwd_agreement == "weak":
        fwd_score = 0.35
        drivers.append("Open-Meteo and NWS disagree on the next 5 days.")
        weak_signals.append("forecast_cross_check")
    else:
        fwd_score = 0.6  # unknown — neutral

    # Backward agreement (Open-Meteo Archive vs NASA POWER, last 7 days).
    rec_agreement = (three_source or {}).get("agreement") if (three_source or {}).get("available") else None
    if rec_agreement == "strong":
        rec_score = 1.0; drivers.append("Open-Meteo and NASA POWER agree on recent history.")
    elif rec_agreement == "fair":
        rec_score = 0.75
        drivers.append("Open-Meteo Archive and NASA POWER show modest dispersion on the last 7 days.")
    elif rec_agreement == "weak":
        rec_score = 0.4
        drivers.append("Open-Meteo Archive and NASA POWER disagree on recent rainfall — antecedent saturation is uncertain.")
        weak_signals.append("recent_history_cross_check")
    else:
        rec_score = 0.65

    # Ensemble spread (mean tmin std across the next 7 days).
    spread_score = 0.7
    avg_spread_f = None
    avg_precip_spread = None
    if (ensemble or {}).get("available"):
        days = (ensemble.get("daily") or [])[:7]
        spreads = [d.get("tmin_std_f") for d in days if d.get("tmin_std_f") is not None]
        precip_spreads = [d.get("precip_std_in") for d in days if d.get("precip_std_in") is not None]
        if spreads:
            avg_spread_f = sum(spreads) / len(spreads)
            if avg_spread_f <= 1.5:
                spread_score = 1.0
                drivers.append(f"Ensemble members agree (avg σ={avg_spread_f:.1f}°F).")
            elif avg_spread_f <= 3.5:
                spread_score = 0.75
                drivers.append(f"Ensemble dispersion moderate (avg σ={avg_spread_f:.1f}°F).")
            else:
                spread_score = 0.4
                drivers.append(f"Ensemble dispersion wide (avg σ={avg_spread_f:.1f}°F) — forecast unsettled.")
                weak_signals.append("ensemble_spread")
        if precip_spreads:
            avg_precip_spread = sum(precip_spreads) / len(precip_spreads)

    # Geometric mean weighting — any single weak signal pulls the overall
    # confidence down sharply, which matches the agronomic reality that one
    # bad input poisons the whole prediction.
    scalar = (fwd_score * rec_score * spread_score) ** (1 / 3)
    scalar = max(0.25, min(1.0, scalar))

    if scalar >= 0.85:
        label = "high"
    elif scalar >= 0.6:
        label = "moderate"
    else:
        label = "low"

    return {
        "label": label,
        "scalar": round(scalar, 2),
        "agreement_forward": fwd_agreement,
        "agreement_recent": rec_agreement,
        "ensemble_spread_f": round(avg_spread_f, 2) if avg_spread_f is not None else None,
        "ensemble_precip_spread_in": round(avg_precip_spread, 2) if avg_precip_spread is not None else None,
        "weak_signals": weak_signals,
        "drivers": drivers,
    }


# ----- helpers ----------------------------------------------------------

def _hourly_window(forecast: dict, key: str, start_h: int, end_h: int) -> list[float]:
    series = forecast["hourly"].get(key, [])
    return [v for v in series[start_h:end_h] if v is not None]


def _avg(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _saturated_hours(moisture: list[float], threshold: float = 0.40) -> int:
    """Hours where volumetric soil moisture exceeds saturation threshold."""
    return sum(1 for v in moisture if v >= threshold)


def build_base50_gdd_lookup(season_archive: dict, extended: dict) -> dict[str, float]:
    """Cumulative GDD base 50°F from Jan 1, keyed by ISO date.

    Reuses the season-to-date archive + extended-forecast payloads already
    fetched for the SCM model so we don't add a network call.
    """
    today = date.today()
    gdd: dict[date, float] = {}
    arc_daily = _ingest_daily_gdd((season_archive or {}).get("daily", {}), 50.0)
    gdd.update(arc_daily)
    ext_daily = _ingest_daily_gdd((extended or {}).get("daily", {}), 50.0)
    for d, g in ext_daily.items():
        gdd.setdefault(d, g)
    if not gdd:
        return {}

    year_start = date(today.year, 1, 1)
    last_known = max(gdd.keys())
    horizon = today + timedelta(days=EXTENDED_HORIZON_DAYS + 2)
    end = max(last_known, horizon)

    out: dict[str, float] = {}
    cum = 0.0
    cur = year_start
    while cur <= end:
        cum += gdd.get(cur, 0.0)
        out[cur.isoformat()] = round(cum, 1)
        cur += timedelta(days=1)
    return out


def recommend_planting_depth(forecast: dict, profile: dict, inputs: UserInputs,
                             start: int = 0) -> dict:
    """Per-field seeding-depth recommendation from current soil conditions.

    Source: Purdue (Nielsen) "How Deep Should Corn Be Planted?" and "Soil
    Moisture & Corn Seed Depth"; ISU "Corn Planting Depth" and "Soybean
    Planting Depth Considerations for Iowa". Both extension services
    converge on:
        * Corn:    1.5-2.0" — go deeper (up to 2.5") only when surface is
                   dry and moisture sits at 2-3" depth.
        * Soybean: 1.0-1.5" — favour the shallow end into cool, wet, fine-
                   textured or crust-prone fields.
    """
    soil_temps = _hourly_window(forecast, "soil_temperature_6cm", start, start + 96)
    moist_top = _hourly_window(forecast, "soil_moisture_1_to_3cm", start, start + 96)
    moist_mid = _hourly_window(forecast, "soil_moisture_3_to_9cm", start, start + 96)
    precip_24 = _hourly_window(forecast, "precipitation", start, start + 24)
    precip_168 = _hourly_window(forecast, "precipitation", start, start + 168)

    avg_soil = _avg(soil_temps) if soil_temps else None
    avg_top = _avg(moist_top) if moist_top else None
    avg_mid = _avg(moist_mid) if moist_mid else None
    rain_24 = sum(precip_24) if precip_24 else 0.0
    rain_7d = sum(precip_168) if precip_168 else 0.0

    depth_min = profile.get("depth_min_in", 1.5)
    depth_max = profile.get("depth_max_in", 2.0)
    target = (depth_min + depth_max) / 2

    notes: list[str] = []

    # Cool-wet topsoil → favour the shallow end (Purdue, ISU).
    if avg_soil is not None and avg_soil < profile.get("preferred_soil_temp_f", 55):
        target = depth_min
        notes.append(
            f"soil averages {avg_soil:.0f}°F — shallow end speeds emergence"
        )

    # Crust-prone forecast → don't bury the seed (esp. soybeans).
    if rain_24 > 0.4 and inputs.tillage == "conventional" and inputs.residue == "low":
        target = min(target, depth_min)
        notes.append("rain-then-bake crusting risk — keep depth shallow")

    # Dry surface but moisture deeper → chase moisture (Purdue Nielsen).
    if (avg_top is not None and avg_top < 0.18
            and avg_mid is not None and avg_mid > 0.22
            and rain_7d < 0.3):
        target = depth_max + (0.5 if profile["label"] == "Corn" else 0.25)
        target = min(target, depth_max + 0.5)
        notes.append("topsoil dry, moisture sits deeper — chase the moisture")

    # Saturated subsoil → too wet, defer; flag rather than recommend.
    deferred = False
    if avg_mid is not None and avg_mid > 0.42 and avg_soil is not None and avg_soil < 55:
        deferred = True
        notes.append("subsoil at saturation and cold — defer planting until it dries/warms")

    target = max(depth_min, min(depth_max + 0.5, round(target * 4) / 4))

    return {
        "recommended_in": target if not deferred else None,
        "min_in": depth_min,
        "max_in": depth_max,
        "deferred": deferred,
        "notes": notes,
        "drivers": {
            "avg_soil_temp_f": round(avg_soil, 1) if avg_soil is not None else None,
            "avg_topsoil_moisture": round(avg_top, 3) if avg_top is not None else None,
            "avg_subsoil_moisture": round(avg_mid, 3) if avg_mid is not None else None,
            "rain_24h_in": round(rain_24, 2),
            "rain_7d_in": round(rain_7d, 2),
        },
    }


# ----- risk evaluators --------------------------------------------------
# Each evaluator takes (forecast, profile, inputs, start_hour) and returns a
# Risk evaluated against the planting window beginning `start_hour` hours from
# the forecast origin. start_hour=0 means "if planted today".

def _imbibitional_chilling(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Chilling injury during imbibition and early germination.

    Source: Purdue (Nielsen) "Cold Soils & Risk of Imbibitional Chilling Injury
    in Corn"; UNL CropWatch "Cold Soil Temperature and Corn Planting Windows";
    ISU Extension (corn needs 18-21 days to emerge at 50°F); ISU ICM
    "Imbibitional Chilling or Cold Injury"; SDSU "Chilling Injury";
    Neththasinghe et al. (2026) Agrosystems, Geosciences & Environment.

    Two-phase window (crop-specific):
      Phase 1: critical imbibition window — seed is actively absorbing water
        and most vulnerable to membrane rupture. Corn: 0-48h (Purdue/Nielsen).
        Soybeans: 0-24h — soybeans imbibe water faster than corn, compressing
        the critical window but intensifying vulnerability (ISU ICM, SDSU).
      Phase 2 (post-imbibition to 120h): extended germination window — seed
        has imbibed but is still underground and vulnerable. ISU documents
        18-21 days to emerge at 50°F. The seed remains at risk for the full
        pre-emergence period, not just imbibition.
    """
    # Fall-planted crops (winter wheat): soil is expected to be cooling. The
    # imbibitional chilling model is designed for spring-planted crops where cold
    # soil is the hazard. For winter wheat, seeds germinate down to 40°F (MSU)
    # and the real risks are handled by crown_rot, winterkill, and take-all
    # evaluators. Instead of the standard chilling model, check for the inverted
    # concern: soil TOO WARM (>65°F, excess fall growth, disease) or TOO COLD
    # (<40°F, no germination).
    if profile.get("fall_planted"):
        soil = _hourly_window(forecast, "soil_temperature_6cm", start, start + 120)
        if not soil:
            return Risk(key="chilling", name="Establishment Temp", level="low",
                        headline="No soil-temperature data.", detail="", metric="—")
        avg_soil = sum(soil) / len(soil)
        max_soil_limit = profile.get("max_soil_temp_f", 65)
        min_soil_limit = profile.get("min_soil_temp_f", 40)
        sev_warm = _sigmoid_severity(avg_soil, midpoint=max_soil_limit, scale=4.0)
        sev_cold = _sigmoid_severity(avg_soil, midpoint=min_soil_limit + 3, scale=4.0, inverted=True)
        severity = max(sev_warm, sev_cold)
        level = _level_from_severity(severity)
        if sev_warm > sev_cold:
            headlines = {
                "high": f"Soil avg {avg_soil:.0f}°F — too warm (>{max_soil_limit}°F). Excess fall growth, disease, Hessian fly risk.",
                "moderate": f"Soil avg {avg_soil:.0f}°F — near upper limit. Monitor for excess vegetative growth.",
                "low": f"Soil avg {avg_soil:.0f}°F — within fall seeding range ({min_soil_limit}-{max_soil_limit}°F).",
            }
        else:
            headlines = {
                "high": f"Soil avg {avg_soil:.0f}°F — below {min_soil_limit}°F, slow/no germination expected.",
                "moderate": f"Soil avg {avg_soil:.0f}°F — near lower limit for establishment.",
                "low": f"Soil avg {avg_soil:.0f}°F — within fall seeding range ({min_soil_limit}-{max_soil_limit}°F).",
            }
        return Risk(
            key="chilling", name="Establishment Temp", level=level,
            headline=headlines[level],
            detail=f"Winter wheat germinates at 40-65°F (optimum 54-60°F). Soil above {max_soil_limit}°F "
                   "promotes excess fall growth, disease buildup (Fusarium crown rot, Septoria), and "
                   f"Hessian fly exposure (OSU, KSU, MSU). Soil below {min_soil_limit}°F provides "
                   "insufficient GDD for pre-dormancy tiller development.",
            metric=f"{avg_soil:.0f}°F avg soil",
            severity=severity, curve_type="sigmoid",
        )

    soil = _hourly_window(forecast, "soil_temperature_6cm", start, start + 120)
    if not soil:
        return Risk(key="chilling", name="Imbibitional Chilling", level="low",
                    headline="No soil-temperature data for this window.",
                    detail="", metric="—")
    # Phase 1: crop-specific imbibition window — the primary severity driver.
    # Soybeans imbibe water faster (6-24h, ISU/SDSU) than corn (24-48h,
    # Purdue/Nielsen), so the critical window is shorter but more intense.
    imbib_hours = 24 if profile.get("cold_imbibitional_sensitive") else 48
    soil_imbib = soil[:imbib_hours]
    min_soil_f = min(soil_imbib)
    threshold = profile["min_soil_temp_f"]
    preferred = profile.get("preferred_soil_temp_f", threshold + 5)

    # Ground-truth calibration from SCAN or IEM measured soil temps. If a
    # nearby station reports an actual soil temp that diverges >3°F from the
    # modeled forecast value, nudge our working minimum toward reality.
    scan = forecast.get("_scan") or {}
    iem = forecast.get("_iem") or {}
    ground_truth_f: float | None = None
    ground_truth_src = ""
    if scan.get("available"):
        st = scan.get("latest_temps_f") or {}
        ground_truth_f = st.get("2in") or st.get("4in")
        if ground_truth_f is not None:
            ground_truth_src = f"SCAN {scan.get('station', '')} ({scan.get('distance_km', '?')} km)"
    if ground_truth_f is None and iem.get("available"):
        ground_truth_f = iem.get("latest_soil_temp_4in_f")
        if ground_truth_f is not None:
            ground_truth_src = f"IEM {iem.get('station', '')} ({iem.get('distance_km', '?')} km)"

    calibration_note = ""
    if ground_truth_f is not None and start <= 24:
        deviation = ground_truth_f - min_soil_f
        if abs(deviation) > 3:
            min_soil_f = min_soil_f + deviation * 0.3
            calibration_note = f" Calibrated toward {ground_truth_f:.0f}°F ({ground_truth_src})."

    # Trend across the imbibition window (crop-specific length):
    # difference between the second-half average and the first-half average
    # (Nielsen, Purdue — "warming forecast" rule).
    half = imbib_hours // 2
    first_half = soil_imbib[:half]
    second_half = soil_imbib[half:imbib_hours]
    if first_half and second_half:
        trend_f = sum(second_half) / len(second_half) - sum(first_half) / len(first_half)
    else:
        trend_f = 0.0

    if trend_f >= 2.0:
        trend_phrase = f", but warming +{trend_f:.1f}°F across {imbib_hours}h"
    elif trend_f <= -2.0:
        trend_phrase = f", and cooling {trend_f:.1f}°F across {imbib_hours}h"
    else:
        trend_phrase = ""

    severity = _sigmoid_severity(min_soil_f, midpoint=threshold - 2, scale=4.0, inverted=True)
    if trend_f >= 2.0:
        severity = max(0.0, severity - min(0.4, 0.05 * trend_f))
    elif trend_f <= -2.0:
        severity = min(1.0, severity + min(0.3, 0.05 * abs(trend_f)))

    # Phase 2: extended germination window (imbib_hours-120h). A cold relapse
    # during this period slows emergence and extends pathogen exposure. ISU
    # documents 18-21 days to emerge at 50°F; seeds remain underground and
    # vulnerable well past the imbibition window.
    soil_extended = soil[imbib_hours:120]
    extended_note = ""
    if soil_extended:
        min_extended_f = min(soil_extended)
        avg_extended_f = sum(soil_extended) / len(soil_extended)
        if min_extended_f < threshold:
            ext_sev = _sigmoid_severity(min_extended_f, midpoint=threshold - 2, scale=4.0, inverted=True)
            ext_sev *= 0.5
            if ext_sev > severity:
                severity = severity * 0.6 + ext_sev * 0.4
            elif ext_sev > 0.20:
                severity = min(1.0, severity + ext_sev * 0.25)
            if min_extended_f < threshold - 3:
                extended_note = f" Soil relapses to {min_extended_f:.0f}°F at day 3-5."

    level = _level_from_severity(severity)

    headlines = {
        "high": f"Soil drops to {min_soil_f:.0f}°F{trend_phrase} — below {profile['label']}'s {threshold}°F floor.{extended_note}{calibration_note}",
        "moderate": f"Soil dips to {min_soil_f:.0f}°F{trend_phrase}, near the {threshold}°F floor for {profile['label']}.{extended_note}{calibration_note}",
        "low": f"Soil holds at {min_soil_f:.0f}°F{trend_phrase} — at or above the {threshold}°F floor.{calibration_note}",
    }
    return Risk(
        key="chilling",
        name="Imbibitional Chilling",
        level=level,
        headline=headlines[level],
        detail=("Seeds absorbing water below ~50°F can rupture cell membranes and die before "
                "germination. Purdue (Nielsen) and UNL extension recommend planting only when "
                "the 24-48h forecast shows steady or rising soil temperatures at planting depth — "
                "a brief dip is recoverable if the trend is warming. Cold relapses at days 3-5 "
                "slow emergence and extend pathogen exposure (ISU Extension)."),
        metric=f"{min_soil_f:.1f}°F · trend {trend_f:+.1f}°F",
        severity=severity,
        curve_type="sigmoid",
    )


def _flooding(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    precip = _hourly_window(forecast, "precipitation", start, start + 48)
    precip_48h_in = sum(precip)
    headlines = {
        "high": f"{precip_48h_in:.2f}\" expected in 48h — saturation likely.",
        "moderate": f"{precip_48h_in:.2f}\" expected in 48h — watch drainage.",
        "low": f"Only {precip_48h_in:.2f}\" forecast over 48h.",
    }

    severity = _sigmoid_severity(precip_48h_in, midpoint=1.5, scale=0.6)

    # USGS streamflow override — gage data captures basin response the
    # precipitation total alone cannot model.
    usgs = forecast.get("_usgs") or {}
    headline_usgs = ""
    if usgs.get("available") and usgs.get("flood_risk") in ("high", "moderate"):
        if usgs["flood_risk"] == "high":
            severity = max(severity, 0.8)
            headline_usgs = f" USGS gage ({usgs.get('site_name', 'nearby')}) at {usgs.get('gage_height_ft', '?')} ft — flood stage."
        else:
            severity = max(severity, 0.5)
            headline_usgs = f" USGS gage ({usgs.get('site_name', 'nearby')}) elevated at {usgs.get('gage_height_ft', '?')} ft."

    # CPC soil moisture amplifier.
    cpc = forecast.get("_cpc_moisture") or {}
    headline_cpc = ""
    if cpc.get("available") and cpc.get("soil_moisture_pctl") is not None:
        sm_pctl = cpc["soil_moisture_pctl"]
        if sm_pctl >= 80:
            severity = min(1.0, severity + 0.1)
            if precip_48h_in > 0.5:
                headline_cpc = f" CPC soil moisture at {sm_pctl:.0f}th percentile — already saturated."

    # NWS active-alerts override.
    alerts = forecast.get("_alerts") or {}
    headline_alert = ""
    if alerts.get("any_flood") and start <= 72:
        flood_event = next(
            (a for a in alerts.get("alerts", []) if a.get("is_flood")),
            None,
        )
        if flood_event:
            severity = max(severity, 0.85)
            headline_alert = f" Active NWS alert: {flood_event['event']}."

    # Tile drainage: tiled fields drain saturated water in 24–48h vs 4–7+ days
    # untiled. Reduce severity by ~60% (multiply by 0.4) per DRAINMOD kinetics.
    tile_phrase = ""
    if inputs.field_tiled:
        severity *= 0.4
        tile_phrase = " Tile drainage active — subsurface water removal reduces saturation window."

    level = _level_from_severity(severity)

    return Risk(
        key="flooding",
        name="Flooding & Waterlogging",
        level=level,
        headline=headlines[level] + headline_usgs + headline_cpc + headline_alert + tile_phrase,
        detail=("Saturated soils starve seeds of oxygen (anoxia). More than ~2 inches in 48 hours, "
                "or several days submerged, will rot the seed. Active NWS flood watches/warnings "
                "and USGS streamflow data are folded in directly when available."),
        metric=f'{precip_48h_in:.2f}"',
        severity=severity,
        curve_type="sigmoid",
    )


def _frost(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Frost risk through the emergence window (~10 days after planting).

    Composes three signals:
      1. Deterministic forecast minimum across the 168h emergence window
         (Open-Meteo single-run, what the prior version used).
      2. Ensemble Pr[T_min ≤ frost_threshold] — fraction of multi-model
         members whose daily tmin lands at or below the crop's threshold.
         A 30% ensemble probability over 7 days is meaningful even if the
         deterministic min stays above the threshold by a couple of degrees.
      3. Climatology Pr[T_min ≤ 32°F] from prior-year archives over the same
         calendar window. Catches cases where the deterministic + ensemble
         both miss a known cold-window pattern (advection freeze, late
         radiational nights).
    """
    air = _hourly_window(forecast, "temperature_2m", start, start + 168)  # 7 days post-planting
    # Growth-stage-aware frost threshold for winter wheat (MU IPM, OSU ANR-93).
    # At planting time in fall, wheat is at seedling/tillering stage; for spring
    # evaluations the growth stage shifts. Use the frost_by_feekes lookup if
    # available and a growth stage is estimable, else fall back to base value.
    frost_by_feekes = profile.get("frost_by_feekes")
    growth_stage = (forecast.get("_growth_stage") or {}).get("feekes_stage")
    if frost_by_feekes and growth_stage:
        stage_map = {
            "dormant": "dormant", "tillering": "tillering",
            "jointing": "jointing", "boot": "boot", "flag_leaf": "boot",
            "heading": "heading", "anthesis": "flowering", "flowering": "flowering",
            "milk": "milk", "dough": "dough", "maturity": "dough",
        }
        mapped = stage_map.get(growth_stage, "dormant")
        threshold = frost_by_feekes.get(mapped, profile["frost_air_temp_f"])
    else:
        threshold = profile["frost_air_temp_f"]
    if not air:
        return Risk(key="frost", name="Frost & Freeze", level="low",
                    headline="No temperature data.", detail="", metric="—")
    min_air = min(air)
    hours_below = sum(1 for t in air if t <= threshold)

    # Ensemble integrated probability across the planting+emergence window.
    # We pick the ensemble days that fall inside the planting window (start_h
    # mapped back to a date) and aggregate their per-day frost_prob. The
    # *integrated* probability of at least one frost event across N days,
    # treating days as independent, is 1 - prod(1 - p_i).
    ensemble = forecast.get("_ensemble") or {}
    plant_date = _planting_date_for_start(forecast, start)
    end_date = plant_date + timedelta(days=7)
    ens_probs: list[float] = []
    for d in (ensemble.get("daily") or []):
        try:
            iso = d.get("date")
            cur = date.fromisoformat(iso) if iso else None
        except (TypeError, ValueError):
            cur = None
        if cur is None or not (plant_date <= cur < end_date):
            continue
        # The ensemble's frost_prob is keyed to 32°F. For corn (28°F) we want
        # a hard-freeze probability; the published 32°F frequency is a strict
        # upper bound, which is what we want for an alert. For soybean
        # (32°F threshold) it lines up exactly.
        p = d.get("frost_prob")
        if isinstance(p, (int, float)):
            ens_probs.append(float(p))
    integrated_ens_prob = None
    if ens_probs:
        prod = 1.0
        for p in ens_probs:
            prod *= max(0.0, 1.0 - p)
        integrated_ens_prob = round(1.0 - prod, 2)

    # Climatology. derive_climatology stores per-day frost_prob (Pr[tmin<=32])
    # using the last 5 prior years of the same calendar dates.
    climo_lookup = forecast.get("_climatology_by_date") or {}
    climo_probs: list[float] = []
    for offset in range(7):
        iso = (plant_date + timedelta(days=offset)).isoformat()
        row = climo_lookup.get(iso) or {}
        p = row.get("frost_prob")
        if isinstance(p, (int, float)):
            climo_probs.append(float(p))
    integrated_climo_prob = None
    if climo_probs:
        prod = 1.0
        for p in climo_probs:
            prod *= max(0.0, 1.0 - p)
        integrated_climo_prob = round(1.0 - prod, 2)

    prob_phrase = ""
    if integrated_ens_prob is not None:
        prob_phrase += f" Ensemble Pr[freeze in 7d]={int(integrated_ens_prob*100)}%."
    if integrated_climo_prob is not None:
        prob_phrase += f" 5-yr climatology Pr[freeze]={int(integrated_climo_prob*100)}%."

    headlines = {
        "high": f"Hard freeze in the emergence window — air drops to {min_air:.0f}°F.{prob_phrase}",
        "moderate": f"Borderline cold night ({min_air:.0f}°F) within emergence window.{prob_phrase}",
        "low": f"Lows hold at {min_air:.0f}°F — above {profile['label']}'s {threshold}°F floor.{prob_phrase}",
    }

    # Sigmoid severity from the deterministic forecast minimum.
    severity = _sigmoid_severity(min_air, midpoint=threshold, scale=3.0, inverted=True)
    hours_sev = _sigmoid_severity(hours_below, midpoint=3.0, scale=2.0)
    severity = max(severity, hours_sev)

    # Ensemble probability escalator — dampened when forecast min is well
    # above the crop threshold (ensemble reports P[T<32°F] but corn dies at
    # 28°F, so a 32°F event is survivable and shouldn't dominate severity).
    frost_gap = max(0.0, min_air - threshold)
    gap_dampener = max(0.0, 1.0 - frost_gap / 15.0)
    if integrated_ens_prob is not None:
        ens_sev = _sigmoid_severity(integrated_ens_prob, midpoint=0.4, scale=0.15)
        severity = max(severity, ens_sev * gap_dampener)
    # Climatology escalator.
    if integrated_climo_prob is not None:
        climo_sev = _sigmoid_severity(integrated_climo_prob, midpoint=0.35, scale=0.15)
        severity = max(severity, climo_sev * 0.7 * gap_dampener)

    # Frost pocket escalator — cold-air drainage pools in concave terrain.
    topo = forecast.get("_topography") or {}
    frost_pocket_phrase = ""
    if topo.get("frost_pocket") and topo["frost_pocket"] != "none":
        fp_risk = topo.get("frost_pocket_risk", 0)
        twi = topo.get("twi", 0)
        severity = min(1.0, severity + fp_risk * 0.2)
        frost_pocket_phrase = (f" Frost pocket: {topo['frost_pocket']} risk "
                              f"(TWI={twi:.1f}, concavity={topo.get('concavity_m', 0):.2f}m).")

    # NWS active-alerts override.
    alerts = forecast.get("_alerts") or {}
    headline_alert = ""
    if alerts.get("any_freeze") and start <= 48:
        freeze_event = next(
            (a for a in alerts.get("alerts", []) if a.get("is_freeze")),
            None,
        )
        if freeze_event:
            ev = (freeze_event.get("event") or "").lower()
            if "freeze" in ev or "hard" in ev:
                severity = max(severity, 0.8)
            else:
                severity = max(severity, 0.5)
            headline_alert = f" Active NWS alert: {freeze_event['event']}."

    level = _level_from_severity(severity)

    return Risk(
        key="frost",
        name="Frost & Freeze",
        level=level,
        headline=headlines[level] + headline_alert + frost_pocket_phrase,
        detail=("Late-spring freezes can damage emerging seedlings. Soybean cotyledons are highly "
                "vulnerable; corn can sometimes recover if its growing point is still below ground. "
                "Four signals are now combined: (1) the deterministic Open-Meteo forecast minimum; "
                "(2) the multi-model ensemble probability of freeze across the emergence window; "
                "(3) the 5-yr climatology probability of freeze for the same calendar dates; "
                "(4) Topographic Wetness Index (TWI) and concavity-based frost pocket detection — "
                "cold air drains into low spots creating 3-5°F colder radiative nights. "
                "Active NWS frost/freeze advisories and warnings escalate further when in effect."),
        metric=f"{min_air:.0f}°F",
        severity=severity,
        curve_type="sigmoid",
    )


def _soil_crusting(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Heavy rain followed by rapid drying with high UV/heat bakes a crust."""
    # scan 7-day window: find any 24h rain >0.5", then look at the next 48h for heat + UV
    precip = _hourly_window(forecast, "precipitation", start, start + 168)
    air = _hourly_window(forecast, "temperature_2m", start, start + 168)
    uv = _hourly_window(forecast, "uv_index", start, start + 168)
    n = min(len(precip), len(air), len(uv))

    worst_score, worst_rain = 0.0, 0.0
    for i in range(0, max(0, n - 72), 6):
        rain_24h = sum(precip[i:i + 24])
        if rain_24h < 0.4:
            continue
        follow_air = air[i + 24:i + 72]
        follow_uv = uv[i + 24:i + 72]
        if not follow_air or not follow_uv:
            continue
        max_t = max(follow_air)
        max_uv = max(follow_uv)
        # crust score: rain magnitude * heat-and-sun factor
        score = rain_24h * max(0.0, max_t - 65) / 10 * max(0.0, max_uv - 4) / 4
        if score > worst_score:
            worst_score, worst_rain = score, rain_24h

    # tillage adjusts surface vulnerability — fine-tilth bare soil crusts hardest
    if inputs.tillage == "conventional" and inputs.residue == "low":
        worst_score *= 1.3
    elif inputs.tillage == "no-till" or inputs.residue == "heavy":
        worst_score *= 0.6

    # SSURGO soil-texture amplifier. Silt loams + low OM are the canonical
    # "crust-prone" combination — silt particles seal under raindrop impact
    # and cement on drying. Sandy soils essentially do not crust regardless
    # of tillage; clays form clods rather than a sheet crust.
    soil_profile = forecast.get("_soil_profile") or {}
    silt = soil_profile.get("silt_pct") or 0.0
    sand = soil_profile.get("sand_pct") or 0.0
    clay = soil_profile.get("clay_pct") or 0.0
    om = soil_profile.get("organic_matter_pct")
    if silt >= 50 and clay < 27:           # silt loam family
        worst_score *= 1.5
    elif sand >= 65 and clay < 12:         # sand / loamy sand
        worst_score *= 0.4
    elif clay >= 35:                       # heavy clays cracking, not crusting
        worst_score *= 0.7
    if om is not None and om < 2.0 and silt >= 40:
        worst_score *= 1.15                # low-OM silt is the worst case

    # Soybeans use epigeal germination — the hypocotyl arch must pull
    # cotyledons through the surface. Corn's rigid coleoptile punches through
    # crusts far more easily. MSU, NC State, UNL document soybeans are ~2-3x
    # more susceptible to crusting-related stand loss.
    crust_crop_phrase = ""
    if profile.get("label") == "Soybeans":
        worst_score *= 1.4
        crust_crop_phrase = " Soybeans — epigeal emergence is more vulnerable to crusting than corn."
    elif profile.get("label") == "Dry Beans":
        worst_score *= 1.3
        crust_crop_phrase = " Dry beans — epigeal emergence increases crusting vulnerability."

    headlines = {
        "high": f"{worst_rain:.2f}\" downpour followed by hot/sunny drying — crust likely.{crust_crop_phrase}",
        "moderate": f"Possible crusting after {worst_rain:.2f}\" rain and a sunny dry-down.{crust_crop_phrase}",
        "low": "No rain-then-bake pattern that would crust the surface.",
    }
    severity = _sigmoid_severity(worst_score, midpoint=1.0, scale=0.5)
    level = _level_from_severity(severity)

    return Risk(
        key="crusting",
        name="Soil Crusting",
        level=level,
        headline=headlines[level],
        detail=("A heavy storm followed by hot, bright drying bakes the topsoil into a hard crust. "
                "Seedlings exhaust their reserves trying to break through and die underground. "
                "Soybeans (epigeal germination) are 2-3× more vulnerable than corn (hypogeal) — "
                "the hypocotyl arch snaps under crust resistance (MSU, NC State, UNL CropWatch)."),
        metric=f"score {worst_score:.1f}",
        severity=severity,
        curve_type="sigmoid",
    )


def _pythium(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Pythium damping-off — species-specific temperature behaviour.

    Source: Matthiesen, Ahmad & Robertson (2016) "Temperature Affects
    Aggressiveness and Fungicide Sensitivity of Four Pythium spp. that Cause
    Soybean and Corn Damping Off in Iowa" (Plant Disease 100:583-591).
    P. ultimum is aggressive across all tested temps (4-28°C) but field damage
    is greatest when cold soils slow the seed's escape. P. sylvaticum and
    P. torulosum are more aggressive at warmer (20-28°C) saturated soils.
    """
    soil = _hourly_window(forecast, "soil_temperature_6cm", start, start + 96)
    moist_top = _hourly_window(forecast, "soil_moisture_1_to_3cm", start, start + 96)
    if not soil or not moist_top:
        return Risk(key="pythium", name="Pythium Damping-Off", level="low",
                    headline="No soil-data signal.", detail="", metric="—")
    avg_soil = _avg(soil)
    avg_moist = _avg(moist_top)
    sat_hours = _saturated_hours(moist_top, 0.38)

    # SSURGO texture amplifier. Heavy clay / silty clay loam holds water near
    # the surface for days after a rain — that's the prolonged free-water
    # window Pythium zoospores need. Coarse sands drain in hours and rarely
    # build the saturation tail to support damping-off, so we de-rate them.
    soil_profile = forecast.get("_soil_profile") or {}
    clay = soil_profile.get("clay_pct") or 0.0
    sand = soil_profile.get("sand_pct") or 0.0
    drainage_text = (soil_profile.get("drainage_class") or "").lower()
    poorly_drained = any(t in drainage_text for t in
                         ("poorly drained", "very poorly drained",
                          "somewhat poorly drained"))
    fine_textured = clay >= 27 or (clay >= 18 and sand < 45)   # clay/silty-clay loams
    coarse_textured = sand >= 70 and clay < 12                 # sand / loamy sand

    cold_wet = avg_soil < 55 and (avg_moist > 0.38 or sat_hours > 24)
    warm_sat = avg_soil >= 65 and sat_hours > 36

    if cold_wet:
        species_note = "P. ultimum (cold-aggressive)"
    elif warm_sat:
        species_note = "P. sylvaticum / P. torulosum (warm-saturated)"
    elif avg_soil < 60 and avg_moist > 0.33:
        species_note = "Pythium spp. (cool-wet)"
    else:
        species_note = ""
    if fine_textured and poorly_drained:
        species_note = (species_note or "Pythium spp.") + " — fine-textured / poorly drained soil"

    drought = forecast.get("_drought") or {}
    dm_class = drought.get("class")
    drought_phrase = ""
    if isinstance(dm_class, int) and dm_class >= 2:
        drought_phrase = f" USDM: {drought.get('label') or ''}."

    cpc = forecast.get("_cpc_moisture") or {}
    cpc_phrase = ""
    if cpc.get("available") and cpc.get("soil_moisture_pctl") is not None:
        sm_pctl = cpc["soil_moisture_pctl"]
        if sm_pctl >= 80:
            cpc_phrase = f" CPC moisture: {sm_pctl:.0f}th pctl."

    texture_phrase = ""
    if soil_profile.get("texture_class"):
        texture_phrase = f" Soil: {soil_profile['texture_class']}."
    headlines = {
        "high": f"Cold ({avg_soil:.0f}°F) and saturated ({sat_hours}h above field capacity) — {species_note}.{texture_phrase}{drought_phrase}{cpc_phrase}",
        "moderate": (f"Warm and saturated ({sat_hours}h above field capacity) — {species_note}.{texture_phrase}{drought_phrase}{cpc_phrase}"
                     if warm_sat else f"Cool soils ({avg_soil:.0f}°F) with elevated moisture — {species_note}.{texture_phrase}{drought_phrase}{cpc_phrase}"),
        "low": f"Soil warmth/drainage are not in the Pythium danger zone.{texture_phrase}{drought_phrase}{cpc_phrase}",
    }
    # Bimodal gaussian severity: cold-aggressive species (P. torulosum,
    # P. oopapillum) peak around 52-55°F (Matthiesen 2016: most aggressive at
    # 13°C/55°F); P. sylvaticum around 72°F. The original 48°F peak was slightly
    # low — shifting to 52°F better fits the Matthiesen field isolate data.
    cold_peak_sev = _gaussian_severity(avg_soil, peak=52.0, sigma=8.0)
    warm_peak_sev = _gaussian_severity(avg_soil, peak=72.0, sigma=10.0)
    moisture_factor = _sigmoid_severity(avg_moist, midpoint=0.32, scale=0.05)
    sat_factor = _sigmoid_severity(sat_hours, midpoint=20.0, scale=8.0)
    severity = max(cold_peak_sev * moisture_factor, warm_peak_sev * sat_factor * 0.75)

    if fine_textured and poorly_drained and severity > 0.10:
        severity = min(1.0, severity + 0.12)
    elif coarse_textured and not cold_wet:
        severity = max(0.0, severity - 0.15)
    if isinstance(dm_class, int) and dm_class >= 2 and not cold_wet:
        severity = max(0.0, severity - 0.2)
    if cpc.get("available") and cpc.get("soil_moisture_pctl") is not None:
        if cpc["soil_moisture_pctl"] >= 80 and severity > 0.10:
            severity = min(1.0, severity + 0.08)

    # Tile drainage shortens the saturation window that Pythium zoospores need
    # to sporulate and infect. Apply 0.7× multiplier to the conduciveness term.
    tile_phrase = ""
    if inputs.field_tiled:
        severity *= 0.7
        tile_phrase = " Tile drainage shortens the saturation window for zoospore infection."

    level = _level_from_severity(severity)

    return Risk(
        key="pythium",
        name="Pythium Damping-Off",
        level=level,
        headline=headlines[level] + tile_phrase,
        detail=("Pythium species partition by temperature. P. ultimum stays aggressive in cold "
                "(<55°F) saturated soils — the classic damping-off scenario. P. sylvaticum and "
                "P. torulosum (Matthiesen et al. 2016) hit hardest in warm (≥65°F), waterlogged "
                "soils. Either way, the seedling is left soft, brown, and pulls easily."
                + (" In dry beans, Pythium is part of a root rot complex with Fusarium and "
                   "Rhizoctonia (MSU, UNL). Cool wet planting conditions favor all three "
                   "simultaneously — check Rhizoctonia risk alongside this factor."
                   if profile.get("label") == "Dry Beans" else "")),
        metric=f"{avg_soil:.0f}°F · {sat_hours}h sat",
        severity=severity,
        curve_type="gaussian",
    )


def _phytophthora(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Phytophthora sojae root and stem rot — soybean post-emergence pathogen.

    Source: SRIN "Phytophthora Root & Stem Rot"; OSU Ohioline PLPATH-SOY-04;
    Helms et al. 2007 (Crop Science) "Soybean Tolerance to Water-Saturated Soil
    and Role of Resistance to Phytophthora sojae". Zoospores require *free*
    water to swim toward roots; infection severity climbs sharply above 60°F
    soil temperature with the optimum disease development at 77-86°F.
    """
    if not profile.get("phytophthora_sensitive"):
        return Risk(key="phytophthora", name="Phytophthora Root Rot", level="low",
                    headline=f"{profile['label']} not a primary host.", detail="",
                    metric="—")
    moist_deep = _hourly_window(forecast, "soil_moisture_3_to_9cm", start, start + 168)
    soil = _hourly_window(forecast, "soil_temperature_18cm", start, start + 168)
    if not moist_deep or not soil:
        return Risk(key="phytophthora", name="Phytophthora Root Rot", level="low",
                    headline="No soil-data signal.", detail="", metric="—")
    sat_hours = _saturated_hours(moist_deep, 0.38)
    avg_soil = _avg(soil)
    poor_drain = inputs.tillage == "no-till" or inputs.residue == "heavy"

    # SSURGO drainage classes give us a measured, soil-survey-based signal.
    # "Poorly drained" / "Very poorly drained" / "Somewhat poorly drained"
    # fields hold free water for the days of saturation P. sojae zoospores
    # need. Hydrologic group D is the runoff-prone end (Conover, the dominant
    # series at Decker MI, is C/D). NRCS class definitions:
    # https://www.nrcs.usda.gov/sites/default/files/2022-09/Field_Indicators_v8.2.pdf
    soil_profile = forecast.get("_soil_profile") or {}
    drainage = (soil_profile.get("drainage_class") or "").lower()
    hydgrp = (soil_profile.get("hydrologic_group") or "").upper()
    soil_poorly_drained = any(t in drainage for t in
                              ("poorly drained", "very poorly drained",
                               "somewhat poorly drained"))
    runoff_prone = hydgrp.startswith("D") or hydgrp == "C/D"
    if soil_poorly_drained or runoff_prone:
        poor_drain = True

    rotation = forecast.get("_rotation") or {}
    rotation_phrase = ""
    if rotation.get("available") and rotation.get("soy_on_soy"):
        rotation_phrase = " Soy-on-soy rotation increases inoculum pressure."

    drain_phrase = ""
    if soil_profile.get("drainage_class"):
        drain_phrase = f" SSURGO: {soil_profile['drainage_class'].lower()}."
    headlines = {
        "high": f"Subsoil saturated {sat_hours}h at {avg_soil:.0f}°F — past the 60°F + saturation threshold.{drain_phrase}{rotation_phrase}",
        "moderate": f"Subsoil moist ({sat_hours}h above capacity, {avg_soil:.0f}°F) — borderline infection conditions.{drain_phrase}{rotation_phrase}",
        "low": f"Subsoil drainage adequate for the forecast window.{drain_phrase}{rotation_phrase}",
    }
    # Trapezoidal on soil temp: zoospore activity plateau 55–86°F, ramps at edges.
    temp_sev = _trapezoidal_severity(avg_soil, a=50.0, b=60.0, c=86.0, d=95.0)
    sat_sev = _sigmoid_severity(sat_hours, midpoint=24.0, scale=10.0)
    severity = temp_sev * sat_sev
    if poor_drain:
        severity = min(1.0, severity + 0.1)
    if rotation.get("available") and rotation.get("soy_on_soy"):
        severity = min(1.0, severity + 0.1)

    # Tile drainage shortens the free-water window P. sojae zoospores need.
    tile_phrase = ""
    if inputs.field_tiled:
        severity *= 0.7
        tile_phrase = " Tile drainage reduces free-water duration for zoospore movement."

    level = _level_from_severity(severity)

    return Risk(
        key="phytophthora",
        name="Phytophthora Root Rot",
        level=level,
        headline=headlines[level] + tile_phrase,
        detail=("Phytophthora sojae zoospores need free water and ≥60°F soil to swim toward and "
                "infect soybean roots (SRIN, OSU Ohioline). Below 55°F the pathogen is largely "
                "quiescent. Resistance genes (Rps) and partial-resistance (field tolerance) plus "
                "drainage are the durable defenses; seed treatments help bridge wet stretches. "
                "SSURGO drainage class and hydrologic group are now folded in as a measured "
                "field-suitability layer."),
        metric=f"{avg_soil:.0f}°F · {sat_hours}h sat",
        severity=severity,
        curve_type="trapezoidal",
    )


def _seedcorn_maggot(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Cool, wet soils with decomposing organic matter (manure / heavy residue) attract egg-laying."""
    soil = _hourly_window(forecast, "soil_temperature_6cm", start, start + 168)
    moist = _hourly_window(forecast, "soil_moisture_1_to_3cm", start, start + 168)
    if not soil or not moist:
        return Risk(key="maggot", name="Seedcorn Maggot", level="low",
                    headline="No soil-data signal.", detail="", metric="—")
    avg_soil = _avg(soil)
    moist_hours = _saturated_hours(moist, 0.30)
    organic_load = inputs.manure_recent or inputs.residue == "heavy"

    rotation = forecast.get("_rotation") or {}
    rotation_note = ""
    if rotation.get("available") and rotation.get("corn_on_corn"):
        rotation_note = " Corn-on-corn rotation increases residue attraction."

    enviro = forecast.get("_enviroweather") or {}
    enviro_note = ""
    if enviro.get("available"):
        gdd39 = (enviro.get("gdd") or {}).get("base39")
        if gdd39 is not None and 200 <= gdd39 <= 450:
            enviro_note = f" Enviroweather GDD (base 39°F): {gdd39:.0f} — active adult flight window."

    organic_note = " with manure/heavy residue feeding flies" if organic_load else ""
    headlines = {
        "high": f"Cool ({avg_soil:.0f}°F), moist soils{organic_note} — peak egg-laying conditions.{rotation_note}{enviro_note}",
        "moderate": f"Cool and damp{organic_note} — some maggot pressure expected.{rotation_note}{enviro_note}",
        "low": f"Soil too warm/dry — limited maggot pressure.{rotation_note}{enviro_note}",
    }
    # Gaussian on soil temp — egg-laying peaks in cool soils (~55°F).
    temp_sev = _gaussian_severity(avg_soil, peak=55.0, sigma=10.0)
    moist_sev = _sigmoid_severity(moist_hours, midpoint=48.0, scale=20.0)
    organic_mult = 1.5 if organic_load else 0.6
    severity = min(1.0, temp_sev * moist_sev * organic_mult)

    if rotation.get("available") and rotation.get("corn_on_corn"):
        severity = min(1.0, severity + 0.1)
    if enviro.get("available"):
        gdd39 = (enviro.get("gdd") or {}).get("base39")
        if gdd39 is not None:
            gdd_sev = _gaussian_severity(gdd39, peak=325.0, sigma=100.0) * 0.5
            severity = max(severity, gdd_sev)
    level = _level_from_severity(severity)

    return Risk(
        key="maggot",
        name="Seedcorn Maggot",
        level=level,
        headline=headlines[level],
        detail=("Adult flies lay eggs near decomposing organic matter; larvae burrow into the seed "
                "and hollow it out. A seed-applied insecticide is the usual defense when risk is high. "
                "CropScape rotation and Enviroweather GDD are used when available."),
        metric=f"{avg_soil:.0f}°F",
        severity=severity,
        curve_type="gaussian",
    )


def _wireworm(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Wireworm pressure climbs in cool, damp soils — and after sod or grass cover."""
    soil = _hourly_window(forecast, "soil_temperature_6cm", start, start + 168)
    moist = _hourly_window(forecast, "soil_moisture_1_to_3cm", start, start + 168)
    if not soil or not moist:
        return Risk(key="wireworm", name="Wireworm", level="low",
                    headline="No soil-data signal.", detail="", metric="—")
    avg_soil = _avg(soil)
    damp_hours = _saturated_hours(moist, 0.28)

    rotation = forecast.get("_rotation") or {}
    rotation_note = ""
    grass_amp = 1.0
    if rotation.get("available"):
        prev = rotation.get("prev_crop_code")
        if prev in (176, 36, 37):
            rotation_note = f" CDL shows {rotation.get('prev_crop_name', 'grass/hay')} last year."
            grass_amp = 1.5
        elif rotation.get("corn_on_corn"):
            rotation_note = " Corn-on-corn rotation builds wireworm populations."
            grass_amp = 1.2
    if inputs.previous_grass:
        grass_amp = max(grass_amp, 1.5)

    grass_note = " — and last year's sod/grass elevates the population" if inputs.previous_grass else ""
    headlines = {
        "high": f"Cool damp soils{grass_note}. High wireworm risk — consider treated seed.{rotation_note}",
        "moderate": f"Cool damp window{grass_note}. Some wireworm pressure possible.{rotation_note}",
        "low": f"Conditions don't favor wireworm activity.{rotation_note}",
    }
    # Trapezoidal on soil temp — wireworm activity plateau at 45–60°F.
    temp_sev = _trapezoidal_severity(avg_soil, a=35.0, b=45.0, c=60.0, d=70.0)
    damp_sev = _sigmoid_severity(damp_hours, midpoint=48.0, scale=15.0)
    severity = min(1.0, temp_sev * damp_sev * grass_amp)
    level = _level_from_severity(severity)

    return Risk(
        key="wireworm",
        name="Wireworm",
        level=level,
        headline=headlines[level],
        detail=("Wireworms (click-beetle larvae) hollow out seeds and tunnel into stems, killing the "
                "central leaves. Risk is highest in fields rotated out of grass or sod, and in "
                "continuous corn. CropScape rotation history is used when available."),
        metric=f"{avg_soil:.0f}°F",
        severity=severity,
        curve_type="trapezoidal",
    )


def _slugs(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Slugs feast on emerging plants in cool, damp, no-till heavy-residue fields."""
    if not (inputs.tillage == "no-till" and inputs.residue in ("moderate", "heavy")):
        return Risk(
            key="slugs",
            name="Slugs",
            level="low",
            headline="Tilled or low-residue field — slugs unlikely.",
            detail=("Slugs need moist, residue-covered cover to survive. Conventional-till fields "
                    "rarely see meaningful pressure."),
            metric="—",
        )
    air = _hourly_window(forecast, "temperature_2m", start, start + 168)
    humid = _hourly_window(forecast, "relative_humidity_2m", start, start + 168)
    if not air or not humid:
        return Risk(key="slugs", name="Slugs", level="moderate",
                    headline="No-till + heavy residue — monitor for slug damage.",
                    detail="", metric="—")
    avg_air = _avg(air)
    humid_hours = sum(1 for h in humid if h > 85)
    headlines = {
        "high": f"Cool ({avg_air:.0f}°F), humid no-till residue — peak slug conditions.",
        "moderate": f"Damp residue under cool air — watch seedlings closely.",
        "low": "Drier or warmer than slug-favored conditions.",
    }
    # Trapezoidal on air temp — slug feeding plateau 50–65°F.
    temp_sev = _trapezoidal_severity(avg_air, a=40.0, b=50.0, c=65.0, d=75.0)
    humid_sev = _sigmoid_severity(humid_hours, midpoint=48.0, scale=15.0)
    severity = temp_sev * humid_sev
    level = _level_from_severity(severity)

    return Risk(
        key="slugs",
        name="Slugs",
        level=level,
        headline=headlines[level],
        detail=("Slugs shred young leaves down to the stem in cool, humid, residue-covered fields. "
                "A row cleaner or rolled residue can suppress populations."),
        metric=f"{avg_air:.0f}°F",
        severity=severity,
        curve_type="trapezoidal",
    )


def _herbicide_carryover(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Driven entirely by user input — no API can see the soil's chemistry."""
    chem = (inputs.herbicide_last_season or "").strip()
    if not chem:
        return Risk(
            key="herbicide",
            name="Herbicide Carryover",
            level="low",
            headline="No carryover concerns reported.",
            detail=("Add a previous-season herbicide on the home page if you applied a residual "
                    "product (e.g., atrazine, fomesafen, clopyralid) so it can be flagged here."),
            metric="—",
        )
    crop_label = profile["label"].lower()
    risky_pairs = {
        "atrazine": ("soybeans", "alfalfa"),
        "fomesafen": ("corn", "alfalfa"),
        "clopyralid": ("soybeans", "alfalfa"),
        "imazethapyr": ("corn", "alfalfa"),
        "mesotrione": ("soybeans",),
        "sulfentrazone": ("corn", "alfalfa"),
        "picloram": ("alfalfa",),
        "aminopyralid": ("alfalfa",),
        "dicamba": ("alfalfa",),
    }
    chem_l = chem.lower()
    matched = next((name for name in risky_pairs if name in chem_l), None)
    if matched and crop_label in risky_pairs[matched]:
        level = "high"
        severity = 0.8
        headline = f"{matched.title()} carryover is documented to injure {profile['label']}."
    elif matched:
        level = "moderate"
        severity = 0.5
        headline = f"{matched.title()} reported — verify rotation interval before planting."
    else:
        level = "low"
        severity = 0.15
        headline = f"\"{chem}\" reported — check the label rotation interval for {profile['label']}."
    return Risk(
        key="herbicide",
        name="Herbicide Carryover",
        level=level,
        headline=headline,
        detail=("Residual herbicides from the previous season can stunt or kill the new crop. "
                "Check the product label's rotation chart and consider a bioassay if in doubt."),
        metric=chem[:18],
        severity=severity,
        curve_type="composite",
    )


def _antecedent_saturation(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Soil saturation from rainfall in the *prior* 30 days.

    Uses `forecast["_history"]` if present (loaded from the Archive API). Same
    forecasted rainfall on already-saturated ground = much faster waterlogging.
    """
    history = forecast.get("_history") or {}
    daily = history.get("daily", {})
    precip = [p for p in (daily.get("precipitation_sum") or []) if p is not None]
    if not precip:
        return Risk(key="antecedent", name="Antecedent Saturation", level="low",
                    headline="No recent rainfall history available.", detail="", metric="—")
    cumulative = sum(precip)
    wet_days = sum(1 for p in precip if p >= 0.1)

    # Forecast rain layered onto already-soaked ground escalates faster.
    next_48 = sum(_hourly_window(forecast, "precipitation", start, start + 48))

    # Hydrologic Soil Group from SSURGO measures runoff/infiltration potential.
    # Group D ≈ low infiltration, high runoff potential — fields refill quickly
    # and stay wet longer. C/D and D shift the thresholds down. A is sandy /
    # high-infiltration; for those the same cumulative number is less stressful.
    soil_profile = forecast.get("_soil_profile") or {}
    hydgrp = (soil_profile.get("hydrologic_group") or "").upper()
    awc = soil_profile.get("available_water_capacity")  # cm/cm; ~0.20 max
    high_runoff = hydgrp.startswith("D") or hydgrp == "C/D"
    fast_infil = hydgrp.startswith("A")
    # Effective thresholds: lower for runoff-prone fields, higher for sandy.
    cum_high = 6.0
    cum_mod = 4.0
    if high_runoff:
        cum_high, cum_mod = 4.5, 3.0
    elif fast_infil:
        cum_high, cum_mod = 7.5, 5.0

    headlines = {
        "high": f'{cumulative:.1f}" of rain in the last 30 days — soil profile is already loaded.',
        "moderate": f'{cumulative:.1f}" over the last 30 days ({wet_days} wet days) — limited drainage capacity.',
        "low": f'Only {cumulative:.1f}" rain over the last 30 days — soil can absorb forecasted precip.',
    }
    mid = (cum_mod + cum_high) / 2.0
    severity = _sigmoid_severity(cumulative, midpoint=mid, scale=(cum_high - cum_mod) / 2.0)
    if next_48 > 0.5 and cumulative > cum_mod:
        severity = min(1.0, severity + 0.08)

    # Tile drainage lowers the water table between rain events, so the field
    # enters each storm with more available pore space. Reduce by ~50%.
    tile_phrase = ""
    if inputs.field_tiled:
        severity *= 0.5
        tile_phrase = " Tile drainage lowers the water table between events — more pore space available."

    level = _level_from_severity(severity)

    return Risk(
        key="antecedent",
        name="Antecedent Saturation",
        level=level,
        headline=headlines[level] + tile_phrase,
        detail=("Antecedent moisture controls how the field responds to forecasted rain. A field "
                "that's been wet for weeks has no buffer — even moderate rain pushes it past field "
                "capacity, slowing germination and inviting damping-off."),
        metric=f'{cumulative:.1f}" / 30d',
        severity=severity,
        curve_type="sigmoid",
    )


def _topography(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Local-depression / ponding risk from a 3×3 elevation sample.

    The modeled soil moisture and the SSURGO drainage class apply to the *map
    unit* — they cannot see whether this exact spot sits in a local depression
    that collects runoff from the surrounding field. Bowl-shaped microsites
    drown out seedlings even on otherwise well-drained soils, and they're
    where most "I planted on a great forecast and still lost the stand"
    complaints come from. We sample 9 elevations around the field point and
    flag concavity (center sits below neighbours) on near-flat fields.
    """
    topo = forecast.get("_topography") or {}
    if not topo.get("available"):
        return Risk(
            key="topography",
            name="Topography & Ponding",
            level="low",
            headline="No elevation data for this point.",
            detail=("Local depressions collect ponded water that the modeled soil-moisture grid "
                    "cannot resolve. Free-tier elevation lookup unavailable for this point."),
            metric="—",
        )

    ponding = topo.get("ponding_risk", "low")
    concavity = topo.get("concavity_m", 0.0)
    slope = topo.get("slope_m_per_km", 0.0)

    # Saturation amplifier. A moderate forecast that lands on a bowl-shaped
    # microsite is worse than the same forecast on a ridge — ponded water
    # stays days longer than the modeled grid suggests. Promote to "high"
    # when topography is concave AND we're already getting rain.
    next_48 = sum(_hourly_window(forecast, "precipitation", start, start + 48))
    next_168 = sum(_hourly_window(forecast, "precipitation", start, start + 168))

    if concavity <= 0.0:
        severity = 0.0
    else:
        severity = _sigmoid_severity(concavity, midpoint=0.2, scale=0.1)
    if next_48 > 1.0 and concavity > 0.0:
        severity = min(1.0, severity + 0.1)
    if next_168 > 2.5 and concavity >= 0.15:
        severity = min(1.0, severity + 0.08)

    # Tile drainage removes subsurface water but cannot fix surface ponding in
    # true low spots. Reduce by ~40% — meaningful but not total mitigation.
    tile_phrase = ""
    if inputs.field_tiled:
        severity *= 0.6
        tile_phrase = " Tile drainage reduces subsurface contribution to ponding."

    level = _level_from_severity(severity)

    if level == "high":
        headline = (f"Field point sits ~{concavity:.1f} m below surroundings on a "
                    f"{slope:.1f} m/km slope — water will pond.")
    elif level == "moderate":
        headline = (f"Mild concavity ({concavity:.1f} m) or near-flat slope "
                    f"({slope:.1f} m/km) — partial ponding likely after heavy rain.")
    else:
        headline = (f"Field point at or above its neighbours ({concavity:+.1f} m) on "
                    f"a {slope:.1f} m/km slope — natural runoff.")

    return Risk(
        key="topography",
        name="Topography & Ponding",
        level=level,
        headline=headline + tile_phrase,
        detail=("A modeled-grid soil-moisture forecast is averaged over ~9 km — it cannot see "
                "the local low spot that collects runoff from the surrounding 40 acres. We sample "
                "a 3×3 elevation grid (~500 m spacing) around the field and flag bowl-shaped "
                "microsites where ponding will outlast what the modeled saturation predicts. "
                "This is the most common cause of stand loss on otherwise well-drained fields."),
        metric=f"Δ{concavity:+.1f}m · {slope:.1f}m/km",
        severity=severity,
        curve_type="sigmoid",
    )


def _planting_date_for_start(forecast: dict, start: int) -> date:
    """Map a window start-hour back to the calendar date it represents."""
    base = forecast.get("_today_planting_date") or date.today()
    # `start` is hours from today's midnight (Open-Meteo aligns hourly to local
    # midnight). 6am offset is added by the caller; integer-divide to recover
    # the day index.
    return base + timedelta(days=max(0, (start) // 24))


def _black_cutworm(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Black cutworm cutting-larvae pressure on emerging corn.

    Source: ISU CIG / Erin Hodgson — "Time to Scout for Black Cutworm in
    Emerged Corn"; UNL CropWatch cutworm trapping. Cutworm pressure peaks
    when the *emergence date* of the seedling falls inside a window of
    cumulative GDD (base 50°F) from the first significant moth flight. In
    that window, larvae are at the 4th-6th instar — large enough to clip a
    corn plant at the soil line. Egg-laying is concentrated on early-season
    weeds and crop residue, so no-till + winter-annual cover (proxied by
    `previous_grass`) raises the pressure substantially.
    """
    if not profile.get("bcw_sensitive"):
        return Risk(key="cutworm", name="Black Cutworm", level="low",
                    headline=f"{profile['label']} not a primary host.",
                    detail="Cutworms target corn at the soil line; soybean stand loss is minor.",
                    metric="—")

    cum_lookup = forecast.get("_gdd_base50_cum") or {}
    if not cum_lookup:
        return Risk(key="cutworm", name="Black Cutworm", level="low",
                    headline="No GDD history available for cutworm scoring.",
                    detail="", metric="—")

    plant_date = _planting_date_for_start(forecast, start)
    typ_emerge = profile.get("typ_emerge_days", 7)
    emerge_date = plant_date + timedelta(days=typ_emerge)

    cum_at_emerge = cum_lookup.get(emerge_date.isoformat())
    if cum_at_emerge is None:
        # Project forward using the last known value + recent daily average.
        last_iso = max(cum_lookup.keys())
        last_date = date.fromisoformat(last_iso)
        if emerge_date <= last_date:
            cum_at_emerge = cum_lookup[last_iso]
        else:
            recent_keys = sorted(cum_lookup.keys())[-14:]
            recent_vals = [cum_lookup[k] for k in recent_keys]
            per_day = ((recent_vals[-1] - recent_vals[0]) / max(1, len(recent_vals) - 1)
                       if len(recent_vals) >= 2 else 12.0)
            cum_at_emerge = cum_lookup[last_iso] + per_day * (emerge_date - last_date).days

    # Prefer ISU's published moth-trap biofix when available — it's the
    # earliest confirmed significant flight in the region this season. Falls
    # back to the upper-Midwest mid-April default if the report hasn't dropped
    # yet or the parser couldn't pin a date.
    flight_year = plant_date.year
    flight_info = forecast.get("_bcw_flight") or {}
    flight_doy = flight_info.get("earliest_doy") if (flight_info.get("year") == flight_year) else None
    if not flight_doy:
        flight_doy = BCW_DEFAULT_FLIGHT_DOY
    flight_date = date(flight_year, 1, 1) + timedelta(days=flight_doy - 1)
    cum_at_flight = cum_lookup.get(flight_date.isoformat())
    if cum_at_flight is None:
        # Approximate by reading the closest available date.
        candidates = sorted(d for d in cum_lookup if d <= flight_date.isoformat())
        cum_at_flight = cum_lookup[candidates[-1]] if candidates else 0.0

    dd_since_flight = max(0.0, cum_at_emerge - cum_at_flight)

    # Egg-laying habitat amplifier — residue and prior grass cover host the
    # winter-annual weeds that BCW moths prefer for ovipositing on.
    habitat_mult = 1.0
    if inputs.tillage == "no-till":
        habitat_mult *= 1.3
    if inputs.residue == "heavy":
        habitat_mult *= 1.2
    if inputs.previous_grass:
        habitat_mult *= 1.4

    in_window = BCW_DAMAGE_WINDOW_GDD[0] <= dd_since_flight <= BCW_DAMAGE_WINDOW_GDD[1]
    near_window = (BCW_DAMAGE_WINDOW_GDD[0] - 80) <= dd_since_flight <= (BCW_DAMAGE_WINDOW_GDD[1] + 100)

    if dd_since_flight < BCW_DAMAGE_WINDOW_GDD[0]:
        phase = f"larvae still small (~{int(dd_since_flight)} DD post-flight)"
    elif dd_since_flight > BCW_DAMAGE_WINDOW_GDD[1]:
        phase = f"larvae have pupated (~{int(dd_since_flight)} DD past flight)"
    else:
        phase = f"4th-6th instar — peak cutting (~{int(dd_since_flight)} DD post-flight)"

    headlines = {
        "high": f"Emergence lands in the cutting window ({phase}); residue/sod amplifies pressure.",
        "moderate": f"Emergence near the cutting window ({phase}). Scout V1-V4 fields nightly.",
        "low": f"Emergence outside the cutting window ({phase}).",
    }
    biofix_phrase = (
        f" Biofix: ISU trap network earliest flight {flight_info.get('earliest_iso')}."
        if flight_info.get("earliest_iso") else
        " Biofix: default upper-Midwest mid-April (no ISU report parsed)."
    )
    # Gaussian severity centered on the damage window midpoint (325 DD).
    window_center = (BCW_DAMAGE_WINDOW_GDD[0] + BCW_DAMAGE_WINDOW_GDD[1]) / 2.0
    window_sigma = (BCW_DAMAGE_WINDOW_GDD[1] - BCW_DAMAGE_WINDOW_GDD[0]) / 2.0
    severity = _gaussian_severity(dd_since_flight, peak=window_center, sigma=window_sigma)
    severity = min(1.0, severity * min(habitat_mult, 2.0) / 1.3)
    level = _level_from_severity(severity)

    return Risk(
        key="cutworm",
        name="Black Cutworm",
        level=level,
        headline=headlines[level] + biofix_phrase,
        detail=("Black cutworm moths overwinter to the south and migrate north on spring storm "
                "fronts, laying eggs on early-spring weeds and crop residue. Larvae reach the "
                "damaging 4th-instar stage roughly 300 GDD (base 50°F) after the first "
                "significant moth flight (ISU CIG, Hodgson). Peak risk lines up when seedling "
                "emergence falls inside that ~200-450 DD window. Biofix is now pulled live from "
                "the ISU Iowa moth-trapping network when available — the earliest confirmed "
                "Iowa flight is used as a regional seed since moths arrive in MI on the same "
                "southern storm fronts a few days later."),
        metric=f"{int(dd_since_flight)} DD post-flight",
        severity=severity,
        curve_type="gaussian",
    )


def _bean_leaf_beetle(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Bean leaf beetle pressure on emerging soybean.

    Source: Lam, W.K.F. & Pedigo, L.P. (2000) "A Predictive Model for Survival
    of Overwintering Bean Leaf Beetles" (Environmental Entomology); ISU CIG
    "Bean Leaf Beetle Management in Soybeans". Overwintered adults emerge in
    spring and seek the *first* emerged soybeans — early-planted fields take
    disproportionate defoliation and seed an outsized first generation.
    """
    if not profile.get("blb_sensitive"):
        return Risk(key="leaf_beetle", name="Bean Leaf Beetle", level="low",
                    headline=f"{profile['label']} not a host.", detail="", metric="—")

    plant_date = _planting_date_for_start(forecast, start)
    doy = plant_date.timetuple().tm_yday

    history = forecast.get("_history") or {}
    daily = history.get("daily", {})
    tmins = [t for t in (daily.get("temperature_2m_min") or []) if t is not None]
    frost_days = sum(1 for t in tmins if t <= 32)
    avg_tmin = sum(tmins) / len(tmins) if tmins else None

    mild_winter_end = (frost_days <= BLB_LOW_FROST_DAYS) or (avg_tmin is not None and avg_tmin > 35)

    winter_phrase = ("mild dormant-season tail" if mild_winter_end
                     else "cold dormant-season tail")
    headlines = {
        "high": f"Very early planting + {winter_phrase} — overwintered beetles will concentrate here.",
        "moderate": f"Early planting and a {winter_phrase} — expect cotyledon and unifoliate feeding.",
        "low": "Planting date / winter survival don't favor heavy beetle pressure.",
    }
    # Sigmoid on DOY — earlier planting = higher risk. Inverted because lower
    # DOY means higher risk.
    timing_sev = _sigmoid_severity(doy, midpoint=BLB_EARLY_PLANTING_DOY - 7, scale=7.0, inverted=True)
    winter_mult = 1.4 if mild_winter_end else 0.6
    severity = min(1.0, timing_sev * winter_mult)
    level = _level_from_severity(severity)

    return Risk(
        key="leaf_beetle",
        name="Bean Leaf Beetle",
        level=level,
        headline=headlines[level],
        detail=("Overwintered bean leaf beetles emerge from leaf litter in spring and gravitate to "
                "the first soybean fields up. Lam & Pedigo (2000) showed winter survival is driven "
                "by accumulated subfreezing temperatures Oct 1-Apr 15; mild end-of-dormancy weather "
                "leaves more beetles alive. Early-planted fields can carry 5-10× the population of "
                "later plantings and seed a larger pod-feeding F1 generation. Insecticidal seed "
                "treatment or a foliar at V1 protects the stand when pressure is high."),
        metric=f"DOY {doy} · {frost_days}d frost/30d",
        severity=severity,
        curve_type="sigmoid",
    )


def _heat_stress(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Heat stress risk from extreme air temperatures in the planting window.

    Corn pollen viability drops sharply above 95°F; tissue necrosis begins at
    ~113°F (Hatfield & Prueger 2015).  Soybeans suffer pod abortion above 95°F
    and tissue damage at ~108°F (Djanaguiraman et al. 2013).  During the
    germination/emergence window, sustained heat above the stress threshold
    desiccates the seed zone and inhibits root elongation even when soil
    moisture is adequate.  This evaluator mirrors the hard-freeze logic on the
    hot end: locations in extreme-heat climates (deserts, deep tropics) will
    dynamically score near 0% survival.
    """
    temps = _hourly_window(forecast, "temperature_2m", start, start + 48)
    if not temps:
        return Risk(key="heat_stress", name="Heat Stress", level="low",
                    headline="No temperature data available.", detail="", metric="—")

    stress_f = profile.get("heat_stress_f", 95)
    lethal_f = profile.get("heat_lethal_f", 113)
    max_temp = max(temps)
    hours_above_stress = sum(1 for t in temps if t >= stress_f)

    headlines = {
        "high": f"Peak temp {max_temp:.0f}°F with {hours_above_stress}h above {stress_f}°F — severe heat stress.",
        "moderate": f"Peak temp {max_temp:.0f}°F — {hours_above_stress}h above {stress_f}°F stress threshold.",
        "low": f"Peak temp {max_temp:.0f}°F — within safe range for {profile.get('label', 'crop')}.",
    }

    severity = _sigmoid_severity(max_temp, midpoint=stress_f, scale=6.0)
    hours_sev = _sigmoid_severity(hours_above_stress, midpoint=8, scale=4.0)
    severity = max(severity, hours_sev)

    if max_temp >= lethal_f:
        severity = 1.0

    level = _level_from_severity(severity)

    return Risk(
        key="heat_stress",
        name="Heat Stress",
        level=level,
        headline=headlines[level],
        detail=(f"Sustained air temperatures above {stress_f}°F stress {profile.get('label', 'crop')} during "
                f"germination and early growth — desiccating the seed zone and inhibiting root elongation. "
                f"Above {lethal_f}°F, leaf tissue dies outright. Locations in arid/desert climates will "
                "dynamically score near 0% because forecast temperatures exceed crop heat tolerances."),
        metric=f"{max_temp:.0f}°F peak · {hours_above_stress}h>{stress_f}°F",
        severity=severity,
        curve_type="sigmoid",
    )


def _water_scarcity(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Water scarcity risk for rainfed agriculture.

    Mirrors the hard-freeze logic for the dry end: desert/arid locations that
    receive essentially no precipitation cannot support rainfed crop
    germination.  Combines four signals:
      1. Forecast precipitation over the next 14 days (daily sum).
      2. Recent 30-day historical precipitation (Archive API).
      3. USDM drought class (D0–D4).
      4. CPC gridded soil moisture percentile.

    A Nevada desert will dynamically show ~0% survival because the forecast
    returns near-zero precipitation and USDM shows D3/D4.
    """
    daily = forecast.get("daily", {})
    daily_precip = daily.get("precipitation_sum") or []
    forecast_precip_14d = sum(p for p in daily_precip[:14] if p is not None)

    history = forecast.get("_history") or {}
    hist_daily = history.get("daily", {})
    hist_precip = [p for p in (hist_daily.get("precipitation_sum") or []) if p is not None]
    recent_30d_precip = sum(hist_precip)

    drought = forecast.get("_drought") or {}
    dm_class = drought.get("class")  # -1 to 4; 3=extreme, 4=exceptional

    cpc = forecast.get("_cpc_moisture") or {}
    sm_pctl = cpc.get("soil_moisture_pctl") if cpc.get("available") else None

    min_precip = profile.get("min_precip_14d_in", 0.5)
    combined_water = forecast_precip_14d + recent_30d_precip

    precip_sev = _sigmoid_severity(forecast_precip_14d, midpoint=min_precip, scale=0.3, inverted=True)

    hist_sev = _sigmoid_severity(recent_30d_precip, midpoint=1.5, scale=0.8, inverted=True)

    drought_sev = 0.0
    if isinstance(dm_class, int) and dm_class >= 0:
        drought_sev = _metric_severity(dm_class, safe=0, moderate=1, high=3, extreme=4)

    moisture_sev = 0.0
    if sm_pctl is not None:
        moisture_sev = _sigmoid_severity(sm_pctl, midpoint=20, scale=10, inverted=True)

    severity = precip_sev * 0.25 + hist_sev * 0.40 + drought_sev * 0.15 + moisture_sev * 0.20
    severity = min(1.0, severity)

    if isinstance(dm_class, int) and dm_class >= 2:
        if forecast_precip_14d < 0.1 and recent_30d_precip < 0.5:
            severity = max(severity, 0.9)
        if forecast_precip_14d < 0.05 and recent_30d_precip < 0.25:
            severity = 1.0

    drought_phrase = ""
    if isinstance(dm_class, int) and dm_class >= 1:
        drought_phrase = f" USDM: {drought.get('label', '')}."
    moisture_phrase = ""
    if sm_pctl is not None and sm_pctl < 20:
        moisture_phrase = f" CPC soil moisture: {sm_pctl:.0f}th percentile."

    headlines = {
        "high": (f"Only {forecast_precip_14d:.2f}\" forecast + {recent_30d_precip:.1f}\" in the last 30 days "
                 f"— insufficient moisture for germination.{drought_phrase}{moisture_phrase}"),
        "moderate": (f"{forecast_precip_14d:.2f}\" forecast + {recent_30d_precip:.1f}\" recent "
                     f"— marginal moisture for rainfed planting.{drought_phrase}{moisture_phrase}"),
        "low": f"{forecast_precip_14d:.2f}\" forecast + {recent_30d_precip:.1f}\" recent — adequate moisture.",
    }

    level = _level_from_severity(severity)

    return Risk(
        key="water_scarcity",
        name="Water Scarcity",
        level=level,
        headline=headlines[level],
        detail=("Rainfed crops require minimum precipitation for seed imbibition and early root growth. "
                f"{profile.get('label', 'Crop')} needs at least ~{min_precip:.1f}\" in the first 14 days. "
                "Arid/desert locations with near-zero precipitation in both the forecast and recent history "
                "will dynamically score near 0% survival — water is as essential as temperature for germination. "
                "USDM drought classification and CPC soil moisture percentile amplify the signal."),
        metric=f'{forecast_precip_14d:.2f}" fcst · {recent_30d_precip:.1f}" 30d',
        severity=severity,
        curve_type="composite",
    )


def _fusarium_head_blight(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Fusarium head blight (scab) risk for wheat — driven by warm, humid conditions at anthesis."""
    if not profile.get("fusarium_sensitive"):
        return Risk(key="fusarium", name="Fusarium Head Blight", level="low",
                    headline="Crop not susceptible.", detail="", metric="—", severity=0.0)

    temps = _hourly_window(forecast, "temperature_2m", start, start + 72)
    rh = _hourly_window(forecast, "relative_humidity_2m", start, start + 72)
    if not temps or not rh:
        return Risk(key="fusarium", name="Fusarium Head Blight", level="low",
                    headline="Insufficient data.", detail="", metric="—", severity=0.0)

    warm_humid_hours = sum(1 for t, h in zip(temps, rh) if 60 <= t <= 85 and h > 80)
    severity = _sigmoid_severity(warm_humid_hours, midpoint=36, scale=12.0)

    # Wheat-after-corn penalty: F. graminearum survives in corn residue; Purdue
    # documents 5-10x higher FHB infection, UMN shows 2x DON levels. Apply a
    # rotation-dependent severity boost for wheat planted after corn.
    rotation = forecast.get("_rotation") or {}
    years = rotation.get("history") or []
    rot_phrase = ""
    corn_prev = any("corn" in str(y.get("crop", "")).lower() for y in years if isinstance(y, dict))
    wheat_prev = any("wheat" in str(y.get("crop", "")).lower() for y in years if isinstance(y, dict))
    if corn_prev and profile.get("fusarium_sensitive"):
        severity = min(1.0, severity * 1.6 + 0.15)
        rot_phrase = " Wheat-after-corn: 5-10× FHB risk (Purdue)."
    elif wheat_prev and profile.get("fusarium_sensitive"):
        severity = min(1.0, severity * 1.3 + 0.10)
        rot_phrase = " Wheat-on-wheat: elevated residue-borne inoculum."

    level = _level_from_severity(severity)
    headlines = {
        "high": f"{warm_humid_hours}h warm & humid — high Fusarium scab risk at anthesis.{rot_phrase}",
        "moderate": f"{warm_humid_hours}h warm & humid — moderate scab risk.{rot_phrase}",
        "low": f"{warm_humid_hours}h warm & humid — low scab pressure.{rot_phrase}",
    }
    return Risk(key="fusarium", name="Fusarium Head Blight", level=level,
                headline=headlines[level],
                detail="Fusarium graminearum thrives at 60–85°F with RH>80%. "
                       "Extended warm, humid conditions during anthesis sharply increase DON/vomitoxin risk. "
                       "Wheat following corn has 5-10× higher FHB infection due to corn-residue inoculum "
                       "(Purdue). Wheat-after-soybeans is the lowest-risk rotation for scab.",
                metric=f"{warm_humid_hours}h warm+humid", severity=severity, curve_type="sigmoid")


def _tan_spot(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Tan spot (Pyrenophora tritici-repentis) risk for spring wheat.

    Source: NDSU Extension PP-1249, SDSU Extension Chapter 23, UMN Crop News.
    Tan spot is the most prevalent leaf spot disease of spring wheat in the
    northern Great Plains. Combined with Septoria/Stagonospora, can reduce
    yield and test weight by up to 50%. The fungus overwinters on wheat residue
    as pseudothecia; prolonged wet periods (≥24h) drive spore release and
    secondary infection across a wide temperature range.
    """
    if not profile.get("tan_spot_sensitive"):
        return Risk(key="tan_spot", name="Tan Spot", level="low",
                    headline="Crop not susceptible.", detail="", metric="—", severity=0.0)

    rh = _hourly_window(forecast, "relative_humidity_2m", start, start + 168)
    precip_daily = forecast.get("daily", {}).get("precipitation_sum") or []
    precip_7d = sum(p for p in precip_daily[:7] if p is not None)

    if not rh:
        return Risk(key="tan_spot", name="Tan Spot", level="low",
                    headline="Insufficient data.", detail="", metric="—", severity=0.0)

    wet_hours = sum(1 for h in rh if h > 85)
    sev_wet = _sigmoid_severity(wet_hours, midpoint=48, scale=18.0)
    sev_rain = _sigmoid_severity(precip_7d, midpoint=1.2, scale=0.5)
    severity = max(sev_wet, sev_rain * 0.6 + sev_wet * 0.4)

    rotation = forecast.get("_rotation") or {}
    years = rotation.get("history") or []
    wheat_prev = any("wheat" in str(y.get("crop", "")).lower() for y in years if isinstance(y, dict))
    barley_prev = any("barley" in str(y.get("crop", "")).lower() for y in years if isinstance(y, dict))
    rot_phrase = ""
    if wheat_prev:
        severity = min(1.0, severity * 1.5 + 0.20)
        rot_phrase = " Wheat-on-wheat: heavy residue inoculum (NDSU)."
    elif barley_prev:
        severity = min(1.0, severity * 1.2 + 0.10)
        rot_phrase = " Barley in rotation: moderate residue carryover."

    level = _level_from_severity(severity)
    headlines = {
        "high": f"{wet_hours}h high humidity + {precip_7d:.1f}\" rain 7d — high tan spot risk.{rot_phrase}",
        "moderate": f"{wet_hours}h high humidity — moderate tan spot pressure.{rot_phrase}",
        "low": f"Dry conditions — low tan spot risk.{rot_phrase}",
    }
    return Risk(
        key="tan_spot", name="Tan Spot", level=level,
        headline=headlines[level],
        detail="Pyrenophora tritici-repentis overwinters on wheat/barley residue. Prolonged wet "
               "periods (≥24h, RH>85%) drive pseudothecial spore release across a wide temperature "
               "range. Wheat-on-wheat dramatically increases inoculum load — yield and test weight "
               "losses up to 50% (NDSU PP-1249). Rotation to broadleaf crops and resistant varieties "
               "are the primary management tools.",
        metric=f"{wet_hours}h humid · {precip_7d:.1f}\" rain",
        severity=severity, curve_type="sigmoid",
    )


def _common_root_rot(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Common root rot (Bipolaris sorokiniana / Cochliobolus sativus) risk.

    Source: MSU Montana Extension MT201007AG, CPN Encyclopedia, PNW Handbooks,
    Frontiers in Cellular and Infection Microbiology (2021 review).
    Bipolaris sorokiniana causes seedling blight, common root rot, and spot
    blotch in wheat. The pathogen overwinters in soil, stubble, and infected
    seed. Seedling blight risk increases with continuous cereals, plant stress
    (drought or cold injury), and warm wet conditions (68-86°F for spot blotch).
    At planting, the primary risk is seedling blight from soil/residue inoculum.
    """
    if not profile.get("common_root_rot_sensitive"):
        return Risk(key="common_root_rot", name="Common Root Rot", level="low",
                    headline="Crop not susceptible.", detail="", metric="—", severity=0.0)

    soil_temps = _hourly_window(forecast, "soil_temperature_6cm", start, start + 120)
    moist = _hourly_window(forecast, "soil_moisture_1_to_3cm", start, start + 120)

    if not soil_temps:
        return Risk(key="common_root_rot", name="Common Root Rot", level="low",
                    headline="Insufficient data.", detail="", metric="—", severity=0.0)

    avg_soil = sum(soil_temps) / len(soil_temps)
    avg_moist = sum(moist) / len(moist) if moist else 0.25

    sev_cold_stress = _sigmoid_severity(avg_soil, midpoint=40, scale=4.0, inverted=True)
    sev_warm_wet = 0.0
    if avg_soil >= 55:
        sev_warmth = _sigmoid_severity(avg_soil, midpoint=68, scale=8.0)
        sev_moisture = _sigmoid_severity(avg_moist, midpoint=0.30, scale=0.08)
        sev_warm_wet = sev_warmth * 0.6 + sev_moisture * 0.4
    severity = max(sev_cold_stress * 0.5, sev_warm_wet)

    rotation = forecast.get("_rotation") or {}
    years = rotation.get("history") or []
    cereal_years = 0
    for y in years:
        if isinstance(y, dict):
            c = str(y.get("crop", "")).lower()
            if any(g in c for g in ("wheat", "barley", "oat", "rye")):
                cereal_years += 1
    rot_phrase = ""
    if cereal_years >= 2:
        severity = min(1.0, severity * 1.4 + 0.25)
        rot_phrase = f" {cereal_years}yr continuous cereals: high residue inoculum."
    elif cereal_years == 1:
        severity = min(1.0, severity * 1.2 + 0.10)
        rot_phrase = " Cereals in prior year: moderate inoculum carryover."

    level = _level_from_severity(severity)
    headlines = {
        "high": f"Soil {avg_soil:.0f}°F — high common root rot risk.{rot_phrase}",
        "moderate": f"Soil {avg_soil:.0f}°F — moderate root rot pressure.{rot_phrase}",
        "low": f"Soil {avg_soil:.0f}°F — low root rot risk.{rot_phrase}",
    }
    return Risk(
        key="common_root_rot", name="Common Root Rot", level=level,
        headline=headlines[level],
        detail="Cochliobolus sativus (Bipolaris sorokiniana) causes seedling blight and common "
               "root rot in wheat. The pathogen thrives in warm (68-86°F) moist soil but also "
               "attacks cold-stressed seedlings. Continuous cereal rotations dramatically increase "
               "soil inoculum. Seed treatments provide ~3 weeks of protection (CPN). Rotation to "
               "broadleaf crops for 2+ years reduces inoculum (MSU Montana MT201007AG).",
        metric=f"{avg_soil:.0f}°F soil · {avg_moist:.2f} VWC · {cereal_years}yr cereal",
        severity=severity, curve_type="composite",
    )


def _white_mold(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """White mold (Sclerotinia) risk for dry beans and soybeans — canopy closure + moisture."""
    if not profile.get("white_mold_sensitive"):
        return Risk(key="white_mold", name="White Mold", level="low",
                    headline="Crop not susceptible.", detail="", metric="—", severity=0.0)

    temps = _hourly_window(forecast, "temperature_2m", start, start + 72)
    rh = _hourly_window(forecast, "relative_humidity_2m", start, start + 72)
    precip_daily = forecast.get("daily", {}).get("precipitation_sum") or []
    precip_7d = sum(p for p in precip_daily[:7] if p is not None)

    if not temps or not rh:
        return Risk(key="white_mold", name="White Mold", level="low",
                    headline="Insufficient data.", detail="", metric="—", severity=0.0)

    cool_moist_hours = sum(1 for t, h in zip(temps, rh) if 55 <= t <= 77 and h > 85)
    sev_hours = _sigmoid_severity(cool_moist_hours, midpoint=30, scale=10.0)
    sev_rain = _sigmoid_severity(precip_7d, midpoint=1.5, scale=0.6)
    severity = max(sev_hours, sev_rain * 0.7 + sev_hours * 0.3)

    # Row spacing modifier: narrow rows close the canopy earlier and create
    # the humid microclimate Sclerotinia needs. CPN + UMN show 30" rows can
    # cut white mold severity ~50% vs 15" rows; this is the most actionable
    # management lever. Defaults to neutral if row spacing not provided.
    row_spacing = getattr(inputs, "row_spacing_in", None)
    row_phrase = ""
    if row_spacing is not None:
        if row_spacing <= 15:
            severity = min(1.0, severity * 1.3)
            row_phrase = " Narrow rows (≤15\") accelerate canopy closure — elevated Sclerotinia risk."
        elif row_spacing >= 30:
            severity *= 0.6
            row_phrase = " Wide rows (≥30\") delay canopy closure — reduced white mold risk."

    # Dry beans: white mold is THE #1 disease in Michigan dry beans (MSU
    # Extension). Shorter, denser canopy creates a more favorable Sclerotinia
    # microclimate than soybeans. Amplify severity 1.2× for beans.
    bean_phrase = ""
    if profile.get("label") == "Dry Beans":
        severity = min(1.0, severity * 1.2)
        bean_phrase = " Dry beans — dense canopy architecture elevates Sclerotinia risk vs. soybeans."

    rotation = forecast.get("_rotation") or {}
    rot_phrase = ""
    if rotation.get("available") and rotation.get("soy_on_soy"):
        severity = min(1.0, severity + 0.1)
        rot_phrase = " Soy-on-soy increases sclerotia bank."
    elif rotation.get("available"):
        prev = rotation.get("prev_crop_code")
        if prev in (42, 5):
            severity = min(1.0, severity + 0.1)
            rot_phrase = " Bean/soy in prior rotation builds sclerotia bank."

    level = _level_from_severity(severity)
    headlines = {
        "high": f"{cool_moist_hours}h favorable for Sclerotinia + {precip_7d:.1f}\" precip — high white mold risk.{row_phrase}{rot_phrase}{bean_phrase}",
        "moderate": f"Moderate white mold conditions: {cool_moist_hours}h cool/moist.{row_phrase}{rot_phrase}{bean_phrase}",
        "low": f"Low white mold pressure in current forecast.{row_phrase}",
    }
    return Risk(key="white_mold", name="White Mold", level=level,
                headline=headlines[level],
                detail="Sclerotinia sclerotiorum apothecia germinate at 55–77°F with prolonged leaf wetness. "
                       "Dense canopy + frequent rain = apothecial survival and ascospore release. "
                       "Moving from 15\" to 30\" rows can reduce white mold severity up to 50% (CPN, UMN). "
                       "Soy-on-soy builds the sclerotia bank in the soil surface.",
                metric=f"{cool_moist_hours}h favorable · {precip_7d:.1f}\" 7d", severity=severity, curve_type="sigmoid")


def _cercospora_leaf_spot(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Cercospora leaf spot risk for sugar beets — NDAWN/MSU DIV-style model.

    Source: NDAWN Sugarbeet Cercospora model; MSU Extension; Windels et al.
    (PMC 8470031). Infection requires RH ≥ 85% with temperature weighting:
    optimal 75-90°F day / >60°F night. DIV 0-7 per day, accumulated over
    3 days. Two-day totals: 1-3 slight, 4-6 moderate, 7-14 severe.
    """
    if not profile.get("cercospora_sensitive"):
        return Risk(key="cercospora", name="Cercospora Leaf Spot", level="low",
                    headline="Crop not susceptible.", detail="", metric="—", severity=0.0)

    temps = _hourly_window(forecast, "temperature_2m", start, start + 72)
    rh = _hourly_window(forecast, "relative_humidity_2m", start, start + 72)
    if not temps or not rh:
        return Risk(key="cercospora", name="Cercospora Leaf Spot", level="low",
                    headline="Insufficient data.", detail="", metric="—", severity=0.0)

    div_total = 0.0
    for day in range(3):
        day_start = day * 24
        day_end = min(day_start + 24, len(temps))
        if day_end <= day_start:
            break
        day_temps = temps[day_start:day_end]
        day_rh = rh[day_start:day_end]
        humid_hours = sum(1 for h in day_rh if h >= 85)
        if humid_hours < 4:
            continue
        night_temps = [t for i, t in enumerate(day_temps) if i < 6 or i >= 18]
        warm_nights = all(t > 60 for t in night_temps) if night_temps else False
        day_max = max(day_temps) if day_temps else 60
        temp_weight = _gaussian_severity(day_max, peak=82.0, sigma=12.0)
        daily_div = min(7.0, (humid_hours / 24.0) * temp_weight * 7.0)
        if warm_nights:
            daily_div = min(7.0, daily_div * 1.3)
        div_total += daily_div

    severity = _sigmoid_severity(div_total, midpoint=7.0, scale=3.0)
    level = _level_from_severity(severity)
    headlines = {
        "high": f"DIV {div_total:.1f} — high Cercospora risk. Begin or continue fungicide program.",
        "moderate": f"DIV {div_total:.1f} — moderate Cercospora pressure. Monitor for first lesions.",
        "low": f"DIV {div_total:.1f} — low Cercospora pressure currently.",
    }
    return Risk(key="cercospora", name="Cercospora Leaf Spot", level=level,
                headline=headlines[level],
                detail="Cercospora beticola sporulation modeled via Daily Infection Values (NDAWN/MSU). "
                       "RH ≥ 85% drives spore germination; optimal infection at 75–90°F daytime with "
                       "nights above 60°F preventing dew-off. 3-day DIV accumulation: "
                       "1–6 slight/moderate, 7+ severe. Primary sugar beet foliar disease in Michigan.",
                metric=f"DIV {div_total:.1f} (3d)", severity=severity, curve_type="sigmoid")


def _bolting_risk(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Bolting risk for sugar beets — vernalization-intensity model.

    Source: Milford et al. (J. Agric. Sci.); PLOS ONE 2024 (10.1371/
    journal.pone.0339856). Vernalization occurs at 32–55°F (0–13°C) with
    optimum near 43–50°F (6–10°C). Each hour in the range accumulates
    vernalization intensity weighted by distance from optimum. Days with
    max temp > 73°F (23°C) cause de-vernalization. Threshold: 107–134
    vernalizing hours depending on genotype; 120h is mid-range.
    """
    if not profile.get("bolting_cold_hours"):
        return Risk(key="bolting", name="Bolting Risk", level="low",
                    headline="Not applicable.", detail="", metric="—", severity=0.0)

    temps = _hourly_window(forecast, "temperature_2m", start, start + 168)
    if not temps:
        return Risk(key="bolting", name="Bolting Risk", level="low",
                    headline="Insufficient data.", detail="", metric="—", severity=0.0)

    bolt_ceiling = profile.get("bolting_temp_f", 50)
    bolt_floor = profile.get("bolting_base_f", 32)
    threshold = profile.get("bolting_cold_hours", 120)
    optimum_f = 46.0

    vern_hours = 0.0
    for day_start in range(0, len(temps), 24):
        day_chunk = temps[day_start:day_start + 24]
        if not day_chunk:
            break
        day_max = max(day_chunk)
        if day_max > 73:
            continue
        for t in day_chunk:
            if bolt_floor <= t <= bolt_ceiling:
                weight = max(0.0, 1.0 - abs(t - optimum_f) / 18.0)
                vern_hours += weight

    severity = _sigmoid_severity(vern_hours, midpoint=threshold * 0.5, scale=threshold * 0.2)
    level = _level_from_severity(severity)
    headlines = {
        "high": f"{vern_hours:.0f} weighted vernalizing hours — significant bolting induction risk.",
        "moderate": f"{vern_hours:.0f} weighted vernalizing hours — some bolting induction possible.",
        "low": f"Only {vern_hours:.0f} weighted vernalizing hours — minimal bolting risk.",
    }
    return Risk(key="bolting", name="Bolting Risk", level=level,
                headline=headlines[level],
                detail=f"Sugar beets vernalize at 32–{bolt_ceiling}°F (optimum ~46°F). "
                       f">{threshold} weighted cold hours can induce bolting → unmarketable roots. "
                       "Days above 73°F reverse vernalization. Based on Milford et al. and "
                       "2024 PLOS ONE vernalization-intensity model across 12 genotypes.",
                metric=f"{vern_hours:.0f}h vern (wtd)", severity=severity, curve_type="sigmoid")


def _aphanomyces(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Aphanomyces cochlioides seedling damping-off — warm, saturated soils.

    Source: UNL EC1897 "Sugarbeet Seedling Diseases"; PNW Pest Handbook;
    Windels & Brantner (2005). A. cochlioides zoospores require free water
    to swim to roots. Favored by warm (60–80°F), wet soils. Rotation with
    host crops (beans, spinach) and prior sugar beet history elevate inoculum.
    Unlike Pythium, Aphanomyces is NOT controlled by metalaxyl seed treatments.
    """
    if not profile.get("aphanomyces_sensitive"):
        return Risk(key="aphanomyces", name="Aphanomyces Damping-Off", level="low",
                    headline="Crop not susceptible.", detail="", metric="—", severity=0.0)

    soil = _hourly_window(forecast, "soil_temperature_6cm", start, start + 120)
    moist = _hourly_window(forecast, "soil_moisture_1_to_3cm", start, start + 120)
    if not soil or not moist:
        return Risk(key="aphanomyces", name="Aphanomyces Damping-Off", level="low",
                    headline="Insufficient soil data.", detail="", metric="—", severity=0.0)

    avg_soil = _avg(soil)
    avg_moist = _avg(moist)
    sat_hours = _saturated_hours(moist, 0.38)

    temp_sev = _gaussian_severity(avg_soil, peak=70.0, sigma=12.0)
    moist_sev = _sigmoid_severity(sat_hours, midpoint=36.0, scale=15.0)
    severity = temp_sev * 0.45 + moist_sev * 0.55

    rotation = forecast.get("_rotation") or {}
    rotation_phrase = ""
    if rotation.get("available"):
        prev = rotation.get("prev_crop_code")
        if prev == 41:
            severity = min(1.0, severity + 0.15)
            rotation_phrase = " Sugar beet-on-sugar beet elevates Aphanomyces inoculum."
        elif prev in (42, 5):
            severity = min(1.0, severity + 0.08)
            rotation_phrase = " Bean/legume in prior rotation can harbor Aphanomyces."

    soil_profile = forecast.get("_soil_profile") or {}
    drainage_text = (soil_profile.get("drainage_class") or "").lower()
    if any(t in drainage_text for t in ("poorly drained", "very poorly drained")):
        severity = min(1.0, severity + 0.10)

    level = _level_from_severity(severity)
    headlines = {
        "high": f"Warm ({avg_soil:.0f}°F) saturated soil ({sat_hours:.0f}h) — high Aphanomyces risk.{rotation_phrase}",
        "moderate": f"Warm, moist conditions — moderate Aphanomyces pressure.{rotation_phrase}",
        "low": f"Conditions not strongly conducive to Aphanomyces.{rotation_phrase}",
    }
    return Risk(
        key="aphanomyces", name="Aphanomyces Damping-Off", level=level,
        headline=headlines[level],
        detail="Aphanomyces cochlioides zoospores require free soil water to swim to sugar beet "
               "roots. Favored by 60–80°F saturated soils. Causes post-emergence damping-off with "
               "characteristic dark, thread-like hypocotyls. NOT controlled by metalaxyl/mefenoxam "
               "seed treatments (unlike Pythium). Hymexazol (Tachigaren) is the primary seed "
               "treatment. Rotation away from sugar beets for 3+ years reduces inoculum (UNL, MSU).",
        metric=f"{avg_soil:.0f}°F · {sat_hours:.0f}h sat",
        severity=severity, curve_type="gaussian",
    )


def _sugar_beet_cyst_nematode(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Sugar beet cyst nematode (Heterodera schachtii) — modifier for root diseases.

    Source: UC IPM; Michigan Sugar Company; SBREB. SBCN weakens root systems
    and creates entry points for Rhizoctonia, Aphanomyces, and Pythium.
    Damage threshold: 1–2 eggs/g soil (Imperial Valley), ~50 eggs/ml in
    northern production. Yield loss 25–50%+ in heavily infested fields.
    Acts as a modifier amplifying downstream root disease factors.
    """
    if not profile.get("sbcn_sensitive"):
        return Risk(key="sbcn", name="Sugar Beet Cyst Nematode", level="low",
                    headline="Crop not susceptible.", detail="", metric="—", severity=0.0)

    soil = _hourly_window(forecast, "soil_temperature_6cm", start, start + 168)
    avg_soil = _avg(soil) if soil else 55.0

    rotation = forecast.get("_rotation") or {}
    severity = 0.0
    rotation_phrase = ""
    if rotation.get("available"):
        years = rotation.get("history") or []
        beet_years = sum(1 for y in years if isinstance(y, dict) and
                         (y.get("cdl_code") == 41 or "sugar" in str(y.get("crop", "")).lower()))
        if beet_years >= 2:
            severity = 0.75
            rotation_phrase = f" {beet_years} sugar beet years in rotation — high SBCN buildup."
        elif beet_years == 1:
            severity = 0.45
            rotation_phrase = " Recent sugar beet crop — moderate SBCN carryover."
    if not rotation.get("available"):
        severity = 0.25
        rotation_phrase = " No rotation data — SBCN risk unknown."

    temp_active = _sigmoid_severity(avg_soil, midpoint=50.0, scale=8.0)
    severity = severity * (0.6 + 0.4 * temp_active)

    level = _level_from_severity(severity)
    headlines = {
        "high": f"High SBCN risk — amplifies root disease pressure.{rotation_phrase}",
        "moderate": f"Moderate SBCN pressure — monitor root health.{rotation_phrase}",
        "low": f"Low SBCN risk in current rotation.{rotation_phrase}",
    }
    return Risk(
        key="sbcn", name="Sugar Beet Cyst Nematode", level=level,
        headline=headlines[level],
        detail="Heterodera schachtii cysts persist in soil for years. Larvae penetrate roots, "
               "creating wound sites that amplify Rhizoctonia, Aphanomyces, and Pythium infection. "
               "Yield losses of 25–50%+ documented in heavily infested fields. Resistant/tolerant "
               "varieties and 3+ year rotations away from beets are primary management (MSU, UC IPM).",
        metric=f"Rotation risk",
        severity=severity, curve_type="sigmoid",
    )


def _wind_damage(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Wind/sand damage to cotyledon-stage sugar beet seedlings.

    Source: Alberta Sugar Beet Growers; MSU Extension; UNL. At cotyledon
    stage, seedlings are susceptible to sand-blasting from wind-driven soil
    particles. At 2-4 true leaf stage, wind can twist/whip leaves off.
    Sustained winds >25 mph with dry, exposed soil are the primary risk.
    Cover crops and surface residue reduce risk substantially.
    """
    if not profile.get("wind_damage_sensitive"):
        return Risk(key="wind_damage", name="Wind / Sand Damage", level="low",
                    headline="Crop not susceptible.", detail="", metric="—", severity=0.0)

    wind = _hourly_window(forecast, "wind_speed_10m", start, start + 72)
    gusts = _hourly_window(forecast, "wind_gusts_10m", start, start + 72)
    precip_daily = forecast.get("daily", {}).get("precipitation_sum") or []
    precip_3d = sum(p for p in precip_daily[:3] if p is not None)
    if not wind:
        return Risk(key="wind_damage", name="Wind / Sand Damage", level="low",
                    headline="Insufficient wind data.", detail="", metric="—", severity=0.0)

    max_sustained = max(wind)
    max_gust = max(gusts) if gusts else max_sustained
    high_wind_hours = sum(1 for w in wind if w > 25)

    wind_sev = _sigmoid_severity(high_wind_hours, midpoint=8.0, scale=4.0)
    gust_sev = _sigmoid_severity(max_gust, midpoint=35.0, scale=10.0)
    severity = max(wind_sev, gust_sev)

    if precip_3d < 0.1:
        severity = min(1.0, severity * 1.3)
    elif precip_3d > 0.5:
        severity *= 0.5

    if inputs.tillage == "no-till" or inputs.residue == "heavy":
        severity *= 0.6

    level = _level_from_severity(severity)
    headlines = {
        "high": f"{high_wind_hours}h >25 mph, gusts {max_gust:.0f} mph — high sand blast risk to seedlings.",
        "moderate": f"Moderate wind exposure ({high_wind_hours}h >25 mph) — monitor emerged seedlings.",
        "low": "Wind conditions acceptable for sugar beet seedlings.",
    }
    return Risk(
        key="wind_damage", name="Wind / Sand Damage", level=level,
        headline=headlines[level],
        detail="Sugar beet cotyledons are highly susceptible to sand-blasting from wind-driven "
               "soil particles. At 2-4 true leaves, wind can twist and whip leaves off the plant. "
               "Sustained winds >25 mph with dry, bare soil create the highest risk. Surface "
               "residue and cover crops provide significant protection (Alberta, MSU Extension).",
        metric=f"{high_wind_hours}h >25mph · gust {max_gust:.0f}",
        severity=severity, curve_type="sigmoid",
    )


def _root_maggot(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Sugar beet root maggot (Tetanops myopaeformis) — GDD phenology model.

    Source: NDAWN SBRM model; Bechinski (Idaho); PNW Pest Handbook. Adults
    emerge when GDD (base 47.5°F) reaches ~300-550, laying eggs near young
    sugar beet plants May-June. Larvae feed on taproots through mid-July.
    Can sever seedling taproots causing stand loss, or scar older roots
    reducing yield and sugar content.
    """
    if not profile.get("root_maggot_sensitive"):
        return Risk(key="root_maggot", name="Root Maggot", level="low",
                    headline="Crop not susceptible.", detail="", metric="—", severity=0.0)

    soil = _hourly_window(forecast, "soil_temperature_6cm", start, start + 168)
    air = _hourly_window(forecast, "temperature_2m", start, start + 168)
    if not air:
        return Risk(key="root_maggot", name="Root Maggot", level="low",
                    headline="Insufficient data.", detail="", metric="—", severity=0.0)

    gdd_base = 47.5
    gdd_accum = 0.0
    for day_start in range(0, len(air), 24):
        day_chunk = air[day_start:day_start + 24]
        if not day_chunk:
            break
        day_max = max(day_chunk)
        day_min = min(day_chunk)
        daily_gdd = max(0.0, ((day_max + day_min) / 2.0) - gdd_base)
        gdd_accum += daily_gdd

    enviro = forecast.get("_enviroweather") or {}
    if enviro.get("available"):
        season_gdd = (enviro.get("gdd") or {}).get("base48")
        if season_gdd is not None:
            gdd_accum = max(gdd_accum, season_gdd)

    flight_sev = _gaussian_severity(gdd_accum, peak=425.0, sigma=120.0)
    soil_moist = _hourly_window(forecast, "soil_moisture_1_to_3cm", start, start + 168)
    moist_sev = _sigmoid_severity(_avg(soil_moist), midpoint=0.28, scale=0.10) if soil_moist else 0.3
    severity = flight_sev * 0.65 + moist_sev * 0.35

    rotation = forecast.get("_rotation") or {}
    rotation_phrase = ""
    if rotation.get("available"):
        prev = rotation.get("prev_crop_code")
        if prev == 41:
            severity = min(1.0, severity + 0.15)
            rotation_phrase = " Prior sugar beets increase root maggot carryover."

    level = _level_from_severity(severity)
    headlines = {
        "high": f"GDD {gdd_accum:.0f} (base 47.5°F) — peak root maggot adult flight window.{rotation_phrase}",
        "moderate": f"GDD {gdd_accum:.0f} — approaching root maggot activity window.{rotation_phrase}",
        "low": f"GDD {gdd_accum:.0f} — outside primary root maggot flight window.{rotation_phrase}",
    }
    return Risk(
        key="root_maggot", name="Root Maggot", level=level,
        headline=headlines[level],
        detail="Tetanops myopaeformis adults emerge at ~300–550 GDD (base 47.5°F), laying eggs "
               "near sugar beet crowns May-June. Larvae feed on taproots — can sever seedling "
               "taproots causing stand loss, or produce black scarring on older roots reducing "
               "yield and sugar content. Clothianidin seed treatment or at-plant granular "
               "insecticide is standard management (NDAWN, PNW Handbook).",
        metric=f"GDD {gdd_accum:.0f} (b47.5)",
        severity=severity, curve_type="gaussian",
    )


def _winterkill_risk(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Winterkill risk for alfalfa — ice sheeting, heaving, cold crown temps.

    Source: CPN "Winterkill and Winter Injury in Alfalfa"; MSU Extension
    E-2310 "Avoiding Winter Injury to Alfalfa"; UW-Extension Team Forage;
    Dairyland Seed spring assessment guide.

    Three mechanisms:
      1. Crown cold injury: hardened crowns survive to 0-5°F; unhardened or
         poorly managed stands are damaged below 15°F.  Without snow cover,
         crown temps track air temps closely.
      2. Freeze-thaw heaving: repeated cycles on heavy / poorly drained soils
         push crowns above the soil surface → desiccation.  Worst in late
         winter / early spring on clay soils.
      3. Ice sheeting: >3 weeks under ice suffocates crowns via toxic
         metabolite accumulation (CO2, ethanol).  >30 days is often lethal.
    """
    if not profile.get("winterkill_sensitive") or profile.get("fall_planted"):
        return Risk(key="winterkill_alfalfa", name="Winterkill Risk", level="low",
                    headline="Not applicable.", detail="", metric="—", severity=0.0)

    temps = _hourly_window(forecast, "temperature_2m", start, start + 168)
    if not temps:
        return Risk(key="winterkill", name="Winterkill Risk", level="low",
                    headline="Insufficient data.", detail="", metric="—", severity=0.0)

    min_temp = min(temps) if temps else 40
    freeze_thaw = 0
    for i in range(1, len(temps)):
        if temps[i - 1] < 32 and temps[i] > 40:
            freeze_thaw += 1

    # Crown cold injury: midpoint 5°F (hardened crown damage onset per CPN).
    sev_cold = _sigmoid_severity(min_temp, midpoint=5, scale=6, inverted=True)
    # Heaving: 2+ freeze-thaw cycles in a week is significant on heavy soils.
    sev_ft = _sigmoid_severity(freeze_thaw, midpoint=2, scale=1.5)

    # Soil drainage amplifier: poorly drained / clay soils heave more.
    soil_profile = forecast.get("_soil_profile") or {}
    drainage = (soil_profile.get("drainage_class") or "").lower()
    clay_pct = soil_profile.get("clay_pct") or 0
    poor_drain = any(t in drainage for t in
                     ("poorly drained", "very poorly drained",
                      "somewhat poorly drained"))
    if poor_drain or clay_pct >= 27:
        sev_ft = min(1.0, sev_ft + 0.15)

    # Precipitation / snow: >1" total precip in the 7-day window at temps below
    # 32°F suggests potential ice sheeting conditions.
    precip = forecast.get("daily", {}).get("precipitation_sum") or []
    total_precip_7d = sum(p for p in precip[:7] if p is not None)
    avg_temp_7d = _avg(temps[:168]) if temps else 40
    sev_ice = 0.0
    if avg_temp_7d < 33 and total_precip_7d > 1.0:
        sev_ice = _sigmoid_severity(total_precip_7d, midpoint=1.5, scale=0.6)

    severity = max(sev_cold, sev_ft, sev_ice)
    level = _level_from_severity(severity)

    drain_note = f" Soil: {soil_profile['drainage_class'].lower()}." if soil_profile.get("drainage_class") else ""
    headlines = {
        "high": f"Min {min_temp:.0f}°F + {freeze_thaw} freeze-thaw cycles — high winterkill risk.{drain_note}",
        "moderate": f"Min {min_temp:.0f}°F, {freeze_thaw} F/T cycles — moderate crown stress.{drain_note}",
        "low": f"Min {min_temp:.0f}°F — acceptable for alfalfa crowns.{drain_note}",
    }
    return Risk(key="winterkill", name="Winterkill Risk", level=level,
                headline=headlines[level],
                detail="Hardened alfalfa crowns survive to 0-5°F with snow cover (CPN). Without snow, "
                       "crown temps track air temps and damage begins at 10-15°F. Repeated freeze-thaw "
                       "cycles on heavy soils cause heaving — crowns forced above the surface desiccate. "
                       "Ice sheeting >3 weeks suffocates crowns via toxic metabolite accumulation.",
                metric=f"{min_temp:.0f}°F min · {freeze_thaw} F/T · {total_precip_7d:.1f}\" precip",
                severity=severity, curve_type="composite")


def _autotoxicity(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Autotoxicity risk when planting alfalfa after alfalfa.

    Source: UW-Extension "Understanding Autotoxicity in Alfalfa"; MSU "Can We
    Solve the Mystery of Alfalfa Autotoxicity"; Frontiers in Plant Science
    2022 (Medicago truncatula autotoxicity).

    Medicarpin and related allelochemicals are concentrated in top growth and
    roots; the 16-inch radius around established plants is the zone of
    influence.  Wisconsin studies:
      - Reseeded 2 weeks after kill: 80% yield reduction
      - Reseeded 4 weeks after kill: 30-50% yield reduction
      - 1+ year rotation: minimal impact
    No-till exacerbates the problem because allelochemicals degrade more slowly
    without incorporation.
    """
    if not profile.get("autotoxicity_sensitive"):
        return Risk(key="autotoxicity", name="Autotoxicity", level="low",
                    headline="Not applicable.", detail="", metric="—", severity=0.0)

    rotation = forecast.get("_rotation") or {}
    years = rotation.get("history") or []

    # Count how many of the last 3 CropScape years show alfalfa (CDL 36).
    alfalfa_years = 0
    most_recent_alfalfa = 0
    for i, y in enumerate(years):
        if not isinstance(y, dict):
            continue
        is_alf = ("alfalfa" in str(y.get("crop", "")).lower() or y.get("cdl_code") == 36
                  or y.get("crop_code") == 36)
        if is_alf:
            alfalfa_years += 1
            if most_recent_alfalfa == 0:
                most_recent_alfalfa = i + 1

    if alfalfa_years == 0:
        return Risk(key="autotoxicity", name="Autotoxicity", level="low",
                    headline="No recent alfalfa detected — autotoxicity unlikely.",
                    detail="", metric="Clear", severity=0.0)

    # Scale severity by recency and number of alfalfa years.
    if most_recent_alfalfa <= 1:
        base_sev = 0.85
    elif most_recent_alfalfa == 2:
        base_sev = 0.55
    else:
        base_sev = 0.30

    # Multiple consecutive alfalfa years = more allelochemical accumulation.
    if alfalfa_years >= 3:
        base_sev = min(1.0, base_sev + 0.10)

    # No-till delays allelochemical degradation (UW-Extension).
    if inputs.tillage == "no-till":
        base_sev = min(1.0, base_sev + 0.10)

    severity = base_sev
    level = _level_from_severity(severity)

    recency_label = f"{most_recent_alfalfa} yr ago" if most_recent_alfalfa > 0 else "recent"
    till_note = " No-till delays allelochemical breakdown." if inputs.tillage == "no-till" else ""
    headlines = {
        "high": f"Alfalfa detected {recency_label} ({alfalfa_years}/3 yr) — strong autotoxicity risk.{till_note}",
        "moderate": f"Alfalfa detected {recency_label} — moderate autotoxicity risk. Consider 1+ yr rotation.{till_note}",
        "low": f"Alfalfa {recency_label} — autotoxicity declining but monitor stand.{till_note}",
    }
    return Risk(key="autotoxicity", name="Autotoxicity", level=level,
                headline=headlines[level],
                detail="Alfalfa releases medicarpin and other allelochemicals that inhibit new alfalfa "
                       "seedling establishment within a 16-inch radius. Wisconsin studies showed 80% yield "
                       "reduction when reseeded 2 weeks after kill, 30-50% at 4 weeks, and minimal impact "
                       "after 1+ year rotation. MSU Extension recommends at least 1 year between stands. "
                       "Tillage accelerates allelochemical breakdown vs. no-till.",
                metric=f"{alfalfa_years}/3 yr alfalfa · {recency_label}",
                severity=severity, curve_type="sigmoid")


def _aphanomyces_alfalfa(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Aphanomyces root rot in alfalfa — Aphanomyces euteiches.

    Source: UMN Extension "Seedling Diseases — Alfalfa"; MSU "Be Familiar with
    Root Rot Diseases of Alfalfa"; UW-Extension "Damping Off and Root Rot
    Caused by Phytophthora and Pythium".

    A. euteiches zoospores require free water to swim to roots.  Unlike
    Phytophthora (which kills seedlings quickly), Aphanomyces stunts plants
    with chlorosis before wilting.  Poorly drained soils with history of
    alfalfa production build inoculum.  Race 2 is dominant in MN.
    """
    if not profile.get("aphanomyces_alfalfa_sensitive"):
        return Risk(key="aphanomyces_alfalfa", name="Aphanomyces Root Rot", level="low",
                    headline=f"{profile['label']} not a primary host.", detail="", metric="—",
                    severity=0.0)

    soil = _hourly_window(forecast, "soil_temperature_6cm", start, start + 168)
    moist = _hourly_window(forecast, "soil_moisture_1_to_3cm", start, start + 168)
    if not soil or not moist:
        return Risk(key="aphanomyces_alfalfa", name="Aphanomyces Root Rot", level="low",
                    headline="No soil-data signal.", detail="", metric="—", severity=0.0)

    avg_soil = _avg(soil)
    sat_hours = _saturated_hours(moist, 0.38)

    # Zoospore activity: optimum 60-77°F, ramps at edges.
    temp_sev = _trapezoidal_severity(avg_soil, a=50.0, b=60.0, c=77.0, d=86.0)
    sat_sev = _sigmoid_severity(sat_hours, midpoint=30.0, scale=12.0)
    severity = temp_sev * 0.55 + sat_sev * 0.45

    # Poorly drained soils hold free water that zoospores need.
    soil_profile = forecast.get("_soil_profile") or {}
    drainage = (soil_profile.get("drainage_class") or "").lower()
    poor_drain = any(t in drainage for t in
                     ("poorly drained", "very poorly drained",
                      "somewhat poorly drained"))
    if poor_drain:
        severity = min(1.0, severity + 0.12)

    # Prior alfalfa builds A. euteiches inoculum in soil.
    rotation = forecast.get("_rotation") or {}
    years = rotation.get("history") or []
    alf_history = any("alfalfa" in str(y.get("crop", "")).lower() or y.get("cdl_code") == 36
                      for y in years if isinstance(y, dict))
    rotation_phrase = ""
    if alf_history:
        severity = min(1.0, severity + 0.12)
        rotation_phrase = " Prior alfalfa increases Aphanomyces inoculum."

    level = _level_from_severity(severity)
    drain_note = f" SSURGO: {soil_profile['drainage_class'].lower()}." if soil_profile.get("drainage_class") else ""
    headlines = {
        "high": f"Saturated {sat_hours}h at {avg_soil:.0f}°F — high Aphanomyces risk.{drain_note}{rotation_phrase}",
        "moderate": f"Moist soil ({sat_hours}h saturated, {avg_soil:.0f}°F) — borderline Aphanomyces conditions.{drain_note}{rotation_phrase}",
        "low": f"Soil drainage adequate — low Aphanomyces risk.{drain_note}{rotation_phrase}",
    }
    return Risk(
        key="aphanomyces_alfalfa", name="Aphanomyces Root Rot", level=level,
        headline=headlines[level],
        detail="Aphanomyces euteiches zoospores require free water (saturated soil) to reach alfalfa "
               "roots. Infected seedlings become stunted and chlorotic before wilting. Poorly drained "
               "fields with prior alfalfa production accumulate oospore inoculum that persists 10+ years "
               "in soil. Aphanomyces-resistant varieties and improved drainage are the primary controls "
               "(UMN Extension, MSU).",
        metric=f"{avg_soil:.0f}°F · {sat_hours}h sat",
        severity=severity, curve_type="composite",
    )


def _sclerotinia_crown(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Sclerotinia crown and stem rot — Sclerotinia trifoliorum.

    Source: CPN "Sclerotinia Crown and Stem Rot in Alfalfa"; OSU Fact Sheet;
    Kentucky IPM; UC IPM.

    S. trifoliorum is most active at 50-68°F (10-20°C) with continuous
    moisture.  Infects fall-seeded alfalfa seedlings under cool moist
    conditions.  Symptoms appear in early spring — white cottony mycelium on
    crowns, soft tan rot.  Sclerotia persist in soil 3-5 years.
    """
    if not profile.get("sclerotinia_crown_sensitive"):
        return Risk(key="sclerotinia_crown", name="Sclerotinia Crown Rot", level="low",
                    headline=f"{profile['label']} not a primary host.", detail="", metric="—",
                    severity=0.0)

    air = _hourly_window(forecast, "temperature_2m", start, start + 168)
    humid = _hourly_window(forecast, "relative_humidity_2m", start, start + 168)
    moist = _hourly_window(forecast, "soil_moisture_1_to_3cm", start, start + 168)
    if not air:
        return Risk(key="sclerotinia_crown", name="Sclerotinia Crown Rot", level="low",
                    headline="Insufficient data.", detail="", metric="—", severity=0.0)

    avg_air = _avg(air)
    # Temperature: peak activity 50-68°F.
    temp_sev = _trapezoidal_severity(avg_air, a=40.0, b=50.0, c=68.0, d=78.0)

    # Moisture: prolonged humidity + wet soil favor mycelium growth.
    humid_hours = sum(1 for h in humid if h > 85) if humid else 0
    humid_sev = _sigmoid_severity(humid_hours, midpoint=60.0, scale=20.0)
    moist_sev = _sigmoid_severity(_avg(moist), midpoint=0.35, scale=0.08) if moist else 0.3

    severity = temp_sev * 0.45 + humid_sev * 0.30 + moist_sev * 0.25

    # Prior alfalfa or clover builds sclerotia inoculum in soil.
    rotation = forecast.get("_rotation") or {}
    years = rotation.get("history") or []
    alf_history = any("alfalfa" in str(y.get("crop", "")).lower() or y.get("cdl_code") == 36
                      for y in years if isinstance(y, dict))
    if alf_history:
        severity = min(1.0, severity + 0.10)

    level = _level_from_severity(severity)
    headlines = {
        "high": f"Cool ({avg_air:.0f}°F) + {humid_hours}h high humidity — peak Sclerotinia conditions.",
        "moderate": f"Cool-moist ({avg_air:.0f}°F, {humid_hours}h humid) — monitor for crown rot.",
        "low": f"Conditions outside Sclerotinia optimum ({avg_air:.0f}°F).",
    }
    return Risk(
        key="sclerotinia_crown", name="Sclerotinia Crown Rot", level=level,
        headline=headlines[level],
        detail="Sclerotinia trifoliorum infects alfalfa crowns in cool (50-68°F), continuously "
               "moist conditions. White cottony mycelium colonizes crowns and lower stems, causing "
               "soft tan rot. Sclerotia persist in soil 3-5 years. Late-summer and fall seedings "
               "are at highest risk. Improved air circulation, wider row spacing, and resistant "
               "varieties are the primary controls (CPN, OSU).",
        metric=f"{avg_air:.0f}°F · {humid_hours}h humid",
        severity=severity, curve_type="composite",
    )


def _potato_leafhopper(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Potato leafhopper (Empoasca fabae) — hopperburn in alfalfa.

    Source: UW-Extension "Potato Leafhopper Damage to Alfalfa"; UNL G1136;
    Oxford Academic / J. Integrated Pest Management 5(1):A1; ISU ICM.

    PLH migrate north annually on weather fronts (typically late May-June in
    the upper Midwest).  Adults and nymphs feed on phloem — lacerate cells
    causing hopperburn (V-shaped yellowing).  New seedings without glandular
    trichomes are most vulnerable.

    Economic threshold: ~0.1 PLH per sweep per inch of plant height.
    GDD-based: PLH buildup correlates with warm (>70°F), humid conditions.
    Peak populations at 800-1200 GDD (base 48°F).
    """
    if not profile.get("potato_leafhopper_sensitive"):
        return Risk(key="potato_leafhopper", name="Potato Leafhopper", level="low",
                    headline=f"{profile['label']} not a primary host.", detail="", metric="—",
                    severity=0.0)

    # Use cumulative GDD (base 48°F) to estimate PLH population timing.
    # PLH arrive on storm fronts typically at 400-600 GDD, peak at 800-1200.
    gdd_lookup = forecast.get("_gdd_base50_cum") or {}
    today = forecast.get("_today_planting_date") or date.today()
    today_gdd = gdd_lookup.get(today.isoformat()) or 0
    # Approximate base-48 from base-50 by adding ~10% (2°F lower base).
    gdd_b48 = today_gdd * 1.1

    air = _hourly_window(forecast, "temperature_2m", start, start + 168)
    humid = _hourly_window(forecast, "relative_humidity_2m", start, start + 168)
    if not air:
        return Risk(key="potato_leafhopper", name="Potato Leafhopper", level="low",
                    headline="Insufficient data.", detail="", metric="—", severity=0.0)

    avg_air = _avg(air)
    warm_hours = sum(1 for t in air if t > 70) if air else 0

    # GDD phenology: Gaussian peak at 900 GDD (base 48), sigma 250.
    gdd_sev = _gaussian_severity(gdd_b48, peak=900.0, sigma=250.0)
    # Warm humid conditions favor PLH reproduction.
    warm_sev = _sigmoid_severity(warm_hours, midpoint=72.0, scale=24.0)

    severity = gdd_sev * 0.60 + warm_sev * 0.40

    level = _level_from_severity(severity)
    headlines = {
        "high": f"GDD ~{gdd_b48:.0f} (b48) + {warm_hours}h >70°F — peak potato leafhopper window.",
        "moderate": f"GDD ~{gdd_b48:.0f} — approaching PLH activity window. Scout weekly.",
        "low": f"GDD ~{gdd_b48:.0f} — outside primary PLH migration/buildup window.",
    }
    return Risk(
        key="potato_leafhopper", name="Potato Leafhopper", level=level,
        headline=headlines[level],
        detail="Potato leafhoppers migrate north on storm fronts in late May-June and feed on "
               "alfalfa phloem, causing hopperburn (V-shaped leaf yellowing). New seedings are most "
               "vulnerable — they lack the glandular trichomes that deter feeding in resistant "
               "varieties. Economic threshold is ~0.1 PLH/sweep/inch of plant height. Scout weekly "
               "with a sweep net starting in late May. Early cutting is the most effective control "
               "for established stands (UW, UNL, ISU ICM).",
        metric=f"GDD ~{gdd_b48:.0f} (b48) · {avg_air:.0f}°F",
        severity=severity, curve_type="gaussian",
    )


def _alfalfa_weevil(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Alfalfa weevil (Hypera postica) — larval defoliation.

    Source: ISU ICM "Alfalfa Weevil"; UMN Crop News 2023; UW-Extension IPCM;
    J. Economic Entomology 114(3):1173 (2021).

    Lower developmental threshold 48°F, upper 90°F.  Eggs hatch ~300 GDD
    (base 48°F).  Larvae feed on terminal leaves (pinholes → skeletonization).
    Greatest injury before first cutting.  Economic threshold: 40% of stems
    with damage and >7 days from scheduled cutting.
    """
    if not profile.get("alfalfa_weevil_sensitive"):
        return Risk(key="alfalfa_weevil", name="Alfalfa Weevil", level="low",
                    headline=f"{profile['label']} not a host.", detail="", metric="—",
                    severity=0.0)

    gdd_lookup = forecast.get("_gdd_base50_cum") or {}
    today = forecast.get("_today_planting_date") or date.today()
    today_gdd = gdd_lookup.get(today.isoformat()) or 0
    gdd_b48 = today_gdd * 1.1

    # Eggs hatch ~300 GDD, peak larval damage 350-550 GDD (base 48°F).
    gdd_sev = _gaussian_severity(gdd_b48, peak=450.0, sigma=120.0)

    air = _hourly_window(forecast, "temperature_2m", start, start + 168)
    if air:
        warm_hours = sum(1 for t in air if 50 < t < 90)
        warm_sev = _sigmoid_severity(warm_hours, midpoint=80.0, scale=25.0)
    else:
        warm_sev = 0.3

    severity = gdd_sev * 0.70 + warm_sev * 0.30

    level = _level_from_severity(severity)
    headlines = {
        "high": f"GDD ~{gdd_b48:.0f} (b48) — peak alfalfa weevil larval activity.",
        "moderate": f"GDD ~{gdd_b48:.0f} — approaching weevil hatch window. Begin scouting.",
        "low": f"GDD ~{gdd_b48:.0f} — outside primary weevil activity window.",
    }
    return Risk(
        key="alfalfa_weevil", name="Alfalfa Weevil", level=level,
        headline=headlines[level],
        detail="Alfalfa weevil larvae hatch at ~300 GDD (base 48°F) and feed on terminal leaves, "
               "progressing from pinholes to full skeletonization. Peak damage occurs at 350-550 GDD "
               "before first cutting. Economic threshold: 40% of stems with damage and >7 days from "
               "scheduled cutting, or one to three large larvae per stem. Early cutting is the most "
               "effective non-chemical control (ISU, UMN, UW).",
        metric=f"GDD ~{gdd_b48:.0f} (b48)",
        severity=severity, curve_type="gaussian",
    )


def _soil_ph_risk(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Soil pH risk for alfalfa establishment and nodulation.

    Source: USU "Alfalfa Nutrient Management Guide"; TAMU "Effect of Soil
    Boron Levels and pH on Yield of Alfalfa"; UW-Extension; Mosaic Crop
    Nutrition.

    Alfalfa requires pH 6.5-7.0 for optimal Rhizobium nodulation and
    nutrient availability.  Below pH 6.0, aluminum toxicity impairs root
    growth and Rhizobium survival drops sharply.  Above pH 7.5, boron and
    micronutrient availability decreases.  Every 0.1 pH below optimum
    costs ~0.1 DM ton/ac/yr.
    """
    if not profile.get("soil_ph_sensitive"):
        return Risk(key="soil_ph", name="Soil pH", level="low",
                    headline="Not applicable.", detail="", metric="—", severity=0.0)

    soil_profile = forecast.get("_soil_profile") or {}
    ph = soil_profile.get("ph")
    if ph is None:
        return Risk(key="soil_ph", name="Soil pH", level="low",
                    headline="No SSURGO pH data available for this location.",
                    detail="", metric="—", severity=0.0)

    # Below 6.0: aluminum toxicity + poor nodulation (steep response).
    # 6.0-6.5: suboptimal but manageable.
    # 6.5-7.0: optimal.
    # 7.0-7.5: acceptable.
    # >7.5: boron/micronutrient lockout.
    if ph < 6.0:
        severity = _sigmoid_severity(ph, midpoint=5.5, scale=0.4, inverted=True)
    elif ph < 6.5:
        severity = 0.3 + 0.2 * (6.5 - ph) / 0.5
    elif ph <= 7.5:
        severity = 0.0
    else:
        severity = _sigmoid_severity(ph, midpoint=8.0, scale=0.4)

    level = _level_from_severity(severity)
    if ph < 6.5:
        headlines = {
            "high": f"Soil pH {ph:.1f} — below 6.0, aluminum toxicity impairs roots + nodulation.",
            "moderate": f"Soil pH {ph:.1f} — below optimal 6.5. Lime recommended before seeding.",
            "low": f"Soil pH {ph:.1f} — near optimal range for alfalfa.",
        }
    else:
        headlines = {
            "high": f"Soil pH {ph:.1f} — above 7.5, boron/micronutrient availability reduced.",
            "moderate": f"Soil pH {ph:.1f} — slightly high. Monitor micronutrient status.",
            "low": f"Soil pH {ph:.1f} — within optimal range for alfalfa.",
        }
    return Risk(
        key="soil_ph", name="Soil pH", level=level,
        headline=headlines[level],
        detail="Alfalfa requires pH 6.5-7.0 for optimal Rhizobium meliloti nodulation and nutrient "
               "uptake. Below pH 6.0, aluminum toxicity damages root tips and Rhizobium survival "
               "drops sharply — lime to pH 6.8 before seeding (USU, UW-Extension). Above pH 7.5, "
               "boron and iron availability decreases; boron deficiency causes stunted growth, poor "
               "flowering, and rosetting (TAMU). Apply 2-3 lb/ac boron on high-pH soils.",
        metric=f"pH {ph:.1f}",
        severity=severity, curve_type="sigmoid",
    )


def _sudden_death_syndrome(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Sudden Death Syndrome (Fusarium virguliforme) — soybean planting-time root infection.

    Source: CPN "Overview of SDS"; SDSU "Are You at Risk for SDS?"; Mueller et al.
    (2019) Plant Disease 103:2; ISU ICM "Assess Soybean Disease Risk"; OSU AC-44.

    F. virguliforme colonises soybean roots early under cool (<60°F), wet soils.
    Foliar symptoms appear at R3-R6 but root infection severity is set at planting.
    SCN creates wound sites that amplify infection (documented synergy).
    """
    if not profile.get("sds_sensitive"):
        return Risk(key="sds", name="Sudden Death Syndrome", level="low",
                    headline=f"{profile['label']} not a host.", detail="", metric="—",
                    severity=0.0)

    soil = _hourly_window(forecast, "soil_temperature_6cm", start, start + 168)
    moist = _hourly_window(forecast, "soil_moisture_1_to_3cm", start, start + 168)
    if not soil or not moist:
        return Risk(key="sds", name="Sudden Death Syndrome", level="low",
                    headline="Insufficient soil data.", detail="", metric="—",
                    severity=0.0)

    avg_soil = _avg(soil)
    sat_hours = _saturated_hours(moist, 0.38)

    temp_sev = _sigmoid_severity(avg_soil, midpoint=60.0, scale=5.0, inverted=True)
    moist_sev = _sigmoid_severity(sat_hours, midpoint=36.0, scale=12.0)
    severity = temp_sev * 0.6 + moist_sev * 0.4
    severity = min(1.0, severity * max(temp_sev, moist_sev) / max(0.01, (temp_sev + moist_sev) / 2))

    rotation = forecast.get("_rotation") or {}
    rotation_phrase = ""
    if rotation.get("available") and rotation.get("soy_on_soy"):
        severity = min(1.0, severity + 0.15)
        rotation_phrase = " Soy-on-soy increases F. virguliforme inoculum."

    soil_profile = forecast.get("_soil_profile") or {}
    drainage = (soil_profile.get("drainage_class") or "").lower()
    drain_phrase = ""
    if any(t in drainage for t in ("poorly drained", "very poorly drained", "somewhat poorly drained")):
        severity = min(1.0, severity + 0.1)
        drain_phrase = f" SSURGO: {drainage} — extended saturation favors SDS."

    if inputs.field_tiled:
        severity *= 0.75

    level = _level_from_severity(severity)
    headlines = {
        "high": f"Avg soil {avg_soil:.0f}°F + {sat_hours}h saturated — high SDS root colonisation risk.{rotation_phrase}{drain_phrase}",
        "moderate": f"Cool soil ({avg_soil:.0f}°F) with moderate moisture — borderline SDS conditions.{rotation_phrase}{drain_phrase}",
        "low": f"Soil conditions ({avg_soil:.0f}°F) not strongly conducive to SDS.{drain_phrase}",
    }
    return Risk(
        key="sds", name="Sudden Death Syndrome", level=level,
        headline=headlines[level],
        detail="Fusarium virguliforme infects soybean roots at planting under cool (<60°F), wet soils. "
               "Root colonisation severity is determined by planting-time conditions even though foliar "
               "symptoms appear months later at R3-R6. SCN wound sites amplify infection. "
               "ILeVO/fluopyram seed treatment, tile drainage, and avoiding early planting into cold "
               "wet ground are the primary defences (CPN, Mueller et al. 2019).",
        metric=f"{avg_soil:.0f}°F · {sat_hours}h sat",
        severity=severity, curve_type="sigmoid",
    )


def _rhizoctonia(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Rhizoctonia solani seedling blight and root rot — warm, damp soil niche.

    Source: CPN "Rhizoctonia Seedling Blight and Root Rot of Soybean"; OSU
    Ohioline PLPATH-SOY-1; UMN "Rhizoctonia Root and Stem Rot"; UMN "Soybean
    Seed and Seedling Diseases". Fills the warm-damp pathogen niche between
    Pythium (cool-wet, peak ~52°F) and Phytophthora (warm-wet, peak ~77-86°F).
    Rhizoctonia peaks at ~80°F with moderate-to-high soil moisture. Causes both
    pre- and post-emergence damping-off (reddish-brown hypocotyl lesions).
    """
    if not profile.get("rhizoctonia_sensitive"):
        return Risk(key="rhizoctonia", name="Rhizoctonia Seedling Blight", level="low",
                    headline=f"{profile['label']} not tracked for Rhizoctonia.", detail="",
                    metric="—", severity=0.0)

    soil = _hourly_window(forecast, "soil_temperature_6cm", start, start + 120)
    moist = _hourly_window(forecast, "soil_moisture_1_to_3cm", start, start + 120)
    if not soil or not moist:
        return Risk(key="rhizoctonia", name="Rhizoctonia Seedling Blight", level="low",
                    headline="Insufficient soil data.", detail="", metric="—",
                    severity=0.0)

    avg_soil = _avg(soil)
    avg_moist = _avg(moist)

    temp_sev = _gaussian_severity(avg_soil, peak=80.0, sigma=12.0)
    moist_sev = _sigmoid_severity(avg_moist, midpoint=0.30, scale=0.08)
    severity = temp_sev * 0.55 + moist_sev * 0.45

    rotation = forecast.get("_rotation") or {}
    rotation_phrase = ""
    is_beet = profile.get("label", "").lower() == "sugar beets"
    if rotation.get("available") and rotation.get("soy_on_soy"):
        severity = min(1.0, severity + 0.1)
        rotation_phrase = " Soy-on-soy elevates Rhizoctonia inoculum."
    elif rotation.get("available"):
        prev = rotation.get("prev_crop_code")
        if prev == 41 and is_beet:
            severity = min(1.0, severity + 0.15)
            rotation_phrase = " Beet-on-beet elevates Rhizoctonia AG 2-2 inoculum."
        elif prev in (42, 5):
            severity = min(1.0, severity + 0.10)
            rotation_phrase = " Bean/soy in prior rotation elevates Rhizoctonia inoculum."

    if inputs.tillage == "no-till" or inputs.residue == "heavy":
        severity = min(1.0, severity + 0.08)

    level = _level_from_severity(severity)
    beet_note = " (AG 2-2 crown/root rot)" if is_beet else ""
    headlines = {
        "high": f"Warm ({avg_soil:.0f}°F) damp soil — high Rhizoctonia{beet_note} risk.{rotation_phrase}",
        "moderate": f"Soil {avg_soil:.0f}°F, moisture {avg_moist:.2f} — moderate Rhizoctonia conditions.{rotation_phrase}",
        "low": f"Soil conditions not strongly conducive to Rhizoctonia.",
    }
    return Risk(
        key="rhizoctonia", name="Rhizoctonia Seedling Blight", level=level,
        headline=headlines[level],
        detail="Rhizoctonia solani thrives in warm (60-95°F, optimum ~80°F), damp soils — the "
               "complementary niche to Pythium (cool-wet) and Phytophthora (warm-saturated). "
               "In sugar beets, AG 2-2 causes crown and root rot (MSU: soil active at 77–91°F) "
               "with tonnage losses of 15–20%. In soybeans/beans, causes hypocotyl lesions. "
               "Fungicide seed treatments are effective. Beet-on-beet and heavy residue "
               "elevate inoculum (CPN, UMN, MSU Extension).",
        metric=f"{avg_soil:.0f}°F · {avg_moist:.2f} VWC",
        severity=severity, curve_type="gaussian",
    )


def _anthracnose(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Anthracnose risk for dry beans — Colletotrichum lindemuthianum.

    Source: Cornell "Bean Anthracnose"; MSU Extension "Dry Bean Anthracnose
    Identification and Management"; CABI Compendium. Favored by cool to
    moderate temps (55-79°F / 13-26°C) with prolonged high humidity (>92% RH)
    and rain splash for spore dispersal. Can cause up to 100% yield loss in
    susceptible cultivars. Seed-borne pathogen — certified seed is critical.
    """
    if not profile.get("anthracnose_sensitive"):
        return Risk(key="anthracnose", name="Anthracnose", level="low",
                    headline="Crop not susceptible.", detail="", metric="—", severity=0.0)

    temps = _hourly_window(forecast, "temperature_2m", start, start + 72)
    rh = _hourly_window(forecast, "relative_humidity_2m", start, start + 72)
    precip_daily = forecast.get("daily", {}).get("precipitation_sum") or []
    precip_7d = sum(p for p in precip_daily[:7] if p is not None)

    if not temps or not rh:
        return Risk(key="anthracnose", name="Anthracnose", level="low",
                    headline="Insufficient data.", detail="", metric="—", severity=0.0)

    # Hours in the 55-79°F sweet spot with high humidity (>90% RH)
    conducive_hours = sum(1 for t, h in zip(temps, rh) if 55 <= t <= 79 and h > 90)
    temp_rh_sev = _sigmoid_severity(conducive_hours, midpoint=24, scale=10.0)
    rain_sev = _sigmoid_severity(precip_7d, midpoint=1.0, scale=0.5)
    severity = max(temp_rh_sev, rain_sev * 0.6 + temp_rh_sev * 0.4)

    rotation = forecast.get("_rotation") or {}
    rot_phrase = ""
    if rotation.get("available"):
        prev = rotation.get("prev_crop_code")
        if prev in (42, 5):
            severity = min(1.0, severity + 0.12)
            rot_phrase = " Bean/soy in rotation increases anthracnose inoculum."

    level = _level_from_severity(severity)
    headlines = {
        "high": f"{conducive_hours}h cool/humid + {precip_7d:.1f}\" rain — high anthracnose risk.{rot_phrase}",
        "moderate": f"Moderate anthracnose conditions: {conducive_hours}h conducive.{rot_phrase}",
        "low": f"Low anthracnose pressure in current forecast.",
    }
    return Risk(
        key="anthracnose", name="Anthracnose", level=level,
        headline=headlines[level],
        detail="Colletotrichum lindemuthianum thrives at 55-79°F with prolonged humidity >92% RH. "
               "Rain splash is the primary dispersal mechanism — frequent moderate rainfall is "
               "more dangerous than one heavy event. Causes sunken dark lesions on pods, stems, "
               "and leaves. Seed-borne: use certified, disease-free seed and 2-3 year rotation "
               "away from beans (Cornell, MSU Extension).",
        metric=f"{conducive_hours}h conducive · {precip_7d:.1f}\" 7d",
        severity=severity, curve_type="sigmoid",
    )


def _bacterial_blight(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Bacterial blight risk for dry beans — CBB + halo blight.

    Source: CSU Extension "Bacterial Diseases of Beans"; Cornell "Bacterial
    Diseases of Beans"; Manitoba Agriculture; Springer (2013). Two pathogens
    with opposite temperature preferences:
      - Common bacterial blight (Xanthomonas): favored >80°F + high humidity
      - Halo blight (Pseudomonas syringae pv. phaseolicola): favored <80°F
    Both require high humidity/leaf wetness and can cause 40-70% yield loss.
    """
    if not profile.get("bacterial_blight_sensitive"):
        return Risk(key="bacterial_blight", name="Bacterial Blight", level="low",
                    headline="Crop not susceptible.", detail="", metric="—", severity=0.0)

    temps = _hourly_window(forecast, "temperature_2m", start, start + 72)
    rh = _hourly_window(forecast, "relative_humidity_2m", start, start + 72)
    wind = _hourly_window(forecast, "wind_speed_10m", start, start + 72)
    precip_daily = forecast.get("daily", {}).get("precipitation_sum") or []
    precip_7d = sum(p for p in precip_daily[:7] if p is not None)

    if not temps or not rh:
        return Risk(key="bacterial_blight", name="Bacterial Blight", level="low",
                    headline="Insufficient data.", detail="", metric="—", severity=0.0)

    avg_temp = _avg(temps)
    humid_hours = sum(1 for h in rh if h > 85)

    # CBB: warm + humid (>80°F optimal, peak severity ~82°F)
    cbb_temp_sev = _sigmoid_severity(avg_temp, midpoint=82.0, scale=6.0)
    cbb_moist_sev = _sigmoid_severity(humid_hours, midpoint=30, scale=12.0)
    cbb_severity = cbb_temp_sev * 0.5 + cbb_moist_sev * 0.5

    # Halo blight: cool + humid (<80°F, peak ~68°F)
    halo_temp_sev = _gaussian_severity(avg_temp, peak=68.0, sigma=10.0)
    halo_moist_sev = _sigmoid_severity(humid_hours, midpoint=30, scale=12.0)
    halo_severity = halo_temp_sev * 0.5 + halo_moist_sev * 0.5

    # Dominant pathogen depends on temperature
    if cbb_severity >= halo_severity:
        severity = cbb_severity
        pathogen_note = "Common bacterial blight (Xanthomonas) — warm + humid"
    else:
        severity = halo_severity
        pathogen_note = "Halo blight (Pseudomonas) — cool + humid"

    # Wind and rain amplify spread via mechanical injury and splash
    wind_hours = sum(1 for w in (wind or []) if w > 25) if wind else 0
    if wind_hours > 6 and precip_7d > 0.5:
        severity = min(1.0, severity + 0.08)
        pathogen_note += " + wind/rain splash"

    rotation = forecast.get("_rotation") or {}
    rot_phrase = ""
    if rotation.get("available"):
        prev = rotation.get("prev_crop_code")
        if prev in (42, 5):
            severity = min(1.0, severity + 0.08)
            rot_phrase = " Bean/soy residue harbors bacterial inoculum."

    level = _level_from_severity(severity)
    headlines = {
        "high": f"{pathogen_note} — {humid_hours}h high humidity.{rot_phrase}",
        "moderate": f"Moderate blight conditions: {pathogen_note}.{rot_phrase}",
        "low": f"Low bacterial blight pressure currently.",
    }
    return Risk(
        key="bacterial_blight", name="Bacterial Blight", level=level,
        headline=headlines[level],
        detail="Two bacterial pathogens with opposite temperature niches. Common bacterial "
               "blight (Xanthomonas) peaks above 80°F with high humidity — typically July/August. "
               "Halo blight (Pseudomonas) peaks at 60-75°F with leaf wetness — more common in "
               "cool wet springs. Both are seed-borne: use certified seed and 2-3 year rotation. "
               "Wind-driven rain creates entry wounds and spreads bacteria (CSU, Cornell, Manitoba Ag).",
        metric=f"{avg_temp:.0f}°F avg · {humid_hours}h humid",
        severity=severity, curve_type="sigmoid",
    )


def _iron_deficiency_chlorosis(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Iron Deficiency Chlorosis (IDC) — soybean on calcareous/high-pH soils.

    Source: UMN "Managing IDC in Soybean"; NDSU "Iron Deficiency Chlorosis in
    Soybean"; SDSU management recs. Calcareous soils (pH>7.4) with free CaCO3
    produce bicarbonate that blocks soybean iron uptake. Wet conditions at
    planting dissolve more carbonate and exacerbate IDC. Primarily a western
    Corn Belt / Great Plains issue but SSURGO pH data makes it universally
    computable.
    """
    if not profile.get("idc_sensitive"):
        return Risk(key="idc", name="Iron Deficiency Chlorosis", level="low",
                    headline=f"{profile['label']} not susceptible.", detail="",
                    metric="—", severity=0.0)

    soil_profile = forecast.get("_soil_profile") or {}
    soil_ph = soil_profile.get("ph")
    if soil_ph is None:
        return Risk(key="idc", name="Iron Deficiency Chlorosis", level="low",
                    headline="No SSURGO pH data available.", detail="", metric="—",
                    severity=0.0)

    if soil_ph < 7.0:
        return Risk(key="idc", name="Iron Deficiency Chlorosis", level="low",
                    headline=f"Soil pH {soil_ph:.1f} — below the calcareous threshold.",
                    detail="", metric=f"pH {soil_ph:.1f}", severity=0.0)

    ph_sev = _sigmoid_severity(soil_ph, midpoint=7.4, scale=0.3)

    moist = _hourly_window(forecast, "soil_moisture_1_to_3cm", start, start + 168)
    wet_amp = 1.0
    if moist:
        avg_moist = _avg(moist)
        if avg_moist > 0.35:
            wet_amp = 1.3
        elif avg_moist > 0.28:
            wet_amp = 1.15

    severity = min(1.0, ph_sev * wet_amp)

    cultivar = forecast.get("_cultivar") or {}
    idc_score = cultivar.get("idc")
    cultivar_phrase = ""
    if isinstance(idc_score, (int, float)):
        if idc_score <= 2:
            severity *= 0.5
            cultivar_phrase = f" Cultivar IDC score {idc_score} (tolerant) — risk halved."
        elif idc_score >= 4:
            severity = min(1.0, severity * 1.3)
            cultivar_phrase = f" Cultivar IDC score {idc_score} (susceptible) — risk elevated."

    level = _level_from_severity(severity)
    wet_phrase = f" Wet soil amplifies bicarbonate." if wet_amp > 1.0 else ""
    headlines = {
        "high": f"pH {soil_ph:.1f} — calcareous soil, high IDC risk for soybeans.{wet_phrase}{cultivar_phrase}",
        "moderate": f"pH {soil_ph:.1f} — borderline IDC conditions.{wet_phrase}{cultivar_phrase}",
        "low": f"pH {soil_ph:.1f} — IDC unlikely.{cultivar_phrase}",
    }
    return Risk(
        key="idc", name="Iron Deficiency Chlorosis", level=level,
        headline=headlines[level],
        detail="Calcareous soils (pH>7.4) produce bicarbonate ions that block soybean iron uptake, "
               "causing interveinal yellowing and stunting. Wet conditions dissolve more CaCO3 and "
               "worsen IDC. Variety selection (low IDC score = tolerant) is the primary defence. "
               "In-furrow iron chelate (EDDHA) or companion crop (oats) can mitigate severe fields. "
               "Annual US losses exceed $260M in the western Corn Belt (UMN, NDSU).",
        metric=f"pH {soil_ph:.1f}",
        severity=severity, curve_type="sigmoid",
    )


def _soybean_cyst_nematode(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Soybean Cyst Nematode (SCN) modifier — amplifies SDS, Pythium, Phytophthora.

    Source: ISU SCN project; CPN "Overview of SCN"; UMN SCN Management Guide;
    Purdue Nematology. SCN is the #1 pest of US soybeans (>$1B annual loss).
    At planting time, SCN does not cause acute stand death but creates wound
    sites on roots that dramatically amplify Fusarium (SDS), Pythium, and
    Phytophthora infection. Treated as a MODIFIER that amplifies downstream
    disease factors rather than a standalone killer.

    J2 juveniles hatch at ~50°F soil; optimal egg hatch at 75°F; optimal
    penetration at 82°F. Soy-on-soy rotation without resistance rotation
    builds populations that overwhelm PI88788 resistance.
    """
    if profile.get("label") != "Soybeans":
        return Risk(key="scn", name="Soybean Cyst Nematode", level="low",
                    headline="Not applicable.", detail="", metric="—", severity=0.0)

    rotation = forecast.get("_rotation") or {}
    soy_on_soy = rotation.get("available") and rotation.get("soy_on_soy")

    soil = _hourly_window(forecast, "soil_temperature_6cm", start, start + 168)
    soil_warm_enough = False
    if soil:
        avg_soil = _avg(soil)
        soil_warm_enough = avg_soil >= 50.0

    cultivar = forecast.get("_cultivar") or {}
    scn_source = cultivar.get("scn_source")

    severity = 0.0
    risk_phrases = []

    if soy_on_soy:
        severity += 0.45
        risk_phrases.append("soy-on-soy rotation builds SCN populations")
    if soil_warm_enough:
        severity += 0.15
        risk_phrases.append(f"soil {avg_soil:.0f}°F (above 50°F hatch threshold)")

    if scn_source == "PI88788" and soy_on_soy:
        severity += 0.1
        risk_phrases.append("PI88788 resistance may be eroding under continuous soy")
    elif scn_source and scn_source != "PI88788":
        severity = max(0.0, severity - 0.1)

    severity = min(1.0, severity)
    if severity < 0.20:
        return Risk(key="scn", name="Soybean Cyst Nematode", level="low",
                    headline="Rotation / conditions don't indicate elevated SCN pressure.",
                    detail="", metric="Low", severity=0.0)

    level = _level_from_severity(severity)
    detail_str = "; ".join(risk_phrases) if risk_phrases else "Moderate SCN pressure"
    headlines = {
        "high": f"High SCN pressure — {detail_str}.",
        "moderate": f"Elevated SCN — {detail_str}.",
        "low": "SCN pressure not elevated.",
    }
    return Risk(
        key="scn", name="Soybean Cyst Nematode", level=level,
        headline=headlines[level],
        detail="SCN (#1 US soybean pest, >$1B annual losses) creates root wound sites that amplify "
               "Fusarium (SDS), Pythium, and Phytophthora infection at planting. This factor acts "
               "as a risk MODIFIER — it doesn't kill seeds directly but escalates disease-complex "
               "severity. Soy-on-soy rotation without alternating SCN resistance sources (PI88788 "
               "vs Peking) accelerates population buildup. Egg counts >10,000/100cc soil indicate "
               "high field pressure (ISU SCN project, CPN).",
        metric=f"{'Soy-on-soy' if soy_on_soy else 'Rotated'} · {scn_source or 'unknown'}",
        severity=severity, curve_type="threshold",
    )


# ----- Winter wheat risk evaluators -----------------------------------------
# Six new evaluators addressing winter-wheat-specific risks identified through
# academic literature review (MU, KSU, OSU, MSU, SDSU, Penn State Extension).

def _hessian_fly(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Hessian fly risk for winter wheat — planting date relative to fly-free date.

    Source: MU Extension G7180, Penn State Extension, OSU Ohioline, KSU Entomology.
    Planting before the regional fly-free date exposes fall-planted wheat to the
    fall generation. Each day before the fly-free date increases risk substantially.
    Fly-free dates are approximated by latitude: ~Sept 1 at 47°N, ~Oct 20 at 35°N.
    """
    if not profile.get("hessian_fly_sensitive"):
        return Risk(key="hessian_fly", name="Hessian Fly", level="low",
                    headline="Crop not susceptible.", detail="", metric="—", severity=0.0)

    lat = forecast.get("latitude") or forecast.get("_lat") or 43.0
    fly_free_doy = int(244 + (47.0 - lat) * 3.5)
    fly_free_doy = max(244, min(293, fly_free_doy))

    plant_date = _planting_date_for_start(forecast, start)
    plant_doy = plant_date.timetuple().tm_yday
    days_before_ff = fly_free_doy - plant_doy

    if days_before_ff <= 0:
        severity = 0.0
    else:
        severity = _sigmoid_severity(days_before_ff, midpoint=7, scale=4.0)

    level = _level_from_severity(severity)
    ff_date_str = date(plant_date.year, 1, 1) + timedelta(days=fly_free_doy - 1)
    headlines = {
        "high": f"Planting {days_before_ff}d before fly-free date ({ff_date_str:%b %d}) — high Hessian fly exposure.",
        "moderate": f"Planting near fly-free date ({ff_date_str:%b %d}) — moderate Hessian fly risk.",
        "low": f"Planting on/after fly-free date ({ff_date_str:%b %d}) — Hessian fly risk minimal.",
    }
    return Risk(
        key="hessian_fly", name="Hessian Fly", level=level,
        headline=headlines[level],
        detail="Hessian fly adults lay 250-300 eggs on fall-planted wheat. Larvae feed at the "
               "base of tillers, stunting growth and causing lodging. Planting after the regional "
               "fly-free date is the most effective management tool (MU Extension G7180, Penn State). "
               "Each day before the fly-free date substantially increases risk of fall infestation.",
        metric=f"{days_before_ff}d before fly-free" if days_before_ff > 0 else "After fly-free date",
        severity=severity, curve_type="sigmoid",
    )


def _barley_yellow_dwarf(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Barley Yellow Dwarf Virus risk — aphid-vectored infection.

    Source: SDSU Extension, UNL CropWatch, OSU Ohioline, UKY IPM, MSU Extension.
    Winter wheat: fall-planted wheat is exposed to viruliferous aphids while
    temps remain above ~50°F. Early planting and warm falls increase risk.
    Spring wheat: the dynamics are inverted — early planting is DEFENSIVE
    because plants reach advanced growth stages before peak aphid populations
    arrive (Field Crop News, SDSU). Later-planted spring wheat that is still
    at seedling stage when aphids arrive suffers greater yield loss. The
    evaluator applies a spring-wheat dampening factor because spring infections
    cause less yield loss than fall infections (SDSU, UKY IPM).
    """
    if not profile.get("bydv_sensitive"):
        return Risk(key="bydv", name="Barley Yellow Dwarf", level="low",
                    headline="Crop not susceptible.", detail="", metric="—", severity=0.0)

    temps = _hourly_window(forecast, "temperature_2m", start, start + 168)
    if not temps:
        return Risk(key="bydv", name="Barley Yellow Dwarf", level="low",
                    headline="Insufficient data.", detail="", metric="—", severity=0.0)

    aphid_active_hours = sum(1 for t in temps if t >= 50)
    avg_temp = sum(temps) / len(temps) if temps else 50

    sev_active = _sigmoid_severity(aphid_active_hours, midpoint=100, scale=30.0)
    sev_warm = _sigmoid_severity(avg_temp, midpoint=58, scale=5.0)
    severity = max(sev_active, sev_warm * 0.8 + sev_active * 0.2)

    is_spring_wheat = not profile.get("fall_planted") and profile.get("label") in ("Spring Wheat",)
    spring_phrase = ""
    if is_spring_wheat:
        severity *= 0.55
        spring_phrase = " Spring wheat: early planting is defensive — plants outgrow peak aphid window."

    level = _level_from_severity(severity)
    headlines = {
        "high": f"{aphid_active_hours}h above 50°F in 7d — high aphid activity / BYDV risk.{spring_phrase}",
        "moderate": f"{aphid_active_hours}h above 50°F — moderate aphid/BYDV pressure.{spring_phrase}",
        "low": f"{aphid_active_hours}h above 50°F — low aphid flight conditions.{spring_phrase}",
    }
    detail_text = (
        "BYDV is transmitted by cereal aphids (bird cherry oat, English grain, greenbug) "
        "active above ~50°F. "
    )
    if is_spring_wheat:
        detail_text += (
            "In spring wheat, early seeding is an excellent defence — plants reach advanced "
            "stages before aphid populations peak, reducing yield losses (Field Crop News, SDSU). "
            "Spring infection causes less yield loss than fall infection. "
        )
    else:
        detail_text += (
            "Fall infection causes greater yield loss than spring infection (SDSU). Early "
            "planting into warm conditions extends the aphid exposure window. "
        )
    detail_text += "Insecticidal seed treatments (imidacloprid, thiamethoxam) provide 3-4 weeks of suppression."
    return Risk(
        key="bydv", name="Barley Yellow Dwarf", level=level,
        headline=headlines[level],
        detail=detail_text,
        metric=f"{aphid_active_hours}h aphid-active · avg {avg_temp:.0f}°F",
        severity=severity, curve_type="sigmoid",
    )


def _take_all(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Take-all root rot (Gaeumannomyces graminis var. tritici) — rotation-dependent.

    Source: MU Extension G4345, WSU STEEP, UMN Extension.
    Wheat-after-wheat or wheat-after-grass dramatically increases take-all.
    Up to 50% yield reduction in 2nd-3rd consecutive wheat crop. Requires
    2+ years out of wheat/barley for inoculum decline.
    """
    if not profile.get("take_all_sensitive"):
        return Risk(key="take_all", name="Take-All Root Rot", level="low",
                    headline="Crop not susceptible.", detail="", metric="—", severity=0.0)

    rotation = forecast.get("_rotation") or {}
    years = rotation.get("history") or []
    wheat_years = 0
    grass_recent = inputs.previous_grass
    for y in years:
        if isinstance(y, dict):
            c = str(y.get("crop", "")).lower()
            if "wheat" in c or "barley" in c:
                wheat_years += 1
            if c in ("grass", "pasture", "sod", "fallow"):
                grass_recent = True

    if wheat_years == 0 and not grass_recent:
        severity = 0.05
    elif wheat_years == 1 or grass_recent:
        severity = 0.55
    elif wheat_years >= 2:
        severity = 0.85
    else:
        severity = 0.05

    soil_temps = _hourly_window(forecast, "soil_temperature_6cm", start, start + 72)
    if soil_temps:
        avg_soil = sum(soil_temps) / len(soil_temps)
        if avg_soil > 60:
            severity = min(1.0, severity * 1.15)

    level = _level_from_severity(severity)
    rot_phrase = f"{wheat_years}yr wheat in rotation" if wheat_years > 0 else "No recent wheat"
    if grass_recent:
        rot_phrase += " + grass/sod history"
    headlines = {
        "high": f"{rot_phrase} — high take-all risk (up to 50% yield loss).",
        "moderate": f"{rot_phrase} — elevated take-all pressure.",
        "low": f"{rot_phrase} — take-all risk minimal.",
    }
    return Risk(
        key="take_all", name="Take-All Root Rot", level=level,
        headline=headlines[level],
        detail="Gaeumannomyces graminis var. tritici survives in wheat/barley stubble and grass roots. "
               "Second-year wheat can lose up to 50% of yield (MU Extension G4345). A 2-year break "
               "with non-host crops (corn, soybeans, alfalfa) is needed for inoculum decline. "
               "Grass/sod fields also harbor the pathogen (UMN Extension).",
        metric=rot_phrase,
        severity=severity, curve_type="threshold",
    )


def _crown_rot(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Crown and root rot risk (Fusarium spp.) for winter wheat at planting.

    Source: Oregon State Extension PNW-639, UNL Extension G1097, KSU.
    Planting into soil warmer than 60°F or into a loose/dry seedbed increases
    Fusarium crown rot colonization. Inadequate crown development before winter
    leads to poor cold hardiness and greater winterkill susceptibility.
    """
    if not profile.get("crown_rot_sensitive"):
        return Risk(key="crown_rot", name="Crown & Root Rot", level="low",
                    headline="Crop not susceptible.", detail="", metric="—", severity=0.0)

    soil_temps = _hourly_window(forecast, "soil_temperature_6cm", start, start + 72)
    moist = _hourly_window(forecast, "soil_moisture_1_to_3cm", start, start + 72)
    if not soil_temps:
        return Risk(key="crown_rot", name="Crown & Root Rot", level="low",
                    headline="Insufficient data.", detail="", metric="—", severity=0.0)

    avg_soil = sum(soil_temps) / len(soil_temps)
    avg_moist = sum(moist) / len(moist) if moist else 0.25

    sev_warm = _sigmoid_severity(avg_soil, midpoint=62, scale=4.0)
    sev_dry = _sigmoid_severity(avg_moist, midpoint=0.15, scale=0.05, inverted=True)
    severity = max(sev_warm * 0.7, sev_dry * 0.5)
    severity = min(1.0, severity + sev_warm * 0.3 * sev_dry)

    rotation = forecast.get("_rotation") or {}
    years = rotation.get("history") or []
    wheat_recent = any("wheat" in str(y.get("crop", "")).lower() for y in years if isinstance(y, dict))
    if wheat_recent:
        severity = min(1.0, severity + 0.15)

    level = _level_from_severity(severity)
    headlines = {
        "high": f"Soil avg {avg_soil:.0f}°F — warm seedbed favors Fusarium crown rot colonization.",
        "moderate": f"Soil avg {avg_soil:.0f}°F — moderate crown rot conditions.",
        "low": f"Soil avg {avg_soil:.0f}°F — crown rot risk low.",
    }
    return Risk(
        key="crown_rot", name="Crown & Root Rot", level=level,
        headline=headlines[level],
        detail="Fusarium crown/root rot colonizes wheat seedlings in warm (>60°F), dry seedbeds. "
               "Loose, dry soil also impairs crown development needed for winter cold hardiness. "
               "A firm, moist seedbed at 45-60°F is ideal (Oregon State PNW-639, UNL G1097). "
               "Continuous wheat amplifies residue-borne inoculum.",
        metric=f"{avg_soil:.0f}°F soil · {avg_moist:.2f} VWC",
        severity=severity, curve_type="composite",
    )


def _snow_mold(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Snow mold risk (Typhula/Microdochium) for winter wheat.

    Source: WSU Extension EB1880, USU Extension, PMC peer-reviewed.
    Prolonged snow cover on unfrozen/thawed ground drives Typhula and Microdochium.
    Primarily a risk in northern-tier states where snow persists >60 days.
    Smaller/weaker fall plants have lower survival. Uses latitude as a proxy
    for geographic risk since snow-cover data may not be available in forecast.
    """
    if not profile.get("snow_mold_sensitive"):
        return Risk(key="snow_mold", name="Snow Mold", level="low",
                    headline="Crop not susceptible.", detail="", metric="—", severity=0.0)

    lat = forecast.get("latitude") or forecast.get("_lat") or 43.0
    temps = _hourly_window(forecast, "temperature_2m", start, start + 168)
    if not temps:
        return Risk(key="snow_mold", name="Snow Mold", level="low",
                    headline="Insufficient data.", detail="", metric="—", severity=0.0)

    sev_lat = _sigmoid_severity(lat, midpoint=44, scale=3.0)
    near_freeze_hours = sum(1 for t in temps if 28 <= t <= 36)
    sev_temps = _sigmoid_severity(near_freeze_hours, midpoint=60, scale=20.0)

    precip_daily = forecast.get("daily", {}).get("precipitation_sum") or []
    precip_7d = sum(p for p in precip_daily[:7] if p is not None)
    sev_precip = _sigmoid_severity(precip_7d, midpoint=1.0, scale=0.5)

    severity = sev_lat * 0.4 + max(sev_temps, sev_precip) * 0.6

    level = _level_from_severity(severity)
    headlines = {
        "high": f"Lat {lat:.1f}°N + {near_freeze_hours}h near-freezing — high snow mold risk zone.",
        "moderate": f"Moderate snow mold risk — {near_freeze_hours}h near-freezing temps.",
        "low": f"Snow mold risk low at this latitude and conditions.",
    }
    return Risk(
        key="snow_mold", name="Snow Mold", level=level,
        headline=headlines[level],
        detail="Typhula (speckled) and Microdochium nivale (pink) snow molds thrive under "
               "prolonged snow cover on unfrozen ground. Risk increases at northern latitudes "
               "(>44°N) where snow persists 60+ days. Larger, more vigorous fall plants have "
               "better snow mold survival (WSU EB1880). Late planting that limits fall growth "
               "can increase susceptibility.",
        metric=f"Lat {lat:.1f}°N · {near_freeze_hours}h near-freeze",
        severity=severity, curve_type="composite",
    )


def _stripe_rust(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Stripe rust (Puccinia striiformis) risk for winter wheat.

    Source: SDSU Extension, UGA Production Guide, USDA Cereal Disease Lab.
    Stripe rust infects at 32-77°F with >8h leaf wetness. Cool, wet conditions
    at establishment favor early infection that persists through spring.
    Early-planted wheat with excess fall growth favors rust buildup.
    """
    if not profile.get("stripe_rust_sensitive"):
        return Risk(key="stripe_rust", name="Stripe Rust", level="low",
                    headline="Crop not susceptible.", detail="", metric="—", severity=0.0)

    temps = _hourly_window(forecast, "temperature_2m", start, start + 168)
    rh = _hourly_window(forecast, "relative_humidity_2m", start, start + 168)
    if not temps or not rh:
        return Risk(key="stripe_rust", name="Stripe Rust", level="low",
                    headline="Insufficient data.", detail="", metric="—", severity=0.0)

    favorable_hours = sum(1 for t, h in zip(temps, rh) if 40 <= t <= 65 and h > 85)
    severity = _sigmoid_severity(favorable_hours, midpoint=40, scale=15.0)

    rotation = forecast.get("_rotation") or {}
    years = rotation.get("history") or []
    wheat_recent = any("wheat" in str(y.get("crop", "")).lower() for y in years if isinstance(y, dict))
    if wheat_recent:
        severity = min(1.0, severity + 0.10)

    level = _level_from_severity(severity)
    headlines = {
        "high": f"{favorable_hours}h cool & humid (40-65°F, RH>85%) — high stripe rust risk.",
        "moderate": f"{favorable_hours}h favorable for stripe rust — moderate risk.",
        "low": f"{favorable_hours}h cool/humid — stripe rust risk low.",
    }
    return Risk(
        key="stripe_rust", name="Stripe Rust", level=level,
        headline=headlines[level],
        detail="Puccinia striiformis infects wheat at 32-77°F (optimal 40-65°F) with >8h leaf "
               "wetness. Fall infection establishes disease foci that erupt in spring. Early "
               "planting into warm, moist conditions extends the infection window. Wheat-after-"
               "wheat increases local inoculum. Resistant varieties are the primary defense "
               "(USDA Cereal Disease Lab, SDSU Extension).",
        metric=f"{favorable_hours}h favorable",
        severity=severity, curve_type="sigmoid",
    )


def _wheat_winterkill(forecast: dict, profile: dict, inputs: UserInputs, start: int) -> Risk:
    """Winterkill risk for winter wheat — crown hardiness, de-hardening, desiccation.

    Source: KSU Agronomy eUpdate, MSU Extension, UMN Extension, UNL G1097.
    Crown cold hardiness depends on adequate fall development (Feekes 3-5),
    cold acclimation (4-6 weeks below 50°F at crown depth), and sustained
    sub-freezing crown temps. De-hardening from mid-winter warm spells is
    irreversible. Desiccation under dry conditions is more common than direct
    cold injury. Snow cover insulates crowns (3+ inches = good protection).

    At planting time this evaluator estimates winterkill RISK based on whether
    conditions allow adequate pre-dormancy development and cold acclimation.
    """
    if not profile.get("winterkill_sensitive") or not profile.get("fall_planted"):
        return Risk(key="winterkill", name="Winterkill Risk", level="low",
                    headline="Not applicable.", detail="", metric="—", severity=0.0)

    soil_temps = _hourly_window(forecast, "soil_temperature_6cm", start, start + 168)
    air_temps = _hourly_window(forecast, "temperature_2m", start, start + 168)
    moist = _hourly_window(forecast, "soil_moisture_1_to_3cm", start, start + 72)

    if not soil_temps or not air_temps:
        return Risk(key="winterkill", name="Winterkill Risk", level="low",
                    headline="Insufficient data.", detail="", metric="—", severity=0.0)

    avg_soil = sum(soil_temps) / len(soil_temps)
    min_air = min(air_temps) if air_temps else 40
    avg_moist = sum(moist) / len(moist) if moist else 0.25

    gdd_target = profile.get("gdd_before_dormancy_target", 400)
    gdd_base = profile.get("gdd_base_f", 32)
    daily_temps_max = forecast.get("daily", {}).get("temperature_2m_max") or []
    daily_temps_min = forecast.get("daily", {}).get("temperature_2m_min") or []
    gdd_accum = 0.0
    for hi, lo in zip(daily_temps_max[:30], daily_temps_min[:30]):
        if hi is not None and lo is not None:
            daily_avg = (hi + lo) / 2.0
            gdd_accum += max(0, daily_avg - gdd_base)

    sev_gdd = _sigmoid_severity(gdd_accum, midpoint=gdd_target * 0.6, scale=gdd_target * 0.25, inverted=True)

    sev_dry = _sigmoid_severity(avg_moist, midpoint=0.15, scale=0.05, inverted=True)

    sev_late = 0.0
    plant_date = _planting_date_for_start(forecast, start)
    plant_doy = plant_date.timetuple().tm_yday
    lat = forecast.get("latitude") or forecast.get("_lat") or 43.0
    ideal_end_doy = int(265 + (47.0 - lat) * 3.0)
    days_late = plant_doy - ideal_end_doy
    if days_late > 0:
        sev_late = _sigmoid_severity(days_late, midpoint=14, scale=7.0)

    severity = max(sev_gdd * 0.5, sev_late * 0.3, sev_dry * 0.2)
    severity = min(1.0, sev_gdd * 0.4 + sev_late * 0.35 + sev_dry * 0.25)

    level = _level_from_severity(severity)
    risk_drivers = []
    if sev_gdd > 0.3:
        risk_drivers.append(f"~{gdd_accum:.0f} GDD (base {gdd_base}°F) in 30d vs {gdd_target} target")
    if sev_late > 0.3:
        risk_drivers.append(f"{days_late}d past ideal window")
    if sev_dry > 0.3:
        risk_drivers.append(f"dry seedbed (VWC {avg_moist:.2f})")
    driver_str = "; ".join(risk_drivers) if risk_drivers else "Conditions adequate"

    headlines = {
        "high": f"High winterkill risk — {driver_str}.",
        "moderate": f"Moderate winterkill risk — {driver_str}.",
        "low": f"Winterkill risk low — {driver_str}.",
    }
    return Risk(
        key="winterkill", name="Winterkill Risk", level=level,
        headline=headlines[level],
        detail="Winter wheat crowns need 4-6 weeks below 50°F to fully harden (KSU). Hardened "
               "crowns survive to -9 to -11°F (UNL G1097). Desiccation from dry, loose seedbeds "
               "is more common than direct cold injury (KSU). Late planting reduces fall tillers — "
               "fall tillers (before Jan 1) contribute ~87% of grain yield (Virginia Tech SPES-431). "
               "Snow cover of 3+ inches insulates crowns effectively (UMN).",
        metric=f"~{gdd_accum:.0f} GDD · {'late' if days_late > 0 else 'on time'} · VWC {avg_moist:.2f}",
        severity=severity, curve_type="composite",
    )


RISK_EVALUATORS: list[Callable[[dict, dict, UserInputs, int], Risk]] = [
    _imbibitional_chilling,
    _flooding,
    _antecedent_saturation,
    _topography,
    _frost,
    _soil_crusting,
    _pythium,
    _phytophthora,
    _seedcorn_maggot,
    _wireworm,
    _slugs,
    _black_cutworm,
    _bean_leaf_beetle,
    _herbicide_carryover,
    _heat_stress,
    _water_scarcity,
    _fusarium_head_blight,
    _tan_spot,
    _common_root_rot,
    _white_mold,
    _cercospora_leaf_spot,
    _bolting_risk,
    _winterkill_risk,
    _autotoxicity,
    _aphanomyces_alfalfa,
    _sclerotinia_crown,
    _potato_leafhopper,
    _alfalfa_weevil,
    _soil_ph_risk,
    _sudden_death_syndrome,
    _rhizoctonia,
    _anthracnose,
    _bacterial_blight,
    _iron_deficiency_chlorosis,
    _soybean_cyst_nematode,
    _hessian_fly,
    _barley_yellow_dwarf,
    _take_all,
    _crown_rot,
    _snow_mold,
    _stripe_rust,
    _wheat_winterkill,
    _aphanomyces,
    _sugar_beet_cyst_nematode,
    _wind_damage,
    _root_maggot,
]


# ----- methodology / explainer content ----------------------------------
# Per-risk methodology consumed by /api/methodology and /methodology. The
# popover on each risk card shows `summary` + `thresholds`; the full page
# also renders `detail`, `inputs`, `references`, and `operator_levers`.
# Single source of truth — keep keys in sync with Risk.key strings above.

SURVIVAL_OVERVIEW = {
    "model_name": "External Risk Factor Survivability",
    "summary": (
        "External Risk Factor Survivability is the probability that an average seed "
        "reaches a healthy, established stand given the external environmental, biological, "
        "and chemical risk factors at the time of planting. Each of the 42 monitored factors "
        "produces its own survival probability using the biologically appropriate model for "
        "its risk type — then all factors multiply together for the final estimate."
    ),
    "math": (
        "Each of the 42 risk factors falls into one of four mathematical categories, matched "
        "to the underlying biology or physics:\n\n"
        "BIOLOGICAL RESPONSE CURVES (Factors 1, 5, 7, 8, 14): Imbibitional chilling uses a "
        "logistic sigmoid tied to soil temperature during the 48h imbibition window "
        "(the critical water-uptake period per Purdue/Nielsen). Frost & "
        "freeze combines P(frost event) × survival(min temp) as a steep sigmoid. Pythium damping-off "
        "uses a Gaussian response curve on temperature/moisture multiplied by inoculum and seed-treatment "
        "efficacy. Phytophthora uses the same structure with a warmer optimum (~65–85°F). Herbicide "
        "carryover models first-order degradation kinetics: residue = initial × e^(-k × time), compared "
        "against the crop's sensitivity threshold.\n\n"
        "TIME/INTENSITY SURVIVAL FUNCTIONS (Factors 2, 6): Flooding uses exponential decay survival = "
        "e^(-k × hours_saturated) — duration matters more than occurrence. Soil crusting models "
        "P(emergence | crust strength), driven by soil texture, rainfall intensity, and surface cover.\n\n"
        "MODIFIERS (Factors 3, 4): Antecedent saturation and topography/ponding do not kill seeds "
        "directly. Instead, they amplify the severity of downstream factors — antecedent moisture "
        "amplifies flooding, crusting, and disease risk; topographic ponding amplifies flooding. "
        "This reflects their biological role as risk multipliers rather than standalone killers.\n\n"
        "HAZARD PROBABILITIES (Factors 9–13): Seedcorn maggot uses a degree-day phenology model plus "
        "an attractant index. Wireworm is a field-history probability model. Slugs use a conducive "
        "conditions probability (residue × moisture × overwintering). Black cutworm tracks moth-flight "
        "phenology via GDD. Bean leaf beetle combines overwintering survival × planting-date vulnerability.\n\n"
        "All non-modifier factor survival probabilities multiply together — modifiers act through "
        "their downstream targets. When a seed brand/cultivar is selected, a cultivar survival factor "
        "further scales the result: emergence score (each point above/below 7 shifts ±3%) and "
        "cold-tolerance class (high +5%, low −7%) both contribute."
    ),
    "categories": [
        {
            "name": "Biological Response Curves / Kinetics",
            "factors": [1, 5, 7, 8, 14, 18, 19, 20, 22, 23],
            "description": (
                "Steep dose-response survival: survival drops sharply once the organism's tolerance "
                "threshold is crossed. Logistic, Gaussian, and exponential kinetic curves. Includes "
                "SDS (cool-wet sigmoid), Rhizoctonia (warm-damp Gaussian), IDC (pH sigmoid), "
                "white mold (temperature/humidity sigmoid), anthracnose (cool-humid Gaussian), and "
                "bacterial blight (dual-temperature sigmoid for CBB/halo blight)."
            ),
        },
        {
            "name": "Time/Intensity Survival Functions",
            "factors": [2, 6],
            "description": (
                "Exponential decay or emergence probability: survival degrades smoothly "
                "with increasing exposure duration or intensity."
            ),
        },
        {
            "name": "Modifiers",
            "factors": [3, 4, 21],
            "description": (
                "Risk amplifiers: these do not directly reduce survival but increase the "
                "severity of downstream factors. Antecedent saturation and topography amplify "
                "flooding/disease; SCN amplifies SDS/Pythium/Phytophthora via root wound sites."
            ),
        },
        {
            "name": "Hazard Probabilities",
            "factors": [9, 10, 11, 12, 13],
            "description": (
                "Pest-encounter probabilities: even at maximum pressure, pests cause partial "
                "stand loss (~35% max), not total kill. Phenology, field history, and "
                "conducive conditions drive the survival estimates."
            ),
        },
    ],
    "curves": (
        "Three suitability curve types compute the raw severity score (0.0–1.0) for each factor: "
        "(1) Sigmoid/logistic — S-shaped transition around a midpoint; used for chilling "
        "(midpoint near soil-temp threshold), frost (midpoint at freeze threshold), flooding "
        "(midpoint at ~1.5\" / 48h), antecedent saturation, topographic concavity, and soil "
        "crusting. The 'scale' parameter controls the width of the transition zone. "
        "(2) Gaussian/bell — peaks at a single optimal condition; used for Pythium (bimodal: "
        "cold peak ~52°F for P. torulosum/oopapillum, warm peak ~72°F for P. sylvaticum), seedcorn maggot "
        "(peak larval feeding ~150 DD after each fly generation; soil attractiveness peaks ~55°F), "
        "and black cutworm (peak cutting at ~325 DD post-flight). "
        "(3) Trapezoidal/plateau — a sustained danger zone; used for Phytophthora (zoospore "
        "activity plateau 55–86°F), wireworm feeding (45–60°F), and slug damage (50–65°F). "
        "All curves are modulated by secondary factors (moisture, organic load, habitat) and "
        "external overrides (NWS alerts, USGS streamflow, CPC moisture). "
        "Each factor's severity then feeds through its category-specific survival formula, "
        "not a single generic mapping — biological response factors have steeper kill curves "
        "than hazard probability factors."
    ),
    "cap": (
        f"The published number is hard-capped at {SURVIVAL_PCT_CAP}%. Even an all-low "
        "vector under a perfect forecast can't honestly promise 100% — there's always "
        "residual seed-lot, equipment, and unmodeled-pest variance."
    ),
    "interval": (
        "The interval (e.g. 87–92%) brackets the point estimate by shifting each factor's "
        "severity ±0.12 to create pessimistic/optimistic scenarios, then interpolating by "
        "forecast confidence. A settled forecast collapses the interval toward the point; "
        "a noisy forecast widens it."
    ),
    "survival_formulas": {
        "biological_response": (
            "Logistic survival: survival = 0.58 + 0.42 / (1 + exp(5.0 × (severity - 0.78))). "
            "Steep dose-response curve. At severity 0.67 (high threshold), survival ≈ 85%; "
            "at severity 1.0 (extreme), survival ≈ 69%. Minimum floor: 25%. Dead zone: "
            "severity < 0.20 returns 1.0 (no contribution) to prevent phantom baseline drag."
        ),
        "time_intensity": (
            "Exponential decay: survival = exp(-0.45 × severity²). Models duration-dependent "
            "exposure. At severity 0.67, survival ≈ 82%; at severity 1.0, survival ≈ 64%. "
            "Minimum floor: 20%. Dead zone: severity < 0.20 returns 1.0."
        ),
        "modifier": (
            "Amplification: factor = 1.0 + 0.20 × severity. Does not reduce survival directly; "
            "multiplies the severity of downstream factors. Multiple modifiers targeting the "
            "same factor compound multiplicatively, capped at 1.35× combined."
        ),
        "hazard_probability": (
            "Quartic partial-loss: survival = 1.0 - 0.35 × severity⁴. "
            "Pests cause partial stand loss, up to ~45% at peak infestation (Penn State: "
            "wireworm 0-75%; ISU: cutworm 10-80%). At severity 0.70, survival ≈ 92%; "
            "at severity 1.0, survival ≈ 65%. Minimum floor: 55%. Dead zone: severity < 0.20 "
            "returns 1.0."
        ),
    },
    "what_high_means": [
        ("≥90%", "Conditions strongly favor a clean stand — go."),
        ("75–89%", "Workable but with one or two cautions — read the risk cards."),
        ("65–74%", "Real stand-loss exposure — wait for a better window if you can."),
        ("<65%", "Multiple risk factors threaten the stand — do not plant."),
    ],
}

RISK_METHODOLOGY: dict[str, dict] = {
    "chilling": {
        "name": "Imbibitional Chilling",
        "curve_type": "sigmoid",
        "curve_detail": (
            "Logistic curve centered 2°F below the crop's soil-temp threshold (corn 48°F, "
            "soybean 53°F) with a 4°F scale. Severity transitions smoothly from ~0 above "
            "the preferred temp to ~1 at extreme cold. Imbibition window is crop-specific: "
            "soybeans 0-24h (faster imbibition, ISU/SDSU), corn 0-48h (Purdue/Nielsen). "
            "A warming trend shifts severity down; a cooling trend shifts it up."
        ),
        "summary": (
            "Seeds drink in water during the first hours after planting (corn: 24-48h, "
            "soybeans: 6-24h). If that water is below ~50°F, cell membranes rupture and "
            "the seed dies before it ever germinates — no visible symptom above ground."
        ),
        "thresholds": [
            ("low", "Severity <0.33 — soil temp well above the crop floor"),
            ("moderate", "Severity 0.33–0.67 — soil temp near or slightly below the floor"),
            ("high", "Severity ≥0.67 — soil temp significantly below the floor"),
        ],
        "inputs": [
            "Open-Meteo forecast — soil_temperature_6cm, hourly across the 48h imbibition window",
            "Crop profile — minimum and preferred soil-temp floors",
        ],
        "references": [
            "Purdue (Nielsen) — Cold Soils & Risk of Imbibitional Chilling Injury in Corn",
            "UNL CropWatch — Cold Soil Temperature and Corn Planting Windows",
            "Cold Shock During Imbibition in Glycine max — establishes the 45°F (7.2°C) cell-membrane rupture threshold during the first 24h of water uptake; basis for the 48h temperature trend logic.",
            "Hydration Kinetics and Chilling Injury in Corn — Phase I water uptake in Zea mays and cold-water mitochondrial disruption; flags sudden post-planting temperature drops.",
            "Soil Temperature Variations and Emergence Penalties — penalty coefficients for chilling stress used to translate sub-threshold hours into percent stand loss.",
            "Seed Coat Permeability and Imbibitional Damage — variety-level water-uptake differences for future seed-specific risk modifiers.",
            "Respiration Rates During Cold Imbibition — ATP-suppression timeline that feeds the GDD tracker's delayed-emergence projection.",
            "Thermodynamics of Seed Hydration — water-velocity equations for modelling uptake rate from soil temperature.",
            "Membrane Phase Transitions in Cold-Stressed Seeds — lipid-phase mechanism behind chilling injury; foundational science for the imbibitional-chilling flag.",
            "Mitigating Chilling Injury through Seed Treatments — fungicidal/biological treatment interactions with chilling stress; supports a seed-treatment input modifier.",
            "Microclimate Modeling for Early Spring Planting — translates surface readings to seed-depth temperature; underpins the 48h forecast integration.",
            "Agronomic Outcomes of Sub-Optimal Planting Windows — longitudinal yield-loss data for chilled stands; economic justification for delaying planting.",
        ],
        "operator_levers": [
            "Wait for a forecast where overnight soil temps stay above the floor.",
            "Plant late morning once the soil starts climbing — avoid evening planting into a cold front.",
            "Trend matters as much as the absolute number: a warming forecast rescues borderline planting.",
        ],
    },
    "flooding": {
        "name": "Flooding & Waterlogging",
        "curve_type": "sigmoid",
        "curve_detail": "Logistic curve on 48h precipitation total, midpoint 1.5\", scale 0.6\". Escalated by USGS streamflow, CPC soil moisture, and NWS flood alerts.",
        "summary": (
            "Saturated soils starve seeds of oxygen. Two inches in 48 hours, or a few "
            "days submerged, will rot the seed before it can germinate."
        ),
        "thresholds": [
            ("low", "Forecast ≤ 1.0\" of rain in the next 48h"),
            ("moderate", "1.0–2.0\" forecast in 48h"),
            ("high", ">2.0\" forecast in 48h, OR an active NWS Flood Watch/Warning overlapping the window"),
        ],
        "inputs": [
            "Open-Meteo forecast — hourly precipitation, summed over 48h",
            "NWS active alerts — Flood Watch, Flood Warning, Flash Flood Warning",
        ],
        "references": [
            "NWS — Flood product definitions",
            "Hypoxia in Germinating Soybean Seeds — 24h-saturation shift to anaerobic respiration and ethanol toxicity; core biology behind the waterlogging flag.",
            "Soil Gas Exchange Rates Under Saturation — soil O2 depletion timeframe by precipitation amount and porosity; links forecast precip to root-zone oxygen.",
            "Ethanol Accumulation and Seed Death — toxicity thresholds used as an underwater-survival countdown for submerged seed.",
            "Waterlogging Tolerance Mechanisms in Zea mays — early seedlings cannot form aerenchyma fast enough; reinforces high risk in the emergence window.",
            "Impact of Standing Water on Soil Pathogen Proliferation — links waterlogging to secondary risks like Pythium for cross-factor escalation.",
            "Drainage Class and Waterlogging Risk — SSURGO drainage classes vs. water retention; underpins the soil-map integration.",
            "Temperature Interaction with Flooding Stress — warm flooding is far more lethal than cold; high-precip + high-temp = critical risk.",
            "Seed Vigor and Hypoxic Stress — initial seed quality vs. underwater survival time; supports advanced seed-quality inputs.",
            "Recovery Potential of Waterlogged Seedlings — recovery time after water recedes; informs delayed-maturity / harvest-date adjustments.",
            "Economic Impact of Early-Season Flooding — yield-penalty curves for 24/48/72h flooding events; basis for revenue-loss estimates.",
        ],
        "operator_levers": [
            "If a watch/warning is active, wait until it clears regardless of the modeled total.",
            "On poorly-drained ground, give an extra 24–48h after heavy rain for the profile to drain.",
            "Tile drainage reduces flooding severity by ~60% — tiled fields drain saturated water in 24–48h vs 4–7+ days untiled (DRAINMOD kinetics).",
        ],
    },
    "antecedent": {
        "name": "Antecedent Saturation",
        "curve_type": "sigmoid",
        "curve_detail": "Logistic curve on 30-day cumulative precipitation, midpoint between moderate and high thresholds (adjusted by SSURGO Hydrologic Soil Group). Additional 48h forecast rain layered on.",
        "summary": (
            "How wet the field already is when forecast rain lands. A profile that's been "
            "loaded for weeks has no buffer — even moderate rain pushes it past field capacity."
        ),
        "thresholds": [
            ("low", "<4\" rain in last 30d (or <3\" on runoff-prone Group D soils)"),
            ("moderate", "4–6\" / 30d, OR ≥12 wet days in 30d"),
            ("high", ">6\" / 30d, OR >4\" with another 0.5\" forecast in 48h"),
        ],
        "inputs": [
            "Open-Meteo Archive — last 30 days of daily precipitation",
            "SSURGO — Hydrologic Soil Group (A/B/C/D) for runoff/infiltration tuning",
            "Open-Meteo forecast — next-48h rain layered onto current saturation",
        ],
        "references": [
            "USDA NRCS — Hydrologic Soil Group classification",
            "Open-Meteo — Historical Weather API",
            "Soil Matric Potential and Hydraulic Conductivity — physics of how wet soil absorbs new rain; equations for remaining pore space from the 30-day total.",
            "Predicting Runoff vs. Infiltration in Saturated Soils — antecedent-moisture control of infiltration vs. ponding; underpins the ponding prediction.",
            "Role of Antecedent Moisture in Pathogen Ecology — 30-day trailing moisture vs. baseline water-mold population; explains why long-term wetness raises disease risk on dry days.",
            "Soil Compaction and Saturation Interactions — equipment on previously saturated soils destroys structure; relevant if planter passes are tracked.",
            "Capillary Action and Water Table Dynamics — high water tables move water up to the seed zone; matters for shallow-water-table portions of the Midwest.",
            "Antecedent Precipitation Index (API) Modeling — math framework that distills 30-day rainfall into a single risk score.",
            "Impact of Cover Crops on Antecedent Moisture — rye/cover-crop transpiration reshapes the 30-day moisture profile.",
            "Evapotranspiration Rates and Soil Drying — temperature/wind-driven drying used to decay the 30-day saturation score over time.",
            "Spatial Variability of Soil Moisture — within-field moisture variation; informs zone-based risk assessments.",
            "Long-term Precipitation Trends and Planting Delays — historical wet-spring impact on planting progress; macro-level context.",
        ],
        "operator_levers": [
            "Sandy (Group A) fields tolerate the same totals that drown clay-heavy (Group D) fields.",
            "Tile drainage reduces antecedent severity by ~50% — tiles lower the water table between rain events, so the field enters each storm with more available pore space.",
        ],
    },
    "topography": {
        "name": "Topography & Ponding",
        "curve_type": "sigmoid",
        "curve_detail": "Logistic curve on concavity (meters below neighbors), midpoint 0.2 m, scale 0.1 m. Escalated by 48h/168h forecast precipitation.",
        "summary": (
            "The modeled soil-moisture grid averages over ~9 km — it can't see whether your "
            "exact spot sits in a bowl that collects runoff. Local depressions drown seedlings "
            "even on otherwise well-drained fields."
        ),
        "thresholds": [
            ("low", "Field point at or above neighbours, OR slope >5 m/km"),
            ("moderate", "Concavity 0.2–0.6 m below neighbours on near-flat ground"),
            ("high", "Concavity >0.6 m, OR moderate concavity with >1\" forecast in 48h"),
        ],
        "inputs": [
            "Open-Meteo Elevation API — 3×3 grid sample around the field point (~90 m DEM)",
            "Open-Meteo forecast — next-48h and 168h precipitation as the saturation amplifier",
        ],
        "references": [
            "Open-Meteo — Elevation API (Copernicus DEM 90 m)",
            "Digital Elevation Models (DEM) in Precision Agriculture — slope/concavity methodology for predicting water accumulation; core logic for this risk factor.",
            "Topographic Wetness Index (TWI) Calculations — the specific TWI algorithm using upstream contributing area and slope.",
            "Micro-topography and Seedling Emergence — field study of stand counts in micro-depressions vs. micro-rises; validates the concavity focus.",
            "Surface Runoff Modeling using LiDAR — high-resolution elevation improves ponding prediction; guides the choice of elevation source.",
            "Topography's Influence on Soil Temperature — south-facing slopes warm and dry faster than flat ground; modifies chilling and saturation risks.",
            "Mapping Ephemeral Gullies and Ponding Zones — techniques for identifying temporary pooling zones inside a field.",
            "Interaction of Topography and Soil Type — depressions in clay are far riskier than in sand; combines DEM with SSURGO.",
            "Precision Drainage Management — tile drainage mitigates topographic risk; matters when a user supplies tile-line inputs.",
            "Topographic Control of Nitrogen Leaching — secondary nutrient-loss risk that the app could surface alongside ponding.",
            "Yield Variability based on Topographic Position — yield-penalty data for predicted ponding zones; supports ROI calculations.",
        ],
        "operator_levers": [
            "If the card flags a depression, walk it after the next rain — visible ponding confirms it.",
            "Plant the field but skip the bowl, or accept it as a known replant zone.",
            "Tile drainage reduces subsurface ponding contribution by ~40% — but cannot fix surface water in true low spots.",
        ],
    },
    "frost": {
        "name": "Frost & Freeze",
        "curve_type": "sigmoid",
        "curve_detail": "Logistic curve on forecast air-temp minimum (midpoint at frost threshold, scale 3°F). Max of temp sigmoid, hours-below sigmoid, and ensemble probability sigmoid. NWS frost/freeze alerts override upward.",
        "summary": (
            "A freeze in the 7-day emergence window kills exposed cotyledons (soybeans) or "
            "growing-point tissue (corn). We combine the deterministic forecast minimum with "
            "a multi-model ensemble probability and a 5-year same-date climatology probability."
        ),
        "thresholds": [
            ("low", "Min air > frost threshold (corn 28°F, soybean 30°F) for the full 168h window"),
            ("moderate", "Min air at or below threshold, OR ensemble Pr[freeze in 7d] ≥ 0.5, OR climatology Pr ≥ 0.4"),
            ("high", "Min air ≥4°F below threshold, OR ≥6 hours sub-freezing, OR ensemble Pr ≥ 0.7, OR active NWS Freeze Watch/Warning"),
        ],
        "inputs": [
            "Open-Meteo forecast — temperature_2m hourly across the 168h emergence window",
            "Open-Meteo Ensemble — 93 members across 4 models, daily frost probability",
            "Open-Meteo Archive — 5-year same-date climatology, daily Pr[T_min ≤ 32°F]",
            "NWS active alerts — Frost Advisory, Freeze Watch/Warning",
        ],
        "references": [
            "Open-Meteo — Ensemble API (ICON, GFS, GEM, ECMWF members)",
            "NWS — Frost/Freeze product definitions",
            "Ice Nucleation in Plant Tissues — when and how water freezes inside plant cells; explains the difference between surface frost and tissue-killing hard freeze.",
            "Critical Temperatures for Soybean Emergence — 30°F (-1.1°C) threshold for irreversible cotyledon damage; hard trigger for the soybean frost warning.",
            "Microclimate Inversions and Frost Pockets — cold air pools in low topographic areas; combines the topography and frost factors for hyper-local warnings.",
            "Growing Degree Days and Vulnerability Windows — emergence timing dictates whether a freeze is dangerous (post-emergence) vs. harmless (pre-emergence).",
            "Role of Soil Moisture in Frost Protection — wet soil retains more heat than dry soil; critical interaction between saturation and frost evaluators.",
            "Radiational vs. Advective Freezes — clear-night vs. cold-front freeze types; helps parse the 72-hour forecast accurately.",
            "Recovery Capacity of Frost-Damaged Corn — corn keeps its growing point below ground early on; foundation of crop-specific frost scoring.",
            "Ensemble Forecasting for Frost Prediction — statistical case for ensemble vs. deterministic models on extreme events; validates the Pr[freeze in 7d] usage.",
            "Residue Cover and Frost Risk — heavy residue insulates the soil but blocks heat radiation, raising surface frost risk; a user-input modifier.",
            "Historical Climatology of Last Spring Freezes — methodology behind the 5-yr climatology Pr[freeze] baseline.",
        ],
        "operator_levers": [
            "Probabilistic signals only ever raise the level here, never lower it — when the deterministic forecast says clear but the ensemble disagrees, trust the ensemble.",
            "Soybean cotyledons die at 32°F; corn growing point sits below ground and tolerates a brief 28°F dip pre-V5.",
        ],
    },
    "crusting": {
        "name": "Soil Crusting",
        "curve_type": "sigmoid",
        "curve_detail": "Logistic curve on a composite rain-heat-UV score, midpoint 1.0, scale 0.5. Score is modulated by tillage, residue cover, and SSURGO silt/clay/OM content.",
        "summary": (
            "A heavy storm followed by hot, sunny drying bakes the topsoil into a hard sheet. "
            "Seedlings exhaust their reserves trying to push through and die underground."
        ),
        "thresholds": [
            ("low", "No 24h rain ≥0.4\" followed by hot/sunny drying in the 7-day window"),
            ("moderate", "Crust score 0.5–1.5 (rain × heat-above-65°F × UV-above-4)"),
            ("high", "Crust score >1.5 — typically 0.5\"+ rain followed by 80°F+ and UV 6+"),
        ],
        "inputs": [
            "Open-Meteo forecast — hourly precipitation, temperature, UV index (7-day window)",
            "User input — tillage and residue (fine-tilth bare soil amplifies; no-till/heavy residue suppresses)",
            "SSURGO — silt/sand/clay percentages and organic matter (silt loams crust hardest, sands rarely crust)",
        ],
        "references": [
            "USDA NRCS — Soil Crusting and Surface Sealing",
            "Iowa State Extension — Managing Crusted Soil for Emergence",
            "Soil Crust Shear Strength and Hypocotyl Penetration — measures the physical force a seedling can exert vs. the strength of a baked crust; physics engine for this risk.",
            "The 'Rain-then-Bake' Phenomenon in Silt Loams — the heavy-rain → rapid-drying meteorological sequence the algorithm must look for.",
            "Organic Matter and Aggregate Stability — high OM reduces crusting; lowers the score when soil-test OM is high.",
            "Tillage Practices and Soil Crusting Risk — conventional-till vs. no-till residue effects; a critical user-input variable.",
            "Rotary Hoeing and Mechanical Crust Breaking — agronomic intervention the app can recommend at critical scores.",
            "Modeling Soil Surface Drying Rates — equations for surface drying based on solar radiation and wind; predicts when the crust hardens.",
            "Impact of Droplet Kinetic Energy on Soil Surfaces — rain intensity (not just total) drives crust formation; suggests using hourly rain rates.",
            "Seed Size and Emergence Force — large-seeded crops like soybeans struggle more with crusts than corn; crop-specific modifier.",
            "Soil Texture and Crusting Susceptibility — silt-loam high-risk categorization that drives the SSURGO texture multiplier.",
            "Chemical Soil Conditioners and Crusting — gypsum and other amendments as a future recommendation feature.",
        ],
        "operator_levers": [
            "On crust-prone silt loams, leave more residue or use a rotary hoe within 48h of the storm.",
            "Sandy soils essentially do not crust; clays clod rather than seal.",
        ],
    },
    "pythium": {
        "name": "Pythium Damping-Off",
        "curve_type": "gaussian",
        "curve_detail": "Bimodal gaussian: P. ultimum peak at 48°F (σ=8°F), P. sylvaticum peak at 72°F (σ=10°F). Each peak is modulated by a sigmoid on soil moisture/saturation hours. The maximum of the two species drives severity.",
        "summary": (
            "Pythium species kill seeds and seedlings in cold-wet (P. ultimum) or warm-saturated "
            "(P. sylvaticum, P. torulosum) soils. The fungus needs free water for its zoospores."
        ),
        "thresholds": [
            ("low", "Avg soil temp 60–75°F with moisture below saturation"),
            ("moderate", "Avg <60°F with elevated moisture, OR ≥36 saturated hours on warm soil"),
            ("high", "Avg <55°F with sustained saturation (cold-wet), amplified by poorly-drained / clay-heavy soils"),
        ],
        "inputs": [
            "Open-Meteo forecast — soil_temperature_6cm and soil_moisture_1_to_3cm, 96h window",
            "SSURGO — drainage class, clay %, sand % (poorly-drained clays amplify; sands de-rate)",
        ],
        "references": [
            "Matthiesen, Ahmad & Robertson (2016) — Plant Disease 100:583–591",
            "ISU Crop Protection Network — Damping-Off",
            "Temperature and Moisture Requirements for Pythium Infection — boolean requirement for both saturated soil and cool temps (often <60°F); core trigger logic.",
            "Chemotaxis of Pythium Zoospores to Seed Exudates — zoospores swim to the germinating seed; explains why standing water is necessary for infection.",
            "Fungicidal Seed Treatment Efficacy Under Extreme Disease Pressure — mefenoxam and analogue durability data; basis for a treatment-expiration timer.",
            "Interactions Between Chilling Injury and Pythium — cold-damaged seeds leak more exudates and attract more Pythium; compounding risk factor.",
            "Soil Drainage Class and Pythium Incidence — links SSURGO drainage to historical outbreaks; validates the soil-type contribution to the disease score.",
            "Pathogen Population Dynamics in No-Till Soils — cooler/wetter no-till soils harbour higher Pythium populations; tillage-input adjustment.",
            "Forecasting Pythium Blight Using Weather Data — early predictive models that the live API data modernises.",
            "Cover Crop Termination Timing and Pythium Risk — green-bridge effect where dying cover crops feed Pythium ahead of the cash crop.",
            "Economic Thresholds for Replanting due to Damping-Off — guidelines for when stand loss justifies replanting; supports actionable advice.",
        ],
        "operator_levers": [
            "Seed treatments (mefenoxam / metalaxyl for cold-wet Pythium; ethaboxam for the warm-soil species) are the standard defense.",
            "Avoid planting cold-wet ground on poorly-drained map units — the texture amplifier is doing real work in the model.",
            "Tile drainage reduces Pythium conduciveness by ~30% — shorter saturation windows mean fewer zoospore infection cycles.",
        ],
    },
    "phytophthora": {
        "name": "Phytophthora Root Rot",
        "curve_type": "trapezoidal",
        "curve_detail": "Trapezoidal on soil temperature with zoospore activity plateau 55–86°F (ramps 50→55 and 86→95). Multiplied by a sigmoid on saturation hours (midpoint 24h). Escalated by poor drainage and soy-on-soy rotation.",
        "summary": (
            "Soybean-specific root rot driven by warm, saturated soils. Risk concentrates in "
            "fine-textured fields with a history of the disease and susceptible varieties."
        ),
        "thresholds": [
            ("low", "Non-host crop (corn), OR ≤24 saturated hours in the 168h post-planting window"),
            ("moderate", "≥36 saturated hours with soil temps approaching 60°F"),
            ("high", "Sustained saturation (≥72h) with warm soils, amplified by clay-heavy or poorly-drained fields"),
        ],
        "inputs": [
            "Open-Meteo forecast — soil_moisture_1_to_3cm and deeper layers across 168h",
            "SSURGO — drainage class and texture (clays/poorly-drained amplify)",
            "Crop profile — soybean only; corn is not a host",
        ],
        "references": [
            "Crop Protection Network — Phytophthora Root Rot of Soybean",
            "Environmental Triggers for Phytophthora sojae Oospore Germination — saturated soil + warm temps (>60°F); the exact contrast logic that distinguishes Phytophthora from Pythium.",
            "Host Resistance and Pathogen Race Dynamics — Rps gene resistance in soybean varieties; can zero out the risk when the user supplies seed variety.",
            "Field Tolerance and Partial Resistance to Phytophthora — partial resistance behaviour for slow-developing yield loss vs. rapid seedling death.",
            "Impact of Soil Compaction on Phytophthora Severity — tire traffic and hardpans drive localized waterlogging and disease; ties into the antecedent saturation risk.",
            "Efficacy of Ethaboxam and Other Seed Treatments — modern chemistry data for adjusting risk by user-applied seed treatment.",
            "Modeling Disease Progress Curves based on Temperature — math for how fast the disease spreads after a warm-rain trigger; supports a disease-progression forecast.",
            "Relationship Between Topography and Phytophthora Hotspots — disease clustering in concavities; validates combining topography with the disease score.",
            "Survival of Oospores in Crop Residue — long-term in-field pathogen presence; justifies persistent baseline risk when conditions are met.",
            "Interactive Effects of Flooding Duration and Phytophthora — 24h of flooding with the pathogen is worse than 72h without it; algorithmic synergy with flooding.",
            "Remote Sensing of Phytophthora Stress — drone/satellite stress detection as a future integration option.",
        ],
        "operator_levers": [
            "Variety choice matters more than anything else — use Rps gene resistance plus partial-resistance ratings.",
            "Avoid planting wet, low spots on fields with a known history.",
            "Tile drainage reduces Phytophthora conduciveness by ~30% — shorter free-water windows limit zoospore swimming distance.",
        ],
    },
    "maggot": {
        "name": "Seedcorn Maggot",
        "curve_type": "gaussian",
        "curve_detail": "Gaussian on soil temperature, peak egg-laying at 55°F (σ=10°F). Multiplied by a sigmoid on moisture hours (midpoint 48h) and an organic-load multiplier (1.5× with manure/heavy residue, 0.6× without). GDD cross-reference uses a second gaussian at 325 DD base 39°F.",
        "summary": (
            "Adult flies lay eggs near decomposing organic matter (manure, heavy residue, "
            "incorporated cover crops). The actual crop damage occurs underground when "
            "larvae (maggots) hatch and burrow into seeds, hollowing them out. "
            "Risk is scored against the larval feeding window — which peaks ~150 DD "
            "after each adult-fly generation (504 / 1230 / 1950 DD base 39°F) — "
            "not the fly activity itself."
        ),
        "thresholds": [
            ("low", "Outside active maggot feeding window, OR soil too warm/dry for larval survival"),
            ("moderate", "Cool damp soil during a larval feeding wave, OR organic load with <60°F soil"),
            ("high", "Peak maggot feeding window + manure/heavy residue + soil <65°F + ≥72h saturation"),
        ],
        "inputs": [
            "Open-Meteo forecast — soil_temperature_6cm and soil_moisture_1_to_3cm, 168h window",
            "User input — manure_recent, residue level",
            "Open-Meteo Archive — cumulative GDD base 39°F from Jan 1; fly peaks at 354 / 1080 / 1800 DD, maggot damage peaks ~150 DD later at 504 / 1230 / 1950 DD",
        ],
        "references": [
            "ISU CIG / NCERA — Seedcorn Maggot Degree-Day Model (base 39°F)",
            "ISU Extension — Seedcorn Maggot in Soybean and Corn",
            "Role of Fresh Organic Matter in Oviposition — adult flies oviposit on freshly tilled green material (weeds, cover crops, manure); the primary biological trigger.",
            "Interactions Between Planting Date and Maggot Damage — delayed emergence from cold soil keeps seed in the vulnerable stage longer; links chilling and insect risk.",
            "Neonicotinoid Seed Treatments and Maggot Control — insecticide-treatment efficacy data; adjusts the baseline risk score.",
            "Tillage Timing as a Cultural Control Strategy — wait 14–21 days after incorporating green material; supports a safe-planting-window countdown.",
            "Soil Temperature Effects on Maggot Development Rates — math for larval growth in soil; tracks the duration of the threat.",
            "Predicting Seedcorn Maggot Risk Using Weather APIs — modern review of meteorological pest forecasting; validates the API-driven approach.",
            "Alternative Hosts and Weed Management — weed species that attract the most flies; relevant if weed-pressure data is integrated.",
            "Economic Injury Levels for Seedcorn Maggot — stand-loss percentages required to impact yield; supports threat-severity assessment.",
            "Climate Change Impacts on Multivoltine Pest Phenology — earlier springs lead to more maggot generations; long-term predictive accuracy.",
        ],
        "operator_levers": [
            "Time planting to avoid the larval feeding window — the maggot damage peak lags each fly generation by ~150 DD.",
            "Insecticide seed treatments (clothianidin, thiamethoxam) are the standard defense when maggot pressure is unavoidable.",
            "Delay planting after manure application if the field has stayed cold and wet — decomposing organic matter attracts egg-laying flies, leading to more maggots.",
        ],
    },
    "wireworm": {
        "name": "Wireworm",
        "curve_type": "trapezoidal",
        "curve_detail": "Trapezoidal on soil temperature, activity plateau 45–60°F (ramps 35→45 and 60→70). Multiplied by a sigmoid on damp hours (midpoint 48h) and a grass/rotation amplifier (1.5× for sod/grass history).",
        "summary": (
            "Click-beetle larvae hollow out seeds and tunnel into stems. Pressure is highest "
            "in fields rotated out of grass or sod, in cool damp soils."
        ),
        "thresholds": [
            ("low", "No grass/sod history AND warm/dry conditions"),
            ("moderate", "Cool damp window OR previous grass with soil <70°F"),
            ("high", "Previous grass/sod AND cool (<60°F) damp (>72h) soils"),
        ],
        "inputs": [
            "Open-Meteo forecast — soil_temperature_6cm and soil_moisture_1_to_3cm, 168h window",
            "User input — previous_grass (was last year sod, pasture, or grass cover?)",
        ],
        "references": [
            "ISU Extension — Wireworms in Field Crops",
            "Vertical Migration of Wireworms in Response to Soil Thermoclines — wireworms move up to the seed zone when cool/damp and retreat deep when hot/dry; core logic for this risk flag.",
            "Baiting Techniques and Population Assessment — physical scouting protocols recommended when predictive risk is high.",
            "Species Composition and Differential Temperature Responses — Melanotus vs. Agriotes thermal preferences; advanced logic for future versions.",
            "Crop Rotation History and Wireworm Pressure — pasture/sod/small-grain history drives current populations; vital user-input variable.",
            "Efficacy of In-Furrow Insecticides vs. Seed Treatments — chemical control comparison; adjusts risk by planter setup.",
            "Moisture Gradients and Larval Survival — wireworms die rapidly in dry soil and migrate accordingly; integrates with precipitation/saturation forecasts.",
            "Modeling Wireworm Feeding Damage on Corn Radicles — exact timing/mechanism of seed destruction; pinpoints vulnerable growth stages.",
            "Impact of Cover Crops on Wireworm Populations — cover crops can both feed and distract wireworms; nuanced logic for the engine.",
            "Long-term Population Dynamics in No-Till Systems — wireworms persist in undisturbed soils; supports the tillage user-input.",
            "Economic Thresholds for Wireworm Management — acceptable stand-loss guidelines; defines what 'high risk' means in dollars.",
        ],
        "operator_levers": [
            "On a known wireworm field, treated seed is the standard defense — there's no in-season rescue.",
            "If risk is moderate-high, scouting baits (3 weeks pre-plant) tell you whether you have a real population.",
        ],
    },
    "slugs": {
        "name": "Slugs",
        "curve_type": "trapezoidal",
        "curve_detail": "Trapezoidal on air temperature, feeding plateau 50–65°F (ramps 40→50 and 65→75). Multiplied by a sigmoid on high-humidity hours (>85% RH, midpoint 48h). Only active in no-till fields with moderate/heavy residue.",
        "summary": (
            "Slugs shred young leaves down to the stem in cool, humid, residue-covered fields. "
            "Almost exclusively a no-till + heavy-residue problem."
        ),
        "thresholds": [
            ("low", "Tilled or low-residue field — slugs need cover to survive"),
            ("moderate", "No-till + moderate/heavy residue with cool damp forecast"),
            ("high", "No-till + heavy residue + avg air <65°F + ≥72h above 85% humidity"),
        ],
        "inputs": [
            "User input — tillage and residue",
            "Open-Meteo forecast — temperature_2m and relative_humidity_2m, 168h window",
        ],
        "references": [
            "Penn State Extension — Slug Management in No-Till Field Crops",
            "Microclimate Regulation of Slug Activity in Crop Canopies — reliance on high humidity and soil moisture; ties slug risk to precipitation/saturation data.",
            "Crop Residue and Overwintering Survival of Gastropods — no-till heavy residue is the primary outbreak driver; basis for 'tilled fields = slugs unlikely'.",
            "Feeding Preferences and Crop Stage Vulnerability — succulent young tissue and pre-V3 damage; defines the risk window.",
            "Efficacy of Metaldehyde and Iron Phosphate Baits — chemical-control data; supports baiting recommendations under high risk.",
            "Predatory Carabid Beetles and Biological Control — some seed treatments kill slug predators and worsen outbreaks; advanced agronomic logic.",
            "Temperature Thresholds for Slug Foraging — cool damp nights for active feeding; uses hourly weather to predict feeding intensity.",
            "Impact of Cover Crop Mats on Slug Populations — rolled rye and dense covers create perfect slug habitat; modifier for regenerative-ag users.",
            "Spatial Distribution of Slugs within Fields — slugs congregate in low/wet areas identified by the topography risk; algorithmic synergy.",
            "Weed Hosts and Alternative Food Sources — early weed control can force starving slugs onto the cash crop; nuanced management advice.",
            "Developing Predictive Models for Slug Outbreaks — review of mathematical forecasting from winter/spring weather; validates the approach.",
        ],
        "operator_levers": [
            "Row cleaners or rolled residue cut populations significantly.",
            "Metaldehyde or iron-phosphate baits work but need scout-confirmed pressure to justify the cost.",
        ],
    },
    "cutworm": {
        "name": "Black Cutworm",
        "curve_type": "gaussian",
        "curve_detail": "Gaussian on cumulative GDD (base 50°F) from first significant moth flight to estimated emergence date, centered at the 200–450 DD damage window midpoint (325 DD, σ=125 DD). Scaled by a habitat multiplier (no-till, residue, sod history).",
        "summary": (
            "Migratory moths fly north in spring; larvae cut corn at the soil line ~300 GDD "
            "(base 50°F) after a significant flight. Soybeans are not a meaningful host."
        ),
        "thresholds": [
            ("low", "Soybean crop, OR window outside the 200–450 GDD post-flight damage band"),
            ("moderate", "Approaching the damage window for corn"),
            ("high", "Corn planted into the active 200–450 GDD post-flight cutting window"),
        ],
        "inputs": [
            "Open-Meteo Archive — cumulative GDD base 50°F since assumed flight start (default DOY 105)",
            "Crop profile — corn only; soybeans have low cutworm exposure",
        ],
        "references": [
            "ISU CIG / Hodgson — Black Cutworm Degree-Day Model",
            "Penn State Extension — Black Cutworm in Field Corn",
            "Synoptic Weather Patterns and Black Cutworm Migration — moth flights from the Gulf Coast to the Midwest on spring storm fronts; core predictive logic for the cutworm module.",
            "Weed Density and Oviposition Preferences — moths preferentially oviposit on dense winter annuals (chickweed, bittercress); raises risk if the field was 'green' pre-plant.",
            "Bt Corn Traits and Cutworm Efficacy — transgenic traits (Vip3A, Cry1F) that control cutworm; can zero out the risk for highly traited corn.",
            "Rescue Treatments and Scouting Thresholds — foliar insecticide guidelines; triggers a scouting alert when GDD reaches the critical threshold.",
            "The 'Green Bridge' Effect of Delayed Herbicide Application — killing weeds too close to planting forces hatched larvae onto emerging corn; a vital management interaction.",
            "Soil Moisture and Subterranean Feeding — in dry soils cutworms feed below ground and rescue treatments fail; integrates with soil-moisture forecasts.",
            "Impact of Tillage on Egg Survival — spring tillage destroys eggs/larvae and lowers risk vs. no-till; supports the tillage input.",
            "Pheromone Trapping Networks as Predictive Inputs — regional trap-catch data the backend can scrape for real-time migration tracking.",
            "Host Plant Non-Preference: Why Soybeans are Rarely Attacked — biological basis for the 'soybeans not a primary host' rule; validates crop-specific filtering.",
        ],
        "operator_levers": [
            "If you're planting into the damage window, scout at V1–V3 — cut plants and dirt-plug holes are the tell.",
            "Bt traits with cutworm coverage handle most pressure; rescue pyrethroid treatments are effective when caught early.",
        ],
    },
    "leaf_beetle": {
        "name": "Bean Leaf Beetle",
        "curve_type": "sigmoid",
        "curve_detail": "Logistic curve on planting DOY, midpoint 7 days before the early-planting threshold (DOY ~123), scale 7 days, inverted (earlier = higher risk). Multiplied by a winter-severity factor: 1.4× for mild dormant-season tail, 0.6× for cold.",
        "summary": (
            "Overwintered adults swarm the first emerged soybean fields. A mild winter end "
            "leaves more beetles alive; an early planting date makes you a magnet for them."
        ),
        "thresholds": [
            ("low", "Corn (non-host), OR planting after the first wave of regional emergence"),
            ("moderate", "Soybean planted near the early-magnet window with average winter survival"),
            ("high", "Early soybean planting (<DOY 130) AND a mild winter end (≤4 frost days in last 30d)"),
        ],
        "inputs": [
            "Open-Meteo Archive — frost-day count over the last 30 days as winter-survival proxy",
            "Planting date — earlier = bigger magnet for overwintered adults",
            "Crop profile — soybean only",
        ],
        "references": [
            "Lam & Pedigo (2000) — Bean Leaf Beetle Winter Mortality Model; ISU CIG",
            "Emergence Timing and Early-Planted Soybeans — the first emerged soybeans act as a regional trap crop for the overwintering generation; validates the early-planting warning.",
            "Transmission of Bean Pod Mottle Virus (BPMV) — early-season feeding spreads a severe viral disease; explains why controlling the overwintering generation matters economically.",
            "Degree Day Models for First and Second Generations — thermal tracking to predict mid-summer generations from the early spring emergence date.",
            "Efficacy of Neonicotinoid Seed Treatments on Overwintering Adults — standard seed-treatment efficacy for early feeding; adjusts risk based on seed tags.",
            "Feeding Preferences on Cotyledon and Unifoliate Leaves — damage mechanics that help users identify the pest while scouting.",
            "Role of Insulating Snow Cover on Winter Survival — harsh winters only kill beetles without snow cover; nuance the algorithm must apply to historical winter weather.",
            "Landscape Ecology and Overwintering Habitat — beetles overwinter in wooded leaf litter; raises baseline risk for fields adjacent to woods.",
            "Economic Injury Levels for Early Season Defoliation — soybeans tolerate massive early defoliation before yield is impacted; balances warning severity.",
            "Climate Change and Range Expansion of the Bean Leaf Beetle — long-term increased survival from milder winters; contextualises the predictive model.",
        ],
        "operator_levers": [
            "Delay soybean planting past the first regional wave if BLB pressure has been a problem in past years.",
            "Insecticide seed treatment buys 2–3 weeks of protection when you can't avoid the magnet window.",
        ],
    },
    "herbicide": {
        "name": "Herbicide Carryover",
        "curve_type": "composite",
        "curve_detail": "Categorical severity based on user-reported herbicide and the crop rotation. Known risky crop/herbicide pairs are assigned high severity (0.8); known herbicides without a direct crop conflict get moderate (0.5); unknown herbicides get cautionary moderate (0.45).",
        "summary": (
            "Residual herbicides from the previous season can stunt or kill the new crop. "
            "Driven entirely by what you tell us — no API can read the soil's chemistry."
        ),
        "thresholds": [
            ("low", "No previous-season residual herbicide reported"),
            ("moderate", "Reported herbicide is unfamiliar OR not on the documented-injury list for this crop"),
            ("high", "Reported herbicide × crop pair is on the documented-injury list (e.g. atrazine → soybeans, fomesafen → corn)"),
        ],
        "inputs": [
            "User input — herbicide_last_season free-text on the home page",
        ],
        "references": [
            "Product labels — rotation/restriction tables; university extension herbicide-injury guides",
            "Microbial Degradation Kinetics of Residual Herbicides — chemical breakdown is biologically driven and requires adequate soil moisture; links last year's rainfall to current risk.",
            "Soil pH Impacts on Herbicide Half-Lives — ALS-inhibitor and similar chemistries persist in high-pH soils; integrates with user soil-test data.",
            "Temperature Constraints on Soil Microbial Activity — degradation halts during cold winters; the breakdown clock only runs in warm/moist periods.",
            "Rotational Crop Sensitivities to Specific Chemistries — tables of which crops are killed by which carryover chemicals; cross-references previous applications with planting plans.",
            "Impact of Drought on Fomesafen and Atrazine Carryover — case studies on high-risk chemicals during dry years; real-world parameters for the engine.",
            "Soil Organic Matter and Chemical Binding — high-OM soils bind herbicides; reduces initial efficacy but can raise long-term carryover risk.",
            "Bioassay Techniques for Detecting Carryover — physical testing methods the app can recommend when the algorithm flags severe risk.",
            "Interaction of Carryover Stress and Seedling Diseases — sub-lethal carryover doses weaken plants and raise Pythium susceptibility; multi-factor synergy.",
            "Modeling Herbicide Dissipation Rates — equations for remaining ppb in soil based on time, temperature, and moisture.",
            "Tillage Effects on Herbicide Dilution — deep tillage mixes/dilutes the chemical band and lowers risk vs. no-till; final adjustment for field practices.",
        ],
        "operator_levers": [
            "Add the herbicide name on the home page so this card can score it — without input we can only return 'low / unknown'.",
            "When in doubt, run a bioassay (plant a few rows of the rotation crop in pots of field soil) before committing the field.",
        ],
    },
    "heat_stress": {
        "name": "Heat Stress",
        "curve_type": "sigmoid",
        "curve_detail": "Logistic curve on peak 48h air temperature (midpoint at crop stress threshold, scale 6°F). Hours above stress threshold provide a secondary sigmoid. At or above the lethal threshold, severity is forced to 1.0 and survival is hard-overridden to 0%.",
        "summary": (
            "Extreme air temperatures above the crop's stress threshold desiccate the seed zone, "
            "inhibit root elongation, and kill pollen/tissue. Locations in arid deserts or extreme-heat "
            "climates will dynamically score near 0% — this is the hot-end mirror of the hard-freeze rule."
        ),
        "thresholds": [
            ("low", "Peak 48h air temp stays well below the stress threshold — no heat concern"),
            ("moderate", "Air temp approaches or briefly exceeds the stress threshold"),
            ("high", "Sustained hours above stress threshold OR peak near lethal threshold — severe damage likely"),
        ],
        "inputs": [
            "Open-Meteo Forecast API — hourly temperature_2m for the 48h planting window",
            "Crop profile — heat_stress_f (95°F for corn/soybeans) and heat_lethal_f (113°F corn, 108°F soybeans)",
        ],
        "references": [
            "Hatfield & Prueger (2015) — Temperature Extremes: Effect on Plant Growth and Development. Weather and Climate Extremes.",
            "Sánchez et al. (2014) — Temperatures and the Growth and Development of Maize and Rice: A Review. Global Change Biology.",
            "Djanaguiraman et al. (2013) — High Temperature Stress and Soybean Leaves: Leaf Anatomy and Photosynthesis. Crop Science.",
            "Lobell et al. (2011) — Nonlinear Heat Effects on African Maize as Evidenced by Historical Yield Trials. Nature Climate Change.",
        ],
        "operator_levers": [
            "Irrigated fields can mitigate some heat stress through evaporative cooling — but this evaluator currently models rainfed conditions.",
            "Consider heat-tolerant cultivars for southern or western locations where heat stress is chronic.",
        ],
    },
    "water_scarcity": {
        "name": "Water Scarcity",
        "curve_type": "composite",
        "curve_detail": "Composite of four signals: (1) sigmoid on 14-day forecast precipitation (inverted — less rain = higher severity), (2) sigmoid on 30-day historical precipitation (inverted), (3) piecewise-linear on USDM drought class (D0→D4), (4) sigmoid on CPC soil moisture percentile (inverted — drier = higher). Hard override to 0% survival when forecast <0.1\" AND recent <0.5\".",
        "summary": (
            "Rainfed crops require minimum precipitation for seed imbibition and early root growth. "
            "Desert and arid locations with near-zero precipitation in both the forecast and recent history "
            "will dynamically score near 0% — water is as essential as temperature for germination. "
            "This is the dry-end mirror of the flooding evaluator."
        ),
        "thresholds": [
            ("low", "Adequate moisture from forecast precipitation and recent rainfall history"),
            ("moderate", "Marginal moisture — borderline for reliable germination without irrigation"),
            ("high", "Insufficient precipitation for rainfed germination; USDM drought class and/or CPC soil moisture confirm aridity"),
        ],
        "inputs": [
            "Open-Meteo Forecast API — daily precipitation_sum for the 14-day window",
            "Open-Meteo Archive API — 30-day historical precipitation",
            "USDM Drought Monitor — drought classification (D0–D4) at the field point",
            "NOAA CPC — gridded soil moisture percentile",
        ],
        "references": [
            "Crop Water Requirements: Doorenbos & Pruitt (1977, FAO Irrigation and Drainage Paper 24) — foundational ET-based water needs by crop and growth stage.",
            "Seed Germination Water Potential Thresholds: Bradford (1990) — minimum matric potential for imbibition varies by species; corn requires wetter conditions than soybeans.",
            "USDM Drought Classification Methodology: Svoboda et al. (2002) — D0–D4 integrates multiple indices (PDSI, SPI, soil moisture, streamflow) into a single severity class.",
            "Drought Impacts on Corn Yields: Lobell et al. (2014) — yield loss accelerates nonlinearly under water deficit, especially at germination and silking.",
            "Soil Moisture and Germination: Hadas & Russo (1974) — seed water uptake rate depends on soil water potential; below -1.5 MPa permanent wilting point, imbibition stalls.",
        ],
        "operator_levers": [
            "If you have irrigation available, water scarcity risk does not apply — this evaluator assumes rainfed conditions.",
            "For fields with center-pivot or drip irrigation, this card can be disregarded for the germination window.",
        ],
    },
    "sds": {
        "name": "Sudden Death Syndrome",
        "curve_type": "sigmoid",
        "curve_detail": (
            "Inverted sigmoid on average soil temperature (midpoint 60°F, scale 5°F) combined "
            "with a moisture sigmoid on hours above field capacity (midpoint 36h, scale 12h). "
            "Cool + wet conditions drive F. virguliforme root colonisation at planting."
        ),
        "summary": (
            "Fusarium virguliforme infects soybean roots at planting under cool, wet soils. "
            "Root colonisation severity is set at planting even though foliar symptoms don't "
            "appear until R3-R6 months later."
        ),
        "thresholds": [
            ("low", "Soil >62°F or well-drained — minimal SDS infection risk"),
            ("moderate", "Soil 55-62°F with moderate moisture — moderate root colonisation"),
            ("high", "Soil <55°F with sustained saturation — high SDS root colonisation risk"),
        ],
        "inputs": [
            "Open-Meteo forecast — soil_temperature_6cm, hourly across 7-day window",
            "Open-Meteo forecast — soil_moisture_1_to_3cm for saturation detection",
            "SSURGO — drainage class for poorly-drained field identification",
            "Crop rotation — soy-on-soy increases F. virguliforme inoculum",
            "Crop profile — soybean only (sds_sensitive flag)",
        ],
        "references": [
            "CPN — An Overview of Sudden Death Syndrome of Soybean",
            "Mueller et al. (2019) Effect of Planting Date, Seed Treatment, and Cultivar on SDS — Plant Disease 103:2",
            "SDSU Extension — Are You at Risk for Sudden Death Syndrome in Soybean?",
            "OSU Ohioline AC-44 — Sudden Death Syndrome of Soybean",
            "ISU ICM — Assess Soybean Disease Risk in Spring Planting",
            "MSU Extension — Soybean Sudden Death Syndrome",
            "SCN-SDS Synergy — SCN wound sites amplify F. virguliforme root colonisation (ISU SCN project)",
        ],
        "operator_levers": [
            "Delay planting until soil warms above 60°F to reduce F. virguliforme infection window.",
            "ILeVO (fluopyram) seed treatment is the primary chemical defence against SDS root colonisation.",
            "Tile drainage shortens the free-water window F. virguliforme needs.",
            "Rotate away from soybeans for 2+ years in fields with SDS history.",
        ],
    },
    "rhizoctonia": {
        "name": "Rhizoctonia Seedling Blight",
        "curve_type": "gaussian",
        "curve_detail": (
            "Gaussian conduciveness curve centred at 80°F soil temperature (σ=12°F) combined "
            "with a moisture sigmoid (midpoint 0.30 VWC, scale 0.08). Occupies the warm-damp "
            "niche between Pythium (cool-wet) and Phytophthora (warm-saturated)."
        ),
        "summary": (
            "Rhizoctonia solani causes reddish-brown lesions at the soil line, killing seedlings "
            "before or just after emergence. Favours warm (60-95°F, peak 80°F), damp soils — "
            "the complementary pathogen niche to Pythium and Phytophthora."
        ),
        "thresholds": [
            ("low", "Soil <60°F or dry — Rhizoctonia largely quiescent"),
            ("moderate", "Soil 65-75°F with moderate moisture — some infection risk"),
            ("high", "Soil 75-90°F with damp conditions — high damping-off risk"),
        ],
        "inputs": [
            "Open-Meteo forecast — soil_temperature_6cm, hourly across 5-day window",
            "Open-Meteo forecast — soil_moisture_1_to_3cm for dampness signal",
            "Tillage — no-till / heavy residue amplifies inoculum",
            "Crop rotation — soy-on-soy increases Rhizoctonia pressure",
            "Crop profile — rhizoctonia_sensitive flag (soybeans, dry beans)",
        ],
        "references": [
            "CPN — Rhizoctonia Seedling Blight and Root Rot of Soybean",
            "OSU Ohioline PLPATH-SOY-1 — Rhizoctonia Damping-off and Root Rot of Soybean",
            "UMN Extension — Rhizoctonia Root and Stem Rot of Soybean",
            "UMN Extension — Soybean Seed and Seedling Diseases",
            "CPN — Overview of Soybean Seedling Diseases (disease complex)",
            "ISU ICM — Assess Soybean Disease Risk in Spring Planting",
        ],
        "operator_levers": [
            "Fungicide seed treatments with SDHI or strobilurin actives are effective against Rhizoctonia.",
            "Rhizoctonia favours warm soil — unlike Pythium, waiting for warmer conditions does not help.",
            "Good seedbed preparation and avoiding compaction reduce damping-off severity.",
        ],
    },
    "anthracnose": {
        "name": "Anthracnose",
        "curve_type": "sigmoid",
        "curve_detail": (
            "Sigmoid on conducive hours (55-79°F + >90% RH in 72h window, midpoint 24h, scale 10). "
            "Rain severity (7d sum, midpoint 1.0\", scale 0.5) provides secondary signal for spore "
            "dispersal via rain splash. Bean/soy rotation adds +0.12 to severity."
        ),
        "summary": (
            "Colletotrichum lindemuthianum causes sunken dark lesions on pods, stems, and leaves. "
            "Favoured by cool-moderate temps (55-79°F) with prolonged high humidity (>92% RH) and "
            "rain splash for spore dispersal. Seed-borne pathogen — up to 100% yield loss in "
            "susceptible cultivars under favorable conditions."
        ),
        "thresholds": [
            ("low", "Dry or hot/cold conditions — anthracnose spores inactive"),
            ("moderate", "12-30h conducive + some rain — monitor for lesions"),
            ("high", ">30h cool/humid + frequent rain — high infection risk"),
        ],
        "inputs": [
            "Open-Meteo forecast — temperature_2m, relative_humidity_2m (72h window)",
            "Open-Meteo forecast — daily precipitation_sum (7-day total)",
            "CropScape CDL — crop rotation history (bean/soy-on-bean penalty)",
            "Crop profile — anthracnose_sensitive flag (dry beans)",
        ],
        "references": [
            "Cornell — Bean Anthracnose disease factsheet",
            "MSU Extension — Dry Bean Anthracnose Identification and Management",
            "CABI Compendium — Colletotrichum lindemuthianum (anthracnose of bean)",
            "Meyer & Badaruddin 2001 — Frost Tolerance of Ten Seedling Legume Species (Crop Science)",
        ],
        "operator_levers": [
            "Use certified, anthracnose-free seed — the pathogen is seed-borne.",
            "Rotate 2-3 years away from beans to reduce field inoculum.",
            "Fungicide applications (azoxystrobin, pyraclostrobin) at R3-R5 if conditions are conducive.",
            "Avoid working in fields when foliage is wet to prevent mechanical spread.",
        ],
    },
    "bacterial_blight": {
        "name": "Bacterial Blight",
        "curve_type": "sigmoid",
        "curve_detail": (
            "Dual-pathogen model: CBB uses a sigmoid on avg temp (midpoint 82°F, scale 6) and "
            "halo blight uses a Gaussian on avg temp (peak 68°F, σ=10°F). Both weighted 50/50 "
            "with humid hours (>85% RH, midpoint 30h). The dominant pathogen is whichever finds "
            "more favorable conditions. Wind + rain amplifies severity +0.08."
        ),
        "summary": (
            "Two bacterial pathogens with opposite temperature preferences target dry beans. "
            "Common bacterial blight (Xanthomonas) peaks above 80°F with high humidity. "
            "Halo blight (Pseudomonas) peaks at 60-75°F with leaf wetness. Both are seed-borne "
            "and can cause 40-70% yield loss."
        ),
        "thresholds": [
            ("low", "Dry conditions or extreme temps — bacterial spread limited"),
            ("moderate", "Moderate humidity with favorable temps for one pathogen"),
            ("high", "Prolonged high humidity + favorable temps + wind/rain — high blight risk"),
        ],
        "inputs": [
            "Open-Meteo forecast — temperature_2m, relative_humidity_2m (72h window)",
            "Open-Meteo forecast — wind_speed_10m for mechanical injury / dispersal",
            "Open-Meteo forecast — daily precipitation_sum (7-day total)",
            "CropScape CDL — crop rotation history (bean/soy residue penalty)",
            "Crop profile — bacterial_blight_sensitive flag (dry beans)",
        ],
        "references": [
            "CSU Extension — Bacterial Diseases of Beans (2.913)",
            "Cornell — Bacterial Diseases of Beans factsheet",
            "Manitoba Agriculture — Bacterial Blight in Dry Beans",
            "Springer (2013) — Bean common bacterial blight: pathogen epiphytic life",
            "Dry Bean Agronomy (drybeanagronomy.ca) — Bacterial Blights: CBB, Halo Blight, BBS",
        ],
        "operator_levers": [
            "Plant certified, disease-free seed — the primary transmission route.",
            "Rotate 2-3 years away from beans; bacterial residue can overwinter on crop debris.",
            "Avoid field operations when foliage is wet — mechanical spread via equipment.",
            "Copper-based bactericides provide partial suppression but are not curative.",
        ],
    },
    "idc": {
        "name": "Iron Deficiency Chlorosis",
        "curve_type": "sigmoid",
        "curve_detail": (
            "Sigmoid on SSURGO soil pH (midpoint 7.4, scale 0.3). Amplified by wet soil "
            "conditions (dissolved bicarbonate blocks iron uptake). Cultivar IDC score "
            "modifies severity: tolerant varieties (score ≤2) halve the risk."
        ),
        "summary": (
            "Calcareous soils (pH>7.4) produce bicarbonate that blocks soybean iron uptake, "
            "causing interveinal yellowing, stunting, and yield loss. Wet conditions worsen IDC "
            "by dissolving more CaCO3. Annual US losses exceed $260M in the western Corn Belt."
        ),
        "thresholds": [
            ("low", "Soil pH <7.2 — IDC essentially zero"),
            ("moderate", "pH 7.2-7.6 — borderline; variety choice and moisture matter"),
            ("high", "pH >7.6 + wet conditions — high IDC risk, especially with susceptible variety"),
        ],
        "inputs": [
            "SSURGO — soil pH (dominant component of map unit)",
            "Open-Meteo forecast — soil_moisture_1_to_3cm for wet-condition amplifier",
            "Cultivar — IDC tolerance score (1-5; lower is more tolerant)",
            "Crop profile — soybean only (idc_sensitive flag)",
        ],
        "references": [
            "UMN Extension — Managing Iron Deficiency Chlorosis in Soybean",
            "NDSU — Iron Deficiency Chlorosis in Soybean",
            "SDSU — Management Recommendations for Soybean IDC",
            "UMN Crop News — IDC in Soybean: 4 Things to Know",
            "Annual US IDC losses estimated at $260M in western Corn Belt / Great Plains",
        ],
        "operator_levers": [
            "Select IDC-tolerant varieties (score 1-2) for fields with pH >7.4.",
            "In-furrow iron chelate (Fe-EDDHA) can rescue severe fields but is expensive.",
            "Companion crop (oats seeded with soybeans) reduces soil moisture and IDC expression.",
            "Improve drainage to reduce dissolved bicarbonate in the root zone.",
        ],
    },
    "scn": {
        "name": "Soybean Cyst Nematode",
        "curve_type": "threshold",
        "curve_detail": (
            "Composite threshold model: soy-on-soy rotation (+0.45), soil temp above 50°F "
            "hatch threshold (+0.15), PI88788 resistance erosion under continuous soy (+0.1). "
            "Acts as a MODIFIER — amplifies SDS, Pythium, and Phytophthora severity rather "
            "than directly reducing survival."
        ),
        "summary": (
            "SCN is the #1 pest of US soybeans (>$1B annual losses). At planting time it doesn't "
            "kill seeds directly but creates root wound sites that amplify Fusarium (SDS), Pythium, "
            "and Phytophthora infection. Continuous soybean without resistance rotation accelerates "
            "population buildup."
        ),
        "thresholds": [
            ("low", "Rotated field with non-soy previous crop — SCN pressure minimal"),
            ("moderate", "Soy-on-soy OR warm soil + limited resistance diversity"),
            ("high", "Soy-on-soy with warm soil + same PI88788 resistance source — high modifier pressure"),
        ],
        "inputs": [
            "Crop rotation — soy-on-soy detection from CDL/rotation history",
            "Open-Meteo forecast — soil_temperature_6cm (J2 hatch at ≥50°F)",
            "Cultivar — SCN resistance source (PI88788, Peking, etc.)",
        ],
        "references": [
            "ISU SCN Project — Life Cycle and Management",
            "CPN — An Overview of Soybean Cyst Nematode",
            "UMN — SCN Management Guide",
            "Purdue Nematology — Soybean Nematodes",
            "OSU Ohioline — Soybean Cyst Nematode (PLPATH-SOY-5)",
            "SCN-SDS synergy — documented interaction where SCN wound sites amplify F. virguliforme root colonisation",
        ],
        "operator_levers": [
            "Rotate away from soybeans for 2+ years — the single most effective SCN management tool.",
            "Alternate SCN resistance sources (PI88788 → Peking → PI88788) to slow adaptation.",
            "Seed-applied nematicide (e.g., Clariva, Ilevo) provides partial protection.",
            "Soil sampling for SCN egg counts (>10,000/100cc = high risk) confirms field pressure.",
        ],
    },
    "hessian_fly": {
        "name": "Hessian Fly",
        "curve_type": "sigmoid",
        "curve_detail": (
            "Logistic curve on days before the regional fly-free date, midpoint 7 days, "
            "scale 4. Fly-free date approximated by latitude: ~Sept 1 at 47°N, ~Oct 20 at "
            "35°N (3.5 days later per degree south)."
        ),
        "summary": (
            "Hessian fly adults lay 250-300 eggs on fall-planted wheat. Larvae stunt growth "
            "and cause lodging. Planting after the regional fly-free date is the most effective "
            "management tool."
        ),
        "thresholds": [
            ("low", "Planting on or after the fly-free date"),
            ("moderate", "Planting 3-7 days before the fly-free date"),
            ("high", "Planting >7 days before the fly-free date"),
        ],
        "inputs": ["Latitude (for fly-free date approximation)", "Planting date"],
        "references": [
            "MU Extension G7180 — Hessian Fly Management in Missouri",
            "Penn State Extension — Hessian Fly on Wheat",
            "KSU Entomology — Hessian Fly",
            "OSU Ohioline — Fly-Free Date and Wheat Planting",
        ],
        "operator_levers": [
            "Delay planting until after the local fly-free date.",
            "Use insecticidal seed treatment (imidacloprid) if planting early.",
            "Plant resistant varieties where available.",
        ],
    },
    "bydv": {
        "name": "Barley Yellow Dwarf Virus",
        "curve_type": "sigmoid",
        "curve_detail": (
            "Composite: sigmoid on aphid-active hours (>50°F) in 7d window, midpoint 100h, "
            "scale 30h; sigmoid on average temp, midpoint 58°F, scale 5°F. Warm falls with "
            "extended aphid activity drive severity."
        ),
        "summary": (
            "BYDV is transmitted by aphids active above ~50°F. Fall infection causes greater "
            "yield loss than spring infection. Early planting into warm conditions extends the "
            "aphid exposure window."
        ),
        "thresholds": [
            ("low", "Few aphid-active hours (<70h above 50°F in 7d)"),
            ("moderate", "Moderate aphid activity (70-130h above 50°F)"),
            ("high", ">130h above 50°F — high aphid pressure and BYDV transmission risk"),
        ],
        "inputs": [
            "Open-Meteo forecast — temperature_2m, hourly across 168h window",
        ],
        "references": [
            "SDSU Extension — Barley Yellow Dwarf in Winter Wheat",
            "UNL CropWatch — Managing BYDV",
            "MSU Extension — BYDV and Seed Treatment Timing",
        ],
        "operator_levers": [
            "Delay planting to reduce aphid exposure window.",
            "Use insecticidal seed treatment for 3-4 weeks of aphid suppression.",
            "Scout for aphid presence in fall — treat if >5 aphids per tiller.",
        ],
    },
    "take_all": {
        "name": "Take-All Root Rot",
        "curve_type": "threshold",
        "curve_detail": (
            "Rotation-based threshold: 0 years wheat = severity ~0.05, 1 year = 0.55, "
            "2+ years = 0.85. Grass/sod history adds to risk. Warm soil (>60°F) escalates "
            "by 15%."
        ),
        "summary": (
            "Gaeumannomyces graminis var. tritici survives in wheat/barley stubble and grass "
            "roots. Second-year wheat can lose up to 50% of yield. A 2-year break with non-host "
            "crops is needed for inoculum decline."
        ),
        "thresholds": [
            ("low", "No wheat/barley in recent rotation — inoculum minimal"),
            ("moderate", "1 year of wheat or grass/sod history in rotation"),
            ("high", "2+ consecutive years of wheat/barley — high inoculum, up to 50% yield loss"),
        ],
        "inputs": [
            "Crop rotation history — CDL/user-reported previous crops",
            "Open-Meteo forecast — soil_temperature_6cm",
        ],
        "references": [
            "MU Extension G4345 — Wheat Take-All",
            "WSU STEEP — Take-All Root Rot",
            "UMN Extension — Take-All in Small Grains",
        ],
        "operator_levers": [
            "Rotate out of wheat/barley for 2+ years — the only effective control.",
            "Non-host crops: corn, soybeans, alfalfa, oats.",
            "No effective fungicide exists for take-all.",
        ],
    },
    "crown_rot": {
        "name": "Crown & Root Rot",
        "curve_type": "composite",
        "curve_detail": (
            "Two-factor composite: sigmoid on soil temp (warm seedbed >62°F favors Fusarium "
            "colonization), sigmoid on soil moisture (dry seedbed impairs crown development). "
            "Wheat-on-wheat adds 0.15 severity from residue-borne inoculum."
        ),
        "summary": (
            "Fusarium crown/root rot colonizes wheat seedlings in warm, dry seedbeds. Loose, "
            "dry soil also impairs the crown development needed for winter cold hardiness."
        ),
        "thresholds": [
            ("low", "Soil 45-60°F with adequate moisture — optimal for crown development"),
            ("moderate", "Soil 60-65°F or moisture marginal"),
            ("high", "Soil >65°F and/or dry seedbed — Fusarium colonization risk high"),
        ],
        "inputs": [
            "Open-Meteo forecast — soil_temperature_6cm, soil_moisture_1_to_3cm",
            "Crop rotation history",
        ],
        "references": [
            "Oregon State Extension PNW-639 — Controlling Root and Crown Diseases",
            "UNL Extension G1097 — Root and Crown Rot of Winter Wheat",
            "KSU Agronomy eUpdate — Seedbed Preparation for Winter Wheat",
        ],
        "operator_levers": [
            "Plant into firm, moist seedbed at 45-60°F soil temp.",
            "Avoid planting into excessively warm (>65°F) or dry soil.",
            "Rotate out of wheat for 2+ years to reduce residue-borne Fusarium inoculum.",
        ],
    },
    "snow_mold": {
        "name": "Snow Mold",
        "curve_type": "composite",
        "curve_detail": (
            "Latitude-weighted composite: sigmoid on latitude (midpoint 44°N, scale 3°), "
            "sigmoid on near-freezing hours (28-36°F, midpoint 60h), sigmoid on 7d precip. "
            "Northern latitudes + persistent near-freezing temps drive risk."
        ),
        "summary": (
            "Typhula (speckled) and Microdochium nivale (pink) snow molds thrive under "
            "prolonged snow cover on unfrozen ground. Primarily a northern-tier risk where "
            "snow persists 60+ days."
        ),
        "thresholds": [
            ("low", "Southern latitudes (<42°N) or dry, cold conditions"),
            ("moderate", "Northern latitude (42-46°N) with moderate near-freeze hours"),
            ("high", "Northern latitude (>44°N) with prolonged near-freezing temps and precipitation"),
        ],
        "inputs": [
            "Latitude (geographic risk proxy)",
            "Open-Meteo forecast — temperature_2m, precipitation_sum",
        ],
        "references": [
            "WSU Extension EB1880 — Snow Mold Diseases of Winter Wheat",
            "USU Extension — Snow Mold on Small Grains",
            "PMC — Soil Temperature Under Snow Cover and Snow Mold",
        ],
        "operator_levers": [
            "Promote vigorous fall growth — larger plants survive snow mold better.",
            "Seed treatment (difenoconazole + mefenoxam) in high-risk areas.",
            "Avoid late planting that limits fall development.",
        ],
    },
    "stripe_rust": {
        "name": "Stripe Rust",
        "curve_type": "sigmoid",
        "curve_detail": (
            "Sigmoid on favorable hours (40-65°F with RH>85%), midpoint 40h, scale 15h. "
            "Wheat-on-wheat adds 0.10 severity from local inoculum buildup."
        ),
        "summary": (
            "Puccinia striiformis infects wheat at 32-77°F (optimal 40-65°F) with prolonged "
            "leaf wetness. Fall infection establishes disease foci that erupt in spring."
        ),
        "thresholds": [
            ("low", "<25h favorable (cool + humid) in 7d window"),
            ("moderate", "25-55h favorable — building infection risk"),
            ("high", ">55h cool and humid — high stripe rust establishment risk"),
        ],
        "inputs": [
            "Open-Meteo forecast — temperature_2m, relative_humidity_2m",
            "Crop rotation history",
        ],
        "references": [
            "SDSU Extension — Stripe Rust in Winter Wheat",
            "UGA Production Guide — Disease Management in Wheat",
            "USDA Cereal Disease Lab — Wheat Stripe Rust",
        ],
        "operator_levers": [
            "Plant resistant varieties — primary defense against stripe rust.",
            "Scout by Feekes 6 (jointing) and apply fungicide if pustules detected.",
            "Propiconazole, tebuconazole, or pyraclostrobin + metconazole at Feekes 6-10.5.",
        ],
    },
    "winterkill": {
        "name": "Winterkill Risk",
        "curve_type": "composite",
        "curve_detail": (
            "Three-factor composite: GDD deficit before dormancy (40% weight, sigmoid on "
            "accumulated GDD vs 400 target base 32°F), late planting penalty (35% weight, "
            "sigmoid on days past ideal window end), dry seedbed desiccation risk (25% weight, "
            "sigmoid on VWC)."
        ),
        "summary": (
            "Winter wheat crowns need 4-6 weeks below 50°F to fully harden. Hardened crowns "
            "survive to -9 to -11°F. Desiccation from dry, loose seedbeds is more common than "
            "direct cold injury. Late planting reduces fall tillers — fall tillers contribute "
            "~87% of grain yield."
        ),
        "thresholds": [
            ("low", "On-time planting with adequate GDD remaining and moist seedbed"),
            ("moderate", "Late planting or marginal GDD or dry seedbed"),
            ("high", "Very late planting + GDD deficit + dry seedbed — high winterkill probability"),
        ],
        "inputs": [
            "Open-Meteo forecast — soil_temperature_6cm, soil_moisture_1_to_3cm, daily temp max/min",
            "Latitude (for ideal planting window end estimation)",
            "Crop profile — gdd_before_dormancy_target, gdd_base_f",
        ],
        "references": [
            "KSU Agronomy eUpdate — Factors in Winter Survival of Wheat",
            "KSU Agronomy eUpdate — Cold Hardening in Winter Wheat",
            "MSU Extension — Fall Wheat Emergence and Vernalization",
            "UMN Extension — Plant Winter-Hardy Adapted Varieties",
            "UNL Extension G1097 — Root and Crown Rot",
            "Virginia Tech SPES-431 — Effective Tiller Management",
        ],
        "operator_levers": [
            "Plant within the recommended window for your latitude.",
            "Ensure a firm, moist seedbed — desiccation is the #1 winterkill cause.",
            "Target 300-400 GDD (base 32°F) before dormancy for 2+ tillers.",
            "Choose winter-hardy varieties for northern locations.",
        ],
    },
    "tan_spot": {
        "name": "Tan Spot",
        "curve_type": "sigmoid",
        "curve_detail": (
            "Sigmoid on high-humidity hours (RH>85%) in the 168h window, midpoint 48h, "
            "scale 18h. Combined with 7-day precipitation (midpoint 1.2\", scale 0.5\"). "
            "Wheat-on-wheat adds a 1.5× severity multiplier + 0.20 base from residue inoculum. "
            "Barley in rotation applies a milder 1.2× + 0.10 boost."
        ),
        "summary": (
            "Pyrenophora tritici-repentis is the #1 leaf spot disease of spring wheat in "
            "the northern Great Plains. The fungus overwinters on wheat/barley residue and "
            "releases spores during prolonged wet periods (≥24h). Combined yield and test "
            "weight losses can reach 50% in severe epidemics."
        ),
        "thresholds": [
            ("low", "<30h high humidity + dry forecast — spore release minimal"),
            ("moderate", "30-65h humidity or moderate rain — building infection pressure"),
            ("high", ">65h humid + rain + wheat residue — high tan spot epidemic risk"),
        ],
        "inputs": [
            "Open-Meteo forecast — relative_humidity_2m, daily precipitation_sum",
            "Crop rotation history (wheat-on-wheat = heavy residue inoculum)",
        ],
        "references": [
            "NDSU Extension PP-1249 — Fungal Leaf Spot Diseases of Wheat",
            "SDSU Extension Chapter 23 — Wheat Diseases in South Dakota",
            "UMN Crop News — Small Grains Disease and Pest Update",
        ],
        "operator_levers": [
            "Rotate to broadleaf crops — reduces residue-borne inoculum dramatically.",
            "Plant resistant or moderately resistant varieties.",
            "Scout late-seeded spring wheat following wheat — add half-rate fungicide to herbicide pass if warranted.",
            "Apply foliar fungicide at flag leaf (Feekes 8-10) if disease on lower leaves + wet forecast.",
        ],
    },
    "common_root_rot": {
        "name": "Common Root Rot",
        "curve_type": "composite",
        "curve_detail": (
            "Two-pathway composite: (1) cold-stress pathway — inverted sigmoid on soil temp "
            "(midpoint 40°F, scale 4°F) weighted 0.5, captures seedling vulnerability to "
            "Bipolaris in cold soils; (2) warm-wet pathway — sigmoid on soil temp (midpoint "
            "68°F, scale 8°F) × 0.6 + moisture sigmoid (midpoint 0.30 VWC, scale 0.08) × 0.4, "
            "captures the pathogen's optimal growth zone. Continuous cereals add 1.4× + 0.25 "
            "severity from accumulated soil inoculum."
        ),
        "summary": (
            "Cochliobolus sativus (Bipolaris sorokiniana) causes seedling blight, common root "
            "rot, and spot blotch in wheat. The pathogen overwinters in soil and cereal residue. "
            "Seedling blight occurs under cold-stress conditions; root rot intensifies through "
            "the season in warm, wet soils. Continuous cereal rotations dramatically increase "
            "soil inoculum and stand loss."
        ),
        "thresholds": [
            ("low", "Broadleaf rotation + favorable soil conditions — minimal inoculum"),
            ("moderate", "1 year cereal in rotation or cold/wet stress at planting"),
            ("high", "2+ years continuous cereals + cold-stressed or warm-wet seedbed"),
        ],
        "inputs": [
            "Open-Meteo forecast — soil_temperature_6cm, soil_moisture_1_to_3cm",
            "Crop rotation history (continuous cereals = high inoculum)",
        ],
        "references": [
            "MSU Montana Extension MT201007AG — Small Grain Root and Crown Diseases",
            "Crop Protection Network — Common Root and Foot Rot of Wheat",
            "PNW Pest Management Handbooks — Wheat Common Root Rot (Seedling Blight)",
            "Frontiers in Cell. Infect. Microbiol. (2021) — Bipolaris sorokiniana Review",
        ],
        "operator_levers": [
            "Rotate to broadleaf crops for 2+ years — the most effective management tool.",
            "Use fungicide seed treatments (provides ~3 weeks seedling protection).",
            "Avoid planting into cold, wet soils when possible — slow emergence increases exposure.",
            "Select varieties with improved seedling vigor.",
        ],
    },
    "cercospora": {
        "name": "Cercospora Leaf Spot",
        "curve_type": "sigmoid",
        "curve_detail": (
            "NDAWN-style Daily Infection Value (DIV) model. Each 24h period scores 0–7 based "
            "on hours with RH ≥ 85% weighted by temperature (Gaussian peak 82°F, σ=12°F). "
            "Warm nights (>60°F) boost DIV by 1.3×. 3-day DIV accumulated; severity sigmoid "
            "midpoint = 7.0, scale = 3.0."
        ),
        "summary": (
            "Cercospora beticola is the #1 foliar disease of sugar beets in Michigan. Spore "
            "germination requires prolonged high humidity (RH ≥ 85%) with warm temperatures "
            "(75–90°F daytime, >60°F nighttime). The NDAWN Daily Infection Value model "
            "accumulates infection pressure over 3 days to predict outbreak risk."
        ),
        "thresholds": [
            ("low", "3-day DIV < 4 — minimal infection pressure"),
            ("moderate", "3-day DIV 4–7 — monitor for first lesions"),
            ("high", "3-day DIV > 7 — begin or intensify fungicide program"),
        ],
        "inputs": [
            "Open-Meteo forecast — temperature_2m, relative_humidity_2m (72h window)",
        ],
        "references": [
            "NDAWN Sugarbeet Cercospora model — Daily Infection Value methodology",
            "MSU Extension — Return of rain / Cercospora leaf spot management",
            "Windels et al. (PMC 8470031) — Weather-based predictive modeling of C. beticola",
            "APS Journal — Fluctuations in C. beticola conidia vs. environment and disease severity",
        ],
        "operator_levers": [
            "Begin fungicide program at BEETcast DSV >55 cumulative or first lesions observed.",
            "Rotate fungicide modes of action — C. beticola develops resistance rapidly.",
            "14–21 day spray intervals during high-pressure periods.",
            "Triphenyltin hydroxide (SuperTin), tetraconazole, prothioconazole are primary options.",
        ],
    },
    "bolting": {
        "name": "Bolting / Vernalization Risk",
        "curve_type": "sigmoid",
        "curve_detail": (
            "Vernalization-intensity model. Accumulates weighted cold hours in the 32–50°F "
            "(0–13°C) range over a 168h window. Each hour weighted by distance from optimum "
            "(46°F peak, ±18°F decay). Days with max temp >73°F excluded (de-vernalization). "
            "Sigmoid on weighted hours: midpoint = threshold × 0.5, scale = threshold × 0.2."
        ),
        "summary": (
            "Sugar beets are biennial plants that bolt (flower prematurely) when exposed to "
            "prolonged cold temperatures. Vernalization occurs at 32–55°F with optimum near "
            "46°F. Once ~120 weighted cold hours accumulate (genotype range 107–134h), bolting "
            "is induced and roots become unmarketable. Days above 73°F can reverse vernalization."
        ),
        "thresholds": [
            ("low", "< 40 weighted vernalizing hours — minimal bolting risk"),
            ("moderate", "40–70 weighted hours — some bolting induction possible"),
            ("high", "> 70 weighted hours — significant bolting risk; consider delayed planting"),
        ],
        "inputs": [
            "Open-Meteo forecast — temperature_2m (168h window)",
            "Crop profile — bolting_cold_hours threshold, bolting_temp_f ceiling, bolting_base_f floor",
        ],
        "references": [
            "Milford et al. (J. Agric. Sci.) — Vernalization-intensity model to predict bolting",
            "PLOS ONE 2024 (10.1371/journal.pone.0339856) — 12-genotype vernalization study",
            "Mutasa-Göttgens et al. (PMC 7534467) — Vernalization alters sink/source identities",
        ],
        "operator_levers": [
            "Delay planting if extended cold is forecast — vernalization is cumulative.",
            "Select bolting-resistant varieties for early planting windows.",
            "Warm days (>73°F) between cold snaps help reverse vernalization accumulation.",
        ],
    },
    "aphanomyces": {
        "name": "Aphanomyces Damping-Off",
        "curve_type": "gaussian",
        "curve_detail": (
            "Gaussian on soil temperature (peak 70°F, σ=12°F) weighted 0.45 + sigmoid on "
            "saturation hours (midpoint 36h, scale 15h) weighted 0.55. Beet-on-beet rotation "
            "adds 0.15 severity. Poorly drained soils add 0.10."
        ),
        "summary": (
            "Aphanomyces cochlioides zoospores require free soil water to swim to sugar beet "
            "roots. Favored by warm (60–80°F), saturated soils. Causes post-emergence damping-off "
            "with dark, thread-like hypocotyls. NOT controlled by metalaxyl/mefenoxam (unlike "
            "Pythium). Hymexazol (Tachigaren) is the primary seed treatment."
        ),
        "thresholds": [
            ("low", "Cool/dry conditions — Aphanomyces zoospores inactive"),
            ("moderate", "Warm soil with intermittent saturation — monitor for symptoms"),
            ("high", "Warm + saturated soil + beet history — high damping-off risk"),
        ],
        "inputs": [
            "Open-Meteo forecast — soil_temperature_6cm, soil_moisture_1_to_3cm (120h)",
            "Crop rotation history — prior sugar beets or host crops",
            "SSURGO soil profile — drainage class",
        ],
        "references": [
            "UNL EC1897 — Sugarbeet Seedling Diseases",
            "PNW Pest Management Handbooks — Sugar Beet Damping-off",
            "Windels & Brantner (2005) — Aphanomyces cochlioides management",
            "BioRxiv (2022) — DNA-based detection of A. cochlioides in soil",
        ],
        "operator_levers": [
            "Rotate away from sugar beets for 3+ years to reduce soil inoculum.",
            "Hymexazol (Tachigaren) seed treatment — Aphanomyces-specific.",
            "Improve drainage and avoid planting into saturated fields.",
            "Plant early into cooler soils when Aphanomyces is less active.",
        ],
    },
    "sbcn": {
        "name": "Sugar Beet Cyst Nematode",
        "curve_type": "sigmoid",
        "curve_detail": (
            "Modifier factor — does not directly reduce survival but amplifies Rhizoctonia, "
            "Aphanomyces, and Pythium severity by up to 1.20×. Severity driven by rotation "
            "history: 2+ beet years = 0.75, 1 beet year = 0.45, unknown = 0.25. Scaled by "
            "soil temperature activity (sigmoid midpoint 50°F)."
        ),
        "summary": (
            "Heterodera schachtii cysts persist in soil for years. Larvae penetrate roots, "
            "creating wound sites that amplify fungal/oomycete root diseases. Yield losses "
            "of 25–50%+ in heavily infested fields. Acts as a risk modifier rather than "
            "a standalone survival factor."
        ),
        "thresholds": [
            ("low", "No recent sugar beets in rotation — low SBCN carryover"),
            ("moderate", "1 sugar beet year in recent history — moderate nematode pressure"),
            ("high", "2+ beet years — heavy SBCN buildup amplifying root disease risk"),
        ],
        "inputs": [
            "Crop rotation history — sugar beet years in rotation",
            "Open-Meteo forecast — soil_temperature_6cm (activity threshold)",
        ],
        "references": [
            "UC IPM — Sugarbeet Nematodes management guidelines",
            "Michigan Sugar Company — SBCN management recommendations",
            "SBREB — Plant-parasitic nematodes on sugarbeet in ND and MN",
            "PMC 9320877 — Varietal resistance/tolerance validation against H. schachtii",
        ],
        "operator_levers": [
            "3+ year rotation away from sugar beets is the primary management tool.",
            "Select SBCN-resistant or tolerant varieties for infested fields.",
            "Soil sampling for egg counts before planting — >50 eggs/ml threshold.",
            "Avoid host crops (brassicas, spinach) in rotation with beets.",
        ],
    },
    "wind_damage": {
        "name": "Wind / Sand Damage",
        "curve_type": "sigmoid",
        "curve_detail": (
            "Two-pathway max: (1) sigmoid on hours with sustained wind >25 mph (midpoint 8h, "
            "scale 4h); (2) sigmoid on max gust speed (midpoint 35 mph, scale 10 mph). "
            "Dry conditions (<0.1\" in 3 days) amplify severity 1.3×. Recent precipitation "
            "(>0.5\") halves severity. No-till or heavy residue reduces severity 0.6×."
        ),
        "summary": (
            "Sugar beet cotyledons are highly susceptible to sand-blasting from wind-driven "
            "soil particles. At the 2-4 true leaf stage, wind can twist and whip leaves off. "
            "Sustained winds >25 mph with dry, bare soil create the highest risk. Surface "
            "residue and cover crops provide significant protection."
        ),
        "thresholds": [
            ("low", "Winds <25 mph or moist/residue-covered soil — minimal risk"),
            ("moderate", "Intermittent high winds or gusts >30 mph — monitor seedlings"),
            ("high", "Sustained >25 mph + dry bare soil — significant sand blast risk"),
        ],
        "inputs": [
            "Open-Meteo forecast — wind_speed_10m, wind_gusts_10m (72h)",
            "Open-Meteo forecast — daily precipitation_sum (3-day moisture check)",
            "User inputs — tillage, residue cover",
        ],
        "references": [
            "Alberta Sugar Beet Growers — replanting decisions for wind damage",
            "MSU Extension — sugar beet seedling protection",
            "UNL CropWatch — stand establishment and wind erosion",
        ],
        "operator_levers": [
            "Use cover crops or residue strips to break wind at soil surface.",
            "Delay planting if sustained high winds are forecast.",
            "No-till or minimum-till preserves surface residue for wind protection.",
            "Irrigate lightly before wind events to stabilize soil surface.",
        ],
    },
    "root_maggot": {
        "name": "Sugar Beet Root Maggot",
        "curve_type": "gaussian",
        "curve_detail": (
            "GDD phenology model (base 47.5°F). Gaussian on accumulated GDD (peak 425, "
            "σ=120) weighted 0.65 + soil moisture sigmoid (midpoint 0.28 VWC, scale 0.10) "
            "weighted 0.35. Prior sugar beet rotation adds 0.15 severity."
        ),
        "summary": (
            "Tetanops myopaeformis adults emerge at ~300–550 GDD (base 47.5°F), laying eggs "
            "near sugar beet crowns May-June. Larvae feed on taproots — can sever seedling "
            "taproots (stand loss) or scar older roots (yield/sugar reduction). Seed treatment "
            "or at-plant granular insecticide is standard management."
        ),
        "thresholds": [
            ("low", "GDD outside 200–600 range — adults not active"),
            ("moderate", "GDD 200–350 or 500–600 — approaching/departing flight window"),
            ("high", "GDD 350–500 — peak adult flight and egg-laying period"),
        ],
        "inputs": [
            "Open-Meteo forecast — temperature_2m (168h GDD accumulation)",
            "Open-Meteo forecast — soil_moisture_1_to_3cm",
            "Enviroweather GDD (base 48°F) when available",
            "Crop rotation history — prior sugar beets",
        ],
        "references": [
            "NDAWN — Sugarbeet Root Maggot Growing Degree Day model",
            "Bechinski (Idaho) — Integrated Pest Management Guide for SBRM",
            "PNW Pest Management Handbooks — Sugar beet root maggot",
            "PMC 6007336 — Screening for resistance against T. myopaeformis",
        ],
        "operator_levers": [
            "Clothianidin seed treatment is standard for at-risk fields.",
            "At-plant granular insecticide (chlorpyrifos) for heavy pressure.",
            "Monitor adult fly catch with yellow sticky traps at field edges.",
            "Rotate away from sugar beets to break the larval overwintering cycle.",
        ],
    },
    "autotoxicity": {
        "name": "Autotoxicity",
        "curve_type": "sigmoid",
        "curve_detail": (
            "Severity scales with recency of prior alfalfa in CropScape rotation history. "
            "Year 1 (immediate prior): 0.85 base. Year 2: 0.55. Year 3+: 0.30. Multiple "
            "consecutive alfalfa years add +0.10. No-till adds +0.10 (delays allelochemical "
            "degradation). Based on UW-Extension yield-reduction studies."
        ),
        "summary": (
            "Alfalfa releases medicarpin and other allelochemicals from roots and foliage that "
            "inhibit new alfalfa seedling establishment within a 16-inch radius. Wisconsin studies "
            "showed 80% yield reduction when reseeded 2 weeks after kill, 30-50% at 4 weeks, and "
            "minimal impact after 1+ year rotation."
        ),
        "thresholds": [
            ("low", "No alfalfa in last 3 years, or 3+ years since last alfalfa"),
            ("moderate", "Alfalfa 2 years ago — declining allelochemical levels"),
            ("high", "Alfalfa in immediate prior year — strong autotoxicity risk"),
        ],
        "inputs": [
            "CropScape CDL rotation history (3 years)",
            "User-selected tillage (no-till delays breakdown)",
        ],
        "references": [
            "UW-Extension — Understanding Autotoxicity in Alfalfa",
            "MSU — Can We Solve the Mystery of Alfalfa Autotoxicity",
            "Frontiers in Plant Science 2022 — Medicago truncatula autotoxicity",
            "Kings AgriSeeds — Seeding Alfalfa back into Alfalfa",
        ],
        "operator_levers": [
            "Rotate to a non-legume crop for at least 1 year before reseeding alfalfa.",
            "Tillage (moldboard plow) accelerates medicarpin degradation vs. no-till.",
            "If reseeding into recent alfalfa, wait as long as possible after killing the old stand.",
            "Frost-seeding into a thin existing stand is less affected than full reseeding.",
        ],
    },
    "aphanomyces_alfalfa": {
        "name": "Aphanomyces Root Rot",
        "curve_type": "composite",
        "curve_detail": (
            "Trapezoidal on soil temperature (zoospore optimum 60-77°F, ramps 50-60 and 77-86°F) "
            "weighted 0.55 + sigmoid on saturated hours (midpoint 30h, scale 12h) weighted 0.45. "
            "Poorly drained soil adds +0.12 severity. Prior alfalfa in rotation adds +0.12."
        ),
        "summary": (
            "Aphanomyces euteiches zoospores require free water to reach alfalfa roots. Infected "
            "seedlings become stunted and chlorotic before wilting — unlike Phytophthora which kills "
            "quickly. Oospore inoculum persists 10+ years in soil. Race 2 is dominant in MN. Poorly "
            "drained fields with alfalfa history are highest risk."
        ),
        "thresholds": [
            ("low", "Well-drained soil + no prior alfalfa + dry forecast"),
            ("moderate", "Some saturation or prior alfalfa + moderate temperature"),
            ("high", "Prolonged saturation + warm soil + poorly drained + alfalfa history"),
        ],
        "inputs": [
            "Open-Meteo forecast — soil_temperature_6cm, soil_moisture_1_to_3cm (168h)",
            "SSURGO — drainage_class",
            "CropScape rotation history — prior alfalfa",
        ],
        "references": [
            "UMN Extension — Seedling Diseases of Alfalfa",
            "MSU Extension — Root Rot Diseases of Alfalfa",
            "UW-Extension — Damping Off and Root Rot (Phytophthora/Pythium)",
            "Plant Disease 2018 — Seed Rot and Damping-off in MN",
        ],
        "operator_levers": [
            "Select Aphanomyces-resistant varieties (Race 1 and Race 2 resistance).",
            "Improve field drainage — tile or surface drainage reduces free-water duration.",
            "Avoid fields with known alfalfa disease history.",
            "Metalaxyl/mefenoxam seed treatments do NOT control Aphanomyces (only Pythium/Phytophthora).",
        ],
    },
    "sclerotinia_crown": {
        "name": "Sclerotinia Crown Rot",
        "curve_type": "composite",
        "curve_detail": (
            "Trapezoidal on air temperature (optimum 50-68°F, ramps 40-50 and 68-78°F) weighted "
            "0.45 + sigmoid on high-humidity hours (midpoint 60h, scale 20h) weighted 0.30 + "
            "sigmoid on soil moisture (midpoint 0.35 VWC, scale 0.08) weighted 0.25. Prior "
            "alfalfa adds +0.10."
        ),
        "summary": (
            "Sclerotinia trifoliorum infects alfalfa crowns in cool (50-68°F), continuously moist "
            "conditions. White cottony mycelium colonizes crowns and lower stems. Sclerotia persist "
            "3-5 years in soil. Late-summer and fall seedings are at highest risk. Symptoms appear "
            "in early spring."
        ),
        "thresholds": [
            ("low", "Warm/dry conditions or good air circulation"),
            ("moderate", "Cool + moderately humid — building infection conditions"),
            ("high", "Cool (50-68°F) + prolonged high humidity + wet soil"),
        ],
        "inputs": [
            "Open-Meteo forecast — temperature_2m, relative_humidity_2m, soil_moisture_1_to_3cm (168h)",
            "CropScape rotation history — prior alfalfa",
        ],
        "references": [
            "CPN — Sclerotinia Crown and Stem Rot in Alfalfa",
            "OSU Extension Fact Sheet — Sclerotinia in Alfalfa",
            "UC IPM — Sclerotinia Stem and Crown Rot",
            "Kentucky IPM — Sclerotinia in Alfalfa",
        ],
        "operator_levers": [
            "Select varieties with Sclerotinia resistance ratings.",
            "Ensure adequate row spacing and air circulation.",
            "Avoid late-fall seedings in regions with cool, wet springs.",
            "Fungicide seed treatments have limited efficacy — variety selection is primary defense.",
        ],
    },
    "potato_leafhopper": {
        "name": "Potato Leafhopper",
        "curve_type": "gaussian",
        "curve_detail": (
            "Gaussian on GDD (base 48°F, peak 900, σ=250) weighted 0.60 + sigmoid on warm hours "
            ">70°F (midpoint 72h, scale 24h) weighted 0.40. PLH migrate on storm fronts; peak "
            "populations correlate with warm humid weather."
        ),
        "summary": (
            "Potato leafhoppers migrate north annually on storm fronts (late May-June). Adults and "
            "nymphs feed on alfalfa phloem causing hopperburn — V-shaped leaf yellowing. New seedings "
            "are most vulnerable (lack glandular trichomes). Economic threshold: ~0.1 PLH per sweep "
            "per inch of plant height."
        ),
        "thresholds": [
            ("low", "GDD <400 or >1400 — outside migration/buildup window"),
            ("moderate", "GDD 400-700 or 1200-1400 — approaching/departing activity window"),
            ("high", "GDD 700-1200 + warm humid conditions — peak PLH pressure"),
        ],
        "inputs": [
            "Open-Meteo forecast — temperature_2m, relative_humidity_2m (168h)",
            "Cumulative GDD (base 50, scaled to base 48)",
        ],
        "references": [
            "UW-Extension — Potato Leafhopper Damage to Alfalfa",
            "UNL G1136 — Potato Leafhopper Management in Alfalfa",
            "J. Integrated Pest Management 5(1):A1 — PLH Ecology and IPM",
            "ISU ICM — Potato Leafhopper",
            "J. Economic Entomology 108(4):1748 — Revisiting EIL/ET",
        ],
        "operator_levers": [
            "Scout weekly with a sweep net starting late May.",
            "Plant glandular-trichome (leafhopper-resistant) varieties for new seedings.",
            "Early cutting is the most effective non-chemical control for established stands.",
            "Pyrethroid or organophosphate if threshold exceeded and >7 days from cutting.",
        ],
    },
    "alfalfa_weevil": {
        "name": "Alfalfa Weevil",
        "curve_type": "gaussian",
        "curve_detail": (
            "Gaussian on GDD (base 48°F, peak 450, σ=120) weighted 0.70 + sigmoid on warm hours "
            "50-90°F (midpoint 80h, scale 25h) weighted 0.30. Eggs hatch at ~300 GDD; peak larval "
            "damage at 350-550 GDD."
        ),
        "summary": (
            "Alfalfa weevil larvae hatch at ~300 GDD (base 48°F) and feed on terminal leaves, "
            "progressing from pinholes to full skeletonization. Peak damage occurs before first "
            "cutting. Economic threshold: 40% of stems with damage and >7 days from scheduled cutting."
        ),
        "thresholds": [
            ("low", "GDD <200 or >700 — outside primary larval activity window"),
            ("moderate", "GDD 200-300 or 550-700 — scouting should begin"),
            ("high", "GDD 300-550 — peak larval defoliation window"),
        ],
        "inputs": [
            "Open-Meteo forecast — temperature_2m (168h)",
            "Cumulative GDD (base 50, scaled to base 48)",
        ],
        "references": [
            "ISU ICM — Alfalfa Weevil",
            "UMN Crop News 2023 — Updated Alfalfa Weevil Management",
            "UW-Extension IPCM — Alfalfa Weevil",
            "J. Economic Entomology 114(3):1173 — Re-evaluating EIL",
        ],
        "operator_levers": [
            "Begin scouting at 200-250 GDD (base 48°F).",
            "Cut 30 stems, shake in bucket to count larvae per stem.",
            "Early cutting is most effective — removes larvae with the foliage.",
            "Insecticide if 40% of stems are damaged and >7 days from cutting.",
        ],
    },
    "soil_ph": {
        "name": "Soil pH",
        "curve_type": "sigmoid",
        "curve_detail": (
            "Two-sided response: pH < 6.0 uses inverted sigmoid (midpoint 5.5, scale 0.4) — "
            "steep penalty from aluminum toxicity + Rhizobium failure. pH 6.0-6.5 is linear "
            "interpolation (mild penalty). pH 6.5-7.5 is optimal (severity 0). pH > 7.5 uses "
            "sigmoid (midpoint 8.0, scale 0.4) for micronutrient lockout."
        ),
        "summary": (
            "Alfalfa requires pH 6.5-7.0 for optimal Rhizobium meliloti nodulation and nutrient "
            "uptake. Below pH 6.0, aluminum toxicity damages root tips and Rhizobium survival drops "
            "sharply. Above pH 7.5, boron and iron availability decreases. Yield loss is ~0.1 "
            "DM ton/ac per 0.1 pH below optimum."
        ),
        "thresholds": [
            ("low", "pH 6.5-7.5 — optimal for alfalfa establishment and nodulation"),
            ("moderate", "pH 6.0-6.5 or 7.5-8.0 — suboptimal but manageable with amendments"),
            ("high", "pH <6.0 or >8.0 — significant establishment risk"),
        ],
        "inputs": [
            "SSURGO soil survey — soil pH",
        ],
        "references": [
            "USU Extension — Alfalfa Nutrient Management Guide",
            "TAMU — Effect of Soil Boron Levels and pH on Yield of Alfalfa",
            "UW-Extension — Alfalfa Establishment Guidelines",
            "Mosaic Crop Nutrition — Potassium and Phosphorus for Quality Alfalfa",
        ],
        "operator_levers": [
            "Lime to pH 6.8 before seeding — apply 6-12 months ahead for full reaction.",
            "On high-pH soils (>7.5), apply 2-3 lb/ac boron to prevent deficiency.",
            "Soil-test phosphorus and potassium before establishment.",
            "Sulfur or acidifying fertilizers can lower pH on alkaline soils, but response is slow.",
        ],
    },
}


def get_methodology() -> dict:
    """Return the full methodology payload — both the survival-math overview
    and per-risk entries. Consumed by /api/methodology and /methodology."""
    return {
        "survival": SURVIVAL_OVERVIEW,
        "survival_pct_cap": SURVIVAL_PCT_CAP,
        "level_survival_factor": LEVEL_SURVIVAL_FACTOR,
        "factor_categories": FACTOR_CATEGORIES,
        "modifier_targets": MODIFIER_TARGETS,
        "risks": [
            {"key": key, "model_category": FACTOR_CATEGORIES.get(key, ""), **entry}
            for key, entry in RISK_METHODOLOGY.items()
        ],
    }


# ----- Fertilizer & pesticide recommendation engine -----------------------
# Rates from Tri-State Fertility Recommendations (MSU/OSU/Purdue, 2020 ed.)
# and ISU Extension PM-1688 / PM-1871. Nitrogen credits from Purdue AY-267.

# Crop nutrient removal rates (lb/bu for corn, lb/bu for soybeans)
_NUTRIENT_REMOVAL = {
    "corn": {"n_lb_bu": 0.67, "p2o5_lb_bu": 0.38, "k2o_lb_bu": 0.27, "s_lb_bu": 0.08},
    "soybeans": {"n_lb_bu": 3.3, "p2o5_lb_bu": 0.80, "k2o_lb_bu": 1.40, "s_lb_bu": 0.16},
    "winter_wheat": {"n_lb_bu": 1.25, "p2o5_lb_bu": 0.50, "k2o_lb_bu": 0.30, "s_lb_bu": 0.06},
    "spring_wheat": {"n_lb_bu": 1.25, "p2o5_lb_bu": 0.50, "k2o_lb_bu": 0.30, "s_lb_bu": 0.06},
    "dry_beans": {"n_lb_bu": 1.4, "p2o5_lb_bu": 0.60, "k2o_lb_bu": 0.90, "s_lb_bu": 0.10},
    "sugar_beets": {"n_lb_bu": 0.35, "p2o5_lb_bu": 0.15, "k2o_lb_bu": 0.50, "s_lb_bu": 0.05},
    "alfalfa": {"n_lb_bu": 0.0, "p2o5_lb_bu": 0.12, "k2o_lb_bu": 0.50, "s_lb_bu": 0.06},
}

# Nitrogen recommendation base rates (lb N/ac) by yield goal tier
_N_RATES_CORN = {
    150: 140, 175: 165, 200: 190, 225: 215, 250: 240, 275: 265, 300: 285,
}

# Nitrogen credits (lb N/ac) for previous crop
_N_CREDITS = {
    "soybeans": 40, "alfalfa_1yr": 100, "alfalfa_2yr": 80,
    "alfalfa_3yr+": 60, "red_clover": 50, "other_legume": 30,
    "corn": 0, "other": 0,
}

# P/K soil test interpretation (Mehlich-3, ppm) — Tri-State ranges
_PK_CATEGORIES = {
    "very_low": {"p_ppm": (0, 7), "k_ppm": (0, 50)},
    "low": {"p_ppm": (8, 15), "k_ppm": (51, 100)},
    "optimum": {"p_ppm": (16, 30), "k_ppm": (101, 150)},
    "high": {"p_ppm": (31, 60), "k_ppm": (151, 200)},
    "very_high": {"p_ppm": (61, 999), "k_ppm": (201, 999)},
}

# P2O5/K2O application rates by soil test category (lb/ac)
_PK_RATES = {
    "corn": {
        "very_low": {"p2o5": 80, "k2o": 120},
        "low": {"p2o5": 50, "k2o": 80},
        "optimum": {"p2o5": 30, "k2o": 40},
        "high": {"p2o5": 0, "k2o": 0},
        "very_high": {"p2o5": 0, "k2o": 0},
    },
    "soybeans": {
        "very_low": {"p2o5": 70, "k2o": 140},
        "low": {"p2o5": 45, "k2o": 100},
        "optimum": {"p2o5": 25, "k2o": 50},
        "high": {"p2o5": 0, "k2o": 0},
        "very_high": {"p2o5": 0, "k2o": 0},
    },
}

# Corn growth stage GDD thresholds (base 50°F, planting date start)
_CORN_STAGES_GDD = {
    "VE": 100, "V2": 200, "V4": 345, "V6": 475, "V8": 610,
    "V10": 740, "V12": 870, "VT": 1135, "R1": 1250, "R2": 1400,
    "R3": 1600, "R4": 1900, "R5": 2200, "R6": 2700,
}

# Soybean growth stages (base 50°F)
_SOY_STAGES_GDD = {
    "VE": 90, "VC": 150, "V1": 220, "V3": 380, "V5": 560,
    "R1": 800, "R2": 950, "R3": 1150, "R4": 1350, "R5": 1550,
    "R6": 1800, "R7": 2100, "R8": 2400,
}

_WHEAT_STAGES_GDD = {
    "VE": 100, "tillering": 400, "jointing": 750, "flag_leaf": 1050,
    "heading": 1200, "anthesis": 1350, "milk": 1550, "dough": 1800, "maturity": 2100,
}

_DRY_BEAN_STAGES_GDD = {
    "VE": 100, "V1": 200, "V3": 380, "V5": 550,
    "R1": 700, "R3": 900, "R5": 1100, "R7": 1400, "R8": 1600,
}

_SUGAR_BEET_STAGES_GDD = {
    "VE": 130, "2-leaf": 250, "4-leaf": 400, "6-leaf": 550,
    "8-leaf": 700, "canopy_closure": 950, "sugar_accumulation": 1600, "harvest": 2800,
}

_ALFALFA_STAGES_GDD = {
    "VE": 80, "2nd_trifoliate": 200, "mid_veg": 400,
    "bud": 650, "early_bloom": 800, "full_bloom": 1000, "seed_set": 1300,
}

_CROP_STAGE_MAP = {
    "corn": _CORN_STAGES_GDD,
    "soybeans": _SOY_STAGES_GDD,
    "winter_wheat": _WHEAT_STAGES_GDD,
    "spring_wheat": _WHEAT_STAGES_GDD,
    "dry_beans": _DRY_BEAN_STAGES_GDD,
    "sugar_beets": _SUGAR_BEET_STAGES_GDD,
    "alfalfa": _ALFALFA_STAGES_GDD,
}

# Herbicide application windows by growth stage
_HERBICIDE_WINDOWS = {
    "corn": {
        "pre_emergence": {
            "window": ("planting", "VE"),
            "products": ["atrazine + S-metolachlor", "acetochlor + atrazine",
                         "mesotrione + S-metolachlor + atrazine"],
            "notes": "Apply within 3 days of planting for best efficacy.",
        },
        "early_post": {
            "window": ("V2", "V6"),
            "products": ["glyphosate (RR)", "nicosulfuron + rimsulfuron",
                         "tembotrione + atrazine", "topramezone + atrazine"],
            "notes": "Target 2-4\" weeds. Include adjuvant per label.",
        },
        "late_post": {
            "window": ("V6", "V10"),
            "products": ["glyphosate (RR)", "2,4-D (drop nozzles)",
                         "dicamba (DGA, drop nozzles)"],
            "notes": "Use drop nozzles beyond V6 to avoid brace root damage.",
        },
    },
    "soybeans": {
        "pre_emergence": {
            "window": ("planting", "VE"),
            "products": ["S-metolachlor + metribuzin", "flumioxazin + pyroxasulfone",
                         "sulfentrazone + cloransulam"],
            "notes": "Apply at planting or within 3 days. Watch for metribuzin sensitivity on sandy soils.",
        },
        "early_post": {
            "window": ("V1", "V3"),
            "products": ["glyphosate (RR)", "fomesafen + glyphosate",
                         "imazethapyr", "acifluorfen"],
            "notes": "Target weeds at 2-3\" height. No 2,4-D or dicamba on non-tolerant beans.",
        },
        "late_post": {
            "window": ("V3", "R1"),
            "products": ["glyphosate (RR)", "glufosinate (LL)",
                         "clethodim (grass escapes)"],
            "notes": "Last pass before canopy closure. No ALS herbicides past V5 on sensitive varieties.",
        },
    },
    "winter_wheat": {
        "fall_post": {
            "window": ("planting", "VE"),
            "products": ["pyroxasulfone", "chlorsulfuron + metsulfuron"],
            "notes": "Fall application for winter annuals (henbit, chickweed). Apply before ground freezes.",
        },
        "spring_post": {
            "window": ("VE", "V4"),
            "products": ["2,4-D amine", "MCPA + dicamba", "tribenuron-methyl",
                         "thifensulfuron + tribenuron"],
            "notes": "Apply before jointing (Feekes 6). Dicamba restricted after jointing.",
        },
    },
    "spring_wheat": {
        "pre_emergence": {
            "window": ("planting", "VE"),
            "products": ["pyroxasulfone + carfentrazone", "triallate (wild oat)"],
            "notes": "Pre-emergence for grass control. Incorporate with rainfall or shallow tillage.",
        },
        "post_emergence": {
            "window": ("VE", "V4"),
            "products": ["2,4-D amine", "bromoxynil + MCPA",
                         "fenoxaprop (grass)", "pinoxaden (grass)"],
            "notes": "Target broadleaves before flag leaf. Grass herbicides before tillering complete.",
        },
    },
    "dry_beans": {
        "pre_plant": {
            "window": ("planting", "VE"),
            "products": ["trifluralin (PPI)", "pendimethalin", "S-metolachlor"],
            "notes": "Preplant incorporate or pre-emergence. Beans sensitive to many post-emergence herbicides.",
        },
        "post_emergence": {
            "window": ("VE", "V4"),
            "products": ["bentazon", "fomesafen", "imazamox (Clearfield)",
                         "sethoxydim or clethodim (grasses)"],
            "notes": "Limited post options. Apply before 3rd trifoliate for best crop safety.",
        },
    },
    "sugar_beets": {
        "pre_emergence": {
            "window": ("planting", "VE"),
            "products": ["ethofumesate + cycloate (Ro-Neet)", "S-metolachlor"],
            "notes": "Preplant incorporated. Critical for early-season weed control in slow-canopy crop.",
        },
        "micro_rate_post": {
            "window": ("VE", "V4"),
            "products": ["desmedipham + phenmedipham", "ethofumesate",
                         "triflusulfuron", "clopyralid"],
            "notes": "Micro-rate program: 3-4 applications at weed cotyledon stage. Do not exceed label rates per pass.",
        },
    },
    "alfalfa": {
        "establishment": {
            "window": ("planting", "VE"),
            "products": ["EPTC (PPI)", "benefin + trifluralin",
                         "imazethapyr (post-emergence on established)"],
            "notes": "Few options at establishment. Once established: imazethapyr or hexazinone for dormant-season.",
        },
    },
}

# Insecticide timing
_INSECTICIDE_WINDOWS = {
    "corn": [
        {
            "pest": "Seedcorn maggot",
            "window": "At planting",
            "treatment": "Seed treatment (clothianidin, thiamethoxam) or granular in-furrow",
            "threshold": "History of damage + high-residue field + cool/wet conditions",
        },
        {
            "pest": "Black cutworm",
            "window": "V1-V4 (scout when BCW flights peak)",
            "treatment": "Chlorantraniliprole or bifenthrin foliar at 3% cut plants",
            "threshold": ">3% plants cut at or below soil surface",
        },
        {
            "pest": "Corn rootworm",
            "window": "At planting (Bt trait or soil insecticide)",
            "treatment": "Bt hybrids (Cry3Bb1, Cry34/35Ab1) or granular tefluthrin",
            "threshold": "Corn-on-corn rotation + >0.75 beetles/plant prior year",
        },
        {
            "pest": "Western bean cutworm",
            "window": "VT-R2 (1400-1600 GDD base 50)",
            "treatment": "Chlorantraniliprole or bifenthrin at egg hatch",
            "threshold": ">5% plants with egg masses + non-Vip3A hybrid",
        },
    ],
    "soybeans": [
        {
            "pest": "Bean leaf beetle",
            "window": "VE-V2 (overwintered adults) and R5-R6 (pod feeding)",
            "treatment": "Lambda-cyhalothrin or bifenthrin at threshold",
            "threshold": ">50% defoliation (veg) or >10% pod damage (repro)",
        },
        {
            "pest": "Soybean aphid",
            "window": "R1-R5 (scout weekly June-August)",
            "treatment": "Lambda-cyhalothrin, chlorpyrifos, or dimethoate",
            "threshold": ">250 aphids/plant on 80% of plants + increasing population",
        },
        {
            "pest": "Japanese beetle",
            "window": "R1-R4 (July-August adult feeding)",
            "treatment": "Carbaryl, bifenthrin, or lambda-cyhalothrin",
            "threshold": ">30% defoliation during R1-R4",
        },
    ],
    "winter_wheat": [
        {
            "pest": "Hessian fly",
            "window": "At planting (plant after fly-free date)",
            "treatment": "Seed treatment (imidacloprid) or resistant varieties",
            "threshold": "Plant after local fly-free date. If before: use treated seed. MU G7180.",
        },
        {
            "pest": "BYDV aphids (bird cherry oat, English grain, greenbug)",
            "window": "Fall establishment through dormancy",
            "treatment": "Insecticidal seed treatment (imidacloprid, thiamethoxam) at planting",
            "threshold": "Early planting + warm fall (>50°F avg) = high aphid pressure. SDSU, UNL.",
        },
        {
            "pest": "Cereal leaf beetle",
            "window": "Flag leaf to heading (Feekes 8-10.5)",
            "treatment": "Lambda-cyhalothrin, dimethoate, or malathion",
            "threshold": ">3 larvae/flag leaf (or >1 per stem on average)",
        },
        {
            "pest": "Armyworm (true armyworm)",
            "window": "Jointing through heading (April-June)",
            "treatment": "Lambda-cyhalothrin, chlorpyrifos, or methomyl",
            "threshold": ">6 larvae/ft² or >50% heading cut. Scout field edges near grassy areas.",
        },
    ],
    "spring_wheat": [
        {
            "pest": "Wheat stem sawfly",
            "window": "Heading (late June-July)",
            "treatment": "Solid-stem varieties; no effective insecticide timing",
            "threshold": ">10% stems infested in previous year",
        },
        {
            "pest": "Aphids (greenbug, bird cherry oat)",
            "window": "Tillering through grain fill",
            "treatment": "Dimethoate, lambda-cyhalothrin, or imidacloprid",
            "threshold": ">20 aphids/tiller before heading",
        },
    ],
    "dry_beans": [
        {
            "pest": "Mexican bean beetle",
            "window": "V2 through R5",
            "treatment": "Carbaryl, lambda-cyhalothrin, or bifenthrin",
            "threshold": ">20% defoliation vegetative or >10% pod scarring",
        },
        {
            "pest": "Western bean cutworm",
            "window": "R1-R4 (flowering through pod fill)",
            "treatment": "Chlorantraniliprole or bifenthrin at egg hatch",
            "threshold": ">5% plants with egg masses",
        },
    ],
    "sugar_beets": [
        {
            "pest": "Root maggot",
            "window": "At planting through 6-leaf",
            "treatment": "Seed treatment (clothianidin) or granular chlorpyrifos at-plant",
            "threshold": "History of damage + adjacent canola/crucifer stubble",
        },
        {
            "pest": "Sugarbeet root aphid",
            "window": "Mid-season (July-August)",
            "treatment": "Terbufos granular or systemic seed treatment",
            "threshold": "Wilting + honeydew on roots at mid-season inspection",
        },
    ],
    "alfalfa": [
        {
            "pest": "Alfalfa weevil",
            "window": "First cutting (300-600 GDD base 48°F)",
            "treatment": "Lambda-cyhalothrin, chlorpyrifos, or indoxacarb",
            "threshold": ">40% tip feeding on stems + 3+ larvae/stem",
        },
        {
            "pest": "Potato leafhopper",
            "window": "After first cutting (June-August)",
            "treatment": "Lambda-cyhalothrin, dimethoate, or imidacloprid",
            "threshold": ">2/sweep (alfalfa <3\"), >1/sweep (3-6\"), or hopperburn visible",
        },
    ],
}

# Fungicide timing
_FUNGICIDE_WINDOWS = {
    "corn": [
        {
            "disease": "Gray leaf spot / Northern corn leaf blight",
            "window": "VT-R2 (tasseling through blister)",
            "treatment": "Azoxystrobin + propiconazole, pyraclostrobin + metconazole",
            "threshold": "Disease on 3rd leaf below ear or above + susceptible hybrid + forecast >3 days high humidity",
        },
    ],
    "soybeans": [
        {
            "disease": "White mold (Sclerotinia)",
            "window": "R1-R2 (beginning bloom)",
            "treatment": "Boscalid, fluopyram, or picoxystrobin",
            "threshold": "History + canopy closure + >5 days forecast with high humidity/dew",
        },
        {
            "disease": "Frogeye leaf spot / Septoria",
            "window": "R3-R5",
            "treatment": "Pyraclostrobin + fluxapyroxad, trifloxystrobin + prothioconazole",
            "threshold": "Disease progressing on upper canopy + >14 days to R7",
        },
    ],
    "winter_wheat": [
        {
            "disease": "Fusarium head blight (scab)",
            "window": "Anthesis (Feekes 10.5.1)",
            "treatment": "Metconazole (Caramba) or prothioconazole (Prosaro)",
            "threshold": "Susceptible variety + >60°F + rain/high humidity at flowering. Check scabsmart.org. "
                         "Wheat-after-corn: 5-10× higher risk (Purdue).",
        },
        {
            "disease": "Septoria leaf blotch / Stagonospora",
            "window": "Flag leaf emergence (Feekes 8-9)",
            "treatment": "Propiconazole, pyraclostrobin + metconazole",
            "threshold": "Disease on leaf below flag + wet forecast >3 days. Wheat-on-wheat increases residue inoculum.",
        },
        {
            "disease": "Stripe rust (Puccinia striiformis)",
            "window": "Jointing through heading (Feekes 6-10.5)",
            "treatment": "Propiconazole, tebuconazole, pyraclostrobin + metconazole",
            "threshold": "Pustules on lower leaves + cool (40-65°F) wet forecast. Scout by Feekes 6. USDA CDL, SDSU.",
        },
        {
            "disease": "Take-all (Gaeumannomyces graminis)",
            "window": "Fall through spring (manage via rotation)",
            "treatment": "No effective fungicide. 2+ year rotation out of wheat/barley is the only control.",
            "threshold": "Wheat-after-wheat or wheat-after-grass. Up to 50% yield loss in 2nd-3rd year. MU G4345.",
        },
        {
            "disease": "Snow mold (Typhula / Microdochium)",
            "window": "Late fall through spring greenup",
            "treatment": "Seed treatment (difenoconazole + mefenoxam); foliar fall application in high-risk areas",
            "threshold": "Northern latitudes (>44°N) + prolonged snow cover forecast. Vigorous fall stands resist better. WSU EB1880.",
        },
    ],
    "spring_wheat": [
        {
            "disease": "Fusarium head blight (scab)",
            "window": "Anthesis (Feekes 10.5.1)",
            "treatment": "Metconazole (Caramba) or prothioconazole (Prosaro)",
            "threshold": "Warm (60-85°F) + humid + rain at flowering. Check local DON risk model.",
        },
        {
            "disease": "Tan spot / leaf rust",
            "window": "Flag leaf (Feekes 8-10)",
            "treatment": "Propiconazole, pyraclostrobin, tebuconazole",
            "threshold": "Disease on flag leaf + continued wet weather forecast",
        },
    ],
    "dry_beans": [
        {
            "disease": "White mold (Sclerotinia)",
            "window": "Bloom (R1-R3)",
            "treatment": "Boscalid (Endura), fluopyram, thiophanate-methyl",
            "threshold": "History + dense canopy + >5 days cool/wet forecast",
        },
        {
            "disease": "Anthracnose",
            "window": "R3-R5 (pod development)",
            "treatment": "Azoxystrobin, pyraclostrobin",
            "threshold": "Rain splash + warm temps + susceptible variety",
        },
    ],
    "sugar_beets": [
        {
            "disease": "Cercospora leaf spot",
            "window": "Canopy closure through harvest (June-Sept)",
            "treatment": "Triphenyltin hydroxide (SuperTin), tetraconazole, prothioconazole",
            "threshold": "BEETcast DSV >55 cumulative or first lesions observed. 14-21 day spray intervals.",
        },
        {
            "disease": "Rhizoctonia crown rot",
            "window": "Post-thinning",
            "treatment": "Azoxystrobin band at 4-8 leaf or at lay-by cultivation",
            "threshold": "History of Rhizoctonia + warm, wet soil conditions",
        },
    ],
    "alfalfa": [
        {
            "disease": "Phytophthora root rot",
            "window": "Establishment and poorly-drained fields",
            "treatment": "Mefenoxam seed treatment or resistant varieties",
            "threshold": "Wet, heavy soils + seedling damping-off or stand thinning",
        },
        {
            "disease": "Anthracnose / Verticillium wilt",
            "window": "Summer stress periods",
            "treatment": "Resistant varieties (primary management)",
            "threshold": "Declining stands + crown symptoms + hot/humid periods",
        },
    ],
}

# Tank mix compatibility warnings
_TANK_MIX_INCOMPATIBLE = [
    {"a": "2,4-D ester", "b": "crop oil concentrate", "reason": "Increased crop injury risk"},
    {"a": "glyphosate", "b": "AMS + high-rate micronutrients", "reason": "Antagonism reduces glyphosate efficacy"},
    {"a": "dicamba", "b": "glyphosate K-salt", "reason": "Use only approved DGA formulations; other combos increase volatility"},
    {"a": "chlorpyrifos", "b": "alkaline water (pH>8)", "reason": "Rapid hydrolysis degrades active ingredient"},
    {"a": "metribuzin", "b": "organophosphate insecticide", "reason": "Synergistic crop injury on soybeans"},
    {"a": "ALS herbicide", "b": "organophosphate insecticide", "reason": "P450 inhibition increases ALS crop injury"},
]


def generate_fertility_recommendations(
    crop: str, soil: dict, yield_goal_bu: int | None = None,
    previous_crop: str = "corn", soil_test_p_ppm: float | None = None,
    soil_test_k_ppm: float | None = None, soil_ph: float | None = None,
) -> dict:
    """Generate fertility and nutrient management recommendations.

    Based on Tri-State Fertility Guide (MSU/OSU/Purdue 2020), ISU PM-1688,
    and Purdue AY-267 nitrogen rate calculator approach.
    """
    profile = CROP_PROFILES.get(crop, CROP_PROFILES["corn"])
    removal = _NUTRIENT_REMOVAL.get(crop, _NUTRIENT_REMOVAL["corn"])

    # Default yield goals by crop if not specified
    _DEFAULT_YIELDS = {
        "corn": 200, "soybeans": 55, "winter_wheat": 70, "spring_wheat": 55,
        "dry_beans": 25, "sugar_beets": 25, "alfalfa": 5,
    }
    if yield_goal_bu is None:
        yield_goal_bu = _DEFAULT_YIELDS.get(crop, 100)

    # --- Nitrogen ---
    om_pct = soil.get("organic_matter_pct")
    om_credit = max(0, ((om_pct or 3.0) - 3.0)) * 15

    if crop == "corn":
        # Interpolate from rate table
        tiers = sorted(_N_RATES_CORN.keys())
        if yield_goal_bu <= tiers[0]:
            base_n = _N_RATES_CORN[tiers[0]]
        elif yield_goal_bu >= tiers[-1]:
            base_n = _N_RATES_CORN[tiers[-1]]
        else:
            for i in range(len(tiers) - 1):
                if tiers[i] <= yield_goal_bu <= tiers[i + 1]:
                    frac = (yield_goal_bu - tiers[i]) / (tiers[i + 1] - tiers[i])
                    base_n = _N_RATES_CORN[tiers[i]] + frac * (
                        _N_RATES_CORN[tiers[i + 1]] - _N_RATES_CORN[tiers[i]]
                    )
                    break
            else:
                base_n = 190

        n_credit = _N_CREDITS.get(previous_crop, 0)
        rec_n = max(0, round(base_n - n_credit - om_credit))
        n_notes = []
        if n_credit > 0:
            n_notes.append(f"−{n_credit} lb/ac credit for previous {previous_crop}")
        if om_credit > 0:
            n_notes.append(f"−{om_credit:.0f} lb/ac credit for {om_pct:.1f}% organic matter")
    elif crop in ("winter_wheat", "spring_wheat"):
        base_n = yield_goal_bu * 1.2
        n_credit = _N_CREDITS.get(previous_crop, 0)
        rec_n = max(0, round(base_n - n_credit - om_credit))
        n_notes = [f"Wheat N: ~1.2 lb N/bu yield goal. Split: 30% fall, 70% spring topdress."]
        if n_credit > 0:
            n_notes.append(f"−{n_credit} lb/ac credit for previous {previous_crop}")
    elif crop == "dry_beans":
        base_n = 30
        n_credit = 0
        rec_n = max(0, round(base_n - om_credit))
        n_notes = ["Dry beans fix some N but benefit from 20-40 lb/ac starter N at planting."]
    elif crop == "sugar_beets":
        base_n = yield_goal_bu * 5.5
        n_credit = _N_CREDITS.get(previous_crop, 0)
        rec_n = max(0, round(min(base_n - n_credit - om_credit, 160)))
        n_notes = ["Sugar beet N: ~5.5 lb N/ton yield. Excess N reduces sugar content."]
        if n_credit > 0:
            n_notes.append(f"−{n_credit} lb/ac credit for previous {previous_crop}")
    elif crop == "alfalfa":
        base_n = 0
        n_credit = 0
        rec_n = 0
        n_notes = ["Alfalfa fixes 150-200 lb N/ac/yr via Rhizobium — no N fertilizer needed."]
    else:
        rec_n = 0
        base_n = 0
        n_credit = 0
        n_notes = ["Soybeans fix atmospheric N via Bradyrhizobium — no N fertilizer needed."]

    # --- P2O5 and K2O ---
    # Determine soil test category
    p_cat = "optimum"
    k_cat = "optimum"
    if soil_test_p_ppm is not None:
        for cat, ranges in _PK_CATEGORIES.items():
            lo, hi = ranges["p_ppm"]
            if lo <= soil_test_p_ppm <= hi:
                p_cat = cat
                break
    if soil_test_k_ppm is not None:
        for cat, ranges in _PK_CATEGORIES.items():
            lo, hi = ranges["k_ppm"]
            if lo <= soil_test_k_ppm <= hi:
                k_cat = cat
                break

    pk_rates = _PK_RATES.get(crop, _PK_RATES["corn"])
    rec_p2o5 = pk_rates.get(p_cat, {}).get("p2o5", 30)
    rec_k2o = pk_rates.get(k_cat, {}).get("k2o", 40)

    # --- Sulfur ---
    # Sandy soils and low-OM soils need supplemental S
    sand_pct = soil.get("sand_pct") or 0
    om = soil.get("organic_matter_pct") or 3.0
    if sand_pct > 50 or om < 2.5:
        rec_s = 15 if crop == "corn" else 10
        s_note = "Sandy/low-OM soil — supplemental S recommended"
    else:
        rec_s = 0
        s_note = "Adequate OM mineralization expected"

    # --- Lime ---
    lime_rec = None
    if soil_ph is not None:
        if crop == "corn" and soil_ph < 6.0:
            lime_rec = {"tons_ac": round((6.3 - soil_ph) * 2.0, 1),
                        "target_ph": 6.3,
                        "note": "Apply 6+ months before planting for full reaction"}
        elif crop == "soybeans" and soil_ph < 6.2:
            lime_rec = {"tons_ac": round((6.5 - soil_ph) * 2.0, 1),
                        "target_ph": 6.5,
                        "note": "Soybeans are pH-sensitive; apply lime in fall"}

    # --- Timing ---
    timing = []
    if crop == "corn":
        if rec_n > 0:
            split = rec_n > 150
            if split:
                timing.append({
                    "product": "Nitrogen (UAN or anhydrous)",
                    "rate": f"{rec_n} lb N/ac total",
                    "timing": f"Split: {int(rec_n * 0.4)} lb/ac pre-plant + {int(rec_n * 0.6)} lb/ac sidedress at V6-V8",
                    "method": "Anhydrous NH3 pre-plant or UAN 28-32% sidedress",
                })
            else:
                timing.append({
                    "product": "Nitrogen",
                    "rate": f"{rec_n} lb N/ac",
                    "timing": "Pre-plant or at-plant",
                    "method": "Anhydrous NH3 or UAN 28-32%",
                })
    if rec_p2o5 > 0:
        timing.append({
            "product": "Phosphorus (P₂O₅)",
            "rate": f"{rec_p2o5} lb P₂O₅/ac",
            "timing": "Fall broadcast or spring band (2x2)",
            "method": "DAP/MAP or liquid 10-34-0",
        })
    if rec_k2o > 0:
        timing.append({
            "product": "Potassium (K₂O)",
            "rate": f"{rec_k2o} lb K₂O/ac",
            "timing": "Fall broadcast preferred (spring OK on non-sandy soils)",
            "method": "Muriate of potash (0-0-60)",
        })
    if rec_s > 0:
        timing.append({
            "product": "Sulfur",
            "rate": f"{rec_s} lb S/ac",
            "timing": "Spring broadcast or with starter",
            "method": "AMS (21-0-0-24S) or gypsum",
        })

    return {
        "crop": crop,
        "yield_goal_bu": yield_goal_bu,
        "previous_crop": previous_crop,
        "soil_texture": soil.get("texture_class"),
        "organic_matter_pct": om,
        "nitrogen": {
            "base_rate": round(base_n),
            "credit_previous_crop": n_credit,
            "credit_om": round(om_credit),
            "recommended_lb_ac": rec_n,
            "notes": n_notes,
        },
        "phosphorus": {
            "soil_test_category": p_cat,
            "soil_test_ppm": soil_test_p_ppm,
            "recommended_lb_ac": rec_p2o5,
        },
        "potassium": {
            "soil_test_category": k_cat,
            "soil_test_ppm": soil_test_k_ppm,
            "recommended_lb_ac": rec_k2o,
        },
        "sulfur": {"recommended_lb_ac": rec_s, "note": s_note},
        "lime": lime_rec,
        "timing": timing,
        "removal_per_bu": removal,
        "source": "Tri-State Fertility Guide (MSU/OSU/Purdue 2020), ISU PM-1688",
    }


def generate_pest_recommendations(crop: str, cum_gdd: float | None = None,
                                  soil: dict | None = None,
                                  rotation: dict | None = None) -> dict:
    """Generate pest management recommendations based on crop, GDD, and field history."""
    soil = soil or {}
    rotation = rotation or {}

    # Determine current growth stage from GDD
    stages = _CROP_STAGE_MAP.get(crop, _CORN_STAGES_GDD)
    current_stage = "pre-plant"
    if cum_gdd is not None:
        for stage, gdd in sorted(stages.items(), key=lambda x: x[1], reverse=True):
            if cum_gdd >= gdd:
                current_stage = stage
                break

    # Herbicide windows
    herb_windows = _HERBICIDE_WINDOWS.get(crop, {})
    herbicides = []
    for phase, info in herb_windows.items():
        start_stage, end_stage = info["window"]
        start_gdd = stages.get(start_stage, 0) if start_stage != "planting" else 0
        end_gdd = stages.get(end_stage, 9999)
        active = (cum_gdd is not None and start_gdd <= cum_gdd <= end_gdd) if cum_gdd else False
        herbicides.append({
            "phase": phase.replace("_", " ").title(),
            "window_stages": f"{start_stage} – {end_stage}",
            "window_gdd": f"{start_gdd}–{end_gdd} GDD",
            "active_now": active,
            "products": info["products"],
            "notes": info["notes"],
        })

    # Insecticide recommendations
    insecticides = _INSECTICIDE_WINDOWS.get(crop, [])

    # Fungicide recommendations
    fungicides = _FUNGICIDE_WINDOWS.get(crop, [])

    # Rotation-based risk flags
    risk_flags = []
    if rotation.get("corn_on_corn"):
        risk_flags.append("Corn-on-corn increases rootworm, seedcorn maggot, and gray leaf spot pressure")
    if rotation.get("soy_on_soy"):
        risk_flags.append("Soy-on-soy increases Phytophthora, SCN, and white mold pressure")

    return {
        "crop": crop,
        "current_stage": current_stage,
        "cum_gdd": round(cum_gdd) if cum_gdd else None,
        "herbicide_windows": herbicides,
        "insecticide_recs": insecticides,
        "fungicide_recs": fungicides,
        "tank_mix_warnings": _TANK_MIX_INCOMPATIBLE,
        "rotation_risk_flags": risk_flags,
        "growth_stages": stages,
        "source": "Purdue Extension, ISU Extension, MSU Extension weed/pest guides",
    }


# ----- Crop Rotation Intelligence -----------------------------------------

_ROTATION_RECS = {
    "corn": {
        "ideal_after": ["soybeans", "alfalfa", "dry_beans", "winter_wheat"],
        "avoid_after": ["corn"],
        "yield_drag_pct": 10,
        "pest_escalation": ["Rootworm", "Seedcorn maggot", "Gray leaf spot"],
    },
    "soybeans": {
        "ideal_after": ["corn", "winter_wheat", "spring_wheat"],
        "avoid_after": ["soybeans"],
        "yield_drag_pct": 8,
        "pest_escalation": ["SCN", "Phytophthora", "White mold", "SDS"],
    },
    "winter_wheat": {
        "ideal_after": ["soybeans", "dry_beans", "alfalfa"],
        "caution_after": ["corn"],
        "avoid_after": ["winter_wheat", "spring_wheat"],
        "yield_drag_pct": 5,
        "yield_drag_wheat_on_wheat_pct": 40,
        "pest_escalation": ["Fusarium (5-10× after corn)", "Take-all", "Septoria",
                            "Stripe rust", "Crown rot"],
    },
    "spring_wheat": {
        "ideal_after": ["soybeans", "corn", "dry_beans", "sugar_beets"],
        "avoid_after": ["winter_wheat", "spring_wheat"],
        "yield_drag_pct": 5,
        "pest_escalation": ["Fusarium", "Tan spot", "Root rot"],
    },
    "dry_beans": {
        "ideal_after": ["corn", "winter_wheat", "sugar_beets"],
        "avoid_after": ["dry_beans", "soybeans"],
        "yield_drag_pct": 12,
        "pest_escalation": ["White mold", "Root rot", "Anthracnose"],
    },
    "sugar_beets": {
        "ideal_after": ["corn", "winter_wheat", "spring_wheat"],
        "avoid_after": ["sugar_beets"],
        "yield_drag_pct": 15,
        "pest_escalation": ["Rhizoctonia", "Cercospora", "Nematodes"],
    },
    "alfalfa": {
        "ideal_after": ["corn", "winter_wheat", "spring_wheat"],
        "avoid_after": ["alfalfa"],
        "yield_drag_pct": 20,
        "pest_escalation": ["Autotoxicity", "Phytophthora"],
    },
}

_CDL_TO_CROP = {
    1: "corn", 5: "soybeans", 24: "winter_wheat", 23: "spring_wheat",
    42: "dry_beans", 41: "sugar_beets", 36: "alfalfa",
    26: "winter_wheat", 22: "spring_wheat",
}

_COVER_CROPS = {
    "cereal_rye": {"n_credit_lb": 30, "biomass": "high", "weed_suppression": "excellent",
                   "best_before": ["corn", "soybeans"], "terminate_gdd": 200},
    "crimson_clover": {"n_credit_lb": 80, "biomass": "moderate", "weed_suppression": "good",
                       "best_before": ["corn"], "terminate_gdd": 150},
    "annual_ryegrass": {"n_credit_lb": 0, "biomass": "high", "weed_suppression": "excellent",
                        "best_before": ["soybeans", "corn"], "terminate_gdd": 200},
    "radish": {"n_credit_lb": 0, "biomass": "low", "weed_suppression": "fair",
               "best_before": ["corn", "soybeans"], "terminate_gdd": 0,
               "notes": "Winterkills in most Midwest winters. Excellent compaction relief."},
    "oats": {"n_credit_lb": 0, "biomass": "moderate", "weed_suppression": "good",
             "best_before": ["soybeans", "dry_beans"], "terminate_gdd": 0,
             "notes": "Winterkills. Low-cost, reliable nurse crop for alfalfa establishment."},
}


def generate_rotation_intelligence(lat: float, lon: float, crop: str = "corn",
                                   cover_crop: str | None = None) -> dict:
    """Analyze rotation history and generate actionable recommendations."""
    rotation = fetch_cropscape_history(lat, lon, 5)
    years = rotation.get("years") or []

    prev_crops = []
    for y in years:
        code = y.get("crop_code")
        name = _CDL_TO_CROP.get(code, y.get("crop_name", "").lower().replace(" ", "_"))
        prev_crops.append({"year": y["year"], "crop": name, "crop_name": y.get("crop_name", "")})

    rec_info = _ROTATION_RECS.get(crop, {})
    ideal = rec_info.get("ideal_after", [])
    avoid = rec_info.get("avoid_after", [])

    # Determine if current selection repeats
    prev_crop = prev_crops[0]["crop"] if prev_crops else None
    is_continuous = prev_crop == crop or (prev_crop in avoid)
    consecutive = 0
    for pc in prev_crops:
        if pc["crop"] == crop or pc["crop"] in avoid:
            consecutive += 1
        else:
            break

    # Rotation score (0-100)
    diversity = len(set(pc["crop"] for pc in prev_crops)) if prev_crops else 0
    score = 100
    if is_continuous:
        score -= 25 * consecutive
    if diversity <= 1 and len(prev_crops) >= 2:
        score -= 20
    score = max(0, min(100, score))

    # Recommendations
    recs = []
    if is_continuous:
        drag = rec_info.get("yield_drag_pct", 5)
        pests = rec_info.get("pest_escalation", [])
        recs.append({
            "type": "warning",
            "title": f"Continuous {CROP_PROFILES.get(crop, {}).get('label', crop)} detected",
            "detail": (f"{consecutive} consecutive year(s) of same/similar crop. "
                       f"Expected yield drag: ~{drag}%. Elevated pest pressure: {', '.join(pests)}."),
        })
        for alt in ideal:
            alt_label = CROP_PROFILES.get(alt, {}).get("label", alt)
            recs.append({
                "type": "suggestion",
                "title": f"Consider rotating to {alt_label}",
                "detail": f"{alt_label} is an ideal rotation partner — breaks pest cycles and diversifies income.",
            })
    elif prev_crop and prev_crop in ideal:
        recs.append({
            "type": "positive",
            "title": f"Good rotation: {crop} after {prev_crop}",
            "detail": "This rotation breaks disease/pest cycles and maximizes nutrient cycling efficiency.",
        })

    # Cover crop recommendations
    cover_recs = []
    for cc_name, cc_data in _COVER_CROPS.items():
        if crop in cc_data["best_before"]:
            entry = {
                "name": cc_name.replace("_", " ").title(),
                "n_credit_lb": cc_data["n_credit_lb"],
                "biomass": cc_data["biomass"],
                "weed_suppression": cc_data["weed_suppression"],
                "terminate_gdd": cc_data["terminate_gdd"],
            }
            if cc_data.get("notes"):
                entry["notes"] = cc_data["notes"]
            cover_recs.append(entry)

    # If user specified a cover crop, give specific advice
    cover_detail = None
    if cover_crop and cover_crop in _COVER_CROPS:
        cc = _COVER_CROPS[cover_crop]
        cover_detail = {
            "name": cover_crop.replace("_", " ").title(),
            "n_credit_lb": cc["n_credit_lb"],
            "terminate_gdd": cc["terminate_gdd"],
            "notes": cc.get("notes", ""),
        }

    return {
        "available": rotation.get("available", False),
        "rotation_score": score,
        "history": prev_crops,
        "consecutive_same": consecutive,
        "is_continuous": is_continuous,
        "current_crop": crop,
        "recommendations": recs,
        "cover_crop_options": cover_recs,
        "cover_crop_selected": cover_detail,
        "diversity_count": diversity,
        "source": "USDA CropScape CDL + Tri-State Extension rotation guidelines",
    }


# ----- Predictive Yield Estimation ----------------------------------------

_MATURITY_GDD = {
    "corn": {"early": (2300, 95), "mid": (2600, 105), "full": (2800, 112)},
    "soybeans": {"early": (2200, 2.0), "mid": (2500, 2.8), "full": (2800, 3.5)},
    "winter_wheat": {"early": (1800, None), "mid": (2000, None), "full": (2200, None)},
    "spring_wheat": {"early": (1600, None), "mid": (1900, None), "full": (2100, None)},
    "dry_beans": {"early": (1400, None), "mid": (1600, None), "full": (1800, None)},
    "sugar_beets": {"early": (2500, None), "mid": (2800, None), "full": (3100, None)},
    "alfalfa": {"early": (700, None), "mid": (850, None), "full": (1000, None)},
}

_COUNTY_YIELD_RANGES = {
    "corn": {"low": 140, "avg": 185, "high": 230},
    "soybeans": {"low": 42, "avg": 55, "high": 68},
    "winter_wheat": {"low": 55, "avg": 70, "high": 85},
    "spring_wheat": {"low": 40, "avg": 55, "high": 70},
    "dry_beans": {"low": 18, "avg": 25, "high": 32},
    "sugar_beets": {"low": 20, "avg": 28, "high": 35},
    "alfalfa": {"low": 3.5, "avg": 5.0, "high": 6.5},
}


def estimate_yield(lat: float, lon: float, crop: str = "corn",
                   relative_maturity: int | None = None,
                   planting_date_iso: str | None = None,
                   seeds_per_acre: int | None = None) -> dict:
    """Predictive yield estimation combining GDD pace, NDVI, drought, and population."""
    from datetime import date as _date, timedelta

    today = _date.today()
    maturity_group = _MATURITY_GDD.get(crop, _MATURITY_GDD["corn"])
    yield_range = _COUNTY_YIELD_RANGES.get(crop, _COUNTY_YIELD_RANGES["corn"])

    # Use mid-season maturity as default
    if relative_maturity is not None:
        if crop == "corn":
            target_gdd = 2200 + (relative_maturity - 90) * 25
        else:
            target_gdd = maturity_group["mid"][0]
    else:
        target_gdd = maturity_group["mid"][0]

    # Get current GDD accumulation
    try:
        scm_archive, scm_extended = fetch_scm_inputs(lat, lon)
        gdd_lookup = build_base50_gdd_lookup(scm_archive, scm_extended)
    except Exception:
        gdd_lookup = {}

    current_gdd = gdd_lookup.get(today.isoformat(), 0)
    pct_complete = min(100, round((current_gdd / target_gdd) * 100, 1)) if target_gdd > 0 else 0

    # Estimate maturity date based on recent GDD pace
    recent_dates = [(today - timedelta(days=i)).isoformat() for i in range(14)]
    recent_gdds = [gdd_lookup.get(d, 0) for d in recent_dates]
    if len(recent_gdds) >= 2 and recent_gdds[0] > recent_gdds[-1]:
        daily_pace = (recent_gdds[0] - recent_gdds[-1]) / 14
    else:
        daily_pace = 15.0  # Midwest average May-Aug

    remaining_gdd = max(0, target_gdd - current_gdd)
    days_to_maturity = round(remaining_gdd / daily_pace) if daily_pace > 0 else 999
    estimated_maturity = today + timedelta(days=days_to_maturity)

    # NDVI-based yield adjustment
    ndvi_adjustment = 1.0
    try:
        ndvi_data = fetch_ndvi_timeseries(lat, lon, days_back=60, crop=crop)
        if ndvi_data.get("available") and ndvi_data.get("readings"):
            latest_ndvi = ndvi_data["readings"][-1]["ndvi"]
            if latest_ndvi > 0.7:
                ndvi_adjustment = 1.1
            elif latest_ndvi > 0.5:
                ndvi_adjustment = 1.0
            elif latest_ndvi > 0.3:
                ndvi_adjustment = 0.85
            else:
                ndvi_adjustment = 0.7
    except Exception:
        pass

    # Drought impact
    drought_adjustment = 1.0
    try:
        drought = fetch_usdm_drought(lat, lon)
        dm_class = drought.get("class", -1) if drought else -1
        if dm_class >= 3:
            drought_adjustment = 0.7
        elif dm_class == 2:
            drought_adjustment = 0.82
        elif dm_class == 1:
            drought_adjustment = 0.92
        elif dm_class == 0:
            drought_adjustment = 0.96
    except Exception:
        pass

    # Population (seed density) adjustment.
    # Optimal populations by crop (seeds/acre) from university research:
    #   Corn: 32,000-36,000 (ISU/Purdue); yield penalty below 28k or above 40k
    #   Soybeans: 120,000-140,000 (ISU); tolerant of wide range
    #   Wheat: 1,200,000-1,600,000 (MSU)
    # Outside the optimal range, yield declines following a quadratic response
    # curve (Duncan 1958 corn population studies; Nafziger 2006 meta-analysis).
    population_adjustment = 1.0
    population_note = None
    if seeds_per_acre is not None and seeds_per_acre > 0:
        _POP_OPTIMA = {
            "corn": (32000, 36000),
            "soybeans": (120000, 140000),
            "winter_wheat": (1200000, 1600000),
            "spring_wheat": (1200000, 1600000),
            "dry_beans": (75000, 90000),
            "sugar_beets": (34000, 40000),
            "alfalfa": (800000, 1000000),
        }
        opt_low, opt_high = _POP_OPTIMA.get(crop, (30000, 36000))
        if seeds_per_acre < opt_low:
            deficit_pct = (opt_low - seeds_per_acre) / opt_low
            population_adjustment = max(0.60, 1.0 - 0.8 * deficit_pct ** 1.5)
            population_note = f"Below optimal ({seeds_per_acre:,} vs {opt_low:,}-{opt_high:,} recommended)"
        elif seeds_per_acre > opt_high:
            excess_pct = (seeds_per_acre - opt_high) / opt_high
            population_adjustment = max(0.75, 1.0 - 0.5 * excess_pct ** 1.5)
            population_note = f"Above optimal ({seeds_per_acre:,} vs {opt_low:,}-{opt_high:,} recommended)"
        else:
            pop_mid = (opt_low + opt_high) / 2
            closeness = 1.0 - abs(seeds_per_acre - pop_mid) / (opt_high - opt_low)
            population_adjustment = 1.0 + 0.03 * closeness
            population_note = f"Within optimal range ({seeds_per_acre:,} seeds/ac)"

    # Calculate estimated yield
    base_yield = yield_range["avg"]
    estimated_yield = round(base_yield * ndvi_adjustment * drought_adjustment * population_adjustment, 1)
    yield_low = round(yield_range["low"] * drought_adjustment * population_adjustment, 1)
    yield_high = round(yield_range["high"] * ndvi_adjustment * population_adjustment, 1)

    # Contract decision support
    contract_notes = []
    if days_to_maturity < 30:
        contract_notes.append("Near maturity — consider locking basis on remaining unpriced bushels.")
    elif pct_complete > 60:
        contract_notes.append(f"At {pct_complete:.0f}% GDD completion, crop trajectory supports current yield estimates.")
    if drought_adjustment < 0.9:
        contract_notes.append("Drought stress detected — yield may be below trend. Caution on new forward contracts.")

    result = {
        "crop": crop,
        "current_gdd": round(current_gdd),
        "target_gdd": target_gdd,
        "pct_complete": pct_complete,
        "daily_gdd_pace": round(daily_pace, 1),
        "days_to_maturity": days_to_maturity,
        "estimated_maturity_date": estimated_maturity.isoformat(),
        "yield_estimate": {
            "low": yield_low,
            "expected": estimated_yield,
            "high": yield_high,
            "unit": "bu/ac" if crop != "alfalfa" else "tons/ac",
        },
        "adjustments": {
            "ndvi_factor": round(ndvi_adjustment, 2),
            "drought_factor": round(drought_adjustment, 2),
            "population_factor": round(population_adjustment, 3),
        },
        "contract_notes": contract_notes,
        "source": "GDD model (base 50°F) + NDVI correlation + USDM drought + population response",
    }
    if seeds_per_acre is not None:
        result["population"] = {
            "seeds_per_acre": seeds_per_acre,
            "adjustment_factor": round(population_adjustment, 3),
            "note": population_note,
        }
    return result


# ----- Real-time alert evaluation ----------------------------------------

def evaluate_alerts(lat: float, lon: float, place: str, crop: str = "corn") -> list[dict]:
    """Evaluate current conditions for alert-worthy events.

    Returns a list of triggered alerts with type, severity, and message.
    This is designed to be called periodically for subscribed fields.
    """
    alerts: list[dict] = []
    profile = CROP_PROFILES.get(crop, CROP_PROFILES["corn"])

    try:
        forecast = fetch_forecast(lat, lon, days=3)
    except Exception:
        return alerts

    hourly = forecast.get("hourly", {})
    temps = hourly.get("temperature_2m", [])
    soil_temps = hourly.get("soil_temperature_6cm", [])
    precip = hourly.get("precipitation", [])
    wind = hourly.get("wind_speed_10m", [])
    times = hourly.get("time", [])

    if not temps:
        return alerts

    # 1. Frost alert — air temp dropping below frost threshold in next 72h
    frost_floor = profile.get("frost_air_temp_f", 28)
    frost_hours = []
    for i, t in enumerate(temps[:72]):
        if t is not None and t <= frost_floor:
            frost_hours.append(i)
    if frost_hours:
        first_hour = frost_hours[0]
        min_temp = min(t for t in temps[:72] if t is not None)
        if first_hour < 24:
            urgency = "critical"
            time_label = "within 24 hours"
        elif first_hour < 48:
            urgency = "warning"
            time_label = "in 24-48 hours"
        else:
            urgency = "watch"
            time_label = "in 48-72 hours"
        alerts.append({
            "type": "frost_alert",
            "urgency": urgency,
            "title": f"Frost Warning — {min_temp:.0f}°F expected",
            "message": f"Air temperature forecast to drop to {min_temp:.0f}°F {time_label}. "
                       f"Frost threshold for {crop} is {frost_floor}°F.",
            "hours_until": first_hour,
            "min_temp_f": round(min_temp, 1),
        })

    # 2. Optimal planting window — soil temp above threshold + no heavy rain
    soil_floor = profile.get("min_soil_temp_f", 50)
    if soil_temps:
        next_48_soil = [t for t in soil_temps[:48] if t is not None]
        next_48_precip = sum(p for p in precip[:48] if p is not None)
        if next_48_soil:
            min_soil = min(next_48_soil)
            avg_soil = sum(next_48_soil) / len(next_48_soil)
            if min_soil >= soil_floor and next_48_precip < 0.75:
                alerts.append({
                    "type": "optimal_window",
                    "urgency": "info",
                    "title": "Planting Window Open",
                    "message": f"Soil temperature holding above {soil_floor}°F "
                               f"(avg {avg_soil:.1f}°F) with minimal rain ({next_48_precip:.2f}\") "
                               f"in the next 48 hours. Good conditions for {crop} planting.",
                    "avg_soil_f": round(avg_soil, 1),
                    "precip_48h_in": round(next_48_precip, 2),
                })

    # 3. Severe weather — high winds, heavy precip events
    max_wind = max((w for w in wind[:72] if w is not None), default=0)
    if max_wind >= 40:
        alerts.append({
            "type": "severe_weather",
            "urgency": "critical" if max_wind >= 58 else "warning",
            "title": f"High Wind Alert — {max_wind:.0f} mph forecast",
            "message": f"Wind speeds up to {max_wind:.0f} mph expected in the next 72 hours. "
                       f"Secure equipment and avoid spraying.",
            "max_wind_mph": round(max_wind, 1),
        })

    heavy_precip_24h = sum(p for p in precip[:24] if p is not None)
    if heavy_precip_24h >= 2.0:
        alerts.append({
            "type": "severe_weather",
            "urgency": "warning",
            "title": f"Heavy Rain — {heavy_precip_24h:.1f}\" in 24h",
            "message": f"Heavy precipitation of {heavy_precip_24h:.1f}\" expected in the next 24 hours. "
                       f"Potential for ponding, erosion, and delayed field access.",
            "precip_24h_in": round(heavy_precip_24h, 2),
        })

    # 4. Soil temperature crossing threshold (going up = opportunity)
    if soil_temps and len(soil_temps) >= 48:
        current_soil = soil_temps[0]
        soil_24h_later = soil_temps[24] if len(soil_temps) > 24 else None
        if (current_soil is not None and soil_24h_later is not None
                and current_soil < soil_floor and soil_24h_later >= soil_floor):
            alerts.append({
                "type": "soil_temp",
                "urgency": "info",
                "title": f"Soil Temp Crossing {soil_floor}°F Tomorrow",
                "message": f"Soil temperature rising from {current_soil:.1f}°F to {soil_24h_later:.1f}°F "
                           f"in the next 24 hours — crossing the {soil_floor}°F planting threshold for {crop}.",
                "current_soil_f": round(current_soil, 1),
                "forecast_soil_f": round(soil_24h_later, 1),
            })

    # 5. Spray window opening in next 24h
    spray_hours = 0
    for i in range(min(24, len(temps))):
        t = temps[i]
        w = wind[i] if i < len(wind) else None
        p = precip[i] if i < len(precip) else None
        if t is None or w is None or p is None:
            continue
        if 40 <= t <= 85 and w < 10 and p < 0.01:
            spray_hours += 1
    if spray_hours >= 6:
        alerts.append({
            "type": "spray_window",
            "urgency": "info",
            "title": f"Spray Window — {spray_hours}h available tomorrow",
            "message": f"{spray_hours} hours of spray-suitable conditions in the next 24 hours "
                       f"(wind <10 mph, no rain, temp 40-85°F).",
            "spray_hours": spray_hours,
        })

    return alerts


# ----- 7-day plant-day scoring ------------------------------------------

def _day_conditions(forecast: dict, start: int) -> dict:
    """Snapshot the agronomic signals that drive plantability for a given day.

    Window choices match the evaluators: 48h for chilling/precip, 168h (the
    emergence window) for the frost low, and 96h for soil-moisture saturation
    near the seed depth.
    """
    soil_48 = _hourly_window(forecast, "soil_temperature_6cm", start, start + 48)
    soil_96 = _hourly_window(forecast, "soil_temperature_6cm", start, start + 96)
    precip_48 = _hourly_window(forecast, "precipitation", start, start + 48)
    air_168 = _hourly_window(forecast, "temperature_2m", start, start + 168)
    moist_96 = _hourly_window(forecast, "soil_moisture_1_to_3cm", start, start + 96)
    return {
        "min_soil_temp_f": round(min(soil_48), 1) if soil_48 else None,
        "avg_soil_temp_f": round(_avg(soil_96), 1) if soil_96 else None,
        "precip_48h_in": round(sum(precip_48), 2) if precip_48 else 0.0,
        "min_air_temp_f": round(min(air_168), 1) if air_168 else None,
        "sat_hours_96h": _saturated_hours(moist_96, 0.38) if moist_96 else 0,
    }


def _cultivar_survival_factor(cultivar: dict | None) -> float:
    """Brand/cultivar-dependent multiplier on survival probability.

    Emergence score and cold-tolerance class each contribute a factor.
    A top-rated cultivar (emergence 8+, high cold tol.) can boost survival
    by ~12%; a weak cultivar (emergence ≤5, low cold tol.) penalises by ~14%.
    """
    if not cultivar:
        return 1.0
    factor = 1.0
    es = cultivar.get("emergence_score")
    if es is not None:
        # 7 is neutral; each point above/below shifts survival ~3%.
        factor *= 1.0 + (es - 7) * 0.03
    cold = (cultivar.get("cold_tolerance") or "standard").lower()
    if cold == "high":
        factor *= 1.05
    elif cold == "low":
        factor *= 0.93
    return factor


def _survival_pct(risks: list[Risk], cultivar: dict | None = None) -> int:
    """Per-factor multiplicative survival probability.

    Each of the 22 risk factors produces its own survival probability using
    the biologically appropriate model for its category (biological response,
    time/intensity, modifier, or hazard probability). Modifier factors amplify
    downstream factors. All non-modifier survival factors multiply together.
    """
    return _external_risk_survivability(risks, cultivar)


def score_planting_window(forecast: dict, profile: dict, inputs: UserInputs,
                          max_days: int = PLAN_HORIZON_DAYS,
                          cultivar: dict | None = None) -> list[dict]:
    """Score each upcoming day as a planting candidate.

    For each daily index we re-run all evaluators against a window starting at
    that day's 06:00 local-equivalent slot and pick the worst level + score.
    """
    daily_times = forecast.get("daily", {}).get("time", [])[:max_days]
    hourly_times = forecast["hourly"]["time"]
    out = []

    hourly_temps = forecast.get("hourly", {}).get("temperature_2m", [])

    for day_idx, iso in enumerate(daily_times):
        # Map day → start-hour in the hourly arrays. Open-Meteo aligns hourly
        # to local midnight, so day_idx*24 + 6 is roughly 6 a.m. on that day.
        start = day_idx * 24 + 6
        if start + 48 > len(hourly_times):
            break
        risks = [ev(forecast, profile, inputs, start) for ev in RISK_EVALUATORS]
        overall_worst = max(LEVEL_RANK[r.level] for r in risks)
        survival = _survival_pct(risks, cultivar)
        day_temps = hourly_temps[start:start + 48]
        _frost_kill = profile.get("frost_air_temp_f", 28)
        _consec = 0
        _has_kill = False
        for _t in day_temps:
            if _t is not None and _t < _frost_kill:
                _consec += 1
                if _consec >= 3:
                    _has_kill = True
                    break
            else:
                _consec = 0
        if _has_kill:
            survival = 0
        _lethal_f = profile.get("heat_lethal_f", 113)
        if any(t >= _lethal_f for t in day_temps if t is not None):
            survival = 0
        _all_daily_precip = forecast.get("daily", {}).get("precipitation_sum") or []
        _end_idx = min(day_idx + 14, len(_all_daily_precip))
        _window_days = _end_idx - day_idx
        if _window_days >= 10:
            _day_precip = sum(p for p in _all_daily_precip[day_idx:_end_idx] if p is not None)
        else:
            _day_precip = float("inf")
        _hist_daily = (forecast.get("_history") or {}).get("daily", {}).get("precipitation_sum") or []
        _hist_len = len(_hist_daily)
        _hist_end = max(0, _hist_len - day_idx)
        _hist_start = max(0, _hist_end - 30)
        _recent_window = _hist_daily[_hist_start:_hist_end] if _hist_end > _hist_start else _hist_daily
        _day_hist_precip = sum(p for p in _recent_window if p is not None)
        if _day_precip < 0.1 and _day_hist_precip < 0.5:
            survival = 0

        # Fall-planted wheat: cap survival if soil is too warm for seeding.
        # OSU/KSU Extension: planting above 65°F promotes excess growth, disease,
        # and Hessian fly exposure. Below 40°F = insufficient GDD.
        if profile.get("fall_planted"):
            _soil_window = _hourly_window(forecast, "soil_temperature_6cm", start, start + 72)
            if _soil_window:
                _avg_soil = sum(_soil_window) / len(_soil_window)
                _max_soil = profile.get("max_soil_temp_f", 65)
                if _avg_soil > _max_soil + 5:
                    survival = min(survival, 40)
                elif _avg_soil > _max_soil:
                    survival = min(survival, 70)
                elif _avg_soil < profile.get("min_soil_temp_f", 40):
                    survival = min(survival, 50)

        score = survival
        verdict = ("DO NOT PLANT" if survival < 65
                   else "WAIT & WATCH" if survival < 90
                   else "OPTIMAL")
        out.append({
            "date": iso,
            "score": score,
            "survival_pct": survival,
            "verdict": verdict,
            "level": ["low", "moderate", "high"][overall_worst],
            "is_climate": False,
            "conditions": _day_conditions(forecast, start),
            "top_risks": [
                {"key": r.key, "name": r.name, "level": r.level,
                 "survival_factor": round(r.survival_factor, 3)}
                for r in sorted(risks, key=lambda r: -LEVEL_RANK[r.level])[:3]
                if r.level != "low"
            ],
        })
    return out


def score_spray_windows(forecast: dict, max_days: int = PLAN_HORIZON_DAYS) -> list[dict]:
    """Score each upcoming day for spray application suitability.

    Spray window requirements (university extension consensus):
      - Wind speed < 10 mph (drift risk above 10)
      - No rain within 4 hours after application (washoff)
      - Air temperature between 40°F and 85°F (efficacy + volatilization)
      - Relative humidity < 90% (droplet evaporation issues below, drift above)

    Returns a list of dicts with spray_ok, spray_hours (best window in the day),
    and limiting factor.
    """
    daily_times = forecast.get("daily", {}).get("time", [])[:max_days]
    hourly = forecast.get("hourly", {})
    temps = hourly.get("temperature_2m", [])
    precip = hourly.get("precipitation", [])
    wind = hourly.get("wind_speed_10m", []) or hourly.get("windspeed_10m", [])

    out = []
    for day_idx, iso in enumerate(daily_times):
        start = day_idx * 24 + 6   # 6 AM
        end = min(start + 14, len(temps))  # spray window 6AM - 8PM

        if start >= len(temps):
            break

        spray_hours = 0
        limiting = []
        for h in range(start, end):
            if h >= len(temps):
                break
            t = temps[h] if h < len(temps) else None
            p = precip[h] if h < len(precip) else 0
            w = wind[h] if h < len(wind) else 0

            if t is None:
                continue

            # Check next 4 hours for rain
            rain_ahead = sum(
                precip[h2] for h2 in range(h, min(h + 4, len(precip)))
                if h2 < len(precip) and precip[h2] is not None
            )

            hour_ok = True
            if w is not None and w >= 10:
                hour_ok = False
                if "wind" not in limiting:
                    limiting.append("wind")
            if rain_ahead > 0.05:
                hour_ok = False
                if "rain" not in limiting:
                    limiting.append("rain")
            if t < 40 or t > 85:
                hour_ok = False
                if "temperature" not in limiting:
                    limiting.append("temperature")

            if hour_ok:
                spray_hours += 1

        if spray_hours >= 6:
            verdict = "GOOD"
            level = "low"
        elif spray_hours >= 3:
            verdict = "MARGINAL"
            level = "moderate"
        else:
            verdict = "NO SPRAY"
            level = "high"

        out.append({
            "date": iso,
            "spray_hours": spray_hours,
            "verdict": verdict,
            "level": level,
            "limiting_factors": limiting,
        })
    return out


def generate_ical_events(plant_days: list[dict], spray_days: list[dict],
                         place: str, crop: str) -> str:
    """Generate an iCalendar (.ics) string for optimal planting and spray windows."""
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//CropSentry//CropSentry Tactical Calendar//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:Crop Sentry - {place}",
    ]

    for d in plant_days:
        if d.get("is_climate"):
            continue
        if d.get("verdict") == "OPTIMAL":
            dt = d["date"].replace("-", "")
            lines.extend([
                "BEGIN:VEVENT",
                f"DTSTART;VALUE=DATE:{dt}",
                f"DTEND;VALUE=DATE:{dt}",
                f"SUMMARY:PLANT {crop.upper()} - Optimal ({d.get('survival_pct', '?')}%)",
                f"DESCRIPTION:Survival score: {d.get('survival_pct', '?')}%. Location: {place}.",
                "STATUS:CONFIRMED",
                f"UID:cropsentry-plant-{dt}-{hash(place) & 0xFFFF:04x}@cropsentry.app",
                "END:VEVENT",
            ])
        elif d.get("verdict") == "WAIT & WATCH":
            dt = d["date"].replace("-", "")
            lines.extend([
                "BEGIN:VEVENT",
                f"DTSTART;VALUE=DATE:{dt}",
                f"DTEND;VALUE=DATE:{dt}",
                f"SUMMARY:WATCH {crop.upper()} - Borderline ({d.get('survival_pct', '?')}%)",
                f"DESCRIPTION:Survival score: {d.get('survival_pct', '?')}%. Re-check conditions. Location: {place}.",
                "STATUS:TENTATIVE",
                f"UID:cropsentry-watch-{dt}-{hash(place) & 0xFFFF:04x}@cropsentry.app",
                "END:VEVENT",
            ])

    for d in spray_days:
        if d.get("verdict") == "GOOD":
            dt = d["date"].replace("-", "")
            lines.extend([
                "BEGIN:VEVENT",
                f"DTSTART;VALUE=DATE:{dt}",
                f"DTEND;VALUE=DATE:{dt}",
                f"SUMMARY:SPRAY WINDOW - {d.get('spray_hours', '?')}h available",
                f"DESCRIPTION:{d.get('spray_hours')}h spray window. Location: {place}.",
                "STATUS:CONFIRMED",
                f"UID:cropsentry-spray-{dt}-{hash(place) & 0xFFFF:04x}@cropsentry.app",
                "END:VEVENT",
            ])

    lines.append("END:VCALENDAR")
    return "\r\n".join(lines)


# ----- historical derivations -------------------------------------------

def _history_daily_series(history: dict) -> dict:
    """Pass-through of the raw daily Archive series for risk-detail timelines."""
    daily = (history or {}).get("daily", {}) or {}
    return {
        "time": list(daily.get("time", []) or []),
        "tmin_f": list(daily.get("temperature_2m_min", []) or []),
        "tmax_f": list(daily.get("temperature_2m_max", []) or []),
        "precip_in": list(daily.get("precipitation_sum", []) or []),
        "soil_f": list(daily.get("soil_temperature_7_to_28cm_mean", []) or []),
    }


def derive_antecedent(history: dict) -> dict:
    """Roll up the recent-history Archive response into a few headline numbers."""
    daily = history.get("daily", {}) if history else {}
    precip = [p for p in (daily.get("precipitation_sum") or []) if p is not None]
    tmin = [t for t in (daily.get("temperature_2m_min") or []) if t is not None]
    tmax = [t for t in (daily.get("temperature_2m_max") or []) if t is not None]
    soil = [s for s in (daily.get("soil_temperature_7_to_28cm_mean") or []) if s is not None]

    # Trend: compare last 7 days to the prior period to see direction.
    soil_trend = None
    if len(soil) >= 14:
        recent_avg = sum(soil[-7:]) / 7
        prior_avg = sum(soil[:-7]) / max(1, len(soil) - 7)
        soil_trend = round(recent_avg - prior_avg, 1)  # +ve = warming

    return {
        "days": len(precip),
        "cumulative_precip_in": round(sum(precip), 2) if precip else None,
        "wet_days": sum(1 for p in precip if p >= 0.1),
        "frost_days": sum(1 for t in tmin if t <= 32),
        "avg_tmin_f": round(sum(tmin) / len(tmin), 1) if tmin else None,
        "avg_tmax_f": round(sum(tmax) / len(tmax), 1) if tmax else None,
        "avg_soil_f": round(sum(soil) / len(soil), 1) if soil else None,
        "soil_temp_trend_f": soil_trend,
    }


def derive_climatology(yearly_archives: list[dict], daily_times: list[str]) -> dict:
    """For each upcoming forecast day, average the same calendar date across prior years."""
    if not yearly_archives or not daily_times:
        return {"days": [], "years_sampled": 0}

    # Pre-index each year's daily entries by (month, day) for fast lookup.
    indexed: list[dict[tuple[int, int], dict]] = []
    for arc in yearly_archives:
        d = arc.get("daily", {}) or {}
        times = d.get("time", []) or []
        tmin = d.get("temperature_2m_min", []) or []
        tmax = d.get("temperature_2m_max", []) or []
        precip = d.get("precipitation_sum", []) or []
        idx = {}
        for i, t in enumerate(times):
            try:
                dt = date.fromisoformat(t)
            except ValueError:
                continue
            idx[(dt.month, dt.day)] = {
                "tmin": tmin[i] if i < len(tmin) else None,
                "tmax": tmax[i] if i < len(tmax) else None,
                "precip": precip[i] if i < len(precip) else None,
            }
        indexed.append(idx)

    out_days = []
    frost_total = 0
    frost_samples = 0
    for iso in daily_times:
        try:
            target = date.fromisoformat(iso)
        except ValueError:
            continue
        key = (target.month, target.day)
        tmins, tmaxs, precips, frosts = [], [], [], 0
        for idx in indexed:
            entry = idx.get(key)
            if not entry:
                continue
            if entry["tmin"] is not None:
                tmins.append(entry["tmin"])
                if entry["tmin"] <= 32:
                    frosts += 1
            if entry["tmax"] is not None:
                tmaxs.append(entry["tmax"])
            if entry["precip"] is not None:
                precips.append(entry["precip"])
        sample = len(tmins)
        out_days.append({
            "date": iso,
            "normal_tmin_f": round(sum(tmins) / sample, 1) if tmins else None,
            "normal_tmax_f": round(sum(tmaxs) / len(tmaxs), 1) if tmaxs else None,
            "normal_precip_in": round(sum(precips) / len(precips), 2) if precips else None,
            "frost_prob": round(frosts / sample, 2) if sample else None,
            "years_sample": sample,
        })
        frost_total += frosts
        frost_samples += sample

    return {
        "days": out_days,
        "years_sampled": len(indexed),
        "frost_prob_window": round(frost_total / frost_samples, 2) if frost_samples else None,
    }


# ----- seedcorn maggot 7-day survival -----------------------------------

def _daily_gdd(tmin: Optional[float], tmax: Optional[float], base: float) -> float:
    """Simple-average growing degree day. Returns 0 when data is missing."""
    if tmin is None or tmax is None:
        return 0.0
    return max(0.0, (tmin + tmax) / 2.0 - base)


def _ingest_daily_gdd(daily_obj: dict, base: float) -> dict[date, float]:
    """Convert an Open-Meteo daily payload into ``{date: daily_gdd}``."""
    out: dict[date, float] = {}
    if not daily_obj:
        return out
    times = daily_obj.get("time") or []
    tmins = daily_obj.get("temperature_2m_min") or []
    tmaxs = daily_obj.get("temperature_2m_max") or []
    for i, t in enumerate(times):
        try:
            d = date.fromisoformat(t)
        except (ValueError, TypeError):
            continue
        tmin = tmins[i] if i < len(tmins) else None
        tmax = tmaxs[i] if i < len(tmaxs) else None
        out[d] = _daily_gdd(tmin, tmax, base)
    return out


def compute_scm_forecast(season_archive: dict, extended: dict, forecast: dict,
                         inputs: UserInputs, max_days: int = PLAN_HORIZON_DAYS) -> dict:
    """Day-by-day seedcorn maggot survival projection.

    Combines the season-to-date GDD accumulation (archive) with the next-week
    forecast to score each upcoming day on its proximity to an adult fly peak,
    soil attractiveness for egg-laying, the seed's exposure window (germination
    speed), and the field's organic load (manure / heavy residue / sod).
    """
    today = date.today()

    gdd_by_date: dict[date, float] = {}
    # Archive is the source of truth for past dates.
    archive_daily = _ingest_daily_gdd((season_archive or {}).get("daily", {}), SCM_BASE_F)
    gdd_by_date.update(archive_daily)
    # Extended (past_days + forecast) fills the archive gap and extends forward.
    extended_daily = _ingest_daily_gdd((extended or {}).get("daily", {}), SCM_BASE_F)
    for d, g in extended_daily.items():
        gdd_by_date.setdefault(d, g)

    if not gdd_by_date:
        return {"available": False, "reason": "no GDD history available"}

    # Cumulative DD from Jan 1 — fill missing days with 0 (cold-winter assumption).
    year_start = date(today.year, 1, 1)
    last_known = max(gdd_by_date.keys())
    horizon = today + timedelta(days=max_days + 2)
    end_cum = max(last_known, horizon)
    cum_by_date: dict[date, float] = {}
    cum = 0.0
    cur = year_start
    while cur <= end_cum:
        cum += gdd_by_date.get(cur, 0.0)
        cum_by_date[cur] = cum
        cur += timedelta(days=1)

    gdd_today = cum_by_date.get(today, cum_by_date[last_known])

    # Sanity check: if archive failed entirely we may only have ~10 days of
    # history. Flag low confidence rather than producing garbage numbers.
    season_days_covered = sum(1 for d in gdd_by_date if year_start <= d <= today)
    elapsed = max(1, (today - year_start).days)
    confidence = "high" if season_days_covered / elapsed >= 0.85 else (
        "moderate" if season_days_covered / elapsed >= 0.5 else "low")

    daily_times = (forecast.get("daily") or {}).get("time", [])[:max_days]

    # Egg-laying amplifier driven by user inputs the API can't see.
    organic_load = inputs.manure_recent or inputs.residue == "heavy"
    organic_mult = 2.2 if organic_load else 1.0
    if inputs.previous_grass:
        organic_mult *= 1.15
    if inputs.tillage == "conventional" and organic_load:
        organic_mult *= 1.1  # fresh-tilled decomposing matter is the worst case

    days_out = []
    for day_idx, iso in enumerate(daily_times):
        try:
            target = date.fromisoformat(iso)
        except (ValueError, TypeError):
            continue
        cum_gdd = cum_by_date.get(target, cum_by_date[last_known])

        # Distance (signed) to the nearest adult-fly peak.
        nearest_dd, nearest_idx = min(
            ((p - cum_gdd, i) for i, p in enumerate(SCM_PEAKS_GDD)),
            key=lambda x: abs(x[0]),
        )

        # Soil & moisture in the post-planting window.
        start_h = day_idx * 24 + 6
        hourly_len = len(forecast.get("hourly", {}).get("time", []))
        end_h = min(start_h + 96, hourly_len)
        soil = _hourly_window(forecast, "soil_temperature_6cm", start_h, end_h)
        moist = _hourly_window(forecast, "soil_moisture_1_to_3cm", start_h, end_h)
        avg_soil = _avg(soil) if soil else 60.0
        moist_hours = _saturated_hours(moist, 0.30) if moist else 0

        # Germination time at this soil temp (corn ~120 GDD base 50°F).
        gdd50_per_day = max(0.5, avg_soil - 50.0)
        germ_days = round(SEED_EMERGE_GDD_50 / gdd50_per_day)
        germ_days = max(4, min(germ_days, 28))

        # Adult fly activity: bell-shaped around peak, suppressed pre-G1 / post-G3.
        fly_factor = max(0.0, 1.0 - abs(nearest_dd) / SCM_PEAK_WIDTH_DD)
        if cum_gdd < SCM_PEAKS_GDD[0] - 250:
            fly_factor *= 0.2
        elif cum_gdd > SCM_PEAKS_GDD[-1] + 200:
            fly_factor *= 0.3

        # Maggot (larval) damage factor — the underground threat that lags fly peaks.
        maggot_peaks = tuple(p + SCM_MAGGOT_PEAK_OFFSET_DD for p in SCM_PEAKS_GDD)
        maggot_dd, maggot_idx = min(
            ((p - cum_gdd, i) for i, p in enumerate(maggot_peaks)),
            key=lambda x: abs(x[0]),
        )
        maggot_factor = max(0.0, 1.0 - abs(maggot_dd) / SCM_MAGGOT_WIDTH_DD)
        if cum_gdd < maggot_peaks[0] - SCM_MAGGOT_WIDTH_DD:
            maggot_factor *= 0.2
        elif cum_gdd > maggot_peaks[-1] + SCM_MAGGOT_WIDTH_DD:
            maggot_factor *= 0.3

        # Soil attractiveness for egg-laying. Optimum is cool-but-not-cold and moist.
        if 50 <= avg_soil <= 65 and moist_hours >= 24:
            attract = 1.0
        elif 50 <= avg_soil <= 70 and moist_hours >= 12:
            attract = 0.65
        elif 45 <= avg_soil < 50 and moist_hours >= 24:
            attract = 0.7
        elif avg_soil < 45 or avg_soil > 75:
            attract = 0.15
        else:
            attract = 0.35

        # Exposure window: long germination = more days at risk.
        vuln = max(0.5, min(2.5, germ_days / 8.0))

        # Risk is driven by maggot presence in soil, not fly activity above ground.
        pressure = maggot_factor * attract * organic_mult * vuln
        # Exponential decay maps pressure → survival. Calibrated so a typical
        # peak-fly + manure + cool-wet day lands near 50% survival.
        survival = round(100 * math.exp(-0.32 * pressure))
        survival = max(15, min(SURVIVAL_PCT_CAP, survival))

        if survival >= 90:
            level = "low"
        elif survival >= 70:
            level = "moderate"
        else:
            level = "high"

        if fly_factor > 0.7:
            fly_label = "Heavy fly activity"
        elif fly_factor > 0.35:
            fly_label = "Active flies"
        elif fly_factor > 0.1:
            fly_label = "Light fly activity"
        else:
            fly_label = "Quiet"

        if maggot_factor > 0.7:
            maggot_label = "Peak maggot feeding"
        elif maggot_factor > 0.35:
            maggot_label = "Active maggots in soil"
        elif maggot_factor > 0.1:
            maggot_label = "Low maggot presence"
        else:
            maggot_label = "Minimal maggot threat"

        gen_label = ("1st", "2nd", "3rd")[nearest_idx]
        maggot_gen_label = ("1st", "2nd", "3rd")[maggot_idx]
        if abs(nearest_dd) <= 60:
            fly_phase = f"At {gen_label} fly peak"
        elif nearest_dd > 0:
            fly_phase = f"~{int(round(nearest_dd))} DD before {gen_label} fly peak"
        else:
            fly_phase = f"~{int(round(abs(nearest_dd)))} DD past {gen_label} fly peak"

        if abs(maggot_dd) <= 60:
            phase = f"Peak {maggot_gen_label}-gen maggot feeding"
        elif maggot_dd > 0:
            phase = f"~{int(round(maggot_dd))} DD before {maggot_gen_label}-gen maggot peak"
        else:
            phase = f"~{int(round(abs(maggot_dd)))} DD past {maggot_gen_label}-gen maggot peak"

        days_out.append({
            "date": iso,
            "survival_pct": survival,
            "level": level,
            "phase": phase,
            "fly_phase": fly_phase,
            "fly_activity": fly_label,
            "fly_factor": round(fly_factor, 2),
            "maggot_activity": maggot_label,
            "maggot_factor": round(maggot_factor, 2),
            "cum_gdd": int(round(cum_gdd)),
            "gdd_to_peak": int(round(nearest_dd)),
            "gdd_to_maggot_peak": int(round(maggot_dd)),
            "peak_label": gen_label,
            "germination_days": germ_days,
            "avg_soil_f": round(avg_soil, 1),
            "moist_hours_96h": moist_hours,
            "attractiveness": round(attract, 2),
            "is_climate": False,
        })

    maggot_peaks_today = tuple(p + SCM_MAGGOT_PEAK_OFFSET_DD for p in SCM_PEAKS_GDD)
    if gdd_today < SCM_PEAKS_GDD[0]:
        fly_gen_label = f"Approaching 1st-generation fly peak ({int(SCM_PEAKS_GDD[0])} DD)"
        cur_gen = 0
    elif gdd_today < SCM_PEAKS_GDD[1]:
        fly_gen_label = "Between 1st and 2nd generation fly peaks"
        cur_gen = 1
    elif gdd_today < SCM_PEAKS_GDD[2]:
        fly_gen_label = "Between 2nd and 3rd generation fly peaks"
        cur_gen = 2
    else:
        fly_gen_label = "Past 3rd-generation fly peak — late-season activity tapers"
        cur_gen = 3

    if gdd_today < maggot_peaks_today[0] - SCM_MAGGOT_WIDTH_DD:
        gen_label_today = f"Pre-season — 1st-gen maggots not yet active (peak ~{int(maggot_peaks_today[0])} DD)"
    elif gdd_today < maggot_peaks_today[0] + SCM_MAGGOT_WIDTH_DD:
        gen_label_today = "1st-generation maggots active in soil"
    elif gdd_today < maggot_peaks_today[1] - SCM_MAGGOT_WIDTH_DD:
        gen_label_today = "Between 1st and 2nd generation maggot waves"
    elif gdd_today < maggot_peaks_today[1] + SCM_MAGGOT_WIDTH_DD:
        gen_label_today = "2nd-generation maggots active in soil"
    elif gdd_today < maggot_peaks_today[2] - SCM_MAGGOT_WIDTH_DD:
        gen_label_today = "Between 2nd and 3rd generation maggot waves"
    elif gdd_today < maggot_peaks_today[2] + SCM_MAGGOT_WIDTH_DD:
        gen_label_today = "3rd-generation maggots active in soil"
    else:
        gen_label_today = "Past 3rd-generation maggot wave — threat tapering"

    next_peak = next((p for p in SCM_PEAKS_GDD if p > gdd_today), None)
    next_maggot_peak = next((p for p in maggot_peaks_today if p > gdd_today), None)
    best_day = max(days_out, key=lambda d: d["survival_pct"])["date"] if days_out else None
    worst_day = min(days_out, key=lambda d: d["survival_pct"])["date"] if days_out else None

    # Recent average daily GDD — used to project the cumulative curve forward
    # past the forecast horizon when the extended outlook is requested.
    recent_window = sorted(gdd_by_date.keys())[-14:]
    recent_gdds = [gdd_by_date[d] for d in recent_window if d in gdd_by_date]
    recent_gdd_per_day = (sum(recent_gdds) / len(recent_gdds)) if recent_gdds else 0.0

    return {
        "available": True,
        "days": days_out,
        "season": {
            "gdd_today": int(round(gdd_today)),
            "gdd_base_f": SCM_BASE_F,
            "generation_label": gen_label_today,
            "fly_generation_label": fly_gen_label,
            "current_generation_idx": cur_gen,
            "peaks_gdd": list(SCM_PEAKS_GDD),
            "maggot_peaks_gdd": list(maggot_peaks_today),
            "next_peak_gdd": int(next_peak) if next_peak else None,
            "dd_to_next_peak": int(next_peak - gdd_today) if next_peak else None,
            "next_maggot_peak_gdd": int(next_maggot_peak) if next_maggot_peak else None,
            "dd_to_next_maggot_peak": int(next_maggot_peak - gdd_today) if next_maggot_peak else None,
            "confidence": confidence,
        },
        "best_day": best_day,
        "worst_day": worst_day,
        "organic_load": organic_load,
        # Internal projection state — consumed by evaluate() to build the 31-day
        # extended outlook, then stripped before the response is returned.
        "_state": {
            "cum_by_date": {d.isoformat(): v for d, v in cum_by_date.items()},
            "organic_mult": organic_mult,
            "recent_gdd_per_day": round(recent_gdd_per_day, 2),
        },
    }


# ----- extended (15–31 day) outlook from prior-year normals --------------
# The forecast endpoint caps at 16 days. To give the user a 31-day option we
# project the trailing 17 days from same-calendar-date averages of the last few
# years, flagging each entry with `is_climate=true` so the UI can dim it and
# show the "less accurate after 14 days" disclaimer.

def _scm_day_from_state(iso: str, cum_gdd: float, avg_soil: float,
                        organic_mult: float) -> dict:
    """Score one SCM day given a precomputed soil temp + cumulative GDD."""
    nearest_dd, nearest_idx = min(
        ((p - cum_gdd, i) for i, p in enumerate(SCM_PEAKS_GDD)),
        key=lambda x: abs(x[0]),
    )

    fly_factor = max(0.0, 1.0 - abs(nearest_dd) / SCM_PEAK_WIDTH_DD)
    if cum_gdd < SCM_PEAKS_GDD[0] - 250:
        fly_factor *= 0.2
    elif cum_gdd > SCM_PEAKS_GDD[-1] + 200:
        fly_factor *= 0.3

    maggot_peaks = tuple(p + SCM_MAGGOT_PEAK_OFFSET_DD for p in SCM_PEAKS_GDD)
    maggot_dd, maggot_idx = min(
        ((p - cum_gdd, i) for i, p in enumerate(maggot_peaks)),
        key=lambda x: abs(x[0]),
    )
    maggot_factor = max(0.0, 1.0 - abs(maggot_dd) / SCM_MAGGOT_WIDTH_DD)
    if cum_gdd < maggot_peaks[0] - SCM_MAGGOT_WIDTH_DD:
        maggot_factor *= 0.2
    elif cum_gdd > maggot_peaks[-1] + SCM_MAGGOT_WIDTH_DD:
        maggot_factor *= 0.3

    # Climate days have no hourly moisture — assume "moderately moist" for
    # spring (most prior-year averages will land here).
    if 50 <= avg_soil <= 65:
        attract = 0.7
    elif 50 <= avg_soil <= 70:
        attract = 0.5
    elif 45 <= avg_soil < 50:
        attract = 0.55
    elif avg_soil < 45 or avg_soil > 75:
        attract = 0.15
    else:
        attract = 0.3

    gdd50_per_day = max(0.5, avg_soil - 50.0)
    germ_days = round(SEED_EMERGE_GDD_50 / gdd50_per_day)
    germ_days = max(4, min(germ_days, 28))
    vuln = max(0.5, min(2.5, germ_days / 8.0))

    pressure = maggot_factor * attract * organic_mult * vuln
    survival = round(100 * math.exp(-0.32 * pressure))
    survival = max(15, min(SURVIVAL_PCT_CAP, survival))

    if survival >= 90:
        level = "low"
    elif survival >= 70:
        level = "moderate"
    else:
        level = "high"

    if fly_factor > 0.7:
        fly_label = "Heavy fly activity"
    elif fly_factor > 0.35:
        fly_label = "Active flies"
    elif fly_factor > 0.1:
        fly_label = "Light fly activity"
    else:
        fly_label = "Quiet"

    if maggot_factor > 0.7:
        maggot_label = "Peak maggot feeding"
    elif maggot_factor > 0.35:
        maggot_label = "Active maggots in soil"
    elif maggot_factor > 0.1:
        maggot_label = "Low maggot presence"
    else:
        maggot_label = "Minimal maggot threat"

    gen_label = ("1st", "2nd", "3rd")[nearest_idx]
    maggot_gen_label = ("1st", "2nd", "3rd")[maggot_idx]
    if abs(nearest_dd) <= 60:
        fly_phase = f"At {gen_label} fly peak"
    elif nearest_dd > 0:
        fly_phase = f"~{int(round(nearest_dd))} DD before {gen_label} fly peak"
    else:
        fly_phase = f"~{int(round(abs(nearest_dd)))} DD past {gen_label} fly peak"

    if abs(maggot_dd) <= 60:
        phase = f"Peak {maggot_gen_label}-gen maggot feeding"
    elif maggot_dd > 0:
        phase = f"~{int(round(maggot_dd))} DD before {maggot_gen_label}-gen maggot peak"
    else:
        phase = f"~{int(round(abs(maggot_dd)))} DD past {maggot_gen_label}-gen maggot peak"

    return {
        "date": iso,
        "survival_pct": survival,
        "level": level,
        "phase": phase,
        "fly_phase": fly_phase,
        "fly_activity": fly_label,
        "fly_factor": round(fly_factor, 2),
        "maggot_activity": maggot_label,
        "maggot_factor": round(maggot_factor, 2),
        "cum_gdd": int(round(cum_gdd)),
        "gdd_to_peak": int(round(nearest_dd)),
        "gdd_to_maggot_peak": int(round(maggot_dd)),
        "peak_label": gen_label,
        "germination_days": germ_days,
        "avg_soil_f": round(avg_soil, 1),
        "moist_hours_96h": None,
        "attractiveness": round(attract, 2),
        "is_climate": True,
    }


def _climate_plant_day(iso: str, climate_day: dict, profile: dict,
                       cultivar_factor: float = 1.0) -> dict:
    """Approximate a plant_day from prior-year normals.

    Only soil-temp proxy and frost are evaluated — the rest of the risk model
    needs hourly soil moisture / soil temp that we don't have past day 16.
    Uses per-factor survival: each factor gets its own biologically-appropriate
    survival probability. Both chilling and frost multiply independently (they
    are separate biological response factors, not grouped).
    """
    soil_floor = profile["min_soil_temp_f"]
    frost_floor = profile["frost_air_temp_f"]
    norm_tmin = climate_day.get("normal_tmin_f")
    norm_tmax = climate_day.get("normal_tmax_f")
    norm_precip = climate_day.get("normal_precip_in")
    frost_prob = climate_day.get("frost_prob")

    soil_proxy = None
    if norm_tmin is not None and norm_tmax is not None:
        soil_proxy = (norm_tmin + norm_tmax) / 2 - 2

    risks: list[dict] = []
    total_survival = 1.0

    if soil_proxy is not None:
        chill_sev = _sigmoid_severity(soil_proxy, midpoint=soil_floor - 2, scale=4.0, inverted=True)
        chill_sf = _biological_response_survival(chill_sev)
        total_survival *= chill_sf
        if chill_sev >= 0.67:
            risks.append({"key": "chilling", "name": "Imbibitional Chilling", "level": "high"})
        elif chill_sev >= 0.33:
            risks.append({"key": "chilling", "name": "Imbibitional Chilling", "level": "moderate"})

    if norm_tmin is not None:
        frost_sev = _sigmoid_severity(norm_tmin, midpoint=frost_floor, scale=3.0, inverted=True)
        if frost_prob is not None and frost_prob >= 0.25:
            prob_sev = _sigmoid_severity(frost_prob, midpoint=0.4, scale=0.15)
            frost_sev = max(frost_sev, prob_sev)
        frost_sf = _biological_response_survival(frost_sev)
        total_survival *= frost_sf
        if frost_sev >= 0.67:
            risks.append({"key": "frost", "name": "Frost & Freeze", "level": "high"})
        elif frost_sev >= 0.33:
            risks.append({"key": "frost", "name": "Frost & Freeze", "level": "moderate"})

    total_survival *= cultivar_factor
    survival = min(SURVIVAL_PCT_CAP, max(0, round(total_survival * 100)))
    worst = max((LEVEL_RANK[r["level"]] for r in risks), default=0)
    score = survival
    verdict = ("DO NOT PLANT" if survival < 65
               else "WAIT & WATCH" if survival < 90
               else "OPTIMAL")

    return {
        "date": iso,
        "score": score,
        # Climate-projected survival uses only the chilling/frost subset of
        # evaluators (the only risks the prior-year normals can speak to).
        "survival_pct": survival,
        "verdict": verdict,
        "level": ["low", "moderate", "high"][worst],
        "is_climate": True,
        "conditions": {
            "min_soil_temp_f": round(soil_proxy, 1) if soil_proxy is not None else None,
            "avg_soil_temp_f": round(soil_proxy, 1) if soil_proxy is not None else None,
            "precip_48h_in": round((norm_precip or 0) * 2, 2),
            "min_air_temp_f": round(norm_tmin, 1) if norm_tmin is not None else None,
            "sat_hours_96h": None,
        },
        "top_risks": risks[:3],
    }


def build_extended_outlook(climatology: dict, profile: dict,
                           scm_state: dict | None,
                           start_date: date, days: int,
                           cultivar_factor: float = 1.0) -> dict:
    """Build climate-projected entries for the requested calendar days.

    Returns daily forecast rows, plant-day rows, and SCM rows whose dates fall
    in [start_date, start_date + days). Each row carries `is_climate=true`.
    """
    by_date: dict[str, dict] = {d["date"]: d for d in (climatology or {}).get("days", [])}

    daily_rows: list[dict] = []
    plant_rows: list[dict] = []
    scm_rows: list[dict] = []

    if not by_date or days <= 0:
        return {"daily": daily_rows, "plant_days": plant_rows, "scm_days": scm_rows}

    cum_lookup: dict[str, float] = {}
    organic_mult = 1.0
    last_cum = None
    per_day_gdd = 0.0
    if scm_state:
        cum_lookup = dict(scm_state.get("cum_by_date") or {})
        organic_mult = float(scm_state.get("organic_mult") or 1.0)
        per_day_gdd = float(scm_state.get("recent_gdd_per_day") or 0.0)
        if cum_lookup:
            last_known_iso = max(cum_lookup.keys())
            last_cum = cum_lookup[last_known_iso]

    for offset in range(days):
        cur = start_date + timedelta(days=offset)
        iso = cur.isoformat()
        climate_day = by_date.get(iso)

        plant_row = _climate_plant_day(iso, climate_day or {}, profile, cultivar_factor)
        plant_rows.append(plant_row)
        survival_pct = plant_row["survival_pct"]

        # Daily strip: prior-year normals if we have them.
        if climate_day and (climate_day.get("normal_tmax_f") is not None
                            or climate_day.get("normal_tmin_f") is not None):
            daily_rows.append({
                "date": iso,
                "tmax_f": climate_day.get("normal_tmax_f"),
                "tmin_f": climate_day.get("normal_tmin_f"),
                "precip_in": climate_day.get("normal_precip_in") or 0.0,
                "uv_max": None,
                "survival_pct": survival_pct,
                "normal_tmin_f": climate_day.get("normal_tmin_f"),
                "normal_tmax_f": climate_day.get("normal_tmax_f"),
                "is_climate": True,
            })
        else:
            # No climate sample either — emit a placeholder so the row count
            # stays in lockstep with plant_days / scm_days.
            daily_rows.append({
                "date": iso,
                "tmax_f": None, "tmin_f": None,
                "precip_in": 0.0, "uv_max": None,
                "survival_pct": survival_pct,
                "normal_tmin_f": None, "normal_tmax_f": None,
                "is_climate": True,
            })

        # SCM extension: project cumulative GDD past the forecast horizon by
        # adding the recent daily GDD average per day until we reach this date.
        if scm_state and last_cum is not None:
            cum = cum_lookup.get(iso)
            if cum is None:
                # offset+1 because we step from the last known day.
                cum = last_cum + per_day_gdd * (offset + 1)
                cum_lookup[iso] = cum
            avg_air = None
            if climate_day:
                tmin = climate_day.get("normal_tmin_f")
                tmax = climate_day.get("normal_tmax_f")
                if tmin is not None and tmax is not None:
                    avg_air = (tmin + tmax) / 2
            avg_soil = (avg_air - 2) if avg_air is not None else 60.0
            scm_rows.append(_scm_day_from_state(iso, cum, avg_soil, organic_mult))

    return {"daily": daily_rows, "plant_days": plant_rows, "scm_days": scm_rows}


# ----- cross-source agreement -------------------------------------------

def compare_forecasts(open_meteo_daily: dict, nws_summary: dict,
                      lookahead_days: int = 5) -> dict:
    """Cross-check Open-Meteo daily highs/lows against NWS for the next ``lookahead_days``.

    Big disagreement (>5°F on either bound) is a yellow flag the user should
    see — it means at least one model is uncertain about the planting window.
    Returns ``{available, agreement, max_dev_f, days:[...]}``. ``agreement`` is
    one of "strong" (≤2°F mean dev), "fair" (≤5°F), or "weak" (>5°F).
    """
    nws_daily = (nws_summary or {}).get("daily") or []
    if not nws_daily or not open_meteo_daily:
        return {"available": False}
    nws_by_date = {d["date"]: d for d in nws_daily}
    om_times = open_meteo_daily.get("time") or []
    om_tmin = open_meteo_daily.get("temperature_2m_min") or []
    om_tmax = open_meteo_daily.get("temperature_2m_max") or []

    om_precip = open_meteo_daily.get("precipitation_sum") or []

    rows = []
    devs: list[float] = []
    for i, iso in enumerate(om_times[:lookahead_days]):
        nws_row = nws_by_date.get(iso)
        if not nws_row:
            continue
        om_lo = om_tmin[i] if i < len(om_tmin) else None
        om_hi = om_tmax[i] if i < len(om_tmax) else None
        nws_lo = nws_row.get("tmin_f")
        nws_hi = nws_row.get("tmax_f")
        d_lo = (om_lo - nws_lo) if (om_lo is not None and nws_lo is not None) else None
        d_hi = (om_hi - nws_hi) if (om_hi is not None and nws_hi is not None) else None
        # NWS publishes probability-of-precipitation (0-100). Open-Meteo gives a
        # forecast quantity in inches. There's no apples-to-apples comparison,
        # but a "NWS thinks rain likely / Open-Meteo total reads dry" mismatch
        # is itself a useful uncertainty flag.
        nws_pop = nws_row.get("precip_pop")
        om_p = om_precip[i] if i < len(om_precip) else None
        precip_disagree = (
            isinstance(nws_pop, (int, float)) and nws_pop >= 50
            and isinstance(om_p, (int, float)) and om_p < 0.05
        ) or (
            isinstance(nws_pop, (int, float)) and nws_pop <= 20
            and isinstance(om_p, (int, float)) and om_p > 0.4
        )
        rows.append({
            "date": iso,
            "open_meteo_tmin_f": round(om_lo, 1) if om_lo is not None else None,
            "open_meteo_tmax_f": round(om_hi, 1) if om_hi is not None else None,
            "open_meteo_precip_in": round(om_p, 2) if isinstance(om_p, (int, float)) else None,
            "nws_tmin_f": round(nws_lo, 1) if nws_lo is not None else None,
            "nws_tmax_f": round(nws_hi, 1) if nws_hi is not None else None,
            "nws_precip_pop": int(nws_pop) if isinstance(nws_pop, (int, float)) else None,
            "precip_disagreement": precip_disagree,
            "tmin_dev_f": round(d_lo, 1) if d_lo is not None else None,
            "tmax_dev_f": round(d_hi, 1) if d_hi is not None else None,
        })
        for d in (d_lo, d_hi):
            if d is not None:
                devs.append(abs(d))
    if not devs:
        return {"available": False}
    mean_dev = sum(devs) / len(devs)
    max_dev = max(devs)
    if mean_dev <= 2.0:
        agreement = "strong"
    elif mean_dev <= 5.0:
        agreement = "fair"
    else:
        agreement = "weak"
    precip_disagreements = sum(1 for row in rows if row.get("precip_disagreement"))
    return {
        "available": True,
        "agreement": agreement,
        "mean_dev_f": round(mean_dev, 1),
        "max_dev_f": round(max_dev, 1),
        "precip_disagreement_days": precip_disagreements,
        "days": rows,
        "office": (nws_summary or {}).get("office"),
    }


def _build_data_sources_block(soil_profile: dict, nws: dict, bcw: dict,
                              cross_check: dict, alerts: dict,
                              drought: dict,
                              ensemble: dict | None = None,
                              power_recent: dict | None = None,
                              topography: dict | None = None,
                              three_source: dict | None = None,
                              confidence: dict | None = None,
                              scan: dict | None = None,
                              iem: dict | None = None,
                              nass: dict | None = None,
                              usgs: dict | None = None,
                              cpc: dict | None = None,
                              enviro: dict | None = None,
                              rotation: dict | None = None) -> dict:
    """Top-level summary the UI uses to show which data layers powered the run."""
    ensemble = ensemble or {}
    power_recent = power_recent or {}
    topography = topography or {}
    three_source = three_source or {}
    confidence = confidence or {}
    scan = scan or {}
    iem = iem or {}
    nass = nass or {}
    usgs = usgs or {}
    cpc = cpc or {}
    enviro = enviro or {}
    rotation = rotation or {}
    return {
        "primary_forecast": {
            "name": "Open-Meteo",
            "url": "https://open-meteo.com/",
            "covers": "Hourly air & soil temperature, soil moisture, precipitation, UV.",
        },
        "ensemble_forecast": {
            "name": "Open-Meteo Ensemble (GFS / ICON / ECMWF / GEM)",
            "url": "https://open-meteo.com/en/docs/ensemble-api",
            "available": bool(ensemble.get("available")),
            "members": ensemble.get("members"),
            "models": ensemble.get("models"),
            "daily": ensemble.get("daily"),
            "informs": ("Multi-model spread quantifies forecast uncertainty. Frost evaluator now "
                        "integrates Pr[freeze] across the emergence window; survival probability "
                        "carries a confidence interval whose width tracks ensemble dispersion."),
        },
        "recent_actuals_cross_check": {
            "name": "NASA POWER (MERRA-2 / GEOS-FP)",
            "url": "https://power.larc.nasa.gov/",
            "available": bool(power_recent.get("available")),
            "daily": power_recent.get("daily"),
            "agreement": three_source.get("agreement") if three_source.get("available") else None,
            "mean_dev_tmin_f": three_source.get("mean_dev_tmin_f") if three_source.get("available") else None,
            "mean_dev_tmax_f": three_source.get("mean_dev_tmax_f") if three_source.get("available") else None,
            "mean_dev_precip_in": three_source.get("mean_dev_precip_in") if three_source.get("available") else None,
            "days": three_source.get("days") if three_source.get("available") else None,
            "informs": ("Independent reanalysis-derived actuals for the last 7 days, used to "
                        "cross-check the Archive history that drives antecedent saturation, "
                        "BLB winter-survival, and SCM GDD accumulation."),
        },
        "topography": {
            "name": "Open-Meteo Elevation API (3×3 sample)",
            "url": "https://open-meteo.com/en/docs/elevation-api",
            "available": bool(topography.get("available")),
            "data": topography or None,
            "informs": ("Local-depression / ponding risk. Modeled grid soil moisture cannot see "
                        "the bowl-shaped microsite that collects runoff from neighbouring acres "
                        "— this layer flags it explicitly."),
        },
        "forecast_confidence": {
            "name": "Forecast Confidence (aggregate)",
            "available": bool(confidence),
            "label": confidence.get("label"),
            "scalar": confidence.get("scalar"),
            "agreement_forward": confidence.get("agreement_forward"),
            "agreement_recent": confidence.get("agreement_recent"),
            "ensemble_spread_f": confidence.get("ensemble_spread_f"),
            "ensemble_precip_spread_in": confidence.get("ensemble_precip_spread_in"),
            "weak_signals": confidence.get("weak_signals"),
            "drivers": confidence.get("drivers"),
            "informs": ("Aggregates Open-Meteo↔NWS forward agreement, Open-Meteo Archive↔NASA "
                        "POWER recent-history agreement, and ensemble dispersion into a single "
                        "confidence label. Confidence widens or tightens the survival interval."),
        },
        "soil_profile": {
            "name": "USDA SSURGO (Soil Data Access)",
            "url": "https://sdmdataaccess.sc.egov.usda.gov/",
            "available": bool(soil_profile),
            "data": soil_profile or None,
            "informs": "Phytophthora drainage, Pythium texture, antecedent buffer, crusting.",
        },
        "forecast_cross_check": {
            "name": "NWS api.weather.gov",
            "url": "https://www.weather.gov/documentation/services-web-api",
            "available": bool(nws),
            "office": (nws or {}).get("office"),
            "agreement": cross_check.get("agreement") if cross_check.get("available") else None,
            "mean_dev_f": cross_check.get("mean_dev_f") if cross_check.get("available") else None,
            "max_dev_f": cross_check.get("max_dev_f") if cross_check.get("available") else None,
            "precip_disagreement_days": (cross_check.get("precip_disagreement_days")
                                         if cross_check.get("available") else None),
            "days": cross_check.get("days") if cross_check.get("available") else None,
            "informs": ("Independent NWS forecast cross-checked against Open-Meteo lows/highs "
                        "and probability-of-precipitation."),
        },
        "weather_alerts": {
            "name": "NWS Active Alerts",
            "url": "https://www.weather.gov/documentation/services-web-api",
            # The fetcher returns {} on outright failure but {"count": 0, ...}
            # when the call succeeded with no active alerts. Treat both as
            # "responded" so the UI shows a green "no alerts" tile rather than
            # an "unavailable" placeholder.
            "available": isinstance(alerts, dict) and "count" in alerts,
            "count": (alerts or {}).get("count"),
            "any_flood": (alerts or {}).get("any_flood"),
            "any_freeze": (alerts or {}).get("any_freeze"),
            "alerts": (alerts or {}).get("alerts"),
            "informs": ("Active flood/freeze watches and warnings escalate the flooding and "
                        "frost evaluators above the modeled forecast."),
        },
        "drought": {
            "name": "U.S. Drought Monitor",
            "url": (drought or {}).get("source_url") or "https://droughtmonitor.unl.edu/",
            "available": bool(drought),
            "data": drought or None,
            "informs": ("D2+ drought down-regulates Pythium pressure (no prolonged saturation "
                        "tail to support zoospore activity)."),
        },
        "bcw_biofix": {
            "name": "ISU Moth Trapping Network",
            "url": (bcw or {}).get("source_url") or "https://crops.extension.iastate.edu/",
            "available": bool(bcw),
            "earliest_flight_iso": (bcw or {}).get("earliest_iso"),
            "earliest_flight_doy": (bcw or {}).get("earliest_doy"),
            "counties_reported": len((bcw or {}).get("counties") or {}),
            "informs": "Replaces the default mid-April biofix in the black-cutworm GDD model.",
        },
        "scan_soil_temps": {
            "name": "USDA NRCS SCAN Network",
            "url": "https://www.wcc.nrcs.usda.gov/scan/",
            "available": bool(scan.get("available")),
            "station": scan.get("station"),
            "distance_km": scan.get("distance_km"),
            "latest_temps_f": scan.get("latest_temps_f"),
            "informs": ("Ground-truth measured soil temperatures at 2/4/8/20\" depth "
                        "calibrate the modeled soil temps used for imbibitional chilling, "
                        "Pythium, and Phytophthora thresholds."),
        },
        "iem_soil_data": {
            "name": "Iowa Environmental Mesonet (ISU)",
            "url": "https://mesonet.agron.iastate.edu/",
            "available": bool(iem.get("available")),
            "station": iem.get("station"),
            "distance_km": iem.get("distance_km"),
            "latest_soil_temp_4in_f": iem.get("latest_soil_temp_4in_f"),
            "informs": ("Actual 4-inch soil temperatures from ISU AgClimate network stations, "
                        "plus high-density precipitation observations for flooding/crusting."),
        },
        "crop_progress": {
            "name": "USDA NASS Crop Progress",
            "url": "https://quickstats.nass.usda.gov/",
            "available": bool(nass.get("available")),
            "state": nass.get("state"),
            "pct_planted": nass.get("pct_planted"),
            "avg_pct_planted": nass.get("avg_pct_planted"),
            "ahead_behind": nass.get("ahead_behind"),
            "informs": ("Weekly state-level planting progress vs 5-year average. Benchmarks "
                        "whether the user is ahead or behind their region's typical pace."),
        },
        "streamflow": {
            "name": "USGS Water Services",
            "url": "https://waterservices.usgs.gov/",
            "available": bool(usgs.get("available")),
            "site_name": usgs.get("site_name"),
            "distance_km": usgs.get("distance_km"),
            "gage_height_ft": usgs.get("gage_height_ft"),
            "discharge_cfs": usgs.get("discharge_cfs"),
            "flood_risk": usgs.get("flood_risk"),
            "informs": ("Real-time gage height and streamflow from the nearest USGS gage. "
                        "Rising stage is a stronger flooding signal than precipitation alone, "
                        "especially for bottomland fields."),
        },
        "cpc_soil_moisture": {
            "name": "NOAA CPC Soil Moisture",
            "url": "https://www.cpc.ncep.noaa.gov/products/Soilmst_Monitoring/",
            "available": bool(cpc.get("available")),
            "percentile": cpc.get("soil_moisture_pctl"),
            "category": cpc.get("category"),
            "informs": ("Daily gridded soil moisture percentile — finer-grained than the "
                        "weekly USDM drought categories for Pythium/Phytophthora saturation."),
        },
        "enviroweather": {
            "name": "MSU Enviroweather",
            "url": "https://enviroweather.msu.edu/",
            "available": bool(enviro.get("available")),
            "station": enviro.get("station"),
            "distance_km": enviro.get("distance_km"),
            "gdd": enviro.get("gdd"),
            "informs": ("Michigan-specific GDD from local weather stations. Refines "
                        "seedcorn maggot larval feeding windows and black cutworm emergence timing."),
        },
        "crop_rotation": {
            "name": "NASS CropScape (Cropland Data Layer)",
            "url": "https://nassgeodata.gmu.edu/CropScape/",
            "available": bool(rotation.get("available")),
            "years": rotation.get("years"),
            "corn_on_corn": rotation.get("corn_on_corn"),
            "soy_on_soy": rotation.get("soy_on_soy"),
            "prev_crop": rotation.get("prev_crop_name"),
            "informs": ("Crop rotation history at this field. Corn-on-corn escalates "
                        "wireworm and seedcorn maggot risk; soy-on-soy escalates "
                        "Phytophthora. Diverse rotation de-escalates pest pressure."),
        },
    }


# ----- top-level orchestration ------------------------------------------

def evaluate(lat: float, lon: float, place: str,
             crop: str = "corn",
             inputs: UserInputs | None = None,
             seed_brand: str | None = None,
             seed_cultivar: str | None = None) -> dict:
    base_profile = CROP_PROFILES.get(crop, CROP_PROFILES["corn"])
    cultivar = find_cultivar(crop, seed_brand, seed_cultivar)
    profile, seed_tailoring = apply_cultivar_to_profile(base_profile, cultivar)
    inputs = inputs or UserInputs()

    # Pull forecast and history in parallel — historical calls are slower and
    # the forecast doesn't depend on them. The three supplementary public
    # sources (SSURGO soil profile, NWS forecast, ISU BCW biofix) are also
    # fetched concurrently — each is independent of the others and degrades
    # gracefully on failure.
    today = date.today()
    # Determine state from place string for NASS queries.
    _state = "MICHIGAN"
    _STATE_ABBREVS = {
        "Michigan": "MI", "Ohio": "OH", "Indiana": "IN",
        "Illinois": "IL", "Iowa": "IA", "Wisconsin": "WI", "Minnesota": "MN",
    }
    if place:
        place_upper = place.upper()
        for st_name, st_abbr in _STATE_ABBREVS.items():
            if st_name.upper() in place_upper or f", {st_abbr}" in place.upper():
                _state = st_name.upper()
                break
    _NASS_CROP_MAP = {
        "corn": "CORN", "soybeans": "SOYBEANS",
        "winter_wheat": "WINTER WHEAT", "spring_wheat": "SPRING WHEAT",
        "dry_beans": "DRY BEANS", "sugar_beets": "SUGARBEETS",
        "alfalfa": "HAY",
    }
    _nass_crop = _NASS_CROP_MAP.get(crop, "CORN")

    with ThreadPoolExecutor(max_workers=19) as pool:
        f_forecast = pool.submit(fetch_forecast, lat, lon)
        f_history = pool.submit(fetch_recent_history, lat, lon, 30)
        f_clim = pool.submit(fetch_climatology, lat, lon, today, 5, 33)
        f_scm = pool.submit(fetch_scm_inputs, lat, lon)
        f_soil = pool.submit(fetch_ssurgo_soil, lat, lon)
        f_nws = pool.submit(fetch_nws_forecast_summary, lat, lon)
        f_bcw = pool.submit(fetch_isu_bcw_flight, today.year)
        f_alerts = pool.submit(fetch_nws_alerts, lat, lon)
        f_drought = pool.submit(fetch_usdm_drought, lat, lon)
        f_ensemble = pool.submit(fetch_openmeteo_ensemble, lat, lon)
        f_power = pool.submit(fetch_nasa_power_recent, lat, lon, 7)
        f_topo = pool.submit(fetch_elevation_grid, lat, lon)
        f_scan = pool.submit(fetch_scan_soil_temps, lat, lon)
        f_iem = pool.submit(fetch_iem_soil_data, lat, lon)
        f_nass = pool.submit(fetch_nass_crop_progress, _state, _nass_crop)
        f_usgs = pool.submit(fetch_usgs_streamflow, lat, lon)
        f_cpc = pool.submit(fetch_cpc_soil_moisture, lat, lon)
        f_enviro = pool.submit(fetch_msu_enviroweather, lat, lon)
        f_rotation = pool.submit(fetch_cropscape_history, lat, lon, 3)
        forecast = f_forecast.result()
        history = f_history.result()
        yearly = f_clim.result()
        scm_archive, scm_extended = f_scm.result()
        soil_profile = f_soil.result()
        nws_summary = f_nws.result()
        bcw_flight = f_bcw.result()
        alerts = f_alerts.result()
        drought = f_drought.result()
        ensemble = f_ensemble.result()
        power_recent = f_power.result()
        topography = f_topo.result()
        scan_temps = f_scan.result()
        iem_data = f_iem.result()
        nass_progress = f_nass.result()
        usgs_flow = f_usgs.result()
        cpc_moisture = f_cpc.result()
        enviroweather = f_enviro.result()
        rotation = f_rotation.result()

    # Stash recent history + base-50 GDD lookup + planting-date base on the
    # forecast dict so evaluators (antecedent saturation, black cutworm,
    # bean leaf beetle) can reach them without changing the evaluator signature.
    forecast["_history"] = history
    forecast["_today_planting_date"] = today
    forecast["_gdd_base50_cum"] = build_base50_gdd_lookup(scm_archive, scm_extended)
    forecast["_soil_profile"] = soil_profile
    forecast["_nws"] = nws_summary
    forecast["_bcw_flight"] = bcw_flight
    forecast["_alerts"] = alerts
    forecast["_drought"] = drought
    forecast["_ensemble"] = ensemble
    forecast["_power_recent"] = power_recent
    forecast["_topography"] = topography
    forecast["_scan"] = scan_temps
    forecast["_iem"] = iem_data
    forecast["_nass_progress"] = nass_progress
    forecast["_usgs"] = usgs_flow
    forecast["_cpc_moisture"] = cpc_moisture
    forecast["_enviroweather"] = enviroweather
    forecast["_rotation"] = rotation

    # Climatology lookup keyed by ISO date — pre-built here so the new
    # _frost evaluator's integrated-probability branch can resolve frost_prob
    # for arbitrary planting dates without re-deriving it per evaluator call.
    early_climatology_times = [
        (today + timedelta(days=i)).isoformat()
        for i in range(EXTENDED_HORIZON_DAYS)
    ]
    early_climatology = derive_climatology(yearly, early_climatology_times)
    forecast["_climatology_by_date"] = {d["date"]: d for d in early_climatology["days"]}

    cv_factor = _cultivar_survival_factor(cultivar)
    risks = [evaluator(forecast, profile, inputs, 0) for evaluator in RISK_EVALUATORS]
    headline_survival = _survival_pct(risks, cultivar)

    # Hard override: sustained air temperature below the crop's frost kill
    # threshold in the 48h emergence window means 0% survival. Requires 3+
    # consecutive hours below the threshold — a single radiational dip at 4 AM
    # is not a killing event (UMN Extension; Purdue/Nielsen).
    hourly_temps = forecast.get("hourly", {}).get("temperature_2m", [])
    _frost_kill_f = profile.get("frost_air_temp_f", 28)
    _consecutive_frost = 0
    has_killing_freeze = False
    for t in hourly_temps[:48]:
        if t is not None and t < _frost_kill_f:
            _consecutive_frost += 1
            if _consecutive_frost >= 3:
                has_killing_freeze = True
                break
        else:
            _consecutive_frost = 0
    if has_killing_freeze:
        headline_survival = 0

    # Hard override: lethal heat — air temp above crop lethal threshold.
    lethal_f = profile.get("heat_lethal_f", 113)
    has_lethal_heat = any(t >= lethal_f for t in hourly_temps if t is not None)
    if has_lethal_heat:
        headline_survival = 0

    # Hard override: extreme water scarcity — near-zero precipitation in both
    # the forecast and recent history means rainfed agriculture is impossible.
    _daily_precip = forecast.get("daily", {}).get("precipitation_sum") or []
    _fcst_precip_14d = sum(p for p in _daily_precip[:14] if p is not None)
    _hist = forecast.get("_history") or {}
    _hist_precip = [p for p in ((_hist.get("daily", {}).get("precipitation_sum") or [])) if p is not None]
    _recent_30d = sum(_hist_precip)
    _drought_class = (forecast.get("_drought") or {}).get("class")
    has_extreme_aridity = (
        _fcst_precip_14d < 0.1 and _recent_30d < 0.5
    ) or (
        _fcst_precip_14d < 0.25 and _recent_30d < 1.0
        and isinstance(_drought_class, int) and _drought_class >= 3
    )
    if has_extreme_aridity:
        headline_survival = 0

    # Hard override: water body — CropScape classifies this point as open water,
    # or both CropScape and SSURGO return no data (typical of lakes / oceans
    # inside the US bounding box that passed the country-code check).
    _rot_years = (rotation.get("years") or []) if rotation else []
    _rot_codes = [h.get("crop_code") for h in _rot_years if h.get("crop_code")]
    is_water_body = bool(_rot_codes) and all(c in (111, 112) for c in _rot_codes)
    if not is_water_body and not _rot_codes and not soil_profile:
        is_water_body = True
    if is_water_body:
        headline_survival = 0

    if headline_survival < 65:
        recommendation = "DO NOT PLANT"
        if is_water_body:
            if _rot_codes:
                water_label = CDL_CROP_NAMES.get(_rot_codes[0], "water")
                verdict_detail = (
                    f"This location is classified as {water_label.replace('_', ' ')} by USDA CropScape. "
                    "Crops cannot survive in a body of water — survival is 0%."
                )
            else:
                verdict_detail = (
                    "No soil survey or crop history data exists for this location — "
                    "it is likely a body of water. Crops cannot grow here — survival is 0%."
                )
        elif has_killing_freeze:
            verdict_detail = f"Air temperature sustains below {_frost_kill_f}°F for 3+ hours in the forecast window — survival is 0%."
        elif has_lethal_heat:
            verdict_detail = f"Air temperature exceeds {lethal_f}°F lethal threshold — survival is 0%."
        elif has_extreme_aridity:
            verdict_detail = (
                f"Only {_fcst_precip_14d:.2f}\" forecast precipitation and {_recent_30d:.1f}\" "
                "in the last 30 days — insufficient water for rainfed germination."
            )
        else:
            verdict_detail = "Multiple risk factors are reducing External Risk Factor Survivability below the safety threshold."
    elif headline_survival < 90:
        recommendation = "WAIT & WATCH"
        verdict_detail = "External Risk Factor Survivability is borderline — re-check in 24 hours before committing."
    else:
        recommendation = "OPTIMAL TO PLANT"
        verdict_detail = "All monitored factors fall within safe thresholds for External Risk Factor Survivability."

    daily = forecast.get("daily", {})
    hourly = forecast["hourly"]
    plant_days = score_planting_window(forecast, profile, inputs,
                                       cultivar=cultivar)
    # "Best days" only ranks the forecast-grade window — climate-projected
    # entries beyond day 14 are too uncertain to recommend.
    best_days = sorted(plant_days, key=lambda d: -d["score"])[:3]

    # Survival % per displayed day, aligned to daily.time. Looked up by date
    # from plant_days; days without a full scoring window get null.
    survival_by_date = {d["date"]: d["survival_pct"] for d in plant_days}
    daily_times = daily.get("time", [])[:PLAN_HORIZON_DAYS]
    survival_pct_daily = [survival_by_date.get(t) for t in daily_times]

    # Historical context: 30-day antecedent stats and same-date prior-year
    # normals. The climatology was already built above (reused for the frost
    # integrated-probability branch); we re-bind it here so downstream code
    # paths that referenced ``climatology`` keep working unchanged.
    antecedent = derive_antecedent(history)
    climatology = early_climatology
    scm_forecast = compute_scm_forecast(scm_archive, scm_extended, forecast, inputs)
    norm_by_date = {d["date"]: d for d in climatology["days"]}
    normal_tmin = [(norm_by_date.get(t) or {}).get("normal_tmin_f") for t in daily_times]
    normal_tmax = [(norm_by_date.get(t) or {}).get("normal_tmax_f") for t in daily_times]

    # Build the climate-projected portion (days 15–31). The SCM state is consumed
    # here and stripped before we return.
    extension_start = today + timedelta(days=PLAN_HORIZON_DAYS)
    extension_days = max(0, EXTENDED_HORIZON_DAYS - PLAN_HORIZON_DAYS)
    extended = build_extended_outlook(
        climatology, profile,
        scm_forecast.pop("_state", None) if scm_forecast.get("available") else None,
        extension_start, extension_days, cv_factor,
    )

    # Concatenate forecast + climate rows so the frontend can slice [:14] for
    # the default view and [:31] when the user opens the extended outlook.
    plant_days_full = plant_days + extended["plant_days"]
    if scm_forecast.get("available"):
        scm_forecast["days"] = list(scm_forecast["days"]) + extended["scm_days"]

    daily_full_time = list(daily_times) + [d["date"] for d in extended["daily"]]
    daily_full = {
        "time": daily_full_time,
        "tmin_f": list(daily.get("temperature_2m_min", [])[:PLAN_HORIZON_DAYS])
                  + [d["tmin_f"] for d in extended["daily"]],
        "tmax_f": list(daily.get("temperature_2m_max", [])[:PLAN_HORIZON_DAYS])
                  + [d["tmax_f"] for d in extended["daily"]],
        "precip_in": list(daily.get("precipitation_sum", [])[:PLAN_HORIZON_DAYS])
                     + [d["precip_in"] for d in extended["daily"]],
        "uv_max": list(daily.get("uv_index_max", [])[:PLAN_HORIZON_DAYS])
                  + [d["uv_max"] for d in extended["daily"]],
        "survival_pct": list(survival_pct_daily)
                        + [d["survival_pct"] for d in extended["daily"]],
        "normal_tmin_f": list(normal_tmin)
                         + [d["normal_tmin_f"] for d in extended["daily"]],
        "normal_tmax_f": list(normal_tmax)
                         + [d["normal_tmax_f"] for d in extended["daily"]],
        "is_climate": [False] * len(daily_times) + [True] * len(extended["daily"]),
    }

    planting_depth = recommend_planting_depth(forecast, profile, inputs, start=0)

    cross_check = compare_forecasts(daily, nws_summary, lookahead_days=5)
    three_source = compare_recent_three_source(history, power_recent, nws_summary)
    confidence = compute_forecast_confidence(cross_check, three_source, ensemble)

    # Confidence-aware survival range using the per-factor model.
    survival_low, survival_high = _external_risk_survival_range(
        risks, confidence.get("scalar", 0.7), cultivar,
    )
    survival_pct = _survival_pct(risks, cultivar)

    if has_killing_freeze or has_lethal_heat or has_extreme_aridity or is_water_body:
        survival_pct = 0
        survival_low = 0
        survival_high = 0

    data_sources = _build_data_sources_block(soil_profile, nws_summary,
                                             bcw_flight, cross_check,
                                             alerts, drought,
                                             ensemble, power_recent,
                                             topography, three_source,
                                             confidence, scan_temps,
                                             iem_data, nass_progress,
                                             usgs_flow, cpc_moisture,
                                             enviroweather, rotation)

    return {
        "location": {"lat": lat, "lon": lon, "place": place},
        "crop": {
            "key": crop,
            "label": profile["label"],
            "min_soil_temp_f": profile["min_soil_temp_f"],
            "preferred_soil_temp_f": profile.get("preferred_soil_temp_f"),
            "frost_air_temp_f": profile["frost_air_temp_f"],
            "depth_min_in": profile.get("depth_min_in"),
            "depth_max_in": profile.get("depth_max_in"),
            "base_min_soil_temp_f": base_profile["min_soil_temp_f"],
            "base_frost_air_temp_f": base_profile["frost_air_temp_f"],
        },
        "inputs": asdict(inputs),
        "recommendation": recommendation,
        "verdict_detail": verdict_detail,
        "planting_depth": planting_depth,
        "summary": {
            "min_soil_temp_f": round(min(hourly["soil_temperature_6cm"][:48]), 1),
            "total_precip_in_48h": round(sum(hourly["precipitation"][:48]), 2),
            "max_uv_48h": round(max(hourly["uv_index"][:48]), 1),
        },
        "risks": [r.to_dict() for r in risks],
        "plant_days": plant_days_full,
        "best_days": best_days,
        "forecast_horizon_days": PLAN_HORIZON_DAYS,
        "extended_horizon_days": EXTENDED_HORIZON_DAYS,
        "hourly": {
            "time": hourly["time"][:72],
            "air_temp_f": hourly["temperature_2m"][:72],
            "soil_temp_f": hourly["soil_temperature_6cm"][:72],
            "precip_in": hourly["precipitation"][:72],
        },
        "past_hourly": {
            "time": (forecast.get("past_hourly") or {}).get("time", []),
            "precip_in": (forecast.get("past_hourly") or {}).get("precipitation", []),
        },
        "daily": daily_full,
        "history": antecedent,
        "history_series": _history_daily_series(history),
        "climatology": climatology,
        "scm_forecast": scm_forecast,
        "soil_profile": soil_profile or None,
        "forecast_cross_check": cross_check if cross_check.get("available") else None,
        "weather_alerts": alerts if isinstance(alerts, dict) and "count" in alerts else None,
        "drought": drought or None,
        "ensemble_forecast": ensemble if ensemble.get("available") else None,
        "recent_actuals_cross_check": three_source if three_source.get("available") else None,
        "topography": topography if topography.get("available") else None,
        "scan_soil_temps": scan_temps if scan_temps.get("available") else None,
        "iem_soil_data": iem_data if iem_data.get("available") else None,
        "crop_progress": nass_progress if nass_progress.get("available") else None,
        "streamflow": usgs_flow if usgs_flow.get("available") else None,
        "cpc_soil_moisture": cpc_moisture if cpc_moisture.get("available") else None,
        "enviroweather": enviroweather if enviroweather.get("available") else None,
        "crop_rotation": rotation if rotation.get("available") else None,
        "forecast_confidence": confidence,
        "survival": {
            "model": "External Risk Factor Survivability",
            "point_pct": survival_pct,
            "low_pct": survival_low,
            "high_pct": survival_high,
            "confidence": confidence.get("label"),
            "confidence_scalar": confidence.get("scalar"),
            "per_factor": [
                {
                    "key": r.key,
                    "name": r.name,
                    "category": r.model_category,
                    "severity": round(r.severity, 3),
                    "survival_factor": round(r.survival_factor, 4),
                }
                for r in risks
            ],
        },
        "spray_windows": score_spray_windows(forecast),
        "commercial_farming": assess_commercial_farming_region(rotation, crop),
        "data_sources": data_sources,
        "seed": _seed_block(crop, cultivar, base_profile, profile, seed_tailoring),
    }


def _seed_block(crop: str, cultivar: dict | None, base_profile: dict,
                tailored_profile: dict, tailoring_notes: list[str]) -> dict | None:
    """Compact summary the results page renders as a 'seed selection' card."""
    if not cultivar:
        return None
    diffs: dict[str, dict] = {}
    for key in ("min_soil_temp_f", "preferred_soil_temp_f", "frost_air_temp_f",
                "phytophthora_sensitive"):
        if base_profile.get(key) != tailored_profile.get(key):
            diffs[key] = {"base": base_profile.get(key), "tailored": tailored_profile.get(key)}
    return {
        "crop": crop,
        "brand": cultivar.get("_brand") or _brand_for_cultivar(crop, cultivar["id"]),
        "cultivar_id": cultivar["id"],
        "rm": cultivar.get("rm"),
        "cold_tolerance": cultivar.get("cold_tolerance"),
        "phytophthora": cultivar.get("phytophthora"),
        "scn_source": cultivar.get("scn_source"),
        "idc": cultivar.get("idc"),
        "emergence_score": cultivar.get("emergence_score"),
        "traits": cultivar.get("traits", []),
        "notes": cultivar.get("notes"),
        "tailoring_notes": tailoring_notes,
        "threshold_diffs": diffs,
    }


def _brand_for_cultivar(crop: str, cultivar_id: str) -> str | None:
    crop_key = crop if crop in SEED_CATALOG else "corn"
    for brand, cultivars in SEED_CATALOG[crop_key].items():
        if any(cv["id"] == cultivar_id for cv in cultivars):
            return brand
    return None
