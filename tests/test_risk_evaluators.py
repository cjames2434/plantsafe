"""Unit tests for the risk evaluators in services.py.

Each test constructs a minimal synthetic forecast dict and runs one
evaluator, checking that the returned Risk.level matches expectations
for known-good and known-bad conditions.
"""

from datetime import date

import pytest
from services import (
    CROP_PROFILES,
    UserInputs,
    _imbibitional_chilling,
    _flooding,
    _frost,
    _soil_crusting,
    _pythium,
    _phytophthora,
    _seedcorn_maggot,
    _wireworm,
    _slugs,
    _herbicide_carryover,
)


def _make_forecast(
    soil_temps_48h: list[float] | None = None,
    air_temps_48h: list[float] | None = None,
    precip_48h: list[float] | None = None,
    uv_48h: list[float] | None = None,
    humidity_48h: list[float] | None = None,
    radiation_48h: list[float] | None = None,
    wind_48h: list[float] | None = None,
    moisture_0_1: list[float] | None = None,
    moisture_1_3: list[float] | None = None,
    moisture_3_9: list[float] | None = None,
    soil_temp_18cm: list[float] | None = None,
) -> dict:
    """Build a minimal forecast dict that the evaluators can index into."""
    n = 72
    hourly = {
        "time": [f"2026-05-01T{h:02d}:00" for h in range(n)],
        "soil_temperature_6cm": _pad(soil_temps_48h, 55.0, n),
        "soil_temperature_18cm": _pad(soil_temp_18cm, 55.0, n),
        "temperature_2m": _pad(air_temps_48h, 60.0, n),
        "precipitation": _pad(precip_48h, 0.0, n),
        "uv_index": _pad(uv_48h, 3.0, n),
        "relative_humidity_2m": _pad(humidity_48h, 50.0, n),
        "shortwave_radiation": _pad(radiation_48h, 200.0, n),
        "wind_speed_10m": _pad(wind_48h, 5.0, n),
        "soil_moisture_0_to_1cm": _pad(moisture_0_1, 0.25, n),
        "soil_moisture_1_to_3cm": _pad(moisture_1_3, 0.25, n),
        "soil_moisture_3_to_9cm": _pad(moisture_3_9, 0.25, n),
    }
    return {
        "hourly": hourly,
        "_history": {},
        "_soil_profile": {},
        "_nws": {},
        "_bcw_flight": {},
        "_alerts": {},
        "_drought": {},
        "_ensemble": {},
        "_power_recent": {},
        "_topography": {},
        "_climatology_by_date": {},
        "_today_planting_date": date(2026, 5, 1),
        "_gdd_base50_cum": {},
    }


def _pad(values: list[float] | None, default: float, n: int) -> list[float]:
    if values is None:
        return [default] * n
    return (values + [default] * n)[:n]


CORN = CROP_PROFILES["corn"]
SOY = CROP_PROFILES["soybeans"]
DEFAULT_INPUTS = UserInputs()


# ---- Imbibitional Chilling ------------------------------------------------

class TestImbibitionalChilling:
    def test_warm_soil_is_low(self):
        fc = _make_forecast(soil_temps_48h=[58.0] * 48)
        risk = _imbibitional_chilling(fc, CORN, DEFAULT_INPUTS, 0)
        assert risk.level == "low"

    def test_cold_soil_is_high(self):
        fc = _make_forecast(soil_temps_48h=[40.0] * 48)
        risk = _imbibitional_chilling(fc, CORN, DEFAULT_INPUTS, 0)
        assert risk.level == "high"

    def test_borderline_soil_is_moderate(self):
        fc = _make_forecast(soil_temps_48h=[48.0] * 48)
        risk = _imbibitional_chilling(fc, CORN, DEFAULT_INPUTS, 0)
        assert risk.level == "moderate"

    def test_warming_trend_rescues_moderate(self):
        cold_then_warm = [46.0] * 24 + [54.0] * 24
        fc = _make_forecast(soil_temps_48h=cold_then_warm)
        risk = _imbibitional_chilling(fc, CORN, DEFAULT_INPUTS, 0)
        assert risk.level == "low"

    def test_soybeans_higher_threshold(self):
        fc = _make_forecast(soil_temps_48h=[53.0] * 48)
        risk = _imbibitional_chilling(fc, SOY, DEFAULT_INPUTS, 0)
        assert risk.level == "moderate"


# ---- Flooding -------------------------------------------------------------

class TestFlooding:
    def test_dry_is_low(self):
        fc = _make_forecast(precip_48h=[0.0] * 48)
        risk = _flooding(fc, CORN, DEFAULT_INPUTS, 0)
        assert risk.level == "low"

    def test_heavy_rain_is_high(self):
        fc = _make_forecast(precip_48h=[0.1] * 48)  # 4.8" total
        risk = _flooding(fc, CORN, DEFAULT_INPUTS, 0)
        assert risk.level == "high"

    def test_moderate_rain(self):
        # 1.5" total over 48h
        fc = _make_forecast(precip_48h=[1.5 / 48] * 48)
        risk = _flooding(fc, CORN, DEFAULT_INPUTS, 0)
        assert risk.level == "moderate"

    def test_nws_flood_alert_escalates(self):
        fc = _make_forecast(precip_48h=[0.01] * 48)
        fc["_alerts"] = {
            "any_flood": True,
            "alerts": [{"event": "Flood Warning", "is_flood": True}],
        }
        risk = _flooding(fc, CORN, DEFAULT_INPUTS, 0)
        assert risk.level == "high"


# ---- Frost ----------------------------------------------------------------

class TestFrost:
    def test_warm_is_low(self):
        fc = _make_forecast(air_temps_48h=[60.0] * 48)
        risk = _frost(fc, CORN, DEFAULT_INPUTS, 0)
        assert risk.level == "low"

    def test_deep_freeze_corn_is_high(self):
        fc = _make_forecast(air_temps_48h=[22.0] * 48)
        risk = _frost(fc, CORN, DEFAULT_INPUTS, 0)
        assert risk.level == "high"


# ---- Soil Crusting --------------------------------------------------------

class TestSoilCrusting:
    def test_no_rain_low_uv_is_low(self):
        fc = _make_forecast(precip_48h=[0.0] * 48, uv_48h=[2.0] * 48)
        risk = _soil_crusting(fc, CORN, DEFAULT_INPUTS, 0)
        assert risk.level == "low"


# ---- Pythium --------------------------------------------------------------

class TestPythium:
    def test_warm_dry_is_low(self):
        fc = _make_forecast(soil_temps_48h=[62.0] * 48, moisture_0_1=[0.15] * 72)
        risk = _pythium(fc, CORN, DEFAULT_INPUTS, 0)
        assert risk.level == "low"

    def test_cold_wet_is_high(self):
        fc = _make_forecast(
            soil_temps_48h=[44.0] * 48,
            moisture_0_1=[0.50] * 72,
            moisture_1_3=[0.50] * 72,
        )
        risk = _pythium(fc, CORN, DEFAULT_INPUTS, 0)
        assert risk.level == "high"


# ---- Phytophthora ---------------------------------------------------------

class TestPhytophthora:
    def test_corn_not_sensitive(self):
        fc = _make_forecast()
        risk = _phytophthora(fc, CORN, DEFAULT_INPUTS, 0)
        assert risk.level == "low"

    def test_soy_on_dry_soil_is_low(self):
        fc = _make_forecast(moisture_0_1=[0.15] * 72)
        risk = _phytophthora(fc, SOY, DEFAULT_INPUTS, 0)
        assert risk.level == "low"


# ---- Seedcorn Maggot ------------------------------------------------------

class TestSeedcornMaggot:
    def test_no_manure_conventional_is_low(self):
        fc = _make_forecast()
        inputs = UserInputs(tillage="conventional", manure_recent=False)
        risk = _seedcorn_maggot(fc, CORN, inputs, 0)
        assert risk.level == "low"


# ---- Wireworm -------------------------------------------------------------

class TestWireworm:
    def test_no_grass_history_is_low(self):
        fc = _make_forecast()
        inputs = UserInputs(previous_grass=False)
        risk = _wireworm(fc, CORN, inputs, 0)
        assert risk.level == "low"


# ---- Slugs ----------------------------------------------------------------

class TestSlugs:
    def test_conventional_low_residue_is_low(self):
        fc = _make_forecast(moisture_0_1=[0.15] * 72)
        inputs = UserInputs(tillage="conventional", residue="low")
        risk = _slugs(fc, CORN, inputs, 0)
        assert risk.level == "low"


# ---- Herbicide Carryover --------------------------------------------------

class TestHerbicideCarryover:
    def test_no_herbicide_is_low(self):
        fc = _make_forecast()
        inputs = UserInputs(herbicide_last_season="")
        risk = _herbicide_carryover(fc, CORN, inputs, 0)
        assert risk.level == "low"

    def test_atrazine_on_soybeans_is_high(self):
        fc = _make_forecast()
        inputs = UserInputs(herbicide_last_season="Atrazine")
        risk = _herbicide_carryover(fc, SOY, inputs, 0)
        assert risk.level == "high"


# ---- Data shapes ----------------------------------------------------------

class TestRiskShape:
    """Every evaluator must return a Risk with key, name, level, headline, detail."""

    def test_all_evaluators_return_valid_risk(self):
        fc = _make_forecast()
        evaluators = [
            _imbibitional_chilling, _flooding, _frost, _soil_crusting,
            _pythium, _phytophthora, _seedcorn_maggot, _wireworm,
            _slugs, _herbicide_carryover,
        ]
        for ev in evaluators:
            risk = ev(fc, CORN, DEFAULT_INPUTS, 0)
            assert risk.key, f"{ev.__name__} returned empty key"
            assert risk.name, f"{ev.__name__} returned empty name"
            assert risk.level in ("low", "moderate", "high"), (
                f"{ev.__name__} returned invalid level: {risk.level}"
            )
            assert risk.headline, f"{ev.__name__} returned empty headline"
