"""Comprehensive validation of all 22 risk evaluator calculation methods.

Tests verify that each factor uses the correct mathematical model (sigmoid,
Gaussian, trapezoidal, exponential decay, etc.) and that the calibration
produces agronomically sensible outputs at key anchor points.

Run: pytest tests/test_calculation_methods.py -v
"""

import math
from datetime import date

import pytest
from services import (
    CROP_PROFILES,
    FACTOR_CATEGORIES,
    MODIFIER_TARGETS,
    UserInputs,
    _biological_response_survival,
    _compute_factor_survival,
    _external_risk_survivability,
    _gaussian_severity,
    _hazard_probability_survival,
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
    _white_mold,
    _cercospora_leaf_spot,
    _bolting_risk,
    _winterkill_risk,
    _autotoxicity,
    _level_from_severity,
    _modifier_amplification,
    _sigmoid_severity,
    _time_intensity_survival,
    _trapezoidal_severity,
    _smooth_precip_spikes,
    RISK_EVALUATORS,
)


# ---- Test helpers -----------------------------------------------------------

def _make_forecast(
    soil_temps_48h=None,
    air_temps_48h=None,
    precip_48h=None,
    uv_48h=None,
    humidity_48h=None,
    radiation_48h=None,
    wind_48h=None,
    moisture_0_1=None,
    moisture_1_3=None,
    moisture_3_9=None,
    soil_temp_18cm=None,
    history_precip_30d=None,
    daily_precip=None,
    topography=None,
    scan=None,
    iem=None,
    alerts=None,
    drought=None,
    ensemble=None,
    rotation=None,
    cpc_moisture=None,
    usgs=None,
):
    n = 168
    hourly = {
        "time": [f"2026-05-01T{h % 24:02d}:00" for h in range(n)],
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
    history = {}
    if history_precip_30d is not None:
        history = {"daily": {"precipitation_sum": history_precip_30d}}

    fc = {
        "hourly": hourly,
        "daily": {"precipitation_sum": daily_precip or [0.0] * 14, "time": []},
        "_history": history,
        "_soil_profile": {},
        "_nws": {},
        "_bcw_flight": {},
        "_alerts": alerts or {},
        "_drought": drought or {},
        "_ensemble": ensemble or {},
        "_power_recent": {},
        "_topography": topography or {},
        "_climatology_by_date": {},
        "_today_planting_date": date(2026, 5, 1),
        "_gdd_base50_cum": {},
        "_scan": scan or {},
        "_iem": iem or {},
        "_nass_progress": {},
        "_usgs": usgs or {},
        "_cpc_moisture": cpc_moisture or {},
        "_enviroweather": {},
        "_rotation": rotation or {},
    }
    return fc


def _pad(values, default, n):
    if values is None:
        return [default] * n
    return (values + [default] * n)[:n]


CORN = CROP_PROFILES["corn"]
SOY = CROP_PROFILES["soybeans"]
WHEAT = CROP_PROFILES["winter_wheat"]
SUGAR_BEETS = CROP_PROFILES["sugar_beets"]
ALFALFA = CROP_PROFILES["alfalfa"]
DEFAULT = UserInputs()


# ===========================================================================
# SECTION 1: Core mathematical primitives
# ===========================================================================

class TestSigmoidSeverity:
    """Logistic sigmoid for threshold-crossing risks."""

    def test_at_midpoint_returns_half(self):
        assert abs(_sigmoid_severity(50.0, midpoint=50.0, scale=4.0) - 0.5) < 0.01

    def test_well_above_midpoint_approaches_one(self):
        assert _sigmoid_severity(70.0, midpoint=50.0, scale=4.0) > 0.99

    def test_well_below_midpoint_approaches_zero(self):
        assert _sigmoid_severity(30.0, midpoint=50.0, scale=4.0) < 0.01

    def test_inverted_flips_direction(self):
        normal = _sigmoid_severity(45.0, midpoint=50.0, scale=4.0)
        inverted = _sigmoid_severity(45.0, midpoint=50.0, scale=4.0, inverted=True)
        assert abs(normal + inverted - 1.0) < 0.01

    def test_scale_controls_steepness(self):
        steep = _sigmoid_severity(52.0, midpoint=50.0, scale=1.0)
        gentle = _sigmoid_severity(52.0, midpoint=50.0, scale=8.0)
        assert steep > gentle


class TestGaussianSeverity:
    """Bell-curve for peak-activity risks."""

    def test_at_peak_returns_one(self):
        assert abs(_gaussian_severity(50.0, peak=50.0, sigma=5.0) - 1.0) < 0.001

    def test_one_sigma_away(self):
        result = _gaussian_severity(55.0, peak=50.0, sigma=5.0)
        expected = math.exp(-0.5)
        assert abs(result - expected) < 0.001

    def test_symmetric(self):
        left = _gaussian_severity(45.0, peak=50.0, sigma=5.0)
        right = _gaussian_severity(55.0, peak=50.0, sigma=5.0)
        assert abs(left - right) < 0.001


class TestTrapezoidalSeverity:
    """Plateau severity for wide-optimum risks."""

    def test_within_plateau_is_one(self):
        assert _trapezoidal_severity(55.0, a=40, b=45, c=60, d=65) == 1.0

    def test_below_onset_is_zero(self):
        assert _trapezoidal_severity(35.0, a=40, b=45, c=60, d=65) == 0.0

    def test_above_offset_is_zero(self):
        assert _trapezoidal_severity(70.0, a=40, b=45, c=60, d=65) == 0.0

    def test_ramp_up_midpoint(self):
        val = _trapezoidal_severity(42.5, a=40, b=45, c=60, d=65)
        assert abs(val - 0.5) < 0.01

    def test_ramp_down_midpoint(self):
        val = _trapezoidal_severity(62.5, a=40, b=45, c=60, d=65)
        assert abs(val - 0.5) < 0.01


class TestLevelFromSeverity:
    """Discrete level mapping from continuous severity."""

    def test_low_boundary(self):
        assert _level_from_severity(0.0) == "low"
        assert _level_from_severity(0.32) == "low"

    def test_moderate_boundary(self):
        assert _level_from_severity(0.33) == "moderate"
        assert _level_from_severity(0.66) == "moderate"

    def test_high_boundary(self):
        assert _level_from_severity(0.67) == "high"
        assert _level_from_severity(1.0) == "high"


# ===========================================================================
# SECTION 2: Per-category survival model validation
# ===========================================================================

class TestBiologicalResponseSurvival:
    """Logistic survival for threshold-crossing factors."""

    def test_zero_severity_full_survival(self):
        assert _biological_response_survival(0.0) == 1.0

    def test_moderate_severity_around_92pct(self):
        result = _biological_response_survival(0.50)
        assert 0.88 <= result <= 0.96

    def test_high_severity_around_85pct(self):
        result = _biological_response_survival(0.67)
        assert 0.80 <= result <= 0.90

    def test_extreme_severity_floor(self):
        result = _biological_response_survival(1.0)
        assert 0.60 <= result <= 0.75

    def test_never_below_floor(self):
        assert _biological_response_survival(1.0) >= 0.40


class TestTimeIntensitySurvival:
    """Exponential decay for duration-dependent factors."""

    def test_zero_severity_full_survival(self):
        assert _time_intensity_survival(0.0) == 1.0

    def test_moderate_severity_around_89pct(self):
        result = _time_intensity_survival(0.50)
        assert 0.85 <= result <= 0.93

    def test_extreme_is_near_floor(self):
        result = _time_intensity_survival(1.0)
        assert 0.55 <= result <= 0.70

    def test_monotonically_decreasing(self):
        prev = 1.0
        for s in [0.1, 0.2, 0.3, 0.5, 0.7, 0.9, 1.0]:
            cur = _time_intensity_survival(s)
            assert cur <= prev
            prev = cur


class TestHazardProbabilitySurvival:
    """Quartic pest stand-loss model — moderate pests barely dent survival,
    severe pests (1.0) cause ~35% stand loss."""

    def test_zero_is_full(self):
        assert _hazard_probability_survival(0.0) == 1.0

    def test_max_severity_caps_at_35pct_loss(self):
        result = _hazard_probability_survival(1.0)
        assert result >= 0.60
        assert result <= 0.70

    def test_moderate_light_loss(self):
        result = _hazard_probability_survival(0.50)
        assert 0.96 <= result <= 0.99


class TestModifierAmplification:
    """Modifiers don't kill directly — they amplify downstream targets."""

    def test_zero_severity_no_amplification(self):
        assert _modifier_amplification(0.0) == 1.0

    def test_max_severity_20pct_amplification(self):
        assert abs(_modifier_amplification(1.0) - 1.20) < 0.01

    def test_moderate_midpoint(self):
        result = _modifier_amplification(0.5)
        assert abs(result - 1.10) < 0.01


class TestFactorCategoryRouting:
    """Verify _compute_factor_survival routes to the correct model."""

    def test_biological_response_keys(self):
        for key in ["chilling", "frost", "pythium", "phytophthora", "herbicide"]:
            assert FACTOR_CATEGORIES[key] == "biological_response"
            result = _compute_factor_survival(key, 0.5)
            expected = _biological_response_survival(0.5)
            assert abs(result - expected) < 0.001

    def test_time_intensity_keys(self):
        for key in ["flooding", "crusting"]:
            assert FACTOR_CATEGORIES[key] == "time_intensity"
            result = _compute_factor_survival(key, 0.5)
            expected = _time_intensity_survival(0.5)
            assert abs(result - expected) < 0.001

    def test_modifier_keys_return_one(self):
        for key in ["antecedent", "topography"]:
            assert FACTOR_CATEGORIES[key] == "modifier"
            assert _compute_factor_survival(key, 0.9) == 1.0

    def test_hazard_probability_keys(self):
        for key in ["maggot", "wireworm", "slugs", "cutworm", "leaf_beetle"]:
            assert FACTOR_CATEGORIES[key] == "hazard_probability"
            result = _compute_factor_survival(key, 0.5)
            expected = _hazard_probability_survival(0.5)
            assert abs(result - expected) < 0.001


class TestModifierTargets:
    """Verify modifier→downstream routing is correct."""

    def test_antecedent_targets(self):
        targets = MODIFIER_TARGETS["antecedent"]
        assert "flooding" in targets
        assert "crusting" in targets
        assert "pythium" in targets
        assert "phytophthora" in targets

    def test_topography_targets(self):
        targets = MODIFIER_TARGETS["topography"]
        assert "flooding" in targets


# ===========================================================================
# SECTION 3: Individual evaluator method accuracy
# ===========================================================================

class TestImbibitionalChillingMethod:
    """Logistic sigmoid on soil temp with trend correction."""

    def test_uses_sigmoid_curve(self):
        fc = _make_forecast(soil_temps_48h=[55.0] * 48)
        risk = _imbibitional_chilling(fc, CORN, DEFAULT, 0)
        assert risk.severity < 0.2
        assert risk.curve_type == "sigmoid"

    def test_cold_soil_high_severity(self):
        fc = _make_forecast(soil_temps_48h=[38.0] * 48)
        risk = _imbibitional_chilling(fc, CORN, DEFAULT, 0)
        assert risk.severity > 0.7
        assert risk.level == "high"

    def test_warming_trend_reduces_severity(self):
        cold_then_warm = [44.0] * 24 + [56.0] * 24
        flat_cold = [44.0] * 48
        fc_warm = _make_forecast(soil_temps_48h=cold_then_warm)
        fc_cold = _make_forecast(soil_temps_48h=flat_cold)
        risk_warm = _imbibitional_chilling(fc_warm, CORN, DEFAULT, 0)
        risk_cold = _imbibitional_chilling(fc_cold, CORN, DEFAULT, 0)
        assert risk_warm.severity < risk_cold.severity

    def test_cooling_trend_increases_severity(self):
        warm_then_cold = [54.0] * 24 + [44.0] * 24
        flat_warm = [54.0] * 48
        fc_cool = _make_forecast(soil_temps_48h=warm_then_cold)
        fc_warm = _make_forecast(soil_temps_48h=flat_warm)
        risk_cool = _imbibitional_chilling(fc_cool, CORN, DEFAULT, 0)
        risk_warm = _imbibitional_chilling(fc_warm, CORN, DEFAULT, 0)
        assert risk_cool.severity > risk_warm.severity

    def test_scan_calibration_nudges_severity(self):
        fc = _make_forecast(
            soil_temps_48h=[52.0] * 48,
            scan={"available": True, "latest_temps_f": {"2in": 44.0}, "station": "TestSCAN", "distance_km": 5},
        )
        risk = _imbibitional_chilling(fc, CORN, DEFAULT, 0)
        assert risk.severity > 0.3


class TestFloodingMethod:
    """Sigmoid on precipitation with USGS/CPC/NWS overrides."""

    def test_uses_sigmoid_on_precip(self):
        fc = _make_forecast(precip_48h=[0.0] * 48)
        risk = _flooding(fc, CORN, DEFAULT, 0)
        assert risk.severity < 0.1
        assert risk.curve_type == "sigmoid"

    def test_heavy_precip_high_severity(self):
        fc = _make_forecast(precip_48h=[0.1] * 48)
        risk = _flooding(fc, CORN, DEFAULT, 0)
        assert risk.severity > 0.67
        assert risk.level == "high"

    def test_usgs_gage_escalates(self):
        fc = _make_forecast(
            precip_48h=[0.01] * 48,
            usgs={"available": True, "flood_risk": "high", "gage_height_ft": 14.0, "site_name": "Test Creek"},
        )
        risk = _flooding(fc, CORN, DEFAULT, 0)
        assert risk.severity >= 0.8

    def test_tile_drainage_reduces_severity(self):
        fc = _make_forecast(precip_48h=[0.05] * 48)
        tiled = UserInputs(field_tiled=True)
        risk_tiled = _flooding(fc, CORN, tiled, 0)
        risk_bare = _flooding(fc, CORN, DEFAULT, 0)
        assert risk_tiled.severity < risk_bare.severity

    def test_nws_flood_alert_override(self):
        fc = _make_forecast(
            precip_48h=[0.01] * 48,
            alerts={"any_flood": True, "alerts": [{"event": "Flood Warning", "is_flood": True}]},
        )
        risk = _flooding(fc, CORN, DEFAULT, 0)
        assert risk.severity >= 0.85


class TestAntecedentSaturationMethod:
    """Sigmoid on 30-day cumulative precip — acts as modifier."""

    def test_dry_history_low(self):
        fc = _make_forecast(history_precip_30d=[0.1] * 30)
        risk = _antecedent_saturation(fc, CORN, DEFAULT, 0)
        assert risk.level == "low"
        assert FACTOR_CATEGORIES[risk.key] == "modifier"

    def test_wet_history_high(self):
        fc = _make_forecast(history_precip_30d=[0.5] * 30)
        risk = _antecedent_saturation(fc, CORN, DEFAULT, 0)
        assert risk.level == "high"

    def test_tile_drainage_halves_severity(self):
        fc = _make_forecast(history_precip_30d=[0.3] * 30)
        tiled = UserInputs(field_tiled=True)
        risk_tiled = _antecedent_saturation(fc, CORN, tiled, 0)
        risk_bare = _antecedent_saturation(fc, CORN, DEFAULT, 0)
        assert risk_tiled.severity < risk_bare.severity * 0.6


class TestTopographyMethod:
    """Sigmoid on concavity — acts as modifier amplifying flooding."""

    def test_no_data_is_low(self):
        fc = _make_forecast(topography={})
        risk = _topography(fc, CORN, DEFAULT, 0)
        assert risk.level == "low"

    def test_concave_terrain_escalates(self):
        fc = _make_forecast(topography={
            "available": True, "ponding_risk": "high",
            "concavity_m": 0.5, "slope_m_per_km": 1.0,
        })
        risk = _topography(fc, CORN, DEFAULT, 0)
        assert risk.severity > 0.5

    def test_convex_terrain_is_low(self):
        fc = _make_forecast(topography={
            "available": True, "ponding_risk": "low",
            "concavity_m": -0.2, "slope_m_per_km": 5.0,
        })
        risk = _topography(fc, CORN, DEFAULT, 0)
        assert risk.level == "low"


class TestFrostMethod:
    """Multi-signal frost: deterministic + ensemble + climatology + NWS."""

    def test_warm_forecast_is_low(self):
        fc = _make_forecast(air_temps_48h=[60.0] * 168)
        risk = _frost(fc, CORN, DEFAULT, 0)
        assert risk.level == "low"
        assert risk.curve_type == "sigmoid"

    def test_hard_freeze_is_high(self):
        fc = _make_forecast(air_temps_48h=[20.0] * 168)
        risk = _frost(fc, CORN, DEFAULT, 0)
        assert risk.level == "high"
        assert risk.severity > 0.8

    def test_soybeans_more_sensitive_threshold(self):
        fc = _make_forecast(air_temps_48h=[30.0] * 168)
        risk_soy = _frost(fc, SOY, DEFAULT, 0)
        risk_corn = _frost(fc, CORN, DEFAULT, 0)
        assert risk_soy.severity > risk_corn.severity

    def test_nws_freeze_alert_escalates(self):
        fc = _make_forecast(
            air_temps_48h=[35.0] * 168,
            alerts={"any_freeze": True, "alerts": [{"event": "Freeze Warning", "is_freeze": True}]},
        )
        risk = _frost(fc, CORN, DEFAULT, 0)
        assert risk.severity >= 0.5


class TestSoilCrustingMethod:
    """Composite: rain intensity + UV drying + clay content."""

    def test_no_rain_no_crust(self):
        fc = _make_forecast(precip_48h=[0.0] * 48, uv_48h=[2.0] * 48)
        risk = _soil_crusting(fc, CORN, DEFAULT, 0)
        assert risk.level == "low"

    def test_heavy_rain_high_uv_crusts(self):
        # Crusting needs: >0.4" rain in 24h, then hot (>65°F) + high UV (>4) in next 48h
        rain = [0.05] * 24 + [0.0] * 144  # 1.2" in first 24h
        air = [55.0] * 24 + [85.0] * 144   # hot after rain
        uv = [3.0] * 24 + [10.0] * 144     # high UV after rain
        fc = _make_forecast(precip_48h=rain, air_temps_48h=air, uv_48h=uv)
        risk = _soil_crusting(fc, CORN, DEFAULT, 0)
        assert risk.severity > 0.3


class TestPythiumMethod:
    """Gaussian peak around 50°F + moisture multiplier."""

    def test_warm_dry_is_safe(self):
        fc = _make_forecast(soil_temps_48h=[62.0] * 48, moisture_0_1=[0.15] * 168)
        risk = _pythium(fc, CORN, DEFAULT, 0)
        assert risk.level == "low"

    def test_cold_wet_peak_activity(self):
        fc = _make_forecast(
            soil_temps_48h=[48.0] * 48,
            moisture_0_1=[0.50] * 168,
            moisture_1_3=[0.50] * 168,
        )
        risk = _pythium(fc, CORN, DEFAULT, 0)
        assert risk.level == "high"

    def test_too_hot_reduces_risk(self):
        fc = _make_forecast(soil_temps_48h=[75.0] * 48, moisture_0_1=[0.50] * 168)
        risk = _pythium(fc, CORN, DEFAULT, 0)
        assert risk.severity < 0.5


class TestPhytophthoraMethod:
    """Trapezoidal temp severity × sigmoid saturation hours — soy-specific."""

    def test_corn_not_sensitive(self):
        fc = _make_forecast()
        risk = _phytophthora(fc, CORN, DEFAULT, 0)
        assert risk.level == "low"

    def test_soy_dry_soil_is_safe(self):
        fc = _make_forecast(moisture_0_1=[0.10] * 168, moisture_3_9=[0.10] * 168)
        risk = _phytophthora(fc, SOY, DEFAULT, 0)
        assert risk.level == "low"

    def test_soy_warm_saturated_is_risky(self):
        # Phytophthora checks soil_temperature_18cm and soil_moisture_3_to_9cm
        # Needs saturation (>0.38) at warm temps (60-86°F plateau)
        fc = _make_forecast(
            soil_temp_18cm=[72.0] * 168,
            moisture_3_9=[0.50] * 168,
        )
        risk = _phytophthora(fc, SOY, DEFAULT, 0)
        assert risk.severity > 0.3


class TestSeedcornMaggotMethod:
    """GDD phenology model × attractant index (manure, residue, no-till)."""

    def test_clean_field_is_low(self):
        fc = _make_forecast()
        inputs = UserInputs(tillage="conventional", manure_recent=False, residue="low")
        risk = _seedcorn_maggot(fc, CORN, inputs, 0)
        assert risk.level == "low"

    def test_manure_notill_escalates_vs_clean(self):
        fc = _make_forecast()
        inputs_clean = UserInputs(tillage="conventional", manure_recent=False, residue="low")
        inputs_dirty = UserInputs(tillage="no-till", manure_recent=True, residue="heavy")
        risk_clean = _seedcorn_maggot(fc, CORN, inputs_clean, 0)
        risk_dirty = _seedcorn_maggot(fc, CORN, inputs_dirty, 0)
        assert risk_dirty.severity > risk_clean.severity


class TestWirewormMethod:
    """Field-history probability: grass/sod rotation = high risk."""

    def test_no_grass_is_safe(self):
        fc = _make_forecast()
        inputs = UserInputs(previous_grass=False)
        risk = _wireworm(fc, CORN, inputs, 0)
        assert risk.level == "low"

    def test_previous_grass_elevates_vs_no_grass(self):
        fc = _make_forecast()
        inputs_no = UserInputs(previous_grass=False)
        inputs_yes = UserInputs(previous_grass=True)
        risk_no = _wireworm(fc, CORN, inputs_no, 0)
        risk_yes = _wireworm(fc, CORN, inputs_yes, 0)
        assert risk_yes.severity > risk_no.severity


class TestSlugsMethod:
    """Conducive conditions: residue × moisture × cool temps."""

    def test_low_residue_dry_is_safe(self):
        fc = _make_forecast(moisture_0_1=[0.10] * 168)
        inputs = UserInputs(tillage="conventional", residue="low")
        risk = _slugs(fc, CORN, inputs, 0)
        assert risk.level == "low"

    def test_heavy_residue_wet_elevates_vs_clean(self):
        fc = _make_forecast(moisture_0_1=[0.45] * 168)
        inputs_clean = UserInputs(tillage="conventional", residue="low")
        inputs_heavy = UserInputs(tillage="no-till", residue="heavy")
        risk_clean = _slugs(fc, CORN, inputs_clean, 0)
        risk_heavy = _slugs(fc, CORN, inputs_heavy, 0)
        assert risk_heavy.severity > risk_clean.severity


class TestBlackCutwormMethod:
    """GDD phenology from moth-flight biofix."""

    def test_default_conditions_low_or_moderate(self):
        fc = _make_forecast()
        risk = _black_cutworm(fc, CORN, DEFAULT, 0)
        assert risk.level in ("low", "moderate")

    def test_corn_sensitive_not_soy(self):
        fc = _make_forecast()
        risk_corn = _black_cutworm(fc, CORN, DEFAULT, 0)
        risk_soy = _black_cutworm(fc, SOY, DEFAULT, 0)
        assert risk_soy.level == "low"


class TestBeanLeafBeetleMethod:
    """Overwintering survival × early planting vulnerability."""

    def test_corn_not_sensitive(self):
        fc = _make_forecast()
        risk = _bean_leaf_beetle(fc, CORN, DEFAULT, 0)
        assert risk.level == "low"

    def test_soybeans_variable(self):
        fc = _make_forecast()
        risk = _bean_leaf_beetle(fc, SOY, DEFAULT, 0)
        assert risk.level in ("low", "moderate", "high")


class TestHerbicideCarryoverMethod:
    """First-order degradation kinetics: residue = initial × e^(-k × time)."""

    def test_no_herbicide_is_safe(self):
        fc = _make_forecast()
        inputs = UserInputs(herbicide_last_season="")
        risk = _herbicide_carryover(fc, CORN, inputs, 0)
        assert risk.level == "low"
        assert risk.severity == 0.0

    def test_atrazine_on_soybeans_is_high(self):
        fc = _make_forecast()
        inputs = UserInputs(herbicide_last_season="Atrazine")
        risk = _herbicide_carryover(fc, SOY, inputs, 0)
        assert risk.level == "high"

    def test_known_injurious_herbicide_is_high(self):
        fc = _make_forecast()
        inputs = UserInputs(herbicide_last_season="Mesotrione")
        risk = _herbicide_carryover(fc, SOY, inputs, 0)
        assert risk.level == "high"


class TestHeatStressMethod:
    """Sigmoid on max temp vs crop stress threshold."""

    def test_normal_temps_safe(self):
        fc = _make_forecast(air_temps_48h=[75.0] * 48)
        risk = _heat_stress(fc, CORN, DEFAULT, 0)
        assert risk.level == "low"
        assert risk.curve_type == "sigmoid"

    def test_extreme_heat_high(self):
        fc = _make_forecast(air_temps_48h=[110.0] * 48)
        risk = _heat_stress(fc, CORN, DEFAULT, 0)
        assert risk.level == "high"
        assert risk.severity > 0.8

    def test_lethal_is_max_severity(self):
        fc = _make_forecast(air_temps_48h=[115.0] * 48)
        risk = _heat_stress(fc, CORN, DEFAULT, 0)
        assert risk.severity == 1.0


class TestWaterScarcityMethod:
    """Composite: forecast precip + history + drought + CPC moisture."""

    def test_adequate_rain_is_low(self):
        fc = _make_forecast(
            daily_precip=[0.2] * 14,
            history_precip_30d=[0.15] * 30,
        )
        risk = _water_scarcity(fc, CORN, DEFAULT, 0)
        assert risk.level == "low"
        assert risk.curve_type == "composite"

    def test_zero_precip_drought_is_extreme(self):
        fc = _make_forecast(
            daily_precip=[0.0] * 14,
            history_precip_30d=[0.0] * 30,
            drought={"class": 4, "label": "Exceptional Drought"},
        )
        risk = _water_scarcity(fc, CORN, DEFAULT, 0)
        assert risk.severity >= 0.9
        assert risk.level == "high"

    def test_drought_class_escalates(self):
        fc_no_drought = _make_forecast(
            daily_precip=[0.05] * 14,
            history_precip_30d=[0.05] * 30,
        )
        fc_drought = _make_forecast(
            daily_precip=[0.05] * 14,
            history_precip_30d=[0.05] * 30,
            drought={"class": 3, "label": "Extreme Drought"},
        )
        risk_no = _water_scarcity(fc_no_drought, CORN, DEFAULT, 0)
        risk_yes = _water_scarcity(fc_drought, CORN, DEFAULT, 0)
        assert risk_yes.severity > risk_no.severity


class TestFusariumHeadBlightMethod:
    """Sigmoid on warm+humid hours — wheat-specific."""

    def test_corn_not_susceptible(self):
        fc = _make_forecast()
        risk = _fusarium_head_blight(fc, CORN, DEFAULT, 0)
        assert risk.level == "low"

    def test_wheat_dry_conditions_low(self):
        fc = _make_forecast(air_temps_48h=[70.0] * 72, humidity_48h=[50.0] * 72)
        risk = _fusarium_head_blight(fc, WHEAT, DEFAULT, 0)
        assert risk.level == "low"

    def test_wheat_warm_humid_escalates(self):
        fc = _make_forecast(air_temps_48h=[72.0] * 168, humidity_48h=[90.0] * 168)
        risk = _fusarium_head_blight(fc, WHEAT, DEFAULT, 0)
        assert risk.severity > 0.5


class TestWhiteMoldMethod:
    """Sigmoid on cool/moist hours — soy/dry beans specific."""

    def test_corn_not_susceptible(self):
        fc = _make_forecast()
        risk = _white_mold(fc, CORN, DEFAULT, 0)
        assert risk.level == "low"

    def test_dry_conditions_low(self):
        fc = _make_forecast(air_temps_48h=[65.0] * 168, humidity_48h=[50.0] * 168)
        risk = _white_mold(fc, SOY, DEFAULT, 0)
        assert risk.level == "low"


class TestCercosporaLeafSpotMethod:
    """Sigmoid on warm+wet hours — sugar beets specific."""

    def test_corn_not_susceptible(self):
        fc = _make_forecast()
        risk = _cercospora_leaf_spot(fc, CORN, DEFAULT, 0)
        assert risk.level == "low"

    def test_sugar_beet_dry_is_low(self):
        fc = _make_forecast(air_temps_48h=[75.0] * 168, humidity_48h=[50.0] * 168)
        risk = _cercospora_leaf_spot(fc, SUGAR_BEETS, DEFAULT, 0)
        assert risk.level == "low"

    def test_sugar_beet_warm_wet_escalates(self):
        fc = _make_forecast(air_temps_48h=[75.0] * 168, humidity_48h=[95.0] * 168)
        risk = _cercospora_leaf_spot(fc, SUGAR_BEETS, DEFAULT, 0)
        assert risk.severity > 0.5


class TestBoltingRiskMethod:
    """Sigmoid on cold hours below vernalization threshold — sugar beets."""

    def test_corn_not_applicable(self):
        fc = _make_forecast()
        risk = _bolting_risk(fc, CORN, DEFAULT, 0)
        assert risk.level == "low"

    def test_warm_sugar_beets_safe(self):
        fc = _make_forecast(air_temps_48h=[60.0] * 168)
        risk = _bolting_risk(fc, SUGAR_BEETS, DEFAULT, 0)
        assert risk.level == "low"

    def test_cold_sugar_beets_elevated(self):
        fc = _make_forecast(air_temps_48h=[35.0] * 168)
        risk = _bolting_risk(fc, SUGAR_BEETS, DEFAULT, 0)
        assert risk.severity > 0.3


class TestWinterkillMethod:
    """Sigmoid on cold + freeze-thaw cycles — alfalfa specific."""

    def test_corn_not_applicable(self):
        fc = _make_forecast()
        risk = _winterkill_risk(fc, CORN, DEFAULT, 0)
        assert risk.level == "low"

    def test_mild_temps_safe(self):
        fc = _make_forecast(air_temps_48h=[40.0] * 168)
        risk = _winterkill_risk(fc, ALFALFA, DEFAULT, 0)
        assert risk.level == "low"

    def test_extreme_cold_high(self):
        fc = _make_forecast(air_temps_48h=[-5.0] * 168)
        risk = _winterkill_risk(fc, ALFALFA, DEFAULT, 0)
        assert risk.level == "high"


class TestAutotoxicityMethod:
    """Autotoxicity: severity scales with recency and alfalfa-year count."""

    def test_corn_not_applicable(self):
        fc = _make_forecast()
        risk = _autotoxicity(fc, CORN, DEFAULT, 0)
        assert risk.level == "low"

    def test_no_prior_alfalfa_safe(self):
        fc = _make_forecast(rotation={"history": [{"crop": "corn", "cdl_code": 1}]})
        risk = _autotoxicity(fc, ALFALFA, DEFAULT, 0)
        assert risk.level == "low"

    def test_prior_alfalfa_triggers(self):
        fc = _make_forecast(rotation={"history": [{"crop": "alfalfa", "cdl_code": 36}]})
        risk = _autotoxicity(fc, ALFALFA, DEFAULT, 0)
        assert risk.level == "high"
        assert risk.severity >= 0.8

    def test_older_alfalfa_lower_severity(self):
        fc = _make_forecast(rotation={"history": [
            {"crop": "corn", "cdl_code": 1},
            {"crop": "corn", "cdl_code": 1},
            {"crop": "alfalfa", "cdl_code": 36},
        ]})
        risk = _autotoxicity(fc, ALFALFA, DEFAULT, 0)
        assert risk.severity < 0.5


# ===========================================================================
# SECTION 3b: Precipitation spike smoothing
# ===========================================================================

class TestPrecipSpikeSmoothing:
    """NWP models sometimes dump a day's convective precip into one hour."""

    def test_no_spike_unchanged(self):
        precip = [0.1] * 24
        result = _smooth_precip_spikes(precip)
        assert result == precip

    def test_spike_capped_at_1_5(self):
        precip = [0.0] * 24
        precip[20] = 4.846
        result = _smooth_precip_spikes(precip)
        assert result[20] == 1.5

    def test_daily_total_preserved(self):
        precip = [0.0] * 24
        precip[20] = 4.846
        result = _smooth_precip_spikes(precip)
        assert abs(sum(result) - 4.846) < 0.001

    def test_excess_concentrated_near_spike(self):
        precip = [0.0] * 48
        precip[20] = 4.846  # spike at hour 20
        result = _smooth_precip_spikes(precip)
        # Hours close to spike (±2) get more than distant hours
        near = result[19] + result[21]
        far = result[14] + result[15]
        assert near > far
        # Hours well outside the ±6 radius stay at zero
        assert result[5] == 0.0
        assert result[40] == 0.0

    def test_empty_input(self):
        assert _smooth_precip_spikes([]) == []

    def test_none_values_handled(self):
        precip = [None] * 24
        precip[10] = 3.0
        result = _smooth_precip_spikes(precip)
        assert result[10] == 1.5
        assert sum(v for v in result if v is not None) - 3.0 < 0.001

    def test_normal_precip_untouched(self):
        precip = [0.05] * 24
        result = _smooth_precip_spikes(precip)
        assert result == precip


# ===========================================================================
# SECTION 4: Multiplicative survival integration
# ===========================================================================

class TestMultiplicativeSurvival:
    """All non-modifier factors multiply; modifiers amplify targets."""

    def test_all_low_positive_survival(self):
        fc = _make_forecast(
            soil_temps_48h=[58.0] * 48,
            air_temps_48h=[65.0] * 168,
            precip_48h=[0.0] * 48,
            moisture_0_1=[0.20] * 168,
            moisture_3_9=[0.20] * 168,
            humidity_48h=[40.0] * 168,
            daily_precip=[0.1] * 14,
            history_precip_30d=[0.1] * 30,
        )
        risks = [ev(fc, CORN, DEFAULT, 0) for ev in RISK_EVALUATORS]
        survival = _external_risk_survivability(risks)
        assert survival >= 60

    def test_multiple_high_reduces_below_65(self):
        fc = _make_forecast(
            soil_temps_48h=[38.0] * 48,
            air_temps_48h=[20.0] * 168,
            precip_48h=[0.15] * 48,
            moisture_0_1=[0.50] * 168,
            moisture_1_3=[0.50] * 168,
            history_precip_30d=[0.5] * 30,
        )
        risks = [ev(fc, CORN, DEFAULT, 0) for ev in RISK_EVALUATORS]
        survival = _external_risk_survivability(risks)
        assert survival < 65

    def test_modifiers_amplify_not_kill(self):
        fc_dry = _make_forecast(
            precip_48h=[0.05] * 48,
            history_precip_30d=[0.1] * 30,
        )
        fc_wet = _make_forecast(
            precip_48h=[0.05] * 48,
            history_precip_30d=[0.4] * 30,
        )
        risks_dry = [ev(fc_dry, CORN, DEFAULT, 0) for ev in RISK_EVALUATORS]
        risks_wet = [ev(fc_wet, CORN, DEFAULT, 0) for ev in RISK_EVALUATORS]
        surv_dry = _external_risk_survivability(risks_dry)
        surv_wet = _external_risk_survivability(risks_wet)
        assert surv_wet <= surv_dry


# ===========================================================================
# SECTION 5: All evaluators return valid Risk shape
# ===========================================================================

class TestAllEvaluatorsShape:
    """Every evaluator returns a well-formed Risk with required fields."""

    @pytest.mark.parametrize("evaluator", RISK_EVALUATORS)
    def test_shape(self, evaluator):
        fc = _make_forecast()
        risk = evaluator(fc, CORN, DEFAULT, 0)
        assert risk.key, f"{evaluator.__name__} empty key"
        assert risk.name, f"{evaluator.__name__} empty name"
        assert risk.level in ("low", "moderate", "high")
        assert risk.headline
        assert 0.0 <= risk.severity <= 1.0

    @pytest.mark.parametrize("evaluator", RISK_EVALUATORS)
    def test_severity_level_consistency(self, evaluator):
        fc = _make_forecast()
        risk = evaluator(fc, CORN, DEFAULT, 0)
        expected_level = _level_from_severity(risk.severity)
        assert risk.level == expected_level, (
            f"{evaluator.__name__}: severity={risk.severity:.2f} "
            f"→ expected level={expected_level}, got {risk.level}"
        )
