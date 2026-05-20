"""API shape and error-handling tests using FastAPI's TestClient.

These tests exercise the HTTP layer without hitting real upstream APIs —
they verify that the router returns the correct status codes and that
the response shape matches what the frontend expects.
"""

import pytest
from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


class TestPages:
    def test_index(self):
        r = client.get("/")
        assert r.status_code == 200
        assert "Crop Sentry" in r.text or "Crop" in r.text

    def test_map(self):
        r = client.get("/map")
        assert r.status_code == 200

    def test_methodology(self):
        r = client.get("/methodology")
        assert r.status_code == 200

    def test_terms(self):
        r = client.get("/terms")
        assert r.status_code == 200

    def test_privacy(self):
        r = client.get("/privacy")
        assert r.status_code == 200


class TestAPISeeds:
    def test_seeds_corn(self):
        r = client.get("/api/seeds?crop=corn")
        assert r.status_code == 200
        data = r.json()
        assert "items" in data
        assert len(data["items"]) > 0
        assert data["items"][0]["brand"]

    def test_seeds_soybeans(self):
        r = client.get("/api/seeds?crop=soybeans")
        assert r.status_code == 200
        data = r.json()
        assert len(data["items"]) > 0


class TestAPILegal:
    def test_legal(self):
        r = client.get("/api/legal")
        assert r.status_code == 200
        data = r.json()
        assert "terms_effective" in data
        assert "privacy_effective" in data
        assert "disclaimer" in data


class TestAPIMethodology:
    def test_methodology(self):
        r = client.get("/api/methodology")
        assert r.status_code == 200
        data = r.json()
        assert "risks" in data
        assert "survival" in data


class TestAPIEvaluate:
    def test_missing_params(self):
        r = client.get("/api/evaluate")
        assert r.status_code == 400
        assert "error" in r.json()

    def test_bad_zip(self):
        r = client.get("/api/evaluate?zip=ZZZZZNOTREAL999")
        assert r.status_code in (404, 422)


class TestAPIConsent:
    def test_consent_post(self):
        r = client.post("/api/consent", json={
            "terms_version": "2026-04-29",
            "privacy_version": "2026-04-29",
            "accepted_at": "2026-05-06T12:00:00Z",
        })
        assert r.status_code == 200
        assert r.json()["ok"] is True

    def test_consent_missing_fields(self):
        r = client.post("/api/consent", json={"terms_version": "x"})
        assert r.status_code == 422


class TestAPIHealth:
    def test_health_returns_upstreams(self):
        r = client.get("/api/health")
        data = r.json()
        assert "status" in data
        assert "upstreams" in data
        assert "cache_entries" in data


class TestCacheIntegration:
    def test_cache_basic(self):
        from services import _cache
        _cache.set("test_key", {"hello": "world"}, 60)
        assert _cache.get("test_key") == {"hello": "world"}
        _cache.clear()
        assert _cache.get("test_key") is None
