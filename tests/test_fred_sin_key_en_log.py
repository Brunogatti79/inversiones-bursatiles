"""FIX 29/09/2026: el api_key de FRED no debe aparecer en el log."""
import logging

import requests

import src.macro_auto as ma


class _Resp:
    def raise_for_status(self):
        raise requests.HTTPError(
            "502 Server Error: Bad Gateway for url: https://api.stlouisfed.org/fred/series/"
            "observations?series_id=X&api_key=CLAVE_SECRETA_123&file_type=json")


def test_latest_y_yoy_no_loguean_la_key(monkeypatch, caplog):
    monkeypatch.setattr(ma.requests, "get", lambda *a, **k: _Resp())
    with caplog.at_level(logging.INFO):
        assert ma._fred_latest("X", api_key="CLAVE_SECRETA_123") == (None, None)
        assert ma._fred_latest("X", api_key="CLAVE_SECRETA_123", expected_gap=True) == (None, None)
        assert ma._fred_yoy("X", api_key="CLAVE_SECRETA_123") is None
    assert "CLAVE_SECRETA_123" not in caplog.text
    assert caplog.text.count("***") == 3
