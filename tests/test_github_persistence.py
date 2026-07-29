"""
tests/test_github_persistence.py

Tests para src/github_persistence.py -- la capa única de persistencia contra
GitHub Contents API que reemplazó 8 implementaciones casi idénticas
(tracker.py x2, monitor.py, backtester.py, macro_auto.py,
opportunities_log.py, trailing_stop.py, bot.py), cada una con su propia
copia del patrón GET-sha/PUT.

Por qué importa: el filesystem de Railway es efímero y se resetea en cada
redeploy (que ocurre en cada push a main, incluidos los automáticos del
propio pipeline). Si push_file/pull_file fallan de forma distinta a la
documentada -- no reintentan ante 409, explotan con KeyError en el primer
push de un archivo nuevo sin sha previo, o el sha usado en el reintento
queda viejo -- se pierde historia real entre redeploys. Es exactamente el
patrón que ya costó 36hs de caída más la pérdida silenciosa de
signals_history.json hasta junio 2026 (ver test_railway_config.py y
test_startup_sync_contract.py).
"""
import base64
import json
import time
from datetime import datetime, timedelta

import pytest
import requests

import src.github_persistence as gp


class FakeResponse:
    """Respuesta mínima compatible con la API de requests.Response que usa
    github_persistence (status_code, .json(), .text)."""

    def __init__(self, status_code, json_data=None, text=""):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.text = text

    def json(self):
        return self._json_data


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    """Los reintentos usan backoff exponencial real (1.5s, 2.25s, 3.375s...)
    -- sin mockear esto, un solo test de reintentos agotados tarda varios
    segundos y la suite se vuelve lenta rápido."""
    monkeypatch.setattr(time, "sleep", lambda seconds: None)


@pytest.fixture
def with_token(monkeypatch):
    monkeypatch.setenv("GH_TOKEN", "fake-token-for-tests")


# ── push_file ────────────────────────────────────────────────────────────

class TestPushFileGuards:

    def test_no_token_returns_false_without_network_call(self, monkeypatch, tmp_path):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        network_calls = []
        monkeypatch.setattr(requests, "get", lambda *a, **k: network_calls.append("get"))
        monkeypatch.setattr(requests, "put", lambda *a, **k: network_calls.append("put"))

        f = tmp_path / "x.json"
        f.write_text("{}")

        assert gp.push_file(str(f)) is False
        assert network_calls == []

    def test_missing_local_file_returns_false(self, with_token, monkeypatch, tmp_path):
        def fail_if_called(*a, **k):
            pytest.fail("push_file no debería llamar a la red si el archivo local no existe")

        monkeypatch.setattr(requests, "put", fail_if_called)
        assert gp.push_file(str(tmp_path / "no_existe.json")) is False


class TestPushFileHappyPath:

    def test_new_file_without_remote_sha_pushes_without_sha_key(self, with_token, monkeypatch, tmp_path):
        """Primera vez que se sube un archivo: GET del sha devuelve 404.
        push_file no debe explotar ni mandar sha=null -- debe omitir la
        clave 'sha' del payload."""
        f = tmp_path / "nuevo.json"
        f.write_text('{"a": 1}')

        captured_payloads = []
        monkeypatch.setattr(requests, "get", lambda *a, **k: FakeResponse(404))

        def fake_put(url, headers=None, json=None, timeout=None):
            captured_payloads.append(json)
            return FakeResponse(201)

        monkeypatch.setattr(requests, "put", fake_put)

        assert gp.push_file(str(f)) is True
        assert len(captured_payloads) == 1
        assert "sha" not in captured_payloads[0]
        assert "content" in captured_payloads[0]

    def test_existing_file_includes_sha_in_payload(self, with_token, monkeypatch, tmp_path):
        f = tmp_path / "existe.json"
        f.write_text('{"a": 1}')

        captured_payloads = []
        monkeypatch.setattr(requests, "get", lambda *a, **k: FakeResponse(200, {"sha": "abc123"}))

        def fake_put(url, headers=None, json=None, timeout=None):
            captured_payloads.append(json)
            return FakeResponse(200)

        monkeypatch.setattr(requests, "put", fake_put)

        assert gp.push_file(str(f)) is True
        assert captured_payloads[0]["sha"] == "abc123"


class TestPushFileRetryBehavior:

    def test_409_conflict_retries_with_fresh_sha_and_succeeds(self, with_token, monkeypatch, tmp_path):
        """Caso documentado en la arquitectura: sha desactualizado -> 409 ->
        hay que reintentar con un GET fresco, nunca reusar el sha viejo."""
        f = tmp_path / "x.json"
        f.write_text("{}")

        shas = iter(["sha-old", "sha-new"])
        monkeypatch.setattr(requests, "get", lambda *a, **k: FakeResponse(200, {"sha": next(shas)}))

        put_shas_used = []

        def fake_put(url, headers=None, json=None, timeout=None):
            put_shas_used.append(json["sha"])
            if len(put_shas_used) == 1:
                return FakeResponse(409, text="conflict")
            return FakeResponse(200)

        monkeypatch.setattr(requests, "put", fake_put)

        assert gp.push_file(str(f)) is True
        assert put_shas_used == ["sha-old", "sha-new"]

    def test_exhausts_retries_on_persistent_409(self, with_token, monkeypatch, tmp_path):
        f = tmp_path / "x.json"
        f.write_text("{}")
        monkeypatch.setattr(requests, "get", lambda *a, **k: FakeResponse(200, {"sha": "s"}))

        put_attempts = {"n": 0}

        def fake_put(*a, **k):
            put_attempts["n"] += 1
            return FakeResponse(409, text="conflict")

        monkeypatch.setattr(requests, "put", fake_put)

        assert gp.push_file(str(f)) is False
        assert put_attempts["n"] == gp.MAX_RETRIES

    def test_non_409_error_fails_fast_without_retrying(self, with_token, monkeypatch, tmp_path):
        """Comportamiento real actual (vale la pena dejarlo explícito): solo
        se reintenta ante 409 o excepción de red. Un 401/422/500 devuelve
        False en el primer intento, sin retry."""
        f = tmp_path / "x.json"
        f.write_text("{}")
        monkeypatch.setattr(requests, "get", lambda *a, **k: FakeResponse(200, {"sha": "s"}))

        put_attempts = {"n": 0}

        def fake_put(*a, **k):
            put_attempts["n"] += 1
            return FakeResponse(422, text="unprocessable")

        monkeypatch.setattr(requests, "put", fake_put)

        assert gp.push_file(str(f)) is False
        assert put_attempts["n"] == 1

    def test_network_exception_retries_and_recovers(self, with_token, monkeypatch, tmp_path):
        f = tmp_path / "x.json"
        f.write_text("{}")
        monkeypatch.setattr(requests, "get", lambda *a, **k: FakeResponse(200, {"sha": "s"}))

        put_attempts = {"n": 0}

        def flaky_put(*a, **k):
            put_attempts["n"] += 1
            if put_attempts["n"] < 2:
                raise requests.exceptions.ConnectionError("boom")
            return FakeResponse(200)

        monkeypatch.setattr(requests, "put", flaky_put)

        assert gp.push_file(str(f)) is True
        assert put_attempts["n"] == 2


# ── pull_file ────────────────────────────────────────────────────────────

class TestPullFile:

    def test_no_token_returns_false_without_network_call(self, monkeypatch, tmp_path):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        network_calls = []
        monkeypatch.setattr(requests, "get", lambda *a, **k: network_calls.append(1))

        assert gp.pull_file(str(tmp_path / "f.json")) is False
        assert network_calls == []

    def test_remote_missing_returns_false_without_raising(self, with_token, monkeypatch, tmp_path):
        monkeypatch.setattr(requests, "get", lambda *a, **k: FakeResponse(404))
        assert gp.pull_file(str(tmp_path / "f.json")) is False

    def test_success_writes_decoded_content_and_creates_parent_dirs(self, with_token, monkeypatch, tmp_path):
        """FIX 29/07/2026: pull_file() ahora prueba raw.githubusercontent.com
        primero (ver github_persistence.py) -- este test fuerza un miss ahí
        (404) para ejercitar específicamente el fallback a Contents API,
        que es lo que el test original quería cubrir."""
        content = json.dumps({"hola": "mundo"})
        b64_content = base64.b64encode(content.encode("utf-8")).decode("utf-8")

        def _fake_get(url, *a, **k):
            if "raw.githubusercontent.com" in url:
                return FakeResponse(404)
            return FakeResponse(200, {"content": b64_content})

        monkeypatch.setattr(requests, "get", _fake_get)

        target = tmp_path / "nested" / "dir" / "f.json"
        assert gp.pull_file(str(target)) is True
        assert json.loads(target.read_text()) == {"hola": "mundo"}

    def test_success_via_raw_githubusercontent_primary_path(self, with_token, monkeypatch, tmp_path):
        """FIX 29/07/2026 (incidente real): raw.githubusercontent.com es
        ahora la vía primaria porque la Contents API omite 'content' para
        archivos >1MB (signals_history.json, 1.27MB, confirmado en
        producción) -- este test cubre ese camino feliz específico, no solo
        el fallback."""
        payload = json.dumps({"hola": "mundo desde raw"})

        def _fake_get(url, *a, **k):
            assert "raw.githubusercontent.com" in url, "no debería llegar a la Contents API si raw responde 200"
            return FakeResponse(200, text=payload)

        monkeypatch.setattr(requests, "get", _fake_get)

        target = tmp_path / "f.json"
        assert gp.pull_file(str(target)) is True
        assert json.loads(target.read_text()) == {"hola": "mundo desde raw"}


# ── save_json / load_json ────────────────────────────────────────────────

class TestSaveAndLoadJson:

    def test_load_missing_file_returns_default(self, tmp_path):
        result = gp.load_json(str(tmp_path / "no_existe.json"), default={"x": 1})
        assert result == {"x": 1}

    def test_load_corrupt_json_returns_default_without_raising(self, tmp_path):
        """Un redeploy a mitad de escritura puede dejar un archivo truncado
        en data/ -- load_json no debe tirar abajo el pipeline por esto."""
        f = tmp_path / "corrupto.json"
        f.write_text("{ esto no es json valido ::: ")
        assert gp.load_json(str(f), default={}) == {}

    def test_save_json_writes_file_and_pushes_by_default(self, monkeypatch, tmp_path):
        pushed_paths = []
        monkeypatch.setattr(gp, "push_file", lambda path, message=None: pushed_paths.append(path) or True)

        target = tmp_path / "out.json"
        assert gp.save_json(str(target), {"a": 1}) is True
        assert json.loads(target.read_text()) == {"a": 1}
        assert pushed_paths == [str(target)]

    def test_save_json_with_push_false_skips_push(self, monkeypatch, tmp_path):
        pushed_paths = []
        monkeypatch.setattr(gp, "push_file", lambda path, message=None: pushed_paths.append(path) or True)

        target = tmp_path / "out.json"
        assert gp.save_json(str(target), {"a": 1}, push=False) is True
        assert pushed_paths == []


# ── append_by_date ───────────────────────────────────────────────────────

class TestAppendByDate:

    def test_overwrites_same_day_on_rerun(self, monkeypatch, tmp_path):
        """El pipeline corre 4x/día -- la 2da-4ta corrida del mismo día debe
        sobreescribir la entrada de hoy, no acumular duplicados."""
        monkeypatch.setattr(gp, "push_file", lambda *a, **k: True)
        path = str(tmp_path / "history.json")

        gp.append_by_date(path, "2026-06-24", [{"run": 1}], max_days=60)
        gp.append_by_date(path, "2026-06-24", [{"run": 2}], max_days=60)

        data = gp.load_json(path)
        assert data["2026-06-24"] == [{"run": 2}]

    def test_purges_entries_older_than_max_days(self, monkeypatch, tmp_path):
        monkeypatch.setattr(gp, "push_file", lambda *a, **k: True)
        path = str(tmp_path / "history.json")

        old_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        gp.append_by_date(path, old_date, [{"old": True}], max_days=60)
        gp.append_by_date(path, "2026-06-24", [{"new": True}], max_days=60)

        data = gp.load_json(path)
        assert old_date not in data
        assert "2026-06-24" in data

    def test_recent_entries_within_window_survive_purge(self, monkeypatch, tmp_path):
        monkeypatch.setattr(gp, "push_file", lambda *a, **k: True)
        path = str(tmp_path / "history.json")

        recent_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
        gp.append_by_date(path, recent_date, [{"recent": True}], max_days=60)
        gp.append_by_date(path, "2026-06-24", [{"new": True}], max_days=60)

        data = gp.load_json(path)
        assert recent_date in data


# ── sync_all_at_startup ──────────────────────────────────────────────────

class TestSyncAllAtStartup:

    def test_calls_pull_file_for_every_filename_with_data_prefix(self, monkeypatch):
        calls = []
        monkeypatch.setattr(gp, "pull_file", lambda path: calls.append(path) or True)

        gp.sync_all_at_startup(["a.json", "b.json"])

        assert sorted(calls) == ["data/a.json", "data/b.json"]

    def test_does_not_double_prefix_paths_already_under_data(self, monkeypatch):
        calls = []
        monkeypatch.setattr(gp, "pull_file", lambda path: calls.append(path) or True)

        gp.sync_all_at_startup(["data/already_prefixed.json"])

        assert calls == ["data/already_prefixed.json"]

    def test_propagates_exception_from_an_individual_pull(self, monkeypatch):
        """Característica actual (no necesariamente deseable, pero real): al
        usar ThreadPoolExecutor.map, si UNA descarga lanzara una excepción
        sin atraparla, list(ex.map(...)) la propaga al recolectar resultados
        -- aunque el resto de las descargas ya se haya ejecutado en paralelo.
        En la práctica pull_file atrapa toda excepción internamente y nunca
        debería llegar a este camino; este test fija el comportamiento
        actual para que cualquier cambio futuro a ese contrato sea una
        decisión consciente y no un efecto secundario de un refactor."""
        def maybe_fail(path):
            if "bad" in path:
                raise RuntimeError("boom")
            return True

        monkeypatch.setattr(gp, "pull_file", maybe_fail)

        with pytest.raises(RuntimeError):
            gp.sync_all_at_startup(["good.json", "bad.json"])
