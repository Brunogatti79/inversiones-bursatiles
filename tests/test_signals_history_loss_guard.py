"""
tests/test_signals_history_loss_guard.py

Incidente real en producción, 27/07/2026: 2 pushes de código muy seguidos
(9 minutos de diferencia) dispararon redeploys de Railway solapados. El
sync de arranque falló silenciosamente para signals_history.json en esa
ventana -- el pipeline arrancó con el archivo vacío, agregó solo el día de
hoy, y lo pusheó, PISANDO 18 días reales de historia con 1. Se recuperó a
mano desde el historial de git (estaban ahí de milagro), pero el código no
tenía ningún seguro contra que esto se repita.

Estos tests cubren el guard agregado en _push_signals_history_to_github():
comparar el conteo de días local contra lo que ya hay en GitHub antes de
pushear, y fusionar en vez de pisar si detecta una caída sospechosa.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json

import pytest

import src.tracker as tracker


@pytest.fixture(autouse=True)
def _isolate_history_file(tmp_path, monkeypatch):
    monkeypatch.setattr(tracker, "HISTORY_PATH", str(tmp_path / "signals_history.json"))
    # El guard reintenta fetch_remote_json con time.sleep() entre intentos
    # (endurecimiento 28/07/2026) -- sin esto, cada test que fuerce fallas
    # de verificación tardaría segundos reales de sleep.
    monkeypatch.setattr(tracker.time, "sleep", lambda *a, **kw: None)


def _write_local(path, days: dict):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(days, f)


class TestSignalsHistoryLossGuard:

    def test_replica_el_incidente_real_y_confirma_que_ahora_se_recupera(self, monkeypatch):
        """El escenario exacto que pasó en producción: local vacío salvo
        hoy, remoto con 18 días reales -- debe fusionar, no pisar."""
        remote_18_dias = {f"2026-07-{d:02d}": [{"ticker": "GGAL.BA"}] for d in range(9, 27)}
        local_solo_hoy = {"2026-07-27": [{"ticker": "GGAL.BA"}]}

        _write_local(tracker.HISTORY_PATH, local_solo_hoy)

        pushed = {}
        # push_file/fetch_remote_json se importan DENTRO de la función con
        # "from src.github_persistence import ..." -- hay que parchear el
        # módulo origen, no un atributo de tracker (no existe como import
        # de nivel de módulo).
        import src.github_persistence as ghp
        monkeypatch.setattr(ghp, "push_file", lambda *a, **kw: pushed.setdefault("called", (a, kw)))
        monkeypatch.setattr(ghp, "fetch_remote_json", lambda path: remote_18_dias)

        tracker._push_signals_history_to_github()

        with open(tracker.HISTORY_PATH, encoding="utf-8") as f:
            resultado = json.load(f)

        assert len(resultado) == 19  # 18 días viejos (9 al 26) + hoy (27, no estaba en remoto)
        assert "2026-07-09" in resultado  # no se perdió el día más viejo
        assert "2026-07-27" in resultado  # tampoco el de hoy
        assert pushed.get("called") is not None  # el push sí se intentó, con el merge ya escrito

    def test_diferencia_chica_no_dispara_el_merge(self, monkeypatch):
        """Una purga normal por max_days (60 días) puede tirar 1-2 días de
        diferencia entre corridas consecutivas -- eso es esperado, no debe
        disparar una fusión innecesaria."""
        remote = {f"2026-07-{d:02d}": [] for d in range(20, 28)}   # 8 días
        local  = {f"2026-07-{d:02d}": [] for d in range(21, 28)}   # 7 días (1 menos, normal)

        _write_local(tracker.HISTORY_PATH, local)

        import src.github_persistence as ghp
        monkeypatch.setattr(ghp, "push_file", lambda *a, **kw: None)
        monkeypatch.setattr(ghp, "fetch_remote_json", lambda path: remote)

        tracker._push_signals_history_to_github()

        with open(tracker.HISTORY_PATH, encoding="utf-8") as f:
            resultado = json.load(f)

        assert len(resultado) == 7  # sin fusión -- se pushea el local tal cual

    def test_si_no_se_puede_verificar_y_local_es_chico_no_pushea(self, monkeypatch):
        """RECURRENCIA real del incidente (28/07/2026): 65 minutos después
        de instalar el guard original, el historial volvió a colapsar de
        19 días a 1 -- confirmado con git log completo. La hipótesis más
        probable: fetch_remote_json también falló justo en ese momento
        (rate limit tras una ráfaga de pushes), y el comportamiento de
        entonces era pushear igual. Ahora, si no se puede verificar Y el
        local tiene menos de SUSPICIOUS_MIN_DAYS (6) días, NO se pushea --
        se prioriza perder un ciclo de datos nuevos por sobre arriesgar
        pisar historia real sin poder confirmarlo."""
        local = {"2026-07-27": [], "2026-07-28": []}  # 2 días -- sospechosamente poco
        _write_local(tracker.HISTORY_PATH, local)

        import src.github_persistence as ghp
        called = {}
        monkeypatch.setattr(ghp, "push_file", lambda *a, **kw: called.setdefault("ok", True))
        monkeypatch.setattr(ghp, "fetch_remote_json", lambda path: None)  # falla las 3 veces

        resultado_push = tracker._push_signals_history_to_github()

        assert resultado_push is False
        assert called.get("ok") is None  # el push NUNCA se intentó
        with open(tracker.HISTORY_PATH, encoding="utf-8") as f:
            assert json.load(f) == local  # el archivo local queda intacto, sin tocar

    def test_si_no_se_puede_verificar_pero_local_ya_es_sano_pushea_igual(self, monkeypatch):
        """Si el local YA tiene suficientes días (≥6), no tiene sentido
        bloquear la operación normal solo porque no se pudo hacer el doble
        chequeo esta vez -- eso sería sobre-corregir el incidente original
        y crear uno nuevo (nunca acumular historia si GitHub tiene un mal
        día)."""
        local = {f"2026-07-{d:02d}": [] for d in range(20, 28)}  # 8 días, sano
        _write_local(tracker.HISTORY_PATH, local)

        import src.github_persistence as ghp
        called = {}
        monkeypatch.setattr(ghp, "push_file", lambda *a, **kw: called.setdefault("ok", True))
        monkeypatch.setattr(ghp, "fetch_remote_json", lambda path: None)

        resultado_push = tracker._push_signals_history_to_github()

        assert resultado_push is True
        assert called.get("ok") is True

    def test_reintenta_fetch_antes_de_rendirse(self, monkeypatch):
        """La verificación se reintenta hasta 3 veces -- un solo hiccup de
        red no debería bastar para bloquear el push."""
        local = {"2026-07-27": [], "2026-07-28": []}
        _write_local(tracker.HISTORY_PATH, local)

        intentos = {"n": 0}
        remote_ok = {f"2026-07-{d:02d}": [] for d in range(1, 27)}

        def _fetch_falla_2_veces(path):
            intentos["n"] += 1
            if intentos["n"] < 3:
                return None  # falla las primeras 2 veces
            return remote_ok  # la 3ra vez sí responde

        import src.github_persistence as ghp
        monkeypatch.setattr(ghp, "push_file", lambda *a, **kw: None)
        monkeypatch.setattr(ghp, "fetch_remote_json", _fetch_falla_2_veces)

        resultado_push = tracker._push_signals_history_to_github()

        assert intentos["n"] == 3  # reintentó hasta lograrlo
        assert resultado_push is True  # y al final sí pudo verificar y pushear

    def test_local_mas_nuevo_gana_en_fechas_solapadas(self, monkeypatch):
        """Si una fecha existe en ambos lados (ej. el día de hoy, recién
        actualizado localmente), el local debe ganar -- es más reciente
        que lo que ya está publicado."""
        remote = {f"2026-07-{d:02d}": [{"n": "vieja"}] for d in range(9, 28)}  # incluye hoy, dato viejo
        local  = {"2026-07-27": [{"n": "nueva"}]}  # solo hoy, pero actualizado

        _write_local(tracker.HISTORY_PATH, local)

        import src.github_persistence as ghp
        monkeypatch.setattr(ghp, "push_file", lambda *a, **kw: None)
        monkeypatch.setattr(ghp, "fetch_remote_json", lambda path: remote)

        tracker._push_signals_history_to_github()

        with open(tracker.HISTORY_PATH, encoding="utf-8") as f:
            resultado = json.load(f)

        assert resultado["2026-07-27"] == [{"n": "nueva"}]
        assert len(resultado) == 19
