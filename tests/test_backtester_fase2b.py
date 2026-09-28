"""
tests/test_backtester_fase2b.py — Fase 2b (28/09/2026)

Cubre:
  - _resolve_entry: entrada del CSV (no del precio archivado), fallback y
    reescalado de stops/targets
  - Caso real de dividendo retroactivo (TRAN.BA): el trade ya no registra
    el dividendo como pérdida
  - _ret_neto / _exceso: costos multiplicativos y alpha invariante a moneda
  - _racha_starts: apertura de rachas (COMPRA y COMPRA FUERTE = misma racha)
  - _BenchmarkContext: índice y equiponderado, mínimo de tickers
  - _benchmark_summary: estructura por mercado / día / racha
  - Override de costo por variable de entorno
Ningún test escribe en data/.
"""
import os
import sys
import unittest
from unittest import mock

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.backtester import (
    _resolve_entry, _ret_neto, _exceso, _racha_starts, _BenchmarkContext,
    _build_trades, _benchmark_summary, _costo_pata_pct, _entry_audit,
)


def _dates(n, start="2026-07-01"):
    return [d.strftime("%Y-%m-%d") for d in pd.bdate_range(start, periods=n)]


class TestResolveEntry(unittest.TestCase):
    def test_usa_csv_si_existe(self):
        pi = {"X": {"2026-07-01": 84.5, "2026-07-02": 85.0}}
        p, src, f = _resolve_entry("X", "2026-07-01", 100.0, pi)
        self.assertEqual((p, src), (84.5, "csv"))
        self.assertAlmostEqual(f, 0.845)

    def test_usa_ultimo_cierre_previo(self):
        pi = {"X": {"2026-06-30": 90.0, "2026-07-03": 95.0}}
        p, src, _ = _resolve_entry("X", "2026-07-01", 91.0, pi)
        self.assertEqual((p, src), (90.0, "csv"))

    def test_fallback_archivo(self):
        p, src, f = _resolve_entry("X", "2026-07-01", 100.0, {"X": {"2026-07-05": 1.0}})
        self.assertEqual((p, src, f), (100.0, "archivo", 1.0))
        p, src, f = _resolve_entry("NOPE", "2026-07-01", 100.0, {})
        self.assertEqual(src, "archivo")


class TestDividendoRetroactivo(unittest.TestCase):
    """TRAN.BA real: archivado 3745 el 27/08; CSV ajustado 3166 (factor
    1,183). Sin el fix, un precio futuro plano del CSV daba -15,5%."""

    def setUp(self):
        self.dates = _dates(30, "2026-08-03")
        self.pi = {"TRAN.BA": {d: 3166.0 for d in self.dates}}
        self.history = {self.dates[0]: [{
            "ticker": "TRAN.BA", "mercado": "MERVAL", "signal_v2": "🟢 COMPRA",
            "precio": 3745.0, "atr_stop": 3600.0, "atr_target": 3900.0,
        }]}

    def test_retorno_no_incluye_dividendo_como_perdida(self):
        t = _build_trades(self.history, self.dates, self.pi)[0]
        self.assertEqual(t["entry_source"], "csv")
        self.assertAlmostEqual(t["ret_21d"], 0.0)
        self.assertEqual(t["precio_archivo"], 3745.0)

    def test_stops_reescalados_mantienen_distancia_pct(self):
        t = _build_trades(self.history, self.dates, self.pi)[0]
        # stop a -3,87% y target a +4,14% de la entrada, en ambas escalas:
        # precio plano -> no toca ninguno -> salida por tiempo
        self.assertEqual(t["st_exit_type"], "time")


class TestCostosYExceso(unittest.TestCase):
    def test_ret_neto_cero_bruto(self):
        # 1,21% ida y vuelta aprox.
        self.assertAlmostEqual(_ret_neto(0.0, 0.605), -1.2027, places=3)

    def test_ret_neto_sin_costo(self):
        self.assertAlmostEqual(_ret_neto(5.0, 0.0), 5.0)

    def test_exceso_invariante_a_moneda(self):
        # misma devaluación aplicada a señal e índice -> mismo alpha
        r, b, fx = 4.0, 1.0, 1.10
        a_local = _exceso(r, b)
        a_usd = _exceso(((1 + r / 100) / fx - 1) * 100, ((1 + b / 100) / fx - 1) * 100)
        self.assertAlmostEqual(a_local, a_usd)

    def test_costo_env_override(self):
        with mock.patch.dict(os.environ, {"BACKTEST_COSTO_PATA_PCT": "0,4"}):
            self.assertEqual(_costo_pata_pct("MERVAL"), 0.4)
        with mock.patch.dict(os.environ, {"BACKTEST_COSTO_PATA_PCT": "basura"}):
            self.assertEqual(_costo_pata_pct("SP500"), 0.605)


class TestRachas(unittest.TestCase):
    def test_compra_y_compra_fuerte_misma_racha(self):
        d = _dates(4)
        h = {
            d[0]: [{"ticker": "A", "signal_v2": "🟢 COMPRA"}],
            d[1]: [{"ticker": "A", "signal_v2": "⭐ COMPRA FUERTE"}],
            d[2]: [{"ticker": "A", "signal_v2": "🟡 NEUTRAL/ESPERAR"}],
            d[3]: [{"ticker": "A", "signal_v2": "🟢 COMPRA"}],
        }
        s = _racha_starts(h, d)
        self.assertEqual(s, {(d[0], "A"), (d[2], "A"), (d[3], "A")})

    def test_ausencia_corta_la_racha(self):
        d = _dates(3)
        h = {d[0]: [{"ticker": "A", "signal_v2": "🟢 COMPRA"}], d[1]: [],
             d[2]: [{"ticker": "A", "signal_v2": "🟢 COMPRA"}]}
        self.assertIn((d[2], "A"), _racha_starts(h, d))


def _price_data(dates, n_tickers=6, idx_growth=0.001, stock_growth=0.002):
    idx = pd.to_datetime(dates)
    cols = {f"T{i}": 100 * (1 + stock_growth) ** np.arange(len(dates)) for i in range(n_tickers)}
    cols["INDICE MERVAL"] = 1000 * (1 + idx_growth) ** np.arange(len(dates))
    return {"merval": pd.DataFrame(cols, index=idx)}


class TestBenchmark(unittest.TestCase):
    def setUp(self):
        from src.backtester import _build_price_index
        self.dates = _dates(40)
        self.pdata = _price_data(self.dates)
        self.pi = _build_price_index(self.pdata, {})
        self.bench = _BenchmarkContext(self.pdata, {}, self.pi)

    def test_contexto_detecta_indice_y_universo(self):
        self.assertEqual(self.bench.index_key["MERVAL"], "INDICE MERVAL")
        self.assertEqual(len(self.bench.universe["MERVAL"]), 6)

    def test_retornos_benchmark(self):
        r_idx, r_ew, n = self.bench.get("MERVAL", self.dates[0], 21)
        self.assertAlmostEqual(r_idx, ((1.001) ** 21 - 1) * 100, places=6)
        self.assertAlmostEqual(r_ew, ((1.002) ** 21 - 1) * 100, places=6)
        self.assertEqual(n, 6)

    def test_equiponderado_requiere_minimo_tickers(self):
        from src.backtester import _build_price_index
        pdata = _price_data(self.dates, n_tickers=3)
        b = _BenchmarkContext(pdata, {}, _build_price_index(pdata, {}))
        r_idx, r_ew, n = b.get("MERVAL", self.dates[0], 21)
        self.assertIsNotNone(r_idx)
        self.assertIsNone(r_ew)

    def test_trades_y_resumen(self):
        h = {d: [{"ticker": "T0", "mercado": "MERVAL", "signal_v2": "🟢 COMPRA",
                  "precio": 999.0}] for d in self.dates[:10]}
        trades = _build_trades(h, self.dates, self.pi, bench=self.bench)
        t = trades[0]
        self.assertAlmostEqual(t["alpha_ew_21d_neto"],
                               _exceso(t["ret_21d_neto"], t["ret_ew_21d"]), delta=0.02)
        self.assertLess(t["alpha_ew_21d_neto"], 0)  # misma acción que el EW, paga costos
        self.assertTrue(trades[0]["racha_inicio"])
        self.assertFalse(trades[1]["racha_inicio"])
        s = _benchmark_summary(trades)
        comp = s["by_market"]["MERVAL"]["compra"]
        self.assertEqual(comp["racha"]["n"], 1)
        self.assertEqual(comp["dia"]["n"], len([x for x in trades if x["ret_21d"] is not None]))
        self.assertIn("ventanas_independientes", comp["dia"])
        audit = _entry_audit(trades)
        self.assertEqual(audit["por_fuente"], {"csv": len(trades)})

    def test_sin_bench_campos_en_none(self):
        h = {self.dates[0]: [{"ticker": "T0", "mercado": "MERVAL", "signal_v2": "🟢 COMPRA", "precio": 100.0}]}
        t = _build_trades(h, self.dates, self.pi)[0]
        self.assertIsNone(t["ret_ew_21d"])
        self.assertIsNone(t["alpha_idx_21d_neto"])
        self.assertIsNotNone(t["ret_21d_neto"])


if __name__ == "__main__":
    unittest.main()


class TestBenchmarkFuenteArchivo(unittest.TestCase):
    """El bloque benchmark usa signals_archive si es al menos tan largo como
    el historial; si no, cae a signals_history. El resto no cambia."""

    def _run(self, archivo_hist):
        import json, tempfile
        from src import backtester as bt
        dates = _dates(40)
        pdata = _price_data(dates)
        hist = {d: [{"ticker": "T0", "mercado": "MERVAL", "signal_v2": "🟢 COMPRA",
                     "precio": 100.0}] for d in dates[:12]}
        with tempfile.TemporaryDirectory() as tmp:
            hp, rp = f"{tmp}/h.json", f"{tmp}/r.json"
            json.dump(hist, open(hp, "w"))
            with mock.patch.object(bt, "HISTORY_PATH", hp), \
                 mock.patch.object(bt, "RESULTS_PATH", rp), \
                 mock.patch.object(bt, "_push_backtest_to_github", lambda: None), \
                 mock.patch.object(bt, "_detect_pattern_discoveries", lambda *a, **k: []), \
                 mock.patch.object(bt, "_load_archive_as_history", lambda: archivo_hist(dates)), \
                 mock.patch("os.makedirs"):
                return bt.run_backtest(pdata, {})

    def test_usa_archivo_si_es_mas_largo(self):
        def arch(dates):
            return {d: [{"ticker": "T1", "mercado": "MERVAL", "signal_v2": "🟢 COMPRA",
                         "precio": 100.0}] for d in dates[:15]}
        r = self._run(arch)
        self.assertEqual(r["benchmark"]["fuente"], "signals_archive")
        self.assertEqual(r["benchmark"]["fechas_fuente"], 15)
        # el resto sigue sobre signals_history (T0)
        self.assertEqual(r["top_performers"][0]["ticker"] if r["top_performers"] else "T0", "T0")

    def test_cae_a_history_si_archivo_corto(self):
        r = self._run(lambda dates: {dates[0]: [{"ticker": "T1", "mercado": "MERVAL",
                                                  "signal_v2": "🟢 COMPRA", "precio": 100.0}]})
        self.assertEqual(r["benchmark"]["fuente"], "signals_history")
