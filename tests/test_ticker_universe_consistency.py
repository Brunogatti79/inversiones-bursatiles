"""
tests/test_ticker_universe_consistency.py

Prioridad 5 del orden acordado (confirmar el universo de 78 tickers, 25/06/2026).

Sin acceso a Yahoo Finance desde este entorno (red restringida a registries
de paquetes + GitHub) no se puede confirmar que los 78 tickers descarguen
datos limpios en la próxima corrida real -- eso queda pendiente de
observar en el primer run de producción. Lo que SÍ se puede confirmar
ahora, y es lo que cubre este archivo:

  1. El universo total es 78 (22 MERVAL + 25 BOVESPA + 31 SP500/CEDEARs/ETFs),
     no ~67-70 como documentaban v4.0-v7.0 -- fijado como regresión.
  2. Los 78 tienen sector mapeado en SECTOR_MAP (analyzer.py) -- ya estaba
     OK, se fija como regresión para que una futura adición no lo rompa
     silenciosamente.
  3. Cobertura real de datos fundamentales en ratios_consolidado_quant.csv
     -- hallazgo de este relevamiento: 14 tickers sin fila o con fila
     vacía. De esos, 3 son ETFs (COPX/IBB/EWZ) donde la ausencia de P/E
     es CORRECTA (los ETFs no tienen utilidad neta propia que reportar).
     Los otros 11 son gaps reales: 5 acciones individuales que estaban
     desde antes (MELI, RIO, PBR, QCOM, GLOB) + 6 de los 8 tickers nuevos
     de esta sesión (YPFD.BA, BBAR.BA, B3SA3.SA, ITSA4.SA, SANB11.SA,
     VIVT3.SA sin fila; EMBR3.SA y JBSS3.SA con fila pero vacía) + 8
     tickers BOVESPA/MERVAL preexistentes también vacíos (SUPV.BA,
     TECO2.BA, COME.BA, HARG.BA, MOLI.BA, RAIZ4.SA, HAPV3.SA, CSNA3.SA).
     Esto es una tarea de carga de datos manual (Bruno), no un bug de
     código -- el test documenta el set ACTUAL de gaps conocidos para que
     una nueva adición sin datos se note como gap NUEVO, no se pierda en
     el ruido de los 11 ya conocidos.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ast
import csv

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _ast_dict_keys(filepath, varname):
    """Extrae las claves de un dict module-level por AST, sin importar el
    módulo (downloader.py requiere yfinance; esto evita la dependencia)."""
    with open(filepath, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == varname:
                    return [k.value for k in node.value.keys if isinstance(k, ast.Constant)]
    raise ValueError(f"No se encontró {varname} en {filepath}")


def _all_tickers():
    downloader_path = os.path.join(REPO_ROOT, "src", "downloader.py")
    merval  = _ast_dict_keys(downloader_path, "MERVAL_TICKERS")
    bovespa = _ast_dict_keys(downloader_path, "BOVESPA_TICKERS")
    sp500   = _ast_dict_keys(downloader_path, "SP500_TICKERS")
    return merval, bovespa, sp500


# ETFs conocidos sin fundamentals de empresa individual -- ausencia de
# P/E/ROE/etc. es CORRECTA, no un gap a resolver.
ETFS_SIN_FUNDAMENTALS = {"COPX", "IBB", "EWZ"}

# Gaps reales conocidos al momento de este relevamiento (25/06/2026) --
# si esta lista deja de coincidir con la realidad del CSV, el test de abajo
# avisa explícitamente qué cambió (se resolvió un gap viejo, o apareció uno
# nuevo) en vez de fallar en silencio o ignorar el problema para siempre.
GAPS_FUNDAMENTALES_CONOCIDOS = {
    "MELI", "RIO", "PBR", "QCOM", "GLOB",                          # preexistentes, no ETF
    "YPFD.BA", "BBAR.BA", "B3SA3.SA", "ITSA4.SA",
    "SANB11.SA", "VIVT3.SA", "EMBR3.SA", "JBSS3.SA",                # nuevos de esta sesión
    "SUPV.BA", "TECO2.BA", "COME.BA", "HARG.BA", "MOLI.BA",
    "RAIZ4.SA", "HAPV3.SA", "CSNA3.SA",                             # preexistentes, no ETF
}


class TestTickerUniverseCount:

    def test_merval_count(self):
        merval, _, _ = _all_tickers()
        assert len(merval) == 22

    def test_bovespa_count(self):
        _, bovespa, _ = _all_tickers()
        assert len(bovespa) == 25

    def test_sp500_count(self):
        _, _, sp500 = _all_tickers()
        assert len(sp500) == 31

    def test_total_is_78(self):
        merval, bovespa, sp500 = _all_tickers()
        assert len(merval) + len(bovespa) + len(sp500) == 78

    def test_new_tickers_present(self):
        """Los 8 tickers agregados sin que ninguna doc de arquitectura
        (v4-v7) los reflejara -- fijados explícitamente para que una
        futura limpieza no los borre sin querer creyendo que son ruido."""
        merval, bovespa, _ = _all_tickers()
        for t in ["YPFD.BA", "BBAR.BA"]:
            assert t in merval
        for t in ["B3SA3.SA", "EMBR3.SA", "JBSS3.SA", "ITSA4.SA", "SANB11.SA", "VIVT3.SA"]:
            assert t in bovespa

    def test_no_duplicate_tickers_across_markets(self):
        merval, bovespa, sp500 = _all_tickers()
        all_t = merval + bovespa + sp500
        assert len(all_t) == len(set(all_t))


class TestSectorMapCoverage:

    def test_every_ticker_has_sector_mapped(self):
        analyzer_path = os.path.join(REPO_ROOT, "src", "analyzer.py")
        sector_keys = set(_ast_dict_keys(analyzer_path, "SECTOR_MAP"))
        merval, bovespa, sp500 = _all_tickers()
        missing = set(merval + bovespa + sp500) - sector_keys
        assert not missing, f"Tickers sin sector mapeado: {sorted(missing)}"


class TestFundamentalDataCoverage:

    def _load_csv_tickers_with_data(self):
        path = os.path.join(REPO_ROOT, "data", "ratios_consolidado_quant.csv")
        with open(path, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=";")
            rows = {r["Ticker"]: r for r in reader}
        con_dato = {t for t, r in rows.items() if (r.get("P/E (trailing)") or "").strip()}
        return set(rows.keys()), con_dato

    def test_known_gaps_are_exactly_documented(self):
        """Si esto falla, cambió el set de gaps reales -- leer el mensaje:
        o se resolvió uno (¡bien!, sacarlo de GAPS_FUNDAMENTALES_CONOCIDOS)
        o aparece uno nuevo no documentado (revisar antes de ignorar)."""
        merval, bovespa, sp500 = _all_tickers()
        all_tickers = set(merval + bovespa + sp500)

        _, con_dato = self._load_csv_tickers_with_data()
        sin_dato_real = all_tickers - con_dato - ETFS_SIN_FUNDAMENTALS

        nuevos_no_documentados = sin_dato_real - GAPS_FUNDAMENTALES_CONOCIDOS
        resueltos = GAPS_FUNDAMENTALES_CONOCIDOS - sin_dato_real

        assert not nuevos_no_documentados, (
            f"Gaps fundamentales NUEVOS sin documentar: {sorted(nuevos_no_documentados)}"
        )
        assert not resueltos, (
            f"Estos gaps ya tienen datos -- actualizar GAPS_FUNDAMENTALES_CONOCIDOS "
            f"sacándolos de la lista: {sorted(resueltos)}"
        )

    def test_etfs_legitimately_lack_fundamentals(self):
        """Confirma que los 3 ETFs siguen sin P/E -- si alguna vez Yahoo
        empezara a reportarles algo, está OK que este test falle y haya
        que revisar (no se rompe nada, solo deja de ser un caso especial)."""
        _, con_dato = self._load_csv_tickers_with_data()
        assert ETFS_SIN_FUNDAMENTALS.isdisjoint(con_dato)
