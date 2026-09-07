"""
scripts/diagnostico_trades_racha.py

DIAGNÓSTICO OFICIAL (shadow) -- NO toca producción, no modifica
backtester.py ni pipeline.py, no afecta señales/Kelly/capital. Solo lee
signals_history.json + CSVs de precio y compara dos formas de contar
"trades":

  (A) POR DÍA (metodología actual de _build_trades() en backtester.py):
      una señal COMPRA activa 15 días consecutivos = 15 "trades" con
      ventanas h21d superpuestas entre sí.

  (B) POR RACHA (propuesto en revisión externa sobre v22 §6.2, confirmado
      con datos reales 07/09/2026 -- ver data/trades_racha_diagnostic.json):
      esa misma señal de 15 días = 1 sola "trade", contada desde el
      primer día de la racha (que es cuando un trader real habría
      entrado), usando la MISMA lógica de cálculo de retorno que (A)
      (reusa _get_future_prices/_calc_stop_target_exit/_detect_split_horizon
      de backtester.py para que el número sea comparable, no una
      metodología paralela inventada).

DECISIÓN 07/09/2026 (Bruno + revisión externa, ambas coincidieron): NO se
hace el cambio metodológico completo en backtester.py todavía -- eso
sigue siendo §6.2, pendiente de go/no-go porque afecta TODAS las métricas
derivadas. Mientras tanto, este diagnóstico se persiste en
data/trades_racha_diagnostic.json como métrica paralela oficial, para:
  1. Convivir con ambas lecturas (día y racha) sin reescribir el sistema.
  2. Cuando llegue el checkpoint de market_exposure_shadow (~23-25/09,
     ver v22 §6.1), evaluarlo TAMBIÉN por racha -- si mejora el EV por
     racha (no solo por día), es evidencia mucho más sólida que si solo
     mejora el EV por día (que puede estar inflado por las mismas rachas
     largas que motivan este diagnóstico).

Hallazgo de la corrida inicial (07/09/2026, 61 días de historia): contar
por racha reduce la muestra de COMPRA un 84.8% (540 -> 82 trades reales).
MERVAL se sostiene significativo con la muestra real (n=39). BOVESPA y
SP500 pierden significancia estadística (n=22 y n=21 respectivamente) --
la dirección se mantiene pero ya no se puede afirmar con confianza que no
sea ruido. Esto NO invalida el fix de apply_prediction_override() (basado
en un corte distinto, desacuerdo-vs-resto dentro de cada mercado), pero sí
templa la certeza de "SP500 gana, MERVAL/BOVESPA pierden" tal como se
venía repitiendo -- ver v22 §1, a corregir en la próxima revisión del
documento con esta salvedad.

Uso:
    python -m scripts.diagnostico_trades_racha            # corre + persiste + pushea
    python -m scripts.diagnostico_trades_racha --no-push  # corre + persiste local, no pushea
"""

import argparse
import sys
import os
import json
import collections
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.downloader import _load_csv, MERVAL_TICKERS, BOVESPA_TICKERS, SP500_TICKERS
from src.backtester import (
    _build_price_index, _build_trades, _metrics_from_rets,
    _get_future_prices, _calc_stop_target_exit, _detect_split_horizon,
    HORIZONS,
)

HISTORY_PATH = "data/signals_history.json"
OUTPUT_PATH = "data/trades_racha_diagnostic.json"


def _is_compra(signal: str) -> bool:
    return "COMPRA" in (signal or "")


def build_trades_by_racha(history: dict, sorted_dates: list, price_index: dict) -> list:
    """
    Misma idea que _build_trades() pero colapsa rachas consecutivas de
    señal COMPRA (cualquier variante: 🟢 COMPRA / ⭐ COMPRA FUERTE) del
    mismo ticker en UN solo trade, contado desde el primer día de la
    racha. Reusa las funciones canónicas de backtester.py para el cálculo
    de retorno -- no reinventa la metodología, solo cambia el conteo.
    """
    evaluation_dates = sorted_dates[:-5] if len(sorted_dates) > 5 else []
    eval_set = set(evaluation_dates)

    # Estado por ticker: racha abierta o no, y desde cuándo
    open_streaks: dict[str, dict] = {}  # ticker -> {"start_date":..., "entry": {...}}
    trades = []

    def _close_streak(ticker, streak):
        """Cierra una racha: si el día de inicio es evaluable, construye el
        trade con la misma lógica que _build_trades()."""
        start_date = streak["start_date"]
        if start_date not in eval_set:
            return
        entry = streak["entry"]
        precio_entry = float(entry.get("precio", 0) or 0)
        if precio_entry <= 0:
            return
        atr_stop = float(entry.get("atr_stop", 0) or 0)
        atr_target = float(entry.get("atr_target", 0) or 0)

        future_prices = _get_future_prices(ticker, start_date, price_index, max_horizon=25)
        if not future_prices:
            return
        split_idx = _detect_split_horizon(precio_entry, future_prices)
        if split_idx < len(future_prices):
            future_prices = future_prices[:split_idx]

        trade = {
            "ticker": ticker,
            "mercado": entry.get("mercado", ""),
            "sector": entry.get("sector", "GENERAL"),
            "signal": entry.get("signal_v2") or entry.get("signal", ""),
            "signal_date": start_date,
            "racha_dias": streak["dias"],
            "precio_entry": precio_entry,
            "confidence_label": entry.get("confidence_label", "") or "UNKNOWN",
        }
        for h in HORIZONS:
            if len(future_prices) >= h:
                exit_price = future_prices[h - 1]
                trade[f"ret_{h}d"] = round((exit_price / precio_entry - 1) * 100, 2)
            else:
                trade[f"ret_{h}d"] = None
        st = _calc_stop_target_exit(future_prices, precio_entry, atr_stop, atr_target, max_days=21)
        trade.update(st)
        trades.append(trade)

    for date in sorted_dates:
        entries_today = {e.get("ticker"): e for e in history.get(date, []) if e.get("ticker")}

        # Cerrar rachas de tickers que hoy ya no están en COMPRA (o desaparecieron)
        for ticker in list(open_streaks.keys()):
            entry_hoy = entries_today.get(ticker)
            sigue_en_compra = entry_hoy is not None and _is_compra(
                entry_hoy.get("signal_v2") or entry_hoy.get("signal", "")
            )
            if not sigue_en_compra:
                _close_streak(ticker, open_streaks.pop(ticker))
            else:
                open_streaks[ticker]["dias"] += 1

        # Abrir rachas nuevas
        for ticker, entry in entries_today.items():
            sig = entry.get("signal_v2") or entry.get("signal", "")
            if _is_compra(sig) and ticker not in open_streaks:
                open_streaks[ticker] = {"start_date": date, "entry": entry, "dias": 1}

    # Cerrar rachas que seguían abiertas al final de la historia
    for ticker, streak in open_streaks.items():
        _close_streak(ticker, streak)

    return trades


def build_comparison_summary(trades_dia: list, trades_racha: list) -> dict:
    """Arma el dict estructurado que se persiste en OUTPUT_PATH."""
    compras_dia = [t for t in trades_dia if _is_compra(t.get("signal", "")) and t.get("ret_21d") is not None]
    compras_racha = [t for t in trades_racha if t.get("ret_21d") is not None]

    por_mercado_dia = collections.defaultdict(list)
    por_mercado_racha = collections.defaultdict(list)
    for t in compras_dia:
        por_mercado_dia[t["mercado"]].append(t["ret_21d"])
    for t in compras_racha:
        por_mercado_racha[t["mercado"]].append(t["ret_21d"])

    by_market = {}
    for m in ["MERVAL", "BOVESPA", "SP500"]:
        md = _metrics_from_rets(por_mercado_dia.get(m, []))
        mr = _metrics_from_rets(por_mercado_racha.get(m, []))
        by_market[m] = {"por_dia": md, "por_racha": mr}

    largos = collections.Counter(t["racha_dias"] for t in trades_racha)

    return {
        "generated": datetime.now().isoformat(),
        "n_compras_por_dia": len(compras_dia),
        "n_compras_por_racha": len(compras_racha),
        "reduccion_pct": round(100 * (1 - len(compras_racha) / len(compras_dia)), 1) if compras_dia else None,
        "by_market": by_market,
        "distribucion_largo_racha": {str(k): v for k, v in sorted(largos.items())},
    }


def _print_comparison(trades_dia: list, trades_racha: list):
    print("=" * 78)
    print("DIAGNÓSTICO: trades por día vs. trades por racha de señal COMPRA")
    print("(shadow -- no afecta producción)")
    print("=" * 78)

    compras_dia = [t for t in trades_dia if _is_compra(t.get("signal", "")) and t.get("ret_21d") is not None]
    compras_racha = [t for t in trades_racha if t.get("ret_21d") is not None]

    print(f"\nTotal trades COMPRA con ret_21d materializado:")
    print(f"  Por día:   {len(compras_dia)}")
    print(f"  Por racha: {len(compras_racha)}  "
          f"(reducción: {100*(1 - len(compras_racha)/len(compras_dia)):.1f}%)" if compras_dia else "")

    print(f"\n{'Mercado':10}{'n_día':>8}{'EV_día':>10}{'sig_día':>9}"
          f"{'n_racha':>10}{'EV_racha':>11}{'sig_racha':>11}")
    print("-" * 78)

    por_mercado_dia = collections.defaultdict(list)
    por_mercado_racha = collections.defaultdict(list)
    for t in compras_dia:
        por_mercado_dia[t["mercado"]].append(t["ret_21d"])
    for t in compras_racha:
        por_mercado_racha[t["mercado"]].append(t["ret_21d"])

    for m in ["MERVAL", "BOVESPA", "SP500"]:
        md = _metrics_from_rets(por_mercado_dia.get(m, []))
        mr = _metrics_from_rets(por_mercado_racha.get(m, []))
        nd = len(por_mercado_dia.get(m, []))
        nr = len(por_mercado_racha.get(m, []))
        ev_d = f'{md["expected_value"]:.2f}%' if md else "—"
        ev_r = f'{mr["expected_value"]:.2f}%' if mr else "—"
        sig_d = str(md["significativo_95"]) if md else "—"
        sig_r = str(mr["significativo_95"]) if mr else "—"
        print(f"{m:10}{nd:>8}{ev_d:>10}{sig_d:>9}{nr:>10}{ev_r:>11}{sig_r:>11}")

    # Distribución de largo de racha -- para entender cuánto pesa la
    # pseudoreplicación real (rachas de 1 día no inflan nada)
    largos = collections.Counter(t["racha_dias"] for t in trades_racha)
    print(f"\nDistribución de largo de racha (días consecutivos en COMPRA):")
    for largo in sorted(largos.keys())[:15]:
        print(f"  {largo:>3} día(s): {largos[largo]} rachas")
    if any(l > 15 for l in largos):
        print(f"  >15 días: {sum(c for l, c in largos.items() if l > 15)} rachas")


def main():
    parser = argparse.ArgumentParser(description="Diagnóstico por racha vs por día (shadow, no toca producción)")
    parser.add_argument("--no-push", action="store_true", help="Persiste local pero no pushea a GitHub")
    args = parser.parse_args()

    with open(HISTORY_PATH) as f:
        history = json.load(f)
    sorted_dates = sorted(history.keys())

    merval_df = _load_csv("merval", "data")
    bovespa_df = _load_csv("bovespa", "data")
    sp500_df = _load_csv("sp500", "data")
    price_data = {"merval": merval_df, "bovespa": bovespa_df, "sp500": sp500_df}
    ticker_cols = {}
    ticker_cols.update(MERVAL_TICKERS)
    ticker_cols.update(BOVESPA_TICKERS)
    ticker_cols.update(SP500_TICKERS)
    price_index = _build_price_index(price_data, ticker_cols)

    trades_dia = _build_trades(history, sorted_dates, price_index)
    trades_racha = build_trades_by_racha(history, sorted_dates, price_index)

    _print_comparison(trades_dia, trades_racha)

    summary = build_comparison_summary(trades_dia, trades_racha)
    from src.github_persistence import save_json
    save_json(
        OUTPUT_PATH, summary,
        message=f"auto: trades_racha_diagnostic {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        push=not args.no_push,
    )
    print(f"\nPersistido en {OUTPUT_PATH}" + (" (sin pushear)" if args.no_push else " y pusheado a GitHub"))
    return summary


if __name__ == "__main__":
    main()
