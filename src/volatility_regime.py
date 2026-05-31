"""
src/volatility_regime.py — Régimen de Volatilidad Global

Detecta si el mercado está en régimen de baja, normal o alta volatilidad
y expone ese contexto para que lo usen:
  • exit_model.py  → multiplicadores de stop más anchos/estrechos
  • portfolio_optimizer.py → escalar Kelly según incertidumbre
  • confidence_score.py  → penalizar señales en alta vol
  • predictor.py         → feature adicional al GBR

LÓGICA:
  1. Calcula volatilidad realizada de los 3 índices (ventana 20d)
  2. La compara con su percentil histórico (12 meses)
  3. Determina régimen por mercado y régimen global

UMBRALES (percentil sobre historia 12m):
  LOW_VOL:    percentil < 30  → vol realizada baja vs historia
  NORMAL_VOL: percentil 30-70
  HIGH_VOL:   percentil > 70  → vol realizada alta vs historia

OUTPUT (por llamada):
  {
    "global_regime": "LOW" | "NORMAL" | "HIGH",
    "global_vol_score": 45.2,   # 0-100, mayor = más volátil
    "MERVAL": {"regime": "HIGH", "vol_ann": 38.5, "percentile": 82},
    "BOVESPA": {"regime": "NORMAL", "vol_ann": 22.1, "percentile": 55},
    "SP500":   {"regime": "LOW",    "vol_ann": 12.3, "percentile": 25},
    "regime_factor": 0.80,   # multiplicador directo para Kelly / stops
    "generated": "2026-05-31T..."
  }

USO desde pipeline.py:
    from src.volatility_regime import compute_volatility_regime
    vol_regime = compute_volatility_regime(price_data, index_cols)
"""

import json
import os
import logging
import numpy as np
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

VOL_REGIME_PATH = "data/volatility_regime.json"
VOL_WINDOW      = 20    # días para vol realizada
HIST_WINDOW     = 252   # días de historia para percentil


# ── Entrypoint principal ────────────────────────────────────────────────

def compute_volatility_regime(price_data: dict, index_cols: dict) -> dict:
    """
    Calcula el régimen de volatilidad global y por mercado.

    Args:
        price_data:  {"merval": df, "bovespa": df, "sp500": df}
        index_cols:  {"merval": col, "bovespa": col, "sp500": col}
    """
    fallback = {
        "global_regime": "NORMAL",
        "global_vol_score": 50.0,
        "MERVAL":  {"regime": "NORMAL", "vol_ann": 0, "percentile": 50},
        "BOVESPA": {"regime": "NORMAL", "vol_ann": 0, "percentile": 50},
        "SP500":   {"regime": "NORMAL", "vol_ann": 0, "percentile": 50},
        "regime_factor": 1.00,
        "generated": datetime.now().isoformat(),
    }

    try:
        market_map = {"merval": "MERVAL", "bovespa": "BOVESPA", "sp500": "SP500"}
        market_results = {}

        for mk, df in price_data.items():
            market_label = market_map.get(mk, mk.upper())
            col          = index_cols.get(mk, "")

            if df is None or df.empty or not col or col not in df.columns:
                # Buscar el índice por nombre
                idx_col = _find_index_col(df) if df is not None else None
                if idx_col:
                    col = idx_col
                else:
                    market_results[market_label] = {"regime": "NORMAL", "vol_ann": 0, "percentile": 50}
                    continue

            serie = df[col].dropna()
            if len(serie) < VOL_WINDOW + 10:
                market_results[market_label] = {"regime": "NORMAL", "vol_ann": 0, "percentile": 50}
                continue

            market_results[market_label] = _calc_regime(serie)

        if not market_results:
            return fallback

        # Régimen global: promedio de percentiles
        percentiles   = [v["percentile"] for v in market_results.values() if v.get("percentile") is not None]
        global_pct    = float(np.mean(percentiles)) if percentiles else 50.0
        global_regime = _pct_to_regime(global_pct)

        # vol_score 0-100 para uso en portfolio/confidence
        global_vol_score = round(global_pct, 1)

        # regime_factor: multiplicador para Kelly y stops
        # LOW vol → Kelly más agresivo (1.10), HIGH vol → más conservador (0.75)
        regime_factor = {"LOW": 1.10, "NORMAL": 1.00, "HIGH": 0.75}.get(global_regime, 1.00)

        result = {
            "global_regime":   global_regime,
            "global_vol_score": global_vol_score,
            **market_results,
            "regime_factor":   regime_factor,
            "generated":       datetime.now().isoformat(),
        }

        # Guardar para uso del dashboard
        os.makedirs("data", exist_ok=True)
        with open(VOL_REGIME_PATH, "w") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        logger.info(
            f"[vol_regime] Global={global_regime} (factor={regime_factor}) | "
            f"MV={market_results.get('MERVAL', {}).get('regime','-')} "
            f"BV={market_results.get('BOVESPA', {}).get('regime','-')} "
            f"SP={market_results.get('SP500', {}).get('regime','-')}"
        )

        return result

    except Exception as e:
        logger.warning(f"[vol_regime] Error: {e}")
        return fallback


def _calc_regime(serie: pd.Series) -> dict:
    """Calcula régimen de volatilidad para una serie de índice."""
    returns  = serie.pct_change().dropna()

    # Volatilidad actual (ventana 20d)
    vol_20d  = float(returns.tail(VOL_WINDOW).std()) * np.sqrt(252) * 100  # anualizada %

    # Historia completa (hasta 252d)
    hist     = returns.tail(HIST_WINDOW)
    # Calcular rolling vol 20d sobre la historia
    if len(hist) >= VOL_WINDOW * 2:
        roll_vol = hist.rolling(VOL_WINDOW).std().dropna() * np.sqrt(252) * 100
        # Percentil de la vol actual vs historia rolling
        pct = float(np.mean(roll_vol <= vol_20d) * 100)
    else:
        pct = 50.0

    regime = _pct_to_regime(pct)

    return {
        "regime":     regime,
        "vol_ann":    round(vol_20d, 1),
        "percentile": round(pct, 1),
    }


def _pct_to_regime(percentile: float) -> str:
    if percentile < 30:
        return "LOW"
    elif percentile > 70:
        return "HIGH"
    return "NORMAL"


def _find_index_col(df: pd.DataFrame) -> str | None:
    """Busca columna de índice en el DataFrame."""
    if df is None or df.empty:
        return None
    for col in df.columns:
        if any(kw in str(col).upper() for kw in ["MERVAL", "BOVESPA", "S&P", "^MERV", "^BVSP", "^GSPC"]):
            return col
    return None


def load_vol_regime() -> dict:
    """Carga el último régimen calculado (para módulos que lo necesiten)."""
    if not os.path.exists(VOL_REGIME_PATH):
        return {"global_regime": "NORMAL", "global_vol_score": 50.0, "regime_factor": 1.00}
    try:
        with open(VOL_REGIME_PATH) as f:
            return json.load(f)
    except Exception:
        return {"global_regime": "NORMAL", "global_vol_score": 50.0, "regime_factor": 1.00}
