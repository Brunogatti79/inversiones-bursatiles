"""
src/execution/ — Capa de ejecución (Fase 2, auditoría 26/06/2026)

Antes de esto, "ejecución" estaba repartida y duplicada en tres lugares:
  - tracker.py:        update_portfolio_usd() (pricing, con cobertura de
                        solo 14/78 tickers vía diccionarios hardcodeados)
                        + check_portfolio_alerts() (detección de stop/target)
  - start_server.py:   una SEGUNDA implementación de pricing distinta,
                        casi correcta, dentro del handler HTTP de
                        /api/portfolio (GET) — y la lógica de compra/venta
                        embebida directamente en el handler de /api/compra
                        y /api/venta (POST).

Esto generaba el bug central de la auditoría: dos universos de pricing
desconectados (78 tickers analizados vs ~14 con precio real en el
portfolio persistido).

src/execution/ consolida pricing y ejecución de órdenes en un solo lugar:
  - pricing_engine.py:  única fuente de verdad para "¿cuánto vale hoy esta
                        posición?", genérica para los 78 tickers.
  - risk_engine.py:     "¿qué stop/target le corresponde a una posición
                        nueva?" — usa el ATR ya calculado por analyzer.py
                        (ver fix de causa raíz en analyzer.py: ATR estaba
                        en 0.0 el 100% de las veces antes de esta sesión).
  - order_engine.py:    ejecución de compra/venta — extraído de
                        start_server.py para que el handler HTTP quede
                        como una capa delgada que solo parsea el request.

check_portfolio_alerts() (detección de stop/target ya tocado, señal de
venta, take-profit parcial) se queda en tracker.py: ya funciona bien y
está testeada por uso; no se duplica acá.
"""
