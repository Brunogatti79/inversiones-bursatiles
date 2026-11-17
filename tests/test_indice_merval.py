"""
Fuente real del índice Merval (11/11/2026): Rava en lugar de Yahoo ^MERV,
que quedó sin datos al terminar el S&P Merval el 16/10/2026.

Las respuestas de Rava están simuladas (ningún test sale a la red). Los
valores del tramo 21/09–16/10/2026 son los reales de Rava y de Yahoo.
"""
import numpy as np
import pandas as pd
import pytest

import src.indice_merval as im
import scripts.download_data as dd

COL = "INDICE MERVAL"
ENCABEZADO = ("especie,apertura,maximo,minimo,ultimo,fecha,cierre,timestamp,"
              "usd_apertura,usd_maximo,usd_minimo,usd_cierre,volumen")
# (fecha, cierre Rava, cierre Yahoo ^MERV) — datos reales
REALES = [
    ("2026-09-21", 105265.0, 2998956.0), ("2026-09-22", 105220.0, 2997660.0),
    ("2026-09-23", 104233.0, 2969546.0), ("2026-09-24", 103195.0, 2939964.0),
    ("2026-09-25", 101573.0, 2893751.0), ("2026-09-28", 98244.2, 2798925.0),
    ("2026-09-29", 97669.9, 2782561.0), ("2026-09-30", 98960.2, 2819323.0),
    ("2026-10-01", 96837.2, 2758840.0), ("2026-10-02", 97146.9, 2767663.0),
    ("2026-10-05", 100721.0, 2869488.0), ("2026-10-06", 101682.0, 2896853.0),
    ("2026-10-07", 99128.7, 2824123.0), ("2026-10-08", 99421.8, 2832472.0),
    ("2026-10-09", 99265.7, 2828027.0), ("2026-10-13", 100709.0, 2869151.0),
    ("2026-10-14", 99540.1, 2835843.0), ("2026-10-15", 100355.0, 2859051.0),
    ("2026-10-16", 100000.0, 2848945.0),
]
MSCI = [("2026-10-19", 98214.7), ("2026-10-20", 97942.6), ("2026-10-21", 99530.9),
        ("2026-10-22", 97993.0), ("2026-10-23", 102278.0), ("2026-10-26", 99227.3)]


def _fechas():
    return pd.bdate_range("2025-11-03", "2026-10-26").difference(pd.DatetimeIndex(["2026-10-12"]))


def _rava_serie():
    """Serie tipo Rava: camino suave hasta el 18/09 y después valores reales."""
    fechas = _fechas()
    s = pd.Series(np.linspace(90000, 105000, len(fechas)), index=fechas)
    for f, c, _ in REALES:
        s[pd.Timestamp(f)] = c
    for f, c in MSCI:
        s[pd.Timestamp(f)] = c
    return s


def _rava_csv(serie=None, mutar=None):
    serie = _rava_serie() if serie is None else serie
    filas = [ENCABEZADO]
    for f, c in serie.items():
        ap = 0 if str(f.date()) == "2026-10-19" else c      # caso real: apertura 0 el 19/10
        filas.append(f"MERVAL,{ap},{c * 1.01},{c * 0.99},{c},{f.date()},{c},0,0,0,0,0,0")
    texto = "\n".join(filas)
    return mutar(texto) if mutar else texto


def _df_yahoo():
    """merval_cierres.csv como lo deja Yahoo: acciones al día, índice en la
    base vieja y sin datos desde el 16/10/2026."""
    fechas = _fechas()
    df = pd.DataFrame({"Grupo Financiero Galicia": 5000.0, "YPF": 6500.0}, index=fechas)
    df[COL] = np.nan
    for f, _, y in REALES:
        df.loc[pd.Timestamp(f), COL] = y
    df.index.name = "Fecha"
    return df


def _aplicar(texto=None, df=None, **kw):
    texto = _rava_csv() if texto is None else texto
    return im.aplicar_indice_merval(_df_yahoo() if df is None else df, COL,
                                    http_get=lambda url: texto, **kw)


class TestAplica:
    def test_llena_el_indice_hasta_la_ultima_rueda(self):
        df, _, _, info = _aplicar()
        assert info["aplicado"] is True
        assert info["ultima_fecha"] == "2026-10-26"
        assert df[COL].notna().all()
        assert df.loc["2026-10-19", COL] == pytest.approx(98214.7)

    def test_toda_la_columna_queda_en_la_base_nueva(self):
        df, _, _, _ = _aplicar()
        assert df.loc["2026-10-16", COL] == pytest.approx(100000.0)
        assert df[COL].max() < 200_000          # nada queda en ~2,8 millones

    def test_retorno_del_empalme_es_el_real(self):
        """16/10 → 19/10: fin del S&P Merval, primer día del MSCI Merval."""
        df, _, _, _ = _aplicar()
        r = df[COL].pct_change()
        assert r.loc["2026-10-19"] == pytest.approx(98214.7 / 100000.0 - 1)
        assert r.loc["2026-10-15"] == pytest.approx(2859051.0 / 2835843.0 - 1, abs=1e-4)

    def test_informa_el_cociente_contra_yahoo(self):
        _, _, _, info = _aplicar()
        assert info["cociente_yahoo_rava"] == pytest.approx(28.4895, abs=0.001)
        assert info["ruedas_comparadas"] == len(REALES)

    def test_no_toca_las_columnas_de_acciones(self):
        original = _df_yahoo()
        df, _, _, _ = _aplicar()
        pd.testing.assert_frame_equal(df.drop(columns=[COL]), original.drop(columns=[COL]))

    def test_high_y_low_y_apertura_cero_no_rompe(self):
        base = _df_yahoo()
        df, hi, lo, _ = _aplicar(df=base, df_high=base.copy(), df_low=base.copy())
        assert hi.loc["2026-10-19", COL] == pytest.approx(98214.7 * 1.01)
        assert lo.loc["2026-10-19", COL] == pytest.approx(98214.7 * 0.99)

    def test_sin_columna_de_yahoo_la_crea(self):
        df, _, _, info = _aplicar(df=_df_yahoo().drop(columns=[COL]))
        assert info["aplicado"] is True
        assert df.columns[-1] == COL and df[COL].notna().all()

    def test_dias_sin_rueda_en_rava_se_descartan(self):
        """Filas con cierre 0 no son un precio."""
        texto = _rava_csv() + "\nMERVAL,0,0,0,0,2026-10-27,0,0,0,0,0,0,0"
        assert im.parsear_rava(texto).index[-1] == pd.Timestamp("2026-10-26")


class TestNoAplica:
    def _sin_cambios(self, resultado):
        df, _, _, info = resultado
        assert info["aplicado"] is False and info.get("motivo")
        pd.testing.assert_frame_equal(df, _df_yahoo())
        return info

    def test_rava_no_responde(self):
        def cae(url):
            raise OSError("timeout")
        self._sin_cambios(im.aplicar_indice_merval(_df_yahoo(), COL, http_get=cae))

    @pytest.mark.parametrize("texto", ["", "<html>Acceso denegado</html>", "a,b\n1,2",
                                       ENCABEZADO + "\nMERVAL,1,1,1,1,2026-10-26,100000,0,0,0,0,0,0"])
    def test_formato_inesperado_o_muy_corto(self, texto):
        self._sin_cambios(_aplicar(texto))

    def test_otra_serie_no_se_usa(self):
        """Si lo que devuelve Rava no se mueve como el índice de Yahoo
        (cociente no constante), no es la misma serie."""
        s = _rava_serie()
        for i, (f, c, _) in enumerate(REALES):
            s[pd.Timestamp(f)] = c * (1 + 0.03 * (i % 3))
        info = self._sin_cambios(_aplicar(_rava_csv(s)))
        assert "misma serie" in info["motivo"]

    def test_rava_atrasado_no_se_usa(self):
        """Rava frenado hace un mes: no alcanza la cobertura reciente."""
        info = self._sin_cambios(_aplicar(_rava_csv(_rava_serie()[:"2026-09-18"])))
        assert "cobertura" in info["motivo"]

    def test_salto_imposible_no_se_usa(self):
        s = _rava_serie()
        s[pd.Timestamp("2026-10-23")] = 10227.8      # un dígito menos
        info = self._sin_cambios(_aplicar(_rava_csv(s)))
        assert "salto" in info["motivo"]


class TestIntegracionDescarga:
    def test_download_data_usa_la_columna_del_indice_y_no_rompe(self, monkeypatch):
        visto = {}

        def fake(df, col, df_high=None, df_low=None, **kw):
            visto["col"] = col
            return df, df_high, df_low, {"aplicado": True}
        monkeypatch.setattr(im, "aplicar_indice_merval", fake)
        _, _, _, info = dd._aplicar_indice_merval(_df_yahoo(), None, None)
        assert visto["col"] == COL and info["aplicado"] is True

        def explota(*a, **k):
            raise RuntimeError("boom")
        monkeypatch.setattr(im, "aplicar_indice_merval", explota)
        df, _, _, info = dd._aplicar_indice_merval(_df_yahoo(), None, None)
        pd.testing.assert_frame_equal(df, _df_yahoo())
        assert info == {"aplicado": False, "motivo": "boom"}
