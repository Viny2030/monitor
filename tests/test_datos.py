"""
test_datos.py — Tests de calidad de datos para el Monitor IRI

Cubre:
  - Integridad del dataset generado por build_monitor_completo()
  - Consistencia entre IRI (Score) y Estado
  - Bounds de los componentes de riesgo (0-100)
  - Reproducibilidad de seeds (datos sintéticos estables)
  - Coherencia de la fórmula IRI sobre datos reales
  - Integridad de las fuentes de datos
"""

import os
import sys
import math
import pytest
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from connector import (
    _iri,
    _score_estado,
    build_monitor_completo,
    _fallback_judicial,
    _fallback_legislative,
    _fallback_senado,
    _fallback_tgn,
    build_ejecutivo_df,
)

RISK_COLS = ["Riesgo Financiero", "Riesgo Contratación", "Riesgo Operativo", "Riesgo Datos"]
REQUIRED_COLS = ["Organismo", "Area", "IRI (Score)", "Estado", "Fuente"] + RISK_COLS
VALID_ESTADOS = {"🔴 ALTO", "🟡 MEDIO", "🟢 BAJO"}


@pytest.fixture(scope="module")
def df_completo():
    """Dataset completo generado sin APIs externas (modo fallback)."""
    return build_monitor_completo()


# ═══════════════════════════════════════════════════════
# 1. Integridad estructural
# ═══════════════════════════════════════════════════════

class TestIntegridadEstructural:

    def test_columnas_requeridas(self, df_completo):
        for col in REQUIRED_COLS:
            assert col in df_completo.columns, f"Falta columna: {col}"

    def test_sin_filas_completamente_nulas(self, df_completo):
        assert not df_completo.isnull().all(axis=1).any()

    def test_organismo_no_nulo(self, df_completo):
        assert df_completo["Organismo"].notna().all()

    def test_area_no_nula(self, df_completo):
        assert df_completo["Area"].notna().all()

    def test_fuente_no_nula(self, df_completo):
        assert df_completo["Fuente"].notna().all()

    def test_iri_no_nulo(self, df_completo):
        assert df_completo["IRI (Score)"].notna().all()

    def test_estado_no_nulo(self, df_completo):
        assert df_completo["Estado"].notna().all()

    def test_tipos_de_datos_iri(self, df_completo):
        assert pd.api.types.is_numeric_dtype(df_completo["IRI (Score)"])

    def test_tipos_de_datos_riesgo_financiero(self, df_completo):
        assert pd.api.types.is_numeric_dtype(df_completo["Riesgo Financiero"])


# ═══════════════════════════════════════════════════════
# 2. Bounds de valores (0-100)
# ═══════════════════════════════════════════════════════

class TestBoundsValores:

    def test_iri_entre_0_100(self, df_completo):
        assert df_completo["IRI (Score)"].between(0, 100).all(), \
            f"IRI fuera de rango: {df_completo['IRI (Score)'].describe()}"

    def test_riesgo_financiero_entre_0_100(self, df_completo):
        assert df_completo["Riesgo Financiero"].between(0, 100).all()

    def test_riesgo_contratacion_entre_0_100(self, df_completo):
        assert df_completo["Riesgo Contratación"].between(0, 100).all()

    def test_riesgo_operativo_entre_0_100(self, df_completo):
        assert df_completo["Riesgo Operativo"].between(0, 100).all()

    def test_riesgo_datos_entre_0_100(self, df_completo):
        assert df_completo["Riesgo Datos"].between(0, 100).all()

    def test_iri_no_infinito(self, df_completo):
        assert not df_completo["IRI (Score)"].apply(math.isinf).any()

    def test_iri_no_nan(self, df_completo):
        assert not df_completo["IRI (Score)"].apply(math.isnan).any()


# ═══════════════════════════════════════════════════════
# 3. Consistencia Estado ↔ IRI
# ═══════════════════════════════════════════════════════

class TestConsistenciaEstadoIri:

    def test_estados_validos(self, df_completo):
        invalidos = df_completo[~df_completo["Estado"].isin(VALID_ESTADOS)]
        assert invalidos.empty, f"Estados inválidos encontrados:\n{invalidos[['Organismo','Estado']]}"

    def test_alto_implica_iri_60_o_mas(self, df_completo):
        altos = df_completo[df_completo["Estado"] == "🔴 ALTO"]
        violaciones = altos[altos["IRI (Score)"] < 60]
        assert violaciones.empty, f"ALTO con IRI < 60:\n{violaciones[['Organismo','IRI (Score)']]}"

    def test_medio_implica_iri_30_a_59(self, df_completo):
        medios = df_completo[df_completo["Estado"] == "🟡 MEDIO"]
        violaciones = medios[(medios["IRI (Score)"] < 30) | (medios["IRI (Score)"] >= 60)]
        assert violaciones.empty, f"MEDIO con IRI fuera de [30,60):\n{violaciones[['Organismo','IRI (Score)']]}"

    def test_bajo_implica_iri_menor_30(self, df_completo):
        bajos = df_completo[df_completo["Estado"] == "🟢 BAJO"]
        violaciones = bajos[bajos["IRI (Score)"] >= 30]
        assert violaciones.empty, f"BAJO con IRI >= 30:\n{violaciones[['Organismo','IRI (Score)']]}"

    def test_iri_60_es_alto_no_medio(self):
        assert _score_estado(60.0) == "🔴 ALTO"

    def test_iri_30_es_medio_no_bajo(self):
        assert _score_estado(30.0) == "🟡 MEDIO"


# ═══════════════════════════════════════════════════════
# 4. Coherencia de la fórmula IRI sobre el dataset
# ═══════════════════════════════════════════════════════

class TestCoherenciaFormula:

    def test_iri_calculado_coincide_con_formula(self, df_completo):
        """
        Para cada fila, el IRI (Score) debe coincidir con la fórmula
        dentro de ±0.05 (tolerancia por redondeos intermedios).
        """
        for _, row in df_completo.iterrows():
            iri_calculado = _iri(
                row["Riesgo Financiero"],
                row["Riesgo Contratación"],
                row["Riesgo Operativo"],
                row["Riesgo Datos"],
            )
            assert abs(row["IRI (Score)"] - iri_calculado) <= 0.05, (
                f"IRI inconsistente en '{row['Organismo']}': "
                f"almacenado={row['IRI (Score)']}, calculado={iri_calculado}"
            )

    def test_iri_promedio_global_razonable(self, df_completo):
        """El IRI promedio debe estar en un rango razonable (no 0 ni 100)."""
        avg = df_completo["IRI (Score)"].mean()
        assert 5 < avg < 95, f"IRI promedio anómalo: {avg}"


# ═══════════════════════════════════════════════════════
# 5. Reproducibilidad de seeds
# ═══════════════════════════════════════════════════════

class TestReproducibilidadSeeds:
    """
    Verifica que los datos sintéticos (fallback) sean idénticos
    en llamadas sucesivas. Esto es crítico para que el dashboard
    no cambie entre reinicios del servidor sin datos reales.
    """

    def test_judicial_seed_42(self):
        df1, df2 = _fallback_judicial(), _fallback_judicial()
        pd.testing.assert_frame_equal(df1.reset_index(drop=True),
                                      df2.reset_index(drop=True))

    def test_legislative_seed_43(self):
        df1, df2 = _fallback_legislative(), _fallback_legislative()
        pd.testing.assert_frame_equal(df1.reset_index(drop=True),
                                      df2.reset_index(drop=True))

    def test_ejecutivo_seed_44(self):
        df1, df2 = build_ejecutivo_df(), build_ejecutivo_df()
        pd.testing.assert_frame_equal(df1.reset_index(drop=True),
                                      df2.reset_index(drop=True))

    def test_senado_seed_45(self):
        df1, df2 = _fallback_senado(), _fallback_senado()
        pd.testing.assert_frame_equal(df1.reset_index(drop=True),
                                      df2.reset_index(drop=True))

    def test_tgn_seed_46(self):
        df1, df2 = _fallback_tgn(), _fallback_tgn()
        pd.testing.assert_frame_equal(df1.reset_index(drop=True),
                                      df2.reset_index(drop=True))


# ═══════════════════════════════════════════════════════
# 6. Fuentes de datos
# ═══════════════════════════════════════════════════════

class TestFuentesDatos:

    def test_todas_las_fuentes_estan_pobladas(self, df_completo):
        """No debe haber organismos con fuente vacía o nula."""
        assert df_completo["Fuente"].notna().all()
        assert (df_completo["Fuente"].str.strip() != "").all()

    def test_fuentes_conocidas_presentes(self, df_completo):
        """Sin APIs activas, las fuentes deben ser fallback o sintéticas."""
        fuentes = df_completo["Fuente"].unique().tolist()
        # Al menos una fuente debe contener 'fallback' o 'sintetico' o 'justicia' o 'senadores'
        keywords = ["fallback", "sintetico", "justicia", "senadores", "monitor_legistativo"]
        assert any(
            any(kw in str(f).lower() for kw in keywords)
            for f in fuentes
        ), f"Fuentes inesperadas: {fuentes}"

    def test_no_hay_fuente_desconocida_en_fallback(self, df_completo):
        """La fuente 'desconocida' sólo debería aparecer si algo salió muy mal."""
        desconocidas = df_completo[df_completo["Fuente"] == "desconocida"]
        assert desconocidas.empty, f"Hay {len(desconocidas)} organismos con fuente 'desconocida'"


# ═══════════════════════════════════════════════════════
# 7. Diversidad del dataset
# ═══════════════════════════════════════════════════════

class TestDiversidadDataset:

    def test_multiples_areas_institucionales(self, df_completo):
        """El dataset debe cubrir al menos 3 áreas institucionales distintas."""
        assert df_completo["Area"].nunique() >= 3

    def test_hay_organismos_de_cada_poder(self, df_completo):
        areas = " ".join(df_completo["Area"].tolist()).lower()
        assert "judicial" in areas or "justicia" in areas, "Faltan organismos judiciales"
        assert "legislativo" in areas, "Faltan organismos legislativos"

    def test_distribucion_estados_no_trivial(self, df_completo):
        """No todos los organismos deben tener el mismo estado."""
        n_estados = df_completo["Estado"].nunique()
        assert n_estados > 1, "Todos los organismos tienen el mismo estado — distribución trivial"
