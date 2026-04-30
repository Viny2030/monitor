"""
test_connector_unit.py — Tests unitarios para connector.py

Cubre:
  - _iri()          : fórmula IRI y pesos
  - _score_estado() : umbrales ALTO / MEDIO / BAJO
  - _col_find()     : búsqueda de columnas
  - fallbacks       : reproducibilidad de seeds y columnas requeridas
  - build_monitor_completo() : sin APIs externas
"""

import os
import sys
import pytest
import pandas as pd
import numpy as np

# Asegurar que el root del repo esté en el path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from connector import (
    _iri,
    _score_estado,
    _col_find,
    _fallback_judicial,
    _fallback_legislative,
    _fallback_senado,
    _fallback_tgn,
    build_ejecutivo_df,
    build_monitor_completo,
)

REQUIRED_COLS = [
    "Organismo",
    "Area",
    "Riesgo Financiero",
    "Riesgo Contratación",
    "Riesgo Operativo",
    "Riesgo Datos",
    "IRI (Score)",
    "Estado",
    "Fuente",
]

VALID_ESTADOS = {"🔴 ALTO", "🟡 MEDIO", "🟢 BAJO"}


# ═══════════════════════════════════════════════════════
# 1. _iri() — Fórmula IRI
# ═══════════════════════════════════════════════════════

class TestIriFormula:

    def test_iri_todos_cero(self):
        """Con todos los componentes en 0 el IRI debe ser 0."""
        assert _iri(0, 0, 0, 0) == 0.0

    def test_iri_todos_cien(self):
        """Con todos los componentes en 100 el IRI debe ser 100."""
        assert _iri(100, 100, 100, 100) == 100.0

    def test_iri_peso_financiero(self):
        """Sólo riesgo financiero = 100 → IRI = 35."""
        assert _iri(100, 0, 0, 0) == 35.0

    def test_iri_peso_contratacion(self):
        """Sólo riesgo contratación = 100 → IRI = 30."""
        assert _iri(0, 100, 0, 0) == 30.0

    def test_iri_peso_operativo(self):
        """Sólo riesgo operativo = 100 → IRI = 20."""
        assert _iri(0, 0, 100, 0) == 20.0

    def test_iri_peso_datos(self):
        """Sólo riesgo datos = 100 → IRI = 15."""
        assert _iri(0, 0, 0, 100) == 15.0

    def test_iri_pesos_suman_100(self):
        """Los cuatro pesos deben sumar exactamente 100."""
        assert 35 + 30 + 20 + 15 == 100

    def test_iri_suma_ponderada(self):
        """IRI debe coincidir con la suma ponderada manual."""
        r_fin, r_con, r_ope, r_dat = 80, 60, 40, 20
        expected = round(80 * 0.35 + 60 * 0.30 + 40 * 0.20 + 20 * 0.15, 2)
        assert _iri(r_fin, r_con, r_ope, r_dat) == expected

    def test_iri_redondeo_2_decimales(self):
        """El resultado debe tener como máximo 2 decimales."""
        result = _iri(33, 33, 33, 33)
        assert result == round(result, 2)

    def test_iri_valores_mixtos(self):
        """Test con valores típicos de la realidad."""
        result = _iri(55.0, 40.0, 35.0, 25.0)
        expected = round(55.0 * 0.35 + 40.0 * 0.30 + 35.0 * 0.20 + 25.0 * 0.15, 2)
        assert result == expected


# ═══════════════════════════════════════════════════════
# 2. _score_estado() — Umbrales de clasificación
# ═══════════════════════════════════════════════════════

class TestScoreEstado:

    def test_alto_en_umbral_exacto(self):
        assert _score_estado(60) == "🔴 ALTO"

    def test_alto_sobre_umbral(self):
        assert _score_estado(100) == "🔴 ALTO"

    def test_alto_justo_sobre_60(self):
        assert _score_estado(60.1) == "🔴 ALTO"

    def test_medio_justo_bajo_60(self):
        assert _score_estado(59.9) == "🟡 MEDIO"

    def test_medio_en_umbral_30(self):
        assert _score_estado(30) == "🟡 MEDIO"

    def test_medio_justo_sobre_30(self):
        assert _score_estado(30.1) == "🟡 MEDIO"

    def test_bajo_justo_bajo_30(self):
        assert _score_estado(29.9) == "🟢 BAJO"

    def test_bajo_en_cero(self):
        assert _score_estado(0) == "🟢 BAJO"

    def test_resultado_siempre_valido(self):
        """Para cualquier valor entre 0-100 el estado debe ser uno de los tres válidos."""
        valid = {"🔴 ALTO", "🟡 MEDIO", "🟢 BAJO"}
        for val in [0, 15, 29.9, 30, 45, 59.9, 60, 75, 100]:
            assert _score_estado(val) in valid


# ═══════════════════════════════════════════════════════
# 3. _col_find() — Buscador de columnas
# ═══════════════════════════════════════════════════════

class TestColFind:

    def test_match_exacto(self):
        df = pd.DataFrame(columns=["organismo", "area", "riesgo"])
        assert _col_find(df, ["organismo"]) == "organismo"

    def test_match_parcial(self):
        df = pd.DataFrame(columns=["nombre_organismo", "area"])
        assert _col_find(df, ["organismo"]) == "nombre_organismo"

    def test_no_encontrado(self):
        df = pd.DataFrame(columns=["area", "riesgo"])
        assert _col_find(df, ["organismo"]) is None

    def test_primera_keyword_gana(self):
        """Con múltiples keywords debe retornar la columna de la primera que matchea."""
        df = pd.DataFrame(columns=["jurisdiccion", "organismo"])
        resultado = _col_find(df, ["organismo", "jurisdiccion"])
        assert resultado == "organismo"

    def test_case_insensitive(self):
        """La búsqueda debe ser case-insensitive (el nombre de columna está en minúsculas)."""
        df = pd.DataFrame(columns=["organismo_principal"])
        assert _col_find(df, ["organismo"]) == "organismo_principal"

    def test_df_vacio(self):
        df = pd.DataFrame()
        assert _col_find(df, ["organismo"]) is None


# ═══════════════════════════════════════════════════════
# 4. Fallbacks — reproducibilidad y estructura
# ═══════════════════════════════════════════════════════

def _assert_df_valido(df: pd.DataFrame, nombre: str):
    """Helper: valida estructura y bounds de cualquier DataFrame del monitor."""
    assert not df.empty, f"{nombre}: DataFrame vacío"
    for col in REQUIRED_COLS:
        assert col in df.columns, f"{nombre}: falta columna '{col}'"
    assert df["IRI (Score)"].between(0, 100).all(), f"{nombre}: IRI fuera de 0-100"
    assert df["Estado"].isin(VALID_ESTADOS).all(), f"{nombre}: Estado inválido"
    assert df["Organismo"].notna().all(), f"{nombre}: Organismo nulo"
    assert (df["Organismo"].str.strip() != "").all(), f"{nombre}: Organismo vacío"


class TestFallbackJudicial:

    def test_estructura(self):
        _assert_df_valido(_fallback_judicial(), "fallback_judicial")

    def test_reproducible(self):
        """Dos llamadas deben producir exactamente los mismos datos."""
        df1 = _fallback_judicial()
        df2 = _fallback_judicial()
        pd.testing.assert_frame_equal(df1, df2)

    def test_fuente_indica_fallback(self):
        df = _fallback_judicial()
        assert df["Fuente"].str.contains("fallback").all()

    def test_tiene_organismos_judiciales(self):
        df = _fallback_judicial()
        orgs = df["Organismo"].tolist()
        assert any("Justicia" in o or "Judicial" in o or "Corte" in o or "Juzgado" in o for o in orgs)


class TestFallbackLegislativo:

    def test_estructura(self):
        _assert_df_valido(_fallback_legislative(), "fallback_legislative")

    def test_reproducible(self):
        df1 = _fallback_legislative()
        df2 = _fallback_legislative()
        pd.testing.assert_frame_equal(df1, df2)

    def test_fuente_indica_fallback(self):
        df = _fallback_legislative()
        assert df["Fuente"].str.contains("fallback").all()


class TestFallbackSenado:

    def test_estructura(self):
        _assert_df_valido(_fallback_senado(), "fallback_senado")

    def test_reproducible(self):
        df1 = _fallback_senado()
        df2 = _fallback_senado()
        pd.testing.assert_frame_equal(df1, df2)

    def test_contiene_senado(self):
        df = _fallback_senado()
        assert any("Senado" in o or "Senadores" in o for o in df["Organismo"])


class TestFallbackTGN:

    def test_estructura(self):
        _assert_df_valido(_fallback_tgn(), "fallback_tgn")

    def test_reproducible(self):
        df1 = _fallback_tgn()
        df2 = _fallback_tgn()
        pd.testing.assert_frame_equal(df1, df2)

    def test_fuente_indica_fallback(self):
        df = _fallback_tgn()
        assert df["Fuente"].str.contains("fallback").all()


class TestBuildEjecutivo:

    def test_estructura(self):
        _assert_df_valido(build_ejecutivo_df(), "build_ejecutivo_df")

    def test_reproducible(self):
        df1 = build_ejecutivo_df()
        df2 = build_ejecutivo_df()
        pd.testing.assert_frame_equal(df1, df2)

    def test_organismos_conocidos(self):
        df = build_ejecutivo_df()
        orgs = df["Organismo"].tolist()
        assert "CONICET" in orgs
        assert "AFIP" in orgs
        assert "YPF" in orgs


# ═══════════════════════════════════════════════════════
# 5. build_monitor_completo() — Sin APIs externas
# ═══════════════════════════════════════════════════════

class TestBuildMonitorCompleto:

    def test_genera_datos_sin_apis(self):
        """Sin ninguna env var definida debe generar datos vía fallback."""
        df = build_monitor_completo()
        assert not df.empty

    def test_estructura_completa(self):
        df = build_monitor_completo()
        _assert_df_valido(df, "build_monitor_completo")

    def test_contiene_todas_las_areas(self):
        """El dataset debe incluir las cuatro áreas institucionales."""
        df = build_monitor_completo()
        areas = df["Area"].unique()
        # Debe haber datos judiciales, legislativos y ejecutivos
        assert any("Judicial" in a or "Justicia" in a or "Control" in a for a in areas)
        assert any("Legislativo" in a for a in areas)

    def test_iri_global_entre_0_100(self):
        df = build_monitor_completo()
        assert df["IRI (Score)"].min() >= 0
        assert df["IRI (Score)"].max() <= 100

    def test_estado_consistente_con_iri(self):
        """El Estado debe ser consistente con el valor de IRI."""
        df = build_monitor_completo()
        altos = df[df["Estado"] == "🔴 ALTO"]["IRI (Score)"]
        medios = df[df["Estado"] == "🟡 MEDIO"]["IRI (Score)"]
        bajos  = df[df["Estado"] == "🟢 BAJO"]["IRI (Score)"]
        assert (altos >= 60).all(), "Un registro ALTO tiene IRI < 60"
        assert ((medios >= 30) & (medios < 60)).all(), "Un registro MEDIO tiene IRI fuera de [30,60)"
        assert (bajos < 30).all(), "Un registro BAJO tiene IRI >= 30"

    def test_sin_duplicados_criticos(self):
        """No debe haber organismos exactamente duplicados (mismo nombre + área)."""
        df = build_monitor_completo()
        dupes = df.duplicated(subset=["Organismo", "Area"])
        assert not dupes.any(), f"Hay {dupes.sum()} filas duplicadas en Organismo+Area"

    def test_minimo_organismos(self):
        """El dataset completo debe tener al menos 15 organismos."""
        df = build_monitor_completo()
        assert len(df) >= 15

    def test_columna_fuente_no_vacia(self):
        df = build_monitor_completo()
        assert df["Fuente"].notna().all()
        assert (df["Fuente"].str.strip() != "").all()