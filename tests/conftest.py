"""
conftest.py — Fixtures compartidas para la suite de tests del Monitor IRI.
"""

import os
import pytest
import pandas as pd
import tempfile


# ── Columnas requeridas por la API y el dashboard ─────────────────────────────
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


@pytest.fixture(scope="session", autouse=True)
def clear_api_env_vars():
    """Asegura que no haya URLs de API en el entorno durante los tests."""
    api_vars = [
        "LEGISTATIVO_API_URL",
        "SENADORES_API_URL",
        "JUSTICIA_API_URL",
        "CONTRATOS_AR_API_URL",
        "TGN_AR_API_URL",
    ]
    original = {v: os.environ.pop(v, None) for v in api_vars}
    yield
    # Restaurar al terminar
    for k, v in original.items():
        if v is not None:
            os.environ[k] = v


@pytest.fixture
def sample_df():
    """DataFrame mínimo válido para poblar el CSV del monitor."""
    return pd.DataFrame([
        {
            "Organismo": "Organismo Test A",
            "Area": "Control y Justicia",
            "Riesgo Financiero": 70.0,
            "Riesgo Contratación": 50.0,
            "Riesgo Operativo": 60.0,
            "Riesgo Datos": 40.0,
            "IRI (Score)": 59.5,
            "Estado": "🟡 MEDIO",
            "Fuente": "test_fixture",
        },
        {
            "Organismo": "Organismo Test B",
            "Area": "Poder Legislativo",
            "Riesgo Financiero": 80.0,
            "Riesgo Contratación": 75.0,
            "Riesgo Operativo": 65.0,
            "Riesgo Datos": 55.0,
            "IRI (Score)": 72.5,
            "Estado": "🔴 ALTO",
            "Fuente": "test_fixture",
        },
        {
            "Organismo": "Organismo Test C",
            "Area": "Poder Legislativo",
            "Riesgo Financiero": 20.0,
            "Riesgo Contratación": 15.0,
            "Riesgo Operativo": 10.0,
            "Riesgo Datos": 25.0,
            "IRI (Score)": 17.25,
            "Estado": "🟢 BAJO",
            "Fuente": "test_fixture",
        },
    ])


@pytest.fixture
def csv_path(sample_df, tmp_path):
    """Crea un CSV temporal con datos de prueba y devuelve su ruta."""
    path = tmp_path / "monitor_completo.csv"
    sample_df.to_csv(path, index=False, encoding="utf-8-sig")
    return str(path)
