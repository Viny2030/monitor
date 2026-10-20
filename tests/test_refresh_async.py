"""
Tests de POST /refresh (sync y async) y GET /refresh/estado.

El motor real no se ejecuta: se reemplaza subprocess.run por un doble que
escribe (o no) el CSV, para probar sólo la lógica del endpoint.
"""
import subprocess
import time

import pandas as pd
import pytest
from fastapi.testclient import TestClient

TOKEN = {"X-Refresh-Token": "test-token-seguro"}


@pytest.fixture
def app_refresh(csv_path, sample_df, monkeypatch):
    import main as m
    sample_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    monkeypatch.setattr(m, "CSV_PATH", csv_path)
    monkeypatch.setattr(m, "REFRESH_TOKEN", "test-token-seguro")
    monkeypatch.setattr(m, "_save_to_db", lambda df: None)
    m._REFRESH_ESTADO.clear()
    m._REFRESH_ESTADO.update(estado="sin_ejecutar")
    if m._REFRESH_LOCK.locked():
        m._REFRESH_LOCK.release()
    return m


def _motor(monkeypatch, m, codigo=0, filas=None, demora=0.0):
    """Reemplaza el motor: escribe `filas` en el CSV (None = no lo toca)."""
    def fake_run(*a, **k):
        time.sleep(demora)
        if filas is not None:
            pd.DataFrame(filas).to_csv(m.CSV_PATH, index=False, encoding="utf-8-sig")
        return subprocess.CompletedProcess(a, codigo)
    monkeypatch.setattr(subprocess, "run", fake_run)


def _fila(org="Nuevo"):
    return {"Organismo": org, "Area": "X", "Riesgo Financiero": 10, "Riesgo Contratación": None,
            "Riesgo Operativo": 10, "Riesgo Datos": 10, "IRI (Score)": 10, "Estado": "🟢 BAJO", "Fuente": "t"}


def test_sync_ok_devuelve_200_y_cantidad(app_refresh, monkeypatch):
    _motor(monkeypatch, app_refresh, filas=[_fila("A"), _fila("B")])
    with TestClient(app_refresh.app) as c:
        r = c.post("/refresh", headers=TOKEN)
    assert r.status_code == 200 and r.json()["status"] == "ok" and r.json()["organismos"] == 2


def test_sync_motor_falla_devuelve_500_y_restaura_csv(app_refresh, monkeypatch, sample_df):
    _motor(monkeypatch, app_refresh, codigo=1, filas=[])   # rompe el CSV y sale con error
    with TestClient(app_refresh.app) as c:
        r = c.post("/refresh", headers=TOKEN)
    assert r.status_code == 500 and r.json()["status"] == "error"
    assert "dataset anterior" in r.json()["detalle"]
    assert len(pd.read_csv(app_refresh.CSV_PATH)) == len(sample_df)


def test_sync_dataset_vacio_es_error(app_refresh, monkeypatch, sample_df):
    _motor(monkeypatch, app_refresh, codigo=0, filas=[])
    with TestClient(app_refresh.app) as c:
        r = c.post("/refresh", headers=TOKEN)
    assert r.status_code == 500 and "vacío" in r.json()["detalle"]
    assert len(pd.read_csv(app_refresh.CSV_PATH)) == len(sample_df)


def test_async_202_y_estado_hasta_ok(app_refresh, monkeypatch):
    _motor(monkeypatch, app_refresh, filas=[_fila()], demora=0.3)
    with TestClient(app_refresh.app) as c:
        r = c.post("/refresh?modo=async", headers=TOKEN)
        assert r.status_code == 202
        assert c.post("/refresh?modo=async", headers=TOKEN).status_code == 409   # ya en curso
        for _ in range(40):
            e = c.get("/refresh/estado", headers=TOKEN).json()
            if e["estado"] != "corriendo":
                break
            time.sleep(0.1)
    assert e["estado"] == "ok" and e["resultado"]["organismos"] == 1
    assert not app_refresh._REFRESH_LOCK.locked()


def test_async_error_queda_en_estado(app_refresh, monkeypatch):
    _motor(monkeypatch, app_refresh, codigo=2)
    with TestClient(app_refresh.app) as c:
        assert c.post("/refresh?modo=async", headers=TOKEN).status_code == 202
        for _ in range(40):
            e = c.get("/refresh/estado", headers=TOKEN).json()
            if e["estado"] != "corriendo":
                break
            time.sleep(0.1)
    assert e["estado"] == "error" and "código 2" in e["resultado"]["detalle"]


def test_estado_requiere_token(app_refresh):
    with TestClient(app_refresh.app) as c:
        assert c.get("/refresh/estado").status_code == 401
        assert c.get("/refresh/estado", headers={"X-Refresh-Token": "malo"}).status_code == 401
        assert c.get("/refresh/estado", headers=TOKEN).json()["estado"] == "sin_ejecutar"
