"""
test_api.py — Tests de integración para los endpoints FastAPI de main.py

Cubre todos los endpoints:
  GET  /             health check
  GET  /datos        dataset completo (con filtros opcionales)
  GET  /por-area/    organismos por área
  GET  /top-riesgo   top N por score IRI
  GET  /resumen      estadísticas globales
  POST /refresh      regeneración del CSV (con y sin token válido)
  GET  /dashboard    HTML del dashboard
"""

import os
import sys
import pytest
import pandas as pd

from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Fixture: cliente con CSV temporal ────────────────────────────────────────

@pytest.fixture
def client(csv_path, monkeypatch):
    """
    Crea un TestClient de FastAPI apuntando al CSV de prueba.
    Parchea CSV_PATH y REFRESH_TOKEN en main directamente (ya importado).
    """
    import main as app_module
    monkeypatch.setattr(app_module, "CSV_PATH", csv_path)
    # REFRESH_TOKEN se resuelve en el import del módulo, hay que parchear la var directamente
    monkeypatch.setattr(app_module, "REFRESH_TOKEN", "test-token-seguro")

    from main import app
    with TestClient(app) as c:
        yield c


@pytest.fixture
def client_sin_csv(tmp_path, monkeypatch):
    """Cliente sin CSV disponible (simula servicio recién levantado)."""
    import main as app_module
    ruta_inexistente = str(tmp_path / "no_existe.csv")
    monkeypatch.setattr(app_module, "CSV_PATH", ruta_inexistente)

    from main import app
    with TestClient(app) as c:
        yield c


# ═══════════════════════════════════════════════════════
# GET / — Health check
# ═══════════════════════════════════════════════════════

class TestHealthCheck:

    def test_status_200(self, client):
        r = client.get("/")
        assert r.status_code == 200

    def test_status_ok(self, client):
        data = r = client.get("/")
        assert r.json()["status"] == "ok"

    def test_campo_version(self, client):
        r = client.get("/")
        assert "version" in r.json()

    def test_campo_endpoints(self, client):
        r = client.get("/")
        assert "endpoints" in r.json()

    def test_dataset_disponible_true(self, client):
        r = client.get("/")
        assert r.json()["dataset_disponible"] is True

    def test_dataset_disponible_false_sin_csv(self, client_sin_csv):
        r = client_sin_csv.get("/")
        assert r.json()["dataset_disponible"] is False

    def test_total_organismos_correcto(self, client):
        r = client.get("/")
        # El CSV de prueba tiene 3 organismos
        assert r.json()["total_organismos"] == 3

    def test_repos_conectados_presentes(self, client):
        r = client.get("/")
        repos = r.json().get("repos_conectados", [])
        assert len(repos) > 0


# ═══════════════════════════════════════════════════════
# GET /datos — Dataset completo
# ═══════════════════════════════════════════════════════

class TestDatos:

    def test_status_200(self, client):
        r = client.get("/datos")
        assert r.status_code == 200

    def test_503_sin_csv(self, client_sin_csv):
        r = client_sin_csv.get("/datos")
        assert r.status_code == 503

    def test_estructura_respuesta(self, client):
        r = client.get("/datos")
        data = r.json()
        assert "total" in data
        assert "datos" in data

    def test_total_coincide_con_lista(self, client):
        r = client.get("/datos")
        data = r.json()
        assert data["total"] == len(data["datos"])

    def test_total_correcto(self, client):
        r = client.get("/datos")
        assert r.json()["total"] == 3

    def test_cada_registro_tiene_campos(self, client):
        r = client.get("/datos")
        for d in r.json()["datos"]:
            assert "Organismo" in d
            assert "IRI (Score)" in d
            assert "Estado" in d

    def test_filtro_area(self, client):
        r = client.get("/datos?area=Legislativo")
        data = r.json()
        assert data["total"] == 2  # fixture tiene 2 registros en Poder Legislativo
        for d in data["datos"]:
            assert "Legislativo" in d["Area"]

    def test_filtro_estado_alto(self, client):
        r = client.get("/datos?estado=ALTO")
        data = r.json()
        assert data["total"] == 1
        assert "ALTO" in data["datos"][0]["Estado"]

    def test_filtro_area_inexistente(self, client):
        r = client.get("/datos?area=AreaQueNoExiste12345")
        data = r.json()
        assert data["total"] == 0
        assert data["datos"] == []

    def test_filtros_combinados(self, client):
        r = client.get("/datos?area=Legislativo&estado=ALTO")
        data = r.json()
        assert data["total"] == 1


# ═══════════════════════════════════════════════════════
# GET /por-area/{area}
# ═══════════════════════════════════════════════════════

class TestPorArea:

    def test_status_200(self, client):
        r = client.get("/por-area/Legislativo")
        assert r.status_code == 200

    def test_503_sin_csv(self, client_sin_csv):
        r = client_sin_csv.get("/por-area/Legislativo")
        assert r.status_code == 503

    def test_area_no_encontrada_404(self, client):
        r = client.get("/por-area/AreaQueNoExisteXYZ")
        assert r.status_code == 404

    def test_estructura_respuesta(self, client):
        r = client.get("/por-area/Legislativo")
        data = r.json()
        assert "area" in data
        assert "total" in data
        assert "iri_promedio" in data
        assert "organismos" in data

    def test_total_correcto(self, client):
        r = client.get("/por-area/Legislativo")
        data = r.json()
        assert data["total"] == 2

    def test_iri_promedio_es_numero(self, client):
        r = client.get("/por-area/Legislativo")
        iri = r.json()["iri_promedio"]
        assert isinstance(iri, (int, float))
        assert 0 <= iri <= 100

    def test_ordenado_por_iri_desc(self, client):
        r = client.get("/por-area/Legislativo")
        scores = [d["IRI (Score)"] for d in r.json()["organismos"]]
        assert scores == sorted(scores, reverse=True)

    def test_error_incluye_areas_disponibles(self, client):
        r = client.get("/por-area/AreaInexistente")
        assert r.status_code == 404
        assert "detail" in r.json()

    def test_busqueda_case_insensitive(self, client):
        # "control" debe matchear "Control y Justicia"
        r = client.get("/por-area/control")
        assert r.status_code == 200
        assert r.json()["total"] >= 1


# ═══════════════════════════════════════════════════════
# GET /top-riesgo
# ═══════════════════════════════════════════════════════

class TestTopRiesgo:

    def test_status_200(self, client):
        r = client.get("/top-riesgo")
        assert r.status_code == 200

    def test_503_sin_csv(self, client_sin_csv):
        r = client_sin_csv.get("/top-riesgo")
        assert r.status_code == 503

    def test_default_n_10(self, client):
        r = client.get("/top-riesgo")
        data = r.json()
        assert data["n"] == 10
        # Sólo hay 3 organismos → devuelve 3
        assert len(data["organismos"]) == 3

    def test_n_personalizado(self, client):
        r = client.get("/top-riesgo?n=2")
        data = r.json()
        assert data["n"] == 2
        assert len(data["organismos"]) == 2

    def test_n_maximo_50(self, client):
        """n se limita a 50 aunque se pida más."""
        r = client.get("/top-riesgo?n=200")
        data = r.json()
        assert data["n"] == 50

    def test_ordenado_por_iri_desc(self, client):
        r = client.get("/top-riesgo")
        scores = [d["IRI (Score)"] for d in r.json()["organismos"]]
        assert scores == sorted(scores, reverse=True)

    def test_primer_registro_es_el_de_mayor_iri(self, client):
        r = client.get("/top-riesgo?n=1")
        org = r.json()["organismos"][0]["Organismo"]
        # El organismo de mayor IRI en el fixture es "Organismo Test B" (72.5)
        assert org == "Organismo Test B"


# ═══════════════════════════════════════════════════════
# GET /resumen
# ═══════════════════════════════════════════════════════

class TestResumen:

    def test_status_200(self, client):
        r = client.get("/resumen")
        assert r.status_code == 200

    def test_503_sin_csv(self, client_sin_csv):
        r = client_sin_csv.get("/resumen")
        assert r.status_code == 503

    def test_estructura_global(self, client):
        r = client.get("/resumen")
        g = r.json()["global"]
        for campo in ["total_organismos", "iri_promedio_global", "iri_max", "iri_min",
                      "alto_riesgo", "medio_riesgo", "bajo_riesgo"]:
            assert campo in g, f"Falta campo '{campo}' en global"

    def test_total_organismos_correcto(self, client):
        r = client.get("/resumen")
        assert r.json()["global"]["total_organismos"] == 3

    def test_suma_estados_igual_total(self, client):
        r = client.get("/resumen")
        g = r.json()["global"]
        suma = g["alto_riesgo"] + g["medio_riesgo"] + g["bajo_riesgo"]
        assert suma == g["total_organismos"]

    def test_iri_max_mayor_que_min(self, client):
        r = client.get("/resumen")
        g = r.json()["global"]
        assert g["iri_max"] >= g["iri_min"]

    def test_iri_promedio_entre_min_y_max(self, client):
        r = client.get("/resumen")
        g = r.json()["global"]
        assert g["iri_min"] <= g["iri_promedio_global"] <= g["iri_max"]

    def test_por_area_presente(self, client):
        r = client.get("/resumen")
        assert "por_area" in r.json()
        assert isinstance(r.json()["por_area"], list)

    def test_conteo_por_estado_fixture(self, client):
        """Fixture: 1 ALTO, 1 MEDIO, 1 BAJO."""
        r = client.get("/resumen")
        g = r.json()["global"]
        assert g["alto_riesgo"] == 1
        assert g["medio_riesgo"] == 1
        assert g["bajo_riesgo"] == 1


# ═══════════════════════════════════════════════════════
# POST /refresh
# ═══════════════════════════════════════════════════════

class TestRefresh:

    def test_401_sin_token(self, client):
        r = client.post("/refresh")
        assert r.status_code == 401

    def test_401_token_incorrecto(self, client):
        r = client.post("/refresh", headers={"X-Refresh-Token": "token-malo"})
        assert r.status_code == 401

    def test_401_mensaje_error(self, client):
        r = client.post("/refresh", headers={"X-Refresh-Token": "token-malo"})
        assert "invalido" in r.json()["detail"].lower() or "invalid" in r.json()["detail"].lower()

    def test_token_valido_acepta_peticion(self, client):
        """Con token correcto no debe devolver 401 (puede fallar el motor, pero no auth)."""
        r = client.post("/refresh", headers={"X-Refresh-Token": "test-token-seguro"})
        assert r.status_code != 401


# ═══════════════════════════════════════════════════════
# GET /dashboard — HTML
# ═══════════════════════════════════════════════════════

class TestDashboard:

    def test_status_200(self, client):
        r = client.get("/dashboard")
        assert r.status_code == 200

    def test_content_type_html(self, client):
        r = client.get("/dashboard")
        assert "text/html" in r.headers["content-type"]

    def test_contiene_titulo(self, client):
        r = client.get("/dashboard")
        assert "Monitor" in r.text

    def test_contiene_plotly(self, client):
        r = client.get("/dashboard")
        assert "plotly" in r.text.lower()

    def test_contiene_iri(self, client):
        r = client.get("/dashboard")
        assert "IRI" in r.text
