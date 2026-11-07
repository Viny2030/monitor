"""Tests del grafo organismo ↔ proveedor (connector._grafo_comprar y GET /grafo)."""
import json
import os
import sys

import pandas as pd
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import connector  # noqa: E402


def _adjudicaciones():
    filas = []
    # Organismo 101: un proveedor grande (directa) y tres chicos; 102: comparte el grande
    for i, (saf, cuit, monto, tipo, moneda) in enumerate([
        ("101", "30-11111111-1", "800", "Contratación Directa", "Peso Argentino"),
        ("101", "30-22222222-2", "100", "Licitación Pública", "Peso Argentino"),
        ("101", "30-33333333-3", "60", "Licitación Pública", "Peso Argentino"),
        ("101", "30-44444444-4", "40", "Licitación Privada", "Peso Argentino"),
        ("101", "30-55555555-5", "9999", "Licitación Pública", "Dólar Estadounidense"),
        ("102", "30-11111111-1", "500", "Licitación Pública", "Peso Argentino"),
        ("102", "30-66666666-6", "500", "Licitación Pública", "Peso Argentino"),
        ("102", "sin cuit", "1", "Licitación Pública", "Peso Argentino"),
    ]):
        filas.append({"Numero_Proceso": f"P{i}", "_saf": saf, "CUIT": cuit, "Monto": monto,
                      "Tipo_de_Procedimiento": tipo, "Moneda": moneda,
                      "Descripcion_Proveedor": f"Proveedor {cuit[-1]}"})
    return pd.DataFrame(filas)


NOMBRES = pd.Series({"101": "101 - Fundación A", "102": "102 - Instituto B"})


def test_grafo_comprar_nodos_aristas_y_concentracion():
    g = connector._grafo_comprar(_adjudicaciones(), NOMBRES, ["101", "102"], "2025-2026")
    org = {n["id"]: n for n in g["nodos"] if n["tipo"] == "organismo"}
    prov = {n["id"]: n for n in g["nodos"] if n["tipo"] == "proveedor"}
    assert set(org) == {"o:101", "o:102"}
    # Solo pesos y CUIT válidos: el de dólares y "sin cuit" no entran
    assert "p:30-55555555-5" not in prov and len(prov) == 5
    assert org["o:101"]["concentracion"] == 80.0  # 800 de 1.000
    assert prov["p:30-11111111-1"]["organismos_total"] == 2
    a = next(x for x in g["aristas"] if x["from"] == "o:101" and x["rango"] == 1)
    assert a["to"] == "p:30-11111111-1" and a["directas"] == 1


def test_grafo_comprar_solo_organismos_con_dato():
    g = connector._grafo_comprar(_adjudicaciones(), NOMBRES, ["102"], "2025-2026")
    assert {n["id"] for n in g["nodos"] if n["tipo"] == "organismo"} == {"o:102"}
    assert connector._grafo_comprar(_adjudicaciones(), NOMBRES, [], "2025-2026") is None


def test_filtrar_grafo_top_y_compartidos():
    import main
    g = connector._grafo_comprar(_adjudicaciones(), NOMBRES, ["101", "102"], "2025-2026")
    f1 = main._filtrar_grafo(g, top=1, solo_compartidos=False)
    assert f1["resumen"]["aristas"] == 2 and f1["resumen"]["compartidos"] == 1
    f2 = main._filtrar_grafo(g, top=10, solo_compartidos=True)
    assert {n["id"] for n in f2["nodos"] if n["tipo"] == "proveedor"} == {"p:30-11111111-1"}
    assert f2["resumen"]["top_compartidos"][0]["organismos_en_grafo"] == 2


def test_endpoint_grafo(tmp_path, monkeypatch):
    import main
    ruta = tmp_path / "grafo.json"
    monkeypatch.setattr(main, "GRAFO_PATH", str(ruta))
    c = TestClient(main.app)
    assert c.get("/grafo").status_code == 503  # todavía no se generó
    g = connector._grafo_comprar(_adjudicaciones(), NOMBRES, ["101", "102"], "2025-2026")
    ruta.write_text(json.dumps(g), encoding="utf-8")
    r = c.get("/grafo", params={"top": 2})
    assert r.status_code == 200
    d = r.json()
    assert d["resumen"]["periodo"] == "2025-2026" and d["resumen"]["aristas"] == 4
    assert c.get("/grafo", params={"top": 11}).status_code == 422


def test_dashboard_tiene_pestana_y_manual_del_grafo():
    import main
    html = main.DASHBOARD_HTML
    assert "showPage('grafo',this)" in html and 'id="page-grafo"' in html
    assert "Grafo organismo ↔ proveedor" in html and "Body ↔ supplier graph" in html
