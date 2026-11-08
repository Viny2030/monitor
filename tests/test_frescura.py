"""GET /api/frescura y la ficha del dashboard."""
import os
import sys

from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import main  # noqa: E402


def test_frescura_monitor():
    c = TestClient(main.app)
    d = c.get("/api/frescura").json()
    assert d["formato"] == "frescura/1" and d["sitio"] == "monitor"
    assert [x["id"] for x in d["datos"]] == ["iri", "comprar"]
    assert c.get("/frescura.js").status_code == 200
    assert 'frescura-ficha' in c.get("/dashboard").text
