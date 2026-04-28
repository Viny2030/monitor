"""
main.py — FastAPI del Monitor IRI
==================================
Endpoints:
  GET /                    → health check + lista de endpoints
  GET /dashboard           → dashboard HTML interactivo (sin Streamlit)
  GET /datos               → dataset completo (JSON)
  GET /por-area/{area}     → organismos filtrados por área
  GET /top-riesgo          → top N organismos de mayor IRI
  GET /resumen             → estadísticas globales por área
  POST /refresh            → regenera el CSV (protegido por REFRESH_TOKEN)

Variables de entorno:
  REFRESH_TOKEN            → token para /refresh (default: "dev")
  LEGISTATIVO_API_URL      → URL Railway de monitor_legistativo (opcional)
  SENADORES_API_URL        → URL Railway de monitor_legistativo_senadores (opcional)
  JUSTICIA_API_URL         → URL Railway de justicia (opcional)
"""

import os
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import pandas as pd

app = FastAPI(
    title="Monitor IRI API",
    description="Monitor de Riesgo Institucional — Argentina",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

REFRESH_TOKEN = os.getenv("REFRESH_TOKEN", "dev")
CSV_PATH = "data/processed/monitor_completo.csv"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_df() -> pd.DataFrame:
    if not os.path.exists(CSV_PATH):
        raise HTTPException(
            status_code=503,
            detail="Dataset no disponible. Ejecutá POST /refresh o python motor_analitico.py",
        )
    return pd.read_csv(CSV_PATH)


def _df_to_records(df: pd.DataFrame) -> list:
    return df.fillna("").to_dict(orient="records")


# ── Endpoints JSON ─────────────────────────────────────────────────────────────

@app.get("/")
def health():
    existe = os.path.exists(CSV_PATH)
    n = len(pd.read_csv(CSV_PATH)) if existe else 0
    return {
        "status": "ok",
        "proyecto": "Monitor IRI — Argentina",
        "version": "2.0",
        "dataset_disponible": existe,
        "total_organismos": n,
        "repos_conectados": [
            "github.com/Viny2030/justicia",
            "github.com/Viny2030/monitor_legistativo",
            "github.com/Viny2030/monitor_legistativo_senadores",
        ],
        "endpoints": {
            "dashboard":  "/dashboard",
            "datos":      "/datos",
            "por_area":   "/por-area/{area}",
            "top_riesgo": "/top-riesgo?n=10",
            "resumen":    "/resumen",
            "refresh":    "POST /refresh  (header X-Refresh-Token)",
            "docs":       "/docs",
        },
    }


@app.get("/datos")
def get_datos(area: str = None, estado: str = None):
    """Dataset completo. Filtros opcionales: ?area=Poder+Judicial&estado=ALTO"""
    df = _load_df()
    if area:
        df = df[df["Area"].str.contains(area, case=False, na=False)]
    if estado:
        df = df[df["Estado"].str.contains(estado, case=False, na=False)]
    return {"total": len(df), "datos": _df_to_records(df)}


@app.get("/por-area/{area}")
def get_por_area(area: str):
    """Organismos de un área, ordenados por IRI descendente."""
    df = _load_df()
    df_area = df[df["Area"].str.contains(area, case=False, na=False)]
    if df_area.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Área '{area}' no encontrada. Disponibles: {df['Area'].unique().tolist()}",
        )
    df_area = df_area.sort_values("IRI (Score)", ascending=False)
    return {
        "area": area,
        "total": len(df_area),
        "iri_promedio": round(df_area["IRI (Score)"].mean(), 2),
        "organismos": _df_to_records(df_area),
    }


@app.get("/top-riesgo")
def get_top_riesgo(n: int = 10):
    """Top N organismos con mayor score IRI (máx 50)."""
    n = min(n, 50)
    df = _load_df()
    return {"n": n, "organismos": _df_to_records(df.nlargest(n, "IRI (Score)"))}


@app.get("/resumen")
def get_resumen():
    """Estadísticas globales y breakdown por área."""
    df = _load_df()
    global_stats = {
        "total_organismos":    len(df),
        "iri_promedio_global": round(df["IRI (Score)"].mean(), 2),
        "iri_max":             round(df["IRI (Score)"].max(), 2),
        "iri_min":             round(df["IRI (Score)"].min(), 2),
        "alto_riesgo":  int((df["Estado"] == "🔴 ALTO").sum()),
        "medio_riesgo": int((df["Estado"] == "🟡 MEDIO").sum()),
        "bajo_riesgo":  int((df["Estado"] == "🟢 BAJO").sum()),
    }
    por_area = (
        df.groupby("Area")
        .agg(organismos=("Organismo", "count"), iri_promedio=("IRI (Score)", "mean"), iri_max=("IRI (Score)", "max"))
        .round(2).reset_index()
        .sort_values("iri_promedio", ascending=False)
        .to_dict(orient="records")
    )
    fuentes = df["Fuente"].value_counts().to_dict() if "Fuente" in df.columns else {}
    return {"global": global_stats, "por_area": por_area, "fuentes_de_datos": fuentes}


@app.post("/refresh")
def refresh(x_refresh_token: str = Header(None)):
    """Regenera el CSV ejecutando motor_analitico.py."""
    if x_refresh_token != REFRESH_TOKEN:
        raise HTTPException(status_code=401, detail="Token inválido")
    import subprocess, sys
    try:
        result = subprocess.run(
            [sys.executable, "motor_analitico.py"],
            capture_output=True, text=True, timeout=300,
        )
        return {
            "status": "ok" if result.returncode == 0 else "error",
            "stdout": result.stdout[-2000:],
            "stderr": result.stderr[-500:],
        }
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Motor timeout (>5min)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Dashboard HTML ─────────────────────────────────────────────────────────────

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    """
    Dashboard interactivo servido directamente desde FastAPI.
    Usa Plotly.js (CDN) para los gráficos y fetch() para consumir /resumen y /datos.
    No requiere Streamlit ni ninguna dependencia adicional.
    """
    html = """<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Monitor IRI — Argentina</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: system-ui, -apple-system, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; }
    header { background: #1e293b; border-bottom: 1px solid #334155; padding: 1rem 1.5rem; display: flex; align-items: center; gap: 1rem; }
    header h1 { font-size: 1.3rem; font-weight: 700; }
    header span { font-size: 0.8rem; color: #94a3b8; }
    #refresh-btn { margin-left: auto; background: #3b82f6; border: none; color: white; padding: 0.4rem 1rem; border-radius: 6px; cursor: pointer; font-size: 0.85rem; }
    #refresh-btn:hover { background: #2563eb; }
    #status { font-size: 0.75rem; color: #94a3b8; margin-left: 0.5rem; }
    .metrics { display: flex; gap: 1rem; padding: 1.2rem 1.5rem; flex-wrap: wrap; }
    .card { background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 1rem 1.4rem; min-width: 150px; flex: 1; }
    .card .label { font-size: 0.72rem; color: #94a3b8; text-transform: uppercase; letter-spacing: .05em; margin-bottom: .3rem; }
    .card .value { font-size: 1.8rem; font-weight: 700; }
    .red   { color: #f87171; }
    .amber { color: #fbbf24; }
    .green { color: #4ade80; }
    .blue  { color: #60a5fa; }
    .section { padding: 0 1.5rem 1.5rem; }
    .section h2 { font-size: 1rem; font-weight: 600; margin-bottom: 0.8rem; color: #cbd5e1; }
    #filters { display: flex; gap: 1rem; margin-bottom: 1rem; flex-wrap: wrap; }
    select, input { background: #1e293b; border: 1px solid #475569; color: #e2e8f0; padding: 0.4rem 0.7rem; border-radius: 6px; font-size: 0.85rem; }
    select:focus, input:focus { outline: none; border-color: #3b82f6; }
    #chart-area { background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 0.5rem; }
    table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }
    th { background: #1e293b; color: #94a3b8; text-align: left; padding: 0.55rem 0.75rem; font-weight: 600; position: sticky; top: 0; z-index: 1; }
    td { padding: 0.5rem 0.75rem; border-top: 1px solid #1e293b; }
    tr:hover td { background: #1e293b55; }
    .badge { display: inline-block; padding: 0.2rem 0.55rem; border-radius: 999px; font-size: 0.75rem; font-weight: 600; }
    .badge-red   { background: #450a0a; color: #f87171; }
    .badge-amber { background: #451a03; color: #fbbf24; }
    .badge-green { background: #052e16; color: #4ade80; }
    #table-wrap { background: #0f172a; border: 1px solid #334155; border-radius: 10px; overflow: auto; max-height: 420px; }
    .row-charts { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-bottom: 1.5rem; }
    @media (max-width: 700px) { .row-charts { grid-template-columns: 1fr; } }
    .chart-box { background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 0.5rem; }
    #loader { text-align: center; padding: 3rem; color: #475569; }
    footer { text-align: center; padding: 1.5rem; font-size: 0.75rem; color: #475569; border-top: 1px solid #1e293b; }
    a { color: #60a5fa; }
  </style>
</head>
<body>

<header>
  <span style="font-size:1.6rem">🚦</span>
  <div>
    <h1>Monitor de Riesgo Institucional (IRI)</h1>
    <span>Argentina · Semáforo de integridad pública · v2.0</span>
  </div>
  <button id="refresh-btn" onclick="loadAll()">⟳ Actualizar</button>
  <span id="status"></span>
</header>

<div id="loader">Cargando datos…</div>

<div id="app" style="display:none">

  <!-- Métricas -->
  <div class="metrics">
    <div class="card"><div class="label">Organismos</div><div class="value blue" id="m-total">—</div></div>
    <div class="card"><div class="label">IRI Promedio</div><div class="value" id="m-avg">—</div></div>
    <div class="card"><div class="label">🔴 Alto riesgo</div><div class="value red" id="m-alto">—</div></div>
    <div class="card"><div class="label">🟡 Riesgo medio</div><div class="value amber" id="m-medio">—</div></div>
    <div class="card"><div class="label">🟢 Bajo riesgo</div><div class="value green" id="m-bajo">—</div></div>
  </div>

  <!-- Gráficos superiores -->
  <div class="section">
    <div class="row-charts">
      <div class="chart-box">
        <div id="chart-area-donut" style="height:280px"></div>
      </div>
      <div class="chart-box">
        <div id="chart-area-bar" style="height:280px"></div>
      </div>
    </div>
  </div>

  <!-- Gráfico principal: top 30 organismos -->
  <div class="section">
    <h2>📊 Score IRI por Organismo (top 30)</h2>
    <div id="chart-area" style="height:520px"></div>
  </div>

  <!-- Tabla -->
  <div class="section">
    <h2>📋 Tabla de Organismos</h2>
    <div id="filters">
      <input id="search" placeholder="🔍 Buscar organismo…" oninput="renderTable()"/>
      <select id="filter-area" onchange="renderTable()"><option value="">Todas las áreas</option></select>
      <select id="filter-estado" onchange="renderTable()">
        <option value="">Todos los estados</option>
        <option value="ALTO">🔴 ALTO</option>
        <option value="MEDIO">🟡 MEDIO</option>
        <option value="BAJO">🟢 BAJO</option>
      </select>
    </div>
    <div id="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Estado</th><th>Organismo</th><th>Área</th>
            <th>IRI</th><th>R.Fin</th><th>R.Con</th><th>R.Ope</th><th>R.Dat</th><th>Fuente</th>
          </tr>
        </thead>
        <tbody id="table-body"></tbody>
      </table>
    </div>
    <div id="table-count" style="font-size:0.75rem;color:#64748b;margin-top:0.4rem;"></div>
  </div>

  <!-- Fuentes -->
  <div class="section">
    <h2>🔎 Fuentes de datos</h2>
    <div id="fuentes" style="font-size:0.8rem;color:#94a3b8;line-height:1.8;"></div>
  </div>

</div>

<footer>
  Monitor IRI v2.0 · <a href="https://github.com/Viny2030/monitor" target="_blank">github.com/Viny2030/monitor</a>
  · Datos actualizados vía <code>POST /refresh</code>
</footer>

<script>
  let allData = [];

  async function loadAll() {
    document.getElementById('status').textContent = 'Actualizando…';
    try {
      const [resData, datosData] = await Promise.all([
        fetch('/resumen').then(r => r.json()),
        fetch('/datos').then(r => r.json()),
      ]);

      const g = resData.global;
      document.getElementById('m-total').textContent = g.total_organismos;
      document.getElementById('m-avg').textContent   = g.iri_promedio_global.toFixed(1);
      document.getElementById('m-alto').textContent  = g.alto_riesgo;
      document.getElementById('m-medio').textContent = g.medio_riesgo;
      document.getElementById('m-bajo').textContent  = g.bajo_riesgo;

      allData = datosData.datos;
      populateAreaFilter(allData);
      renderTable();
      renderBarChart(allData);
      renderDonut(g);
      renderAreaBar(resData.por_area);
      renderFuentes(resData.fuentes_de_datos);

      document.getElementById('loader').style.display = 'none';
      document.getElementById('app').style.display    = 'block';
      document.getElementById('status').textContent   = '✓ ' + new Date().toLocaleTimeString();
    } catch(e) {
      document.getElementById('loader').textContent = '❌ Error al cargar: ' + e.message;
      document.getElementById('status').textContent = '';
    }
  }

  function populateAreaFilter(data) {
    const areas = [...new Set(data.map(d => d.Area))].sort();
    const sel = document.getElementById('filter-area');
    sel.innerHTML = '<option value="">Todas las áreas</option>';
    areas.forEach(a => { const o = document.createElement('option'); o.value = a; o.textContent = a; sel.appendChild(o); });
  }

  function filterData() {
    const search = document.getElementById('search').value.toLowerCase();
    const area   = document.getElementById('filter-area').value;
    const estado = document.getElementById('filter-estado').value;
    return allData.filter(d => {
      const matchSearch = !search || d.Organismo.toLowerCase().includes(search);
      const matchArea   = !area   || d.Area === area;
      const matchEstado = !estado || (d.Estado || '').includes(estado);
      return matchSearch && matchArea && matchEstado;
    });
  }

  function badgeClass(estado) {
    if (estado.includes('ALTO'))  return 'badge badge-red';
    if (estado.includes('MEDIO')) return 'badge badge-amber';
    return 'badge badge-green';
  }

  function iriColor(v) {
    if (v >= 60) return '#f87171';
    if (v >= 30) return '#fbbf24';
    return '#4ade80';
  }

  function renderTable() {
    const rows = filterData().sort((a,b) => b['IRI (Score)'] - a['IRI (Score)']);
    const tbody = document.getElementById('table-body');
    tbody.innerHTML = rows.map(d => `
      <tr>
        <td><span class="${badgeClass(d.Estado || '')}">${d.Estado || '—'}</span></td>
        <td>${d.Organismo}</td>
        <td style="color:#94a3b8">${d.Area}</td>
        <td style="color:${iriColor(d['IRI (Score)'])};font-weight:700">${(+d['IRI (Score)']).toFixed(1)}</td>
        <td>${(+d['Riesgo Financiero']).toFixed(0)}</td>
        <td>${(+d['Riesgo Contratación']).toFixed(0)}</td>
        <td>${(+d['Riesgo Operativo']).toFixed(0)}</td>
        <td>${(+d['Riesgo Datos']).toFixed(0)}</td>
        <td style="color:#475569;font-size:0.72rem">${(d.Fuente || '').split('_')[0]}</td>
      </tr>`).join('');
    document.getElementById('table-count').textContent =
      rows.length + ' de ' + allData.length + ' organismos';
  }

  function renderBarChart(data) {
    const top = [...data].sort((a,b) => b['IRI (Score)'] - a['IRI (Score)']).slice(0, 30).reverse();
    const colors = top.map(d => d.Estado.includes('ALTO') ? '#f87171' : d.Estado.includes('MEDIO') ? '#fbbf24' : '#4ade80');
    Plotly.react('chart-area', [{
      type: 'bar', orientation: 'h',
      x: top.map(d => d['IRI (Score)']),
      y: top.map(d => d.Organismo),
      marker: { color: colors },
      hovertemplate: '<b>%{y}</b><br>IRI: %{x:.1f}<extra></extra>',
    }], {
      paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
      font: { color: '#e2e8f0', size: 11 },
      margin: { l: 260, r: 30, t: 10, b: 40 },
      xaxis: { range: [0, 100], gridcolor: '#1e293b', title: 'Score IRI' },
      yaxis: { automargin: true, tickfont: { size: 10 } },
      shapes: [
        { type: 'line', x0: 60, x1: 60, y0: 0, y1: 1, yref: 'paper', line: { color: '#f87171', dash: 'dot', width: 1 } },
        { type: 'line', x0: 30, x1: 30, y0: 0, y1: 1, yref: 'paper', line: { color: '#fbbf24', dash: 'dot', width: 1 } },
      ],
    }, { responsive: true });
  }

  function renderDonut(g) {
    Plotly.react('chart-area-donut', [{
      type: 'pie', hole: 0.55,
      values: [g.alto_riesgo, g.medio_riesgo, g.bajo_riesgo],
      labels: ['🔴 Alto', '🟡 Medio', '🟢 Bajo'],
      marker: { colors: ['#f87171', '#fbbf24', '#4ade80'] },
      textinfo: 'label+percent',
      textfont: { color: '#e2e8f0', size: 11 },
      hovertemplate: '%{label}: %{value} organismos<extra></extra>',
    }], {
      paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
      font: { color: '#e2e8f0' },
      showlegend: false,
      title: { text: 'Distribución por riesgo', font: { size: 12, color: '#94a3b8' }, x: 0.5 },
      margin: { l: 20, r: 20, t: 40, b: 20 },
    }, { responsive: true });
  }

  function renderAreaBar(porArea) {
    if (!porArea || !porArea.length) return;
    const sorted = [...porArea].sort((a,b) => b.iri_promedio - a.iri_promedio);
    const colors = sorted.map(d => d.iri_promedio >= 60 ? '#f87171' : d.iri_promedio >= 30 ? '#fbbf24' : '#4ade80');
    Plotly.react('chart-area-bar', [{
      type: 'bar', orientation: 'h',
      x: sorted.map(d => d.iri_promedio),
      y: sorted.map(d => d.Area),
      marker: { color: colors },
      hovertemplate: '<b>%{y}</b><br>IRI prom: %{x:.1f}<extra></extra>',
    }], {
      paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
      font: { color: '#e2e8f0', size: 11 },
      margin: { l: 170, r: 20, t: 10, b: 40 },
      xaxis: { range: [0, 100], gridcolor: '#1e293b', title: 'IRI Promedio' },
      yaxis: { automargin: true },
      title: { text: 'IRI promedio por área', font: { size: 12, color: '#94a3b8' }, x: 0.5 },
    }, { responsive: true });
  }

  function renderFuentes(fuentes) {
    if (!fuentes) return;
    document.getElementById('fuentes').innerHTML =
      Object.entries(fuentes).map(([k,v]) =>
        `<span style="margin-right:1.5rem"><code style="color:#7dd3fc">${k}</code> → <b>${v}</b> organismos</span>`
      ).join('');
  }

  // Carga inicial + auto-refresh cada 5 min
  loadAll();
  setInterval(loadAll, 300_000);
</script>
</body>
</html>"""
    return HTMLResponse(content=html)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)