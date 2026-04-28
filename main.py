"""
main.py - FastAPI del Monitor IRI
Endpoints:
  GET /             health check
  GET /dashboard    dashboard HTML interactivo (sin Streamlit)
  GET /datos        dataset completo (JSON)
  GET /por-area/    organismos filtrados por area
  GET /top-riesgo   top N organismos de mayor IRI
  GET /resumen      estadisticas globales por area
  POST /refresh     regenera el CSV (protegido por REFRESH_TOKEN)

Variables de entorno:
  REFRESH_TOKEN, LEGISTATIVO_API_URL, SENADORES_API_URL, JUSTICIA_API_URL
"""

import os
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import pandas as pd

app = FastAPI(
    title="Monitor IRI API",
    description="Monitor de Riesgo Institucional - Argentina",
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


def _load_df() -> pd.DataFrame:
    if not os.path.exists(CSV_PATH):
        raise HTTPException(
            status_code=503,
            detail="Dataset no disponible. Ejecuta POST /refresh o python motor_analitico.py",
        )
    return pd.read_csv(CSV_PATH)


def _df_to_records(df: pd.DataFrame) -> list:
    return df.fillna("").to_dict(orient="records")


@app.get("/")
def health():
    existe = os.path.exists(CSV_PATH)
    n = len(pd.read_csv(CSV_PATH)) if existe else 0
    return {
        "status": "ok",
        "proyecto": "Monitor IRI - Argentina",
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
            "refresh":    "POST /refresh (header X-Refresh-Token)",
            "docs":       "/docs",
        },
    }


@app.get("/datos")
def get_datos(area: str = None, estado: str = None):
    df = _load_df()
    if area:
        df = df[df["Area"].str.contains(area, case=False, na=False)]
    if estado:
        df = df[df["Estado"].str.contains(estado, case=False, na=False)]
    return {"total": len(df), "datos": _df_to_records(df)}


@app.get("/por-area/{area}")
def get_por_area(area: str):
    df = _load_df()
    df_area = df[df["Area"].str.contains(area, case=False, na=False)]
    if df_area.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Area '{area}' no encontrada. Disponibles: {df['Area'].unique().tolist()}",
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
    n = min(n, 50)
    df = _load_df()
    return {"n": n, "organismos": _df_to_records(df.nlargest(n, "IRI (Score)"))}


@app.get("/resumen")
def get_resumen():
    df = _load_df()
    global_stats = {
        "total_organismos":    len(df),
        "iri_promedio_global": round(df["IRI (Score)"].mean(), 2),
        "iri_max":             round(df["IRI (Score)"].max(), 2),
        "iri_min":             round(df["IRI (Score)"].min(), 2),
        "alto_riesgo":  int((df["Estado"] == "\U0001f534 ALTO").sum()),
        "medio_riesgo": int((df["Estado"] == "\U0001f7e1 MEDIO").sum()),
        "bajo_riesgo":  int((df["Estado"] == "\U0001f7e2 BAJO").sum()),
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
    if x_refresh_token != REFRESH_TOKEN:
        raise HTTPException(status_code=401, detail="Token invalido")
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


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Monitor IRI - Argentina</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: system-ui, -apple-system, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; }
    header { background: #1e293b; border-bottom: 1px solid #334155; padding: 1rem 1.5rem; display: flex; align-items: center; gap: 1rem; }
    header h1 { font-size: 1.3rem; font-weight: 700; }
    header span.sub { font-size: 0.8rem; color: #94a3b8; }
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
    #filters { display: flex; gap: 1rem; margin-bottom: 1rem; flex-wrap: wrap; align-items: center; }
    select, input { background: #1e293b; border: 1px solid #475569; color: #e2e8f0; padding: 0.4rem 0.7rem; border-radius: 6px; font-size: 0.85rem; }
    select:focus, input:focus { outline: none; border-color: #3b82f6; }
    .chart-main { background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 0.5rem; }
    table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }
    th { background: #1e293b; color: #94a3b8; text-align: left; padding: 0.6rem 0.75rem; font-weight: 600; position: sticky; top: 0; z-index: 1; cursor: help; border-bottom: 1px solid #334155; }
    th:hover { color: #e2e8f0; }
    td { padding: 0.5rem 0.75rem; border-top: 1px solid #1e293b55; }
    tr:hover td { background: #1e293b88; }
    .badge { display: inline-block; padding: 0.2rem 0.55rem; border-radius: 999px; font-size: 0.75rem; font-weight: 600; }
    .badge-red   { background: #450a0a; color: #f87171; }
    .badge-amber { background: #451a03; color: #fbbf24; }
    .badge-green { background: #052e16; color: #4ade80; }
    #table-wrap { background: #0f172a; border: 1px solid #334155; border-radius: 10px; overflow: auto; max-height: 440px; }
    .row-charts { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-bottom: 1.5rem; }
    @media (max-width: 700px) { .row-charts { grid-template-columns: 1fr; } }
    .chart-box { background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 0.5rem; }
    #loader { text-align: center; padding: 3rem; color: #475569; }
    .leyenda { display: flex; gap: 1.5rem; flex-wrap: wrap; margin-top: 0.7rem; padding: 0.6rem 0.8rem; background: #1e293b55; border-radius: 6px; font-size: 0.75rem; color: #64748b; }
    .leyenda b { color: #94a3b8; }
    .formula { background: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 0.8rem 1.2rem; font-size: 0.8rem; color: #94a3b8; line-height: 1.8; }
    .formula b { color: #cbd5e1; }
    .formula .dim { display: inline-block; background: #0f172a; border-radius: 4px; padding: 0.1rem 0.5rem; margin: 0.1rem; font-family: monospace; }
    footer { text-align: center; padding: 1.5rem; font-size: 0.75rem; color: #475569; border-top: 1px solid #1e293b; margin-top: 1rem; }
    a { color: #60a5fa; }
    .tag-real { color: #4ade80; font-size: 0.7rem; }
    .tag-sint { color: #fbbf24; font-size: 0.7rem; }
  </style>
</head>
<body>

<header>
  <span style="font-size:1.6rem">&#x1F6A6;</span>
  <div>
    <h1>Monitor de Riesgo Institucional (IRI)</h1>
    <span class="sub">Argentina &middot; Semaforo de integridad publica &middot; v2.0</span>
  </div>
  <button id="refresh-btn" onclick="loadAll()">&#x21BA; Actualizar</button>
  <span id="status"></span>
</header>

<div id="loader">Cargando datos&#x2026;</div>

<div id="app" style="display:none">

  <div class="metrics">
    <div class="card"><div class="label">Organismos monitoreados</div><div class="value blue" id="m-total">-</div></div>
    <div class="card"><div class="label">IRI Promedio (0-100)</div><div class="value" id="m-avg">-</div></div>
    <div class="card"><div class="label">&#x1F534; Alto riesgo (IRI &ge;60)</div><div class="value red" id="m-alto">-</div></div>
    <div class="card"><div class="label">&#x1F7E1; Riesgo medio (IRI 30-59)</div><div class="value amber" id="m-medio">-</div></div>
    <div class="card"><div class="label">&#x1F7E2; Bajo riesgo (IRI &lt;30)</div><div class="value green" id="m-bajo">-</div></div>
  </div>

  <div class="section">
    <div class="row-charts">
      <div class="chart-box"><div id="chart-donut" style="height:280px"></div></div>
      <div class="chart-box"><div id="chart-area-bar" style="height:280px"></div></div>
    </div>
  </div>

  <div class="section">
    <h2>&#x1F4CA; Score IRI por Organismo (top 30 de mayor riesgo)</h2>
    <div class="chart-main"><div id="chart-main" style="height:520px"></div></div>
  </div>

  <div class="section">
    <h2>&#x1F4CB; Tabla de Organismos</h2>
    <div id="filters">
      <input id="search" placeholder="&#x1F50D; Buscar organismo..." oninput="renderTable()" style="min-width:220px"/>
      <select id="filter-area" onchange="renderTable()"><option value="">Todas las areas</option></select>
      <select id="filter-estado" onchange="renderTable()">
        <option value="">Todos los estados</option>
        <option value="ALTO">&#x1F534; ALTO</option>
        <option value="MEDIO">&#x1F7E1; MEDIO</option>
        <option value="BAJO">&#x1F7E2; BAJO</option>
      </select>
    </div>
    <div id="table-wrap">
      <table>
        <thead>
          <tr>
            <th title="Nivel de riesgo segun score IRI: ALTO (>=60) | MEDIO (30-59) | BAJO (<30)">Estado</th>
            <th>Organismo</th>
            <th>Area institucional</th>
            <th title="Indice de Riesgo Institucional: suma ponderada de las 4 dimensiones. Escala 0-100.">Score IRI</th>
            <th title="Riesgo Financiero (peso 35%): irregularidades presupuestarias, desvios en ejecucion del gasto, opacidad contable.">Riesgo Financiero</th>
            <th title="Riesgo Contratacion (peso 30%): proporcion de contrataciones directas vs licitaciones, irregularidades en compras publicas.">Riesgo Contratacion</th>
            <th title="Riesgo Operativo (peso 20%): inasistencia legislativa, mora judicial, vacancia de cargos, clearance rate.">Riesgo Operativo</th>
            <th title="Riesgo Datos (peso 15%): falta de transparencia, documentos inaccesibles, baja calidad de datos publicados.">Riesgo Datos</th>
            <th title="Repositorio de origen del dato. Pasa el cursor para ver el nombre completo.">Fuente</th>
          </tr>
        </thead>
        <tbody id="table-body"></tbody>
      </table>
    </div>
    <div id="table-count" style="font-size:0.75rem;color:#64748b;margin-top:0.4rem;"></div>
    <div class="leyenda">
      <span><b>Score IRI</b> = Financiero x35% + Contratacion x30% + Operativo x20% + Datos x15%</span>
      <span><b>&#x1F534; ALTO</b> &ge; 60 &nbsp;&middot;&nbsp; <b>&#x1F7E1; MEDIO</b> 30-59 &nbsp;&middot;&nbsp; <b>&#x1F7E2; BAJO</b> &lt; 30</span>
      <span>Pasa el cursor sobre cada columna para ver su descripcion</span>
    </div>
  </div>

  <div class="section">
    <h2>&#x2139;&#xFE0F; Que mide cada dimension del IRI</h2>
    <div class="formula">
      <p style="margin-bottom:0.6rem">El <b>Indice de Riesgo Institucional (IRI)</b> es un score compuesto (0-100) que agrega cuatro dimensiones de riesgo:</p>
      <p>
        <span class="dim">Financiero 35%</span> Irregularidades en presupuesto, ejecucion del gasto y contratos. Fuente: Presupuesto Abierto / compr.ar (pendiente).
      </p>
      <p>
        <span class="dim">Contratacion 30%</span> Proporcion de contrataciones directas vs licitaciones publicas, adjudicaciones irregulares. Fuente: compr.ar (pendiente).
      </p>
      <p>
        <span class="dim">Operativo 20%</span> Para el Poder Judicial: mora procesal, clearance rate, IRA. Para el Legislativo: inasistencia (NAPE), participation_pct. Fuente: repos justicia y monitor_legistativo.
      </p>
      <p>
        <span class="dim">Datos 15%</span> Accesibilidad documental (IAD), publicacion de informacion, calidad de datos abiertos. Fuente: auditoria manual + repos especializados.
      </p>
      <p style="margin-top:0.6rem;color:#475569">
        <span class="tag-real">&#x2705; Dato real</span> = proviene de un repo especializado (justicia / monitor_legistativo / senadores) &nbsp;&nbsp;
        <span class="tag-sint">&#x26A0;&#xFE0F; Sintetico</span> = generado con seed fija hasta conectar compr.ar
      </p>
    </div>
  </div>

  <div class="section">
    <h2>&#x1F50E; Fuentes de datos</h2>
    <div id="fuentes" style="font-size:0.8rem;color:#94a3b8;line-height:2;"></div>
  </div>

</div>

<footer>
  Monitor IRI v2.0 &middot;
  <a href="https://github.com/Viny2030/monitor" target="_blank">github.com/Viny2030/monitor</a> &middot;
  Datos actualizados via <code>POST /refresh</code>
</footer>

<script>
  let allData = [];

  async function loadAll() {
    document.getElementById('status').textContent = 'Actualizando...';
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
      renderMainBar(allData);
      renderDonut(g);
      renderAreaBar(resData.por_area);
      renderFuentes(resData.fuentes_de_datos);

      document.getElementById('loader').style.display = 'none';
      document.getElementById('app').style.display    = 'block';
      document.getElementById('status').textContent   = 'Actualizado ' + new Date().toLocaleTimeString();
    } catch(e) {
      document.getElementById('loader').textContent = 'Error al cargar: ' + e.message;
      document.getElementById('status').textContent = '';
    }
  }

  function populateAreaFilter(data) {
    const areas = [...new Set(data.map(d => d.Area))].sort();
    const sel = document.getElementById('filter-area');
    sel.innerHTML = '<option value="">Todas las areas</option>';
    areas.forEach(a => {
      const o = document.createElement('option');
      o.value = a; o.textContent = a; sel.appendChild(o);
    });
  }

  function filterData() {
    const search = document.getElementById('search').value.toLowerCase();
    const area   = document.getElementById('filter-area').value;
    const estado = document.getElementById('filter-estado').value;
    return allData.filter(d => {
      return (!search || d.Organismo.toLowerCase().includes(search))
          && (!area   || d.Area === area)
          && (!estado || (d.Estado || '').includes(estado));
    });
  }

  function badgeClass(estado) {
    if ((estado || '').includes('ALTO'))  return 'badge badge-red';
    if ((estado || '').includes('MEDIO')) return 'badge badge-amber';
    return 'badge badge-green';
  }

  function iriColor(v) {
    if (v >= 60) return '#f87171';
    if (v >= 30) return '#fbbf24';
    return '#4ade80';
  }

  function fuenteLabel(fuente) {
    const f = fuente || '';
    if (f.includes('sintetico'))           return '⚠️ Sintetico';
    if (f.includes('senadores'))           return '✅ Senadores';
    if (f.includes('monitor_legistativo')) return '✅ Legislativo';
    if (f.includes('justicia'))            return '✅ Justicia';
    if (f.includes('fallback'))            return 'ὐ4 Fallback';
    return f.split('/')[0] || '-';
  }

  function renderTable() {
    const rows = filterData().sort((a,b) => b['IRI (Score)'] - a['IRI (Score)']);
    document.getElementById('table-body').innerHTML = rows.map(d => `
      <tr>
        <td><span class="${badgeClass(d.Estado)}">${d.Estado || '-'}</span></td>
        <td><b>${d.Organismo}</b></td>
        <td style="color:#94a3b8">${d.Area}</td>
        <td style="color:${iriColor(+d['IRI (Score)'])};font-weight:700;font-size:1rem">${(+d['IRI (Score)']).toFixed(1)}</td>
        <td>${(+d['Riesgo Financiero']).toFixed(0)}</td>
        <td>${(+d['Riesgo Contratación']).toFixed(0)}</td>
        <td>${(+d['Riesgo Operativo']).toFixed(0)}</td>
        <td>${(+d['Riesgo Datos']).toFixed(0)}</td>
        <td title="${d.Fuente || ''}">${fuenteLabel(d.Fuente)}</td>
      </tr>`).join('');
    document.getElementById('table-count').textContent =
      rows.length + ' de ' + allData.length + ' organismos mostrados';
  }

  function renderMainBar(data) {
    const top = [...data].sort((a,b) => b['IRI (Score)'] - a['IRI (Score)']).slice(0,30).reverse();
    const colors = top.map(d => (d.Estado||'').includes('ALTO') ? '#f87171' : (d.Estado||'').includes('MEDIO') ? '#fbbf24' : '#4ade80');
    Plotly.react('chart-main', [{
      type: 'bar', orientation: 'h',
      x: top.map(d => d['IRI (Score)']),
      y: top.map(d => d.Organismo),
      marker: { color: colors },
      hovertemplate: '<b>%{y}</b><br>IRI: %{x:.1f}<extra></extra>',
    }], {
      paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
      font: { color: '#e2e8f0', size: 11 },
      margin: { l: 260, r: 30, t: 10, b: 40 },
      xaxis: { range: [0,100], gridcolor: '#1e293b', title: 'Score IRI (0-100)' },
      yaxis: { automargin: true, tickfont: { size: 10 } },
      shapes: [
        { type:'line', x0:60, x1:60, y0:0, y1:1, yref:'paper', line:{ color:'#f87171', dash:'dot', width:1 } },
        { type:'line', x0:30, x1:30, y0:0, y1:1, yref:'paper', line:{ color:'#fbbf24', dash:'dot', width:1 } },
      ],
    }, { responsive: true });
  }

  function renderDonut(g) {
    Plotly.react('chart-donut', [{
      type: 'pie', hole: 0.55,
      values: [g.alto_riesgo, g.medio_riesgo, g.bajo_riesgo],
      labels: ['Alto riesgo', 'Riesgo medio', 'Bajo riesgo'],
      marker: { colors: ['#f87171','#fbbf24','#4ade80'] },
      textinfo: 'label+percent',
      textfont: { color: '#e2e8f0', size: 11 },
      hovertemplate: '%{label}: %{value} organismos<extra></extra>',
    }], {
      paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
      font: { color: '#e2e8f0' },
      showlegend: false,
      title: { text: 'Distribucion por nivel de riesgo', font: { size: 12, color: '#94a3b8' }, x: 0.5 },
      margin: { l:20, r:20, t:40, b:20 },
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
      hovertemplate: '<b>%{y}</b><br>IRI promedio: %{x:.1f}<extra></extra>',
    }], {
      paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
      font: { color: '#e2e8f0', size: 11 },
      margin: { l:170, r:20, t:40, b:40 },
      xaxis: { range: [0,100], gridcolor: '#1e293b', title: 'IRI Promedio' },
      yaxis: { automargin: true },
      title: { text: 'IRI promedio por area institucional', font: { size: 12, color: '#94a3b8' }, x: 0.5 },
    }, { responsive: true });
  }

  function renderFuentes(fuentes) {
    if (!fuentes) return;
    document.getElementById('fuentes').innerHTML = Object.entries(fuentes).map(([k,v]) => {
      const icon = k.includes('sintetico') ? '⚠️' : '✅';
      return `<span style="margin-right:2rem">${icon} <code style="color:#7dd3fc">${k}</code> &rarr; <b>${v}</b> organismos</span>`;
    }).join('');
  }

  loadAll();
  setInterval(loadAll, 300000);
</script>
</body>
</html>"""


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(content=DASHBOARD_HTML)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)