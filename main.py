"""
main.py - FastAPI del Monitor IRI
Endpoints:
  GET /             redirige a /dashboard
  GET /info         health check con metadata
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
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
import pandas as pd

CSV_PATH = "data/processed/monitor_completo.csv"

# ── Postgres config ───────────────────────────────────────────────────────────
_DB_URL = (os.getenv("MONITOR_DATABASE_URL") or os.getenv("DATABASE_URL") or "").replace("postgres://", "postgresql://", 1)

def _get_conn():
    if not _DB_URL:
        return None
    try:
        import psycopg2
        return psycopg2.connect(_DB_URL)
    except Exception as e:
        print(f"⚠️  DB connect error: {e}")
        return None

def _save_to_db(df: pd.DataFrame):
    """Guarda el dataset en Postgres para persistencia entre redeploys."""
    conn = _get_conn()
    if not conn:
        return
    try:
        import psycopg2.extras
        cur = conn.cursor()
        cur.execute("DELETE FROM monitor_iri WHERE fecha_datos = CURRENT_DATE")
        rows = []
        for _, row in df.iterrows():
            rows.append((
                str(row.get("Organismo", "")),
                str(row.get("Area", "")),
                float(row.get("Riesgo Financiero", 0) or 0),
                float(row.get("Riesgo Contratación", 0) or 0),
                float(row.get("Riesgo Operativo", 0) or 0),
                float(row.get("Riesgo Datos", 0) or 0),
                float(row.get("IRI (Score)", 0) or 0),
                str(row.get("Estado", "")),
                str(row.get("Fuente", "")),
            ))
        psycopg2.extras.execute_values(cur, """
            INSERT INTO monitor_iri
                (organismo, area, riesgo_financiero, riesgo_contratacion,
                 riesgo_operativo, riesgo_datos, iri_score, estado, fuente)
            VALUES %s
        """, rows)
        conn.commit()
        conn.close()
        print(f"✅ DB: {len(rows)} registros guardados en monitor_iri")
    except Exception as e:
        print(f"⚠️  DB save error: {e}")

def _load_from_db() -> pd.DataFrame | None:
    """Carga el dataset desde Postgres si está disponible."""
    conn = _get_conn()
    if not conn:
        return None
    try:
        df = pd.read_sql("""
            SELECT organismo AS "Organismo", area AS "Area",
                   riesgo_financiero AS "Riesgo Financiero",
                   riesgo_contratacion AS "Riesgo Contratación",
                   riesgo_operativo AS "Riesgo Operativo",
                   riesgo_datos AS "Riesgo Datos",
                   iri_score AS "IRI (Score)",
                   estado AS "Estado", fuente AS "Fuente"
            FROM monitor_iri
            WHERE fecha_datos = (SELECT MAX(fecha_datos) FROM monitor_iri)
            ORDER BY iri_score DESC
        """, conn)
        conn.close()
        if len(df) > 0:
            print(f"✅ DB: {len(df)} registros cargados desde monitor_iri")
            return df
    except Exception as e:
        print(f"⚠️  DB load error: {e}")
    return None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup no bloqueante — corre motor en background thread."""
    import threading
    os.makedirs("data/processed", exist_ok=True)

    def _init_data():
        df_db = _load_from_db()
        if df_db is not None and len(df_db) > 0:
            df_db.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
            print(f"✅ Dataset restaurado desde DB: {len(df_db)} organismos")
            return
        if os.path.exists(CSV_PATH):
            print(f"✅ CSV ya disponible: {CSV_PATH}")
            return
        try:
            import subprocess, sys
            print("🔄 Sin datos — corriendo motor_analitico.py en background...")
            result = subprocess.run(
                [sys.executable, "motor_analitico.py"],
                capture_output=True, text=True, timeout=180
            )
            if result.returncode == 0:
                print("✅ motor_analitico.py completado")
                if os.path.exists(CSV_PATH):
                    df_new = pd.read_csv(CSV_PATH)
                    _save_to_db(df_new)
            else:
                print(f"⚠️  motor_analitico error: {result.stderr[-300:]}")
        except Exception as e:
            print(f"⚠️  No se pudo correr motor: {e}")

    t = threading.Thread(target=_init_data, daemon=True)
    t.start()
    yield

app = FastAPI(
    lifespan=lifespan,
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

def _load_df() -> pd.DataFrame:
    if not os.path.exists(CSV_PATH):
        raise HTTPException(
            status_code=503,
            detail="Dataset no disponible. Ejecuta POST /refresh o python motor_analitico.py",
        )
    return pd.read_csv(CSV_PATH)


def _df_to_records(df: pd.DataFrame) -> list:
    return df.fillna("").to_dict(orient="records")


# ── CAMBIO 1: / redirige al dashboard ────────────────────────────────────────
@app.get("/")
def raiz():
    """Redirige al dashboard principal."""
    return RedirectResponse(url="/dashboard", status_code=302)


@app.get("/health")
def health_simple():
    """Healthcheck simple — siempre responde 200 inmediatamente."""
    return {"status": "ok"}


@app.get("/info")
def info():
    """Health check con metadata del proyecto."""
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
        if result.returncode == 0 and os.path.exists(CSV_PATH):
            df_new = pd.read_csv(CSV_PATH)
            _save_to_db(df_new)
        return {
            "status": "ok" if result.returncode == 0 else "error",
            "stdout": result.stdout[-2000:],
            "stderr": result.stderr[-500:],
        }
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Motor timeout (>5min)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── DASHBOARD HTML ────────────────────────────────────────────────────────────
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
    #refresh-btn { background: #3b82f6; border: none; color: white; padding: 0.4rem 1rem; border-radius: 6px; cursor: pointer; font-size: 0.85rem; }
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

    /* === NAV TABS === */
    .nav-tabs { display: flex; gap: 0; border-bottom: 1px solid #334155; background: #0f172a; padding: 0 1.5rem; }
    .nav-tab { padding: 0.7rem 1.4rem; font-size: 0.88rem; font-weight: 600; color: #64748b; cursor: pointer; border: none; background: none; border-bottom: 3px solid transparent; transition: all .2s; }
    .nav-tab:hover { color: #cbd5e1; }
    .nav-tab.active { color: #60a5fa; border-bottom-color: #3b82f6; }
    .tab-page { display: none; }
    .tab-page.active { display: block; }

    /* === MANUAL === */
    .manual-wrap { padding: 1.5rem; max-width: 860px; }
    .manual-wrap h2 { font-size: 1.05rem; font-weight: 700; color: #cbd5e1; margin: 1.8rem 0 0.6rem; padding-bottom: 0.3rem; border-bottom: 1px solid #334155; }
    .manual-wrap h3 { font-size: 0.9rem; font-weight: 700; color: #94a3b8; margin: 1.2rem 0 0.4rem; }
    .manual-wrap p  { font-size: 0.85rem; color: #94a3b8; line-height: 1.7; margin-bottom: 0.5rem; }
    .manual-wrap ul { padding-left: 1.4rem; margin-bottom: 0.6rem; }
    .manual-wrap li { font-size: 0.85rem; color: #94a3b8; line-height: 1.8; }
    .manual-wrap code { background: #1e293b; color: #7dd3fc; padding: 0.1rem 0.45rem; border-radius: 4px; font-size: 0.82rem; }
    .manual-table { width: 100%; border-collapse: collapse; font-size: 0.83rem; margin-bottom: 1rem; }
    .manual-table th { background: #1e293b; color: #94a3b8; text-align: left; padding: 0.55rem 0.75rem; border-bottom: 1px solid #334155; font-weight: 600; }
    .manual-table td { padding: 0.5rem 0.75rem; border-top: 1px solid #1e293b55; color: #cbd5e1; }
    .manual-table tr:hover td { background: #1e293b55; }
    .manual-table td:first-child { color: #60a5fa; font-family: monospace; white-space: nowrap; }
    .info-box { background: #1e293b; border-left: 4px solid #3b82f6; border-radius: 6px; padding: 0.75rem 1rem; margin: 0.8rem 0; font-size: 0.83rem; color: #94a3b8; line-height: 1.7; }
    .info-box.warn { border-left-color: #fbbf24; }
    .info-box.ok   { border-left-color: #4ade80; }
    .formula-big { background: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 1rem 1.5rem; text-align: center; font-size: 1rem; font-weight: 700; color: #60a5fa; letter-spacing: .02em; margin: 0.8rem 0; }

    /* === NUEVAS SECCIONES === */
    .aviso-banner { margin: 1rem 1.5rem 0; background: #451a03; border: 1px solid #92400e; border-radius: 8px; padding: 0.75rem 1rem; font-size: 0.8rem; color: #fcd34d; display: flex; gap: 0.75rem; align-items: flex-start; }
    .aviso-banner b { color: #fbbf24; }
    .autor-card { background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 1.5rem 2rem; display: flex; gap: 1.5rem; align-items: flex-start; flex-wrap: wrap; }
    .autor-foto { width: 90px; height: 90px; border-radius: 50%; object-fit: cover; border: 3px solid #334155; flex-shrink: 0; }
    .autor-info h3 { font-size: 1.1rem; font-weight: 700; margin-bottom: 0.2rem; }
    .autor-info .subtitulo { color: #60a5fa; font-size: 0.82rem; margin-bottom: 0.6rem; }
    .autor-info p { font-size: 0.82rem; color: #94a3b8; line-height: 1.6; margin-bottom: 0.4rem; }
    .autor-btns { display: flex; gap: 0.5rem; flex-wrap: wrap; margin-top: 0.8rem; }
    .autor-btn { padding: 0.35rem 0.85rem; border-radius: 6px; font-size: 0.78rem; font-weight: 600; text-decoration: none; border: none; cursor: pointer; color: white; }
    .btn-blue { background: #1d4ed8; }
    .btn-green { background: #15803d; }
    .btn-gray { background: #334155; }
    .apoyar-btn { background: #15803d; border: none; color: white; padding: 0.4rem 1rem; border-radius: 6px; cursor: pointer; font-size: 0.85rem; font-weight: 600; margin-left: auto; }
    .apoyar-btn:hover { background: #16a34a; }
    .modal-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.75); z-index: 1000; align-items: center; justify-content: center; }
    .modal-overlay.active { display: flex; }
    .modal-box { background: #1e293b; border: 1px solid #334155; border-radius: 14px; padding: 2rem; width: min(500px, 95vw); position: relative; }
    .modal-close { position: absolute; top: 1rem; right: 1rem; background: none; border: none; color: #94a3b8; font-size: 1.4rem; cursor: pointer; line-height: 1; }
    .modal-close:hover { color: #e2e8f0; }
    .tab-btns { display: flex; gap: 0.5rem; margin: 1.2rem 0; }
    .tab-btn { flex: 1; padding: 0.5rem; border-radius: 8px; border: 1px solid #334155; background: #0f172a; color: #94a3b8; cursor: pointer; font-size: 0.85rem; }
    .tab-btn.active { border-color: #22c55e; color: #22c55e; background: #052e16; }
    .tab-panel { display: none; }
    .tab-panel.active { display: block; }
    .don-table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
    .don-table td { padding: 0.55rem 0.5rem; border-top: 1px solid #334155; }
    .don-table td:first-child { color: #64748b; }
    .don-table td:nth-child(2) { color: #e2e8f0; font-weight: 600; font-family: monospace; }
    .copy-btn { background: #1e293b; border: 1px solid #334155; color: #60a5fa; padding: 0.2rem 0.6rem; border-radius: 5px; cursor: pointer; font-size: 0.75rem; }
    .copy-btn:active { background: #22c55e; color: white; }
  </style>
</head>
<body>

<header>
  <span style="font-size:1.6rem">&#x1F6A6;</span>
  <div>
    <h1>Monitor de Riesgo Institucional (IRI)</h1>
    <span class="sub">Argentina &middot; Semaforo de integridad publica &middot; v2.0</span>
  </div>
  <button class="apoyar-btn" onclick="openModal()">&#x1F49A; Apoyar</button>
  <button id="refresh-btn" onclick="loadAll()">&#x21BA; Actualizar</button>
  <span id="status"></span>
</header>

<div class="aviso-banner">
  <span>&#x26A0;&#xFE0F;</span>
  <span><b>HERRAMIENTA EXPERIMENTAL Y ACAD&Eacute;MICA.</b> Los datos provienen de fuentes p&uacute;blicas oficiales del Estado argentino. Los resultados son indicadores algor&iacute;tmicos de riesgo &mdash; no implican juicio de valor, acusaci&oacute;n ni determinaci&oacute;n de responsabilidad sobre ninguna empresa, organismo o persona. El objetivo es promover la transparencia y el debate informado sobre el gasto p&uacute;blico.</span>
</div>

<!-- Nav tabs -->
<nav class="nav-tabs">
  <button class="nav-tab active" onclick="showPage('dashboard',this)">&#x1F4CA; Dashboard</button>
  <button class="nav-tab" onclick="showPage('manual',this)">&#x1F4D6; Manual</button>
  <button class="nav-tab" onclick="showPage('autor',this)">&#x1F464; Sobre el Autor</button>
</nav>

<!-- ══════════ TAB: DASHBOARD ══════════ -->
<div class="tab-page active" id="page-dashboard">

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
            <th title="Repositorio de origen del dato.">Fuente</th>
          </tr>
        </thead>
        <tbody id="table-body"></tbody>
      </table>
    </div>
    <div id="table-count" style="font-size:0.75rem;color:#64748b;margin-top:0.4rem;"></div>
    <div class="leyenda">
      <span><b>Score IRI</b> = Financiero x35% + Contratacion x30% + Operativo x20% + Datos x15%</span>
      <span><b>&#x1F534; ALTO</b> &ge; 60 &nbsp;&middot;&nbsp; <b>&#x1F7E1; MEDIO</b> 30-59 &nbsp;&middot;&nbsp; <b>&#x1F7E2; BAJO</b> &lt; 30</span>
    </div>
  </div>

  <div class="section">
    <h2>&#x2139;&#xFE0F; Que mide cada dimension del IRI</h2>
    <div class="formula">
      <p style="margin-bottom:0.6rem">El <b>Indice de Riesgo Institucional (IRI)</b> es un score compuesto (0-100) que agrega cuatro dimensiones de riesgo:</p>
      <p><span class="dim">Financiero 35%</span> Irregularidades en presupuesto, ejecucion del gasto y contratos.</p>
      <p><span class="dim">Contratacion 30%</span> Proporcion de contrataciones directas vs licitaciones publicas.</p>
      <p><span class="dim">Operativo 20%</span> Mora procesal, inasistencia legislativa, participation_pct senadores.</p>
      <p><span class="dim">Datos 15%</span> Accesibilidad documental, calidad de datos abiertos.</p>
      <p style="margin-top:0.6rem;color:#475569">
        <span class="tag-real">&#x2705; Dato real</span> = proviene de repo especializado &nbsp;&nbsp;
        <span class="tag-sint">&#x26A0;&#xFE0F; Sintetico</span> = generado con seed fija
      </p>
    </div>
  </div>

  <div class="section">
    <h2>&#x1F50E; Fuentes de datos</h2>
    <div id="fuentes" style="font-size:0.8rem;color:#94a3b8;line-height:2;"></div>
  </div>

</div><!-- /#app -->
</div><!-- /#page-dashboard -->

<!-- ══════════ TAB: MANUAL ══════════ -->
<div class="tab-page" id="page-manual">
<div class="manual-wrap">

  <h2>&#x2139;&#xFE0F; &iquest;Qu&eacute; es el Monitor IRI?</h2>
  <p>El <b>Monitor de Riesgo Institucional (IRI)</b> es una herramienta acad&eacute;mica de c&oacute;digo abierto que cuantifica niveles de riesgo de corrupci&oacute;n e ineficiencia en organismos del Estado argentino.</p>

  <h2>&#x1F9EE; F&oacute;rmula IRI</h2>
  <div class="formula-big">IRI = R_Financiero &times; 35% &nbsp;+&nbsp; R_Contrataci&oacute;n &times; 30% &nbsp;+&nbsp; R_Operativo &times; 20% &nbsp;+&nbsp; R_Datos &times; 15%</div>
  <table class="manual-table">
    <tr><th>Componente</th><th>Peso</th><th>Descripci&oacute;n</th></tr>
    <tr><td>R_Financiero</td><td>35%</td><td>Irregularidades presupuestarias, desvios en ejecucion del gasto.</td></tr>
    <tr><td>R_Contrataci&oacute;n</td><td>30%</td><td>Proporci&oacute;n de contrataciones directas vs. licitaciones.</td></tr>
    <tr><td>R_Operativo</td><td>20%</td><td>Inasistencia legislativa, tasa de vacancia judicial, participation_pct senado.</td></tr>
    <tr><td>R_Datos</td><td>15%</td><td>Calidad y disponibilidad de informaci&oacute;n p&uacute;blica.</td></tr>
  </table>
  <table class="manual-table">
    <tr><th>Score IRI</th><th>Nivel de riesgo</th></tr>
    <tr><td>0 &ndash; 29</td><td>&#x1F7E2; BAJO &mdash; dentro de par&aacute;metros normales</td></tr>
    <tr><td>30 &ndash; 59</td><td>&#x1F7E1; MEDIO &mdash; requiere seguimiento</td></tr>
    <tr><td>60 &ndash; 100</td><td>&#x1F534; ALTO &mdash; alerta de riesgo institucional</td></tr>
  </table>

  <h2>&#x1F3D7; Repositorios integrados</h2>
  <table class="manual-table">
    <tr><th>Repositorio</th><th>Datos que aporta</th></tr>
    <tr><td>monitor (central)</td><td>FastAPI principal. Dashboard HTML con Plotly.js.</td></tr>
    <tr><td>justicia</td><td>IRA por juzgado, vacantes judiciales, magistrados.</td></tr>
    <tr><td>monitor_legistativo</td><td>NAPE, IQP, asistencia &mdash; C&aacute;mara de Diputados.</td></tr>
    <tr><td>monitor_legistativo_senadores</td><td>Participaci&oacute;n, reporte por partido &mdash; Senado.</td></tr>
    <tr><td>monitor_contratos_v2</td><td>BORA + COMPR.AR + TGN. Detecci&oacute;n de irregularidades.</td></tr>
    <tr><td>gob_bo_comprar_tgn</td><td>Tesorer&iacute;a General de la Naci&oacute;n Argentina.</td></tr>
  </table>

  <h2>&#x1F511; Variables de entorno</h2>
  <table class="manual-table">
    <tr><th>Variable</th><th>Descripci&oacute;n</th></tr>
    <tr><td>JUSTICIA_API_URL</td><td>URL del servicio justicia en Railway</td></tr>
    <tr><td>LEGISTATIVO_API_URL</td><td>URL del servicio monitor_legistativo en Railway</td></tr>
    <tr><td>SENADORES_API_URL</td><td>URL del servicio senadores en Railway</td></tr>
    <tr><td>CONTRATOS_AR_API_URL</td><td>URL de monitor_contratos_v2 en Railway</td></tr>
    <tr><td>TGN_AR_API_URL</td><td>URL de gob_bo_comprar_tgn en Railway</td></tr>
    <tr><td>REFRESH_TOKEN</td><td>Token para <code>POST /refresh</code> (default: <code>dev</code>)</td></tr>
  </table>

  <h2>&#x1F4CB; Endpoints de la API</h2>
  <table class="manual-table">
    <tr><th>Endpoint</th><th>Descripci&oacute;n</th></tr>
    <tr><td>GET /</td><td>Redirige a /dashboard</td></tr>
    <tr><td>GET /dashboard</td><td>Este dashboard</td></tr>
    <tr><td>GET /datos</td><td>Dataset completo en JSON</td></tr>
    <tr><td>GET /por-area/{area}</td><td>Organismos filtrados por &aacute;rea</td></tr>
    <tr><td>GET /top-riesgo?n=10</td><td>Top N organismos de mayor IRI</td></tr>
    <tr><td>GET /resumen</td><td>Estad&iacute;sticas globales por &aacute;rea</td></tr>
    <tr><td>POST /refresh</td><td>Regenera el CSV (requiere <code>X-Refresh-Token</code>)</td></tr>
    <tr><td>GET /docs</td><td>Swagger UI</td></tr>
  </table>

</div>
</div><!-- /#page-manual -->

<!-- ══════════ TAB: SOBRE EL AUTOR ══════════ -->
<div class="tab-page" id="page-autor">
  <div class="section" style="max-width:860px;margin-top:1.5rem">
    <div class="autor-card">
      <!-- CAMBIO 2: foto desde raiz del repo -->
      <img class="autor-foto"
           src="https://raw.githubusercontent.com/Viny2030/monitor/main/foto.jpg"
           alt="Ph.D. Vicente Humberto Monteverde"
           onerror="this.style.display='none'"/>
      <div class="autor-info">
        <h3>Ph.D. Vicente Humberto Monteverde</h3>
        <div class="subtitulo">Doctor en Ciencias Econ&oacute;micas &middot; Investigador en Transparencia P&uacute;blica</div>
        <p>Investigador en econom&iacute;a pol&iacute;tica y fen&oacute;menos de corrupci&oacute;n. Autor de la teor&iacute;a de <em>Transferencia Regresiva de Ingresos</em> y desarrollador del algoritmo XAI aplicado al an&aacute;lisis de contrataciones p&uacute;blicas.</p>
        <p>Publicaciones en <em>Journal of Financial Crime</em> (Emerald Publishing). Asesor en transparencia y auditor&iacute;a algor&iacute;tmica del gasto p&uacute;blico.</p>
        <div class="autor-btns">
          <a class="autor-btn btn-blue" href="mailto:vhmonte@retina.ar">&#x2709; vhmonte@retina.ar</a>
          <a class="autor-btn btn-green" href="mailto:viny01958@gmail.com">&#x2709; viny01958@gmail.com</a>
          <a class="autor-btn btn-gray" href="https://github.com/Viny2030" target="_blank">&#x1F4BB; github.com/Viny2030</a>
        </div>
      </div>
    </div>

    <div style="margin-top:2rem">
      <h2 style="font-size:1rem;font-weight:600;color:#cbd5e1;margin-bottom:1rem">&#x1F49A; Apoyar el proyecto</h2>
      <p style="font-size:0.83rem;color:#64748b;margin-bottom:1rem">Este portal es software libre y sin publicidad. Tu apoyo permite mantener los servidores activos y mejorar las herramientas de transparencia p&uacute;blica.</p>

      <h3 style="color:#94a3b8;font-size:0.85rem;margin:1rem 0 0.4rem">&#x1F4B2; En Pesos Argentinos (ARS)</h3>
      <table class="manual-table">
        <tr><td>CBU</td><td style="font-family:monospace;color:#e2e8f0">0140005203400552652310 <button class="copy-btn" onclick="cp('0140005203400552652310')">copiar</button></td></tr>
        <tr><td>Alias</td><td style="color:#e2e8f0">ALGORIT.MONTE.PESOS <button class="copy-btn" onclick="cp('ALGORIT.MONTE.PESOS')">copiar</button></td></tr>
        <tr><td>Titular</td><td style="color:#e2e8f0">Vicente Humberto Monteverde</td></tr>
      </table>

      <h3 style="color:#94a3b8;font-size:0.85rem;margin:1rem 0 0.4rem">&#x1F4B5; En D&oacute;lares (USD)</h3>
      <table class="manual-table">
        <tr><td>CBU</td><td style="font-family:monospace;color:#e2e8f0">0140005204400550329709 <button class="copy-btn" onclick="cp('0140005204400550329709')">copiar</button></td></tr>
        <tr><td>Alias</td><td style="color:#e2e8f0">ALGO.MONTE.DOLARES <button class="copy-btn" onclick="cp('ALGO.MONTE.DOLARES')">copiar</button></td></tr>
        <tr><td>Titular</td><td style="color:#e2e8f0">Vicente Humberto Monteverde</td></tr>
      </table>

      <h3 style="color:#94a3b8;font-size:0.85rem;margin:1rem 0 0.4rem">&#x1F30E; Desde el Exterior</h3>
      <table class="manual-table">
        <tr><td>Banco</td><td style="color:#e2e8f0">Banco Santander Montevideo</td></tr>
        <tr><td>N&uacute;mero de cuenta</td><td style="font-family:monospace;color:#e2e8f0">005200183500 <button class="copy-btn" onclick="cp('005200183500')">copiar</button></td></tr>
        <tr><td>SWIFT / BIC</td><td style="font-family:monospace;color:#e2e8f0">BSCHUYMM <button class="copy-btn" onclick="cp('BSCHUYMM')">copiar</button></td></tr>
        <tr><td>Titular</td><td style="color:#e2e8f0">Vicente Humberto Monteverde</td></tr>
      </table>
    </div>
  </div>
</div><!-- /#page-autor -->

<!-- Modal donacion -->
<div class="modal-overlay" id="modal-donacion">
  <div class="modal-box">
    <button class="modal-close" onclick="closeModal()">&#x2715;</button>
    <div style="font-size:1.3rem;font-weight:700;margin-bottom:0.4rem">&#x1F49A; Apoyar el Monitor IRI</div>
    <p style="font-size:0.85rem;color:#94a3b8;line-height:1.6">Este portal es software libre y sin publicidad. Tu apoyo permite mantener los scrapers activos y mejorar las herramientas de transparencia.</p>
    <div class="tab-btns">
      <button class="tab-btn active" onclick="switchTab('pesos',this)">AR Pesos</button>
      <button class="tab-btn" onclick="switchTab('dolares',this)">&#x1F4B5; D&oacute;lares</button>
      <button class="tab-btn" onclick="switchTab('exterior',this)">&#x1F30E; Exterior</button>
    </div>

    <div class="tab-panel active" id="tab-pesos">
      <p style="font-size:0.78rem;font-weight:600;color:#94a3b8;margin-bottom:0.5rem;letter-spacing:.05em">TRANSFERENCIA EN PESOS</p>
      <table class="don-table">
        <tr><td>Tipo</td><td>Caja Ahorro Pesos</td><td><button class="copy-btn" onclick="cp('Caja Ahorro Pesos')">copiar</button></td></tr>
        <tr><td>CBU</td><td>0140005203400552652310</td><td><button class="copy-btn" onclick="cp('0140005203400552652310')">copiar</button></td></tr>
        <tr><td>Alias</td><td>ALGORIT.MONTE.PESOS</td><td><button class="copy-btn" onclick="cp('ALGORIT.MONTE.PESOS')">copiar</button></td></tr>
        <tr><td>Titular</td><td>Vicente Humberto Monteverde</td><td><button class="copy-btn" onclick="cp('Vicente Humberto Monteverde')">copiar</button></td></tr>
      </table>
    </div>

    <div class="tab-panel" id="tab-dolares">
      <p style="font-size:0.78rem;font-weight:600;color:#94a3b8;margin-bottom:0.5rem;letter-spacing:.05em">TRANSFERENCIA EN D&Oacute;LARES</p>
      <table class="don-table">
        <tr><td>Tipo</td><td>Caja Ahorro D&oacute;lares</td><td><button class="copy-btn" onclick="cp('Caja Ahorro Dólares')">copiar</button></td></tr>
        <tr><td>CBU</td><td>0140005204400550329709</td><td><button class="copy-btn" onclick="cp('0140005204400550329709')">copiar</button></td></tr>
        <tr><td>Alias</td><td>ALGO.MONTE.DOLARES</td><td><button class="copy-btn" onclick="cp('ALGO.MONTE.DOLARES')">copiar</button></td></tr>
        <tr><td>Titular</td><td>Vicente Humberto Monteverde</td><td><button class="copy-btn" onclick="cp('Vicente Humberto Monteverde')">copiar</button></td></tr>
      </table>
    </div>

    <div class="tab-panel" id="tab-exterior">
      <p style="font-size:0.78rem;font-weight:600;color:#94a3b8;margin-bottom:0.5rem;letter-spacing:.05em">DESDE EL EXTERIOR</p>
      <table class="don-table">
        <tr><td>Banco</td><td>Banco Santander Montevideo</td><td><button class="copy-btn" onclick="cp('Banco Santander Montevideo')">copiar</button></td></tr>
        <tr><td>Titular</td><td>Vicente Humberto Monteverde</td><td><button class="copy-btn" onclick="cp('Vicente Humberto Monteverde')">copiar</button></td></tr>
        <tr><td>Cuenta</td><td>Caja de Ahorro en D&oacute;lares</td><td></td></tr>
        <tr><td>N&uacute;mero</td><td>005200183500</td><td><button class="copy-btn" onclick="cp('005200183500')">copiar</button></td></tr>
        <tr><td>SWIFT</td><td>BSCHUYMM</td><td><button class="copy-btn" onclick="cp('BSCHUYMM')">copiar</button></td></tr>
      </table>
    </div>

    <p style="font-size:0.75rem;color:#475569;margin-top:1.2rem;text-align:center">
      Proyecto open source &middot; <a href="https://github.com/Viny2030" target="_blank">github.com/Viny2030</a>
    </p>
  </div>
</div>

<footer>
  Monitor IRI v2.0 &middot;
  <a href="https://github.com/Viny2030/monitor" target="_blank">github.com/Viny2030/monitor</a> &middot;
  Datos actualizados diariamente via GitHub Actions
</footer>

<script>
  function showPage(name, btn) {
    document.querySelectorAll('.tab-page').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.nav-tab').forEach(b => b.classList.remove('active'));
    document.getElementById('page-' + name).classList.add('active');
    btn.classList.add('active');
  }

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
      const avgVal = g.iri_promedio_global.toFixed(1);
      const avgEl  = document.getElementById('m-avg');
      avgEl.textContent = avgVal;
      avgEl.className   = 'value ' + (g.iri_promedio_global >= 60 ? 'red' : g.iri_promedio_global >= 30 ? 'amber' : 'green');
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
    if (f.includes('sintetico') || f.includes('seed'))     return '⚠️ Sintético';
    if (f.includes('fallback'))                             return '↩ Fallback';
    if (f.includes('monitor_contratos_v2/comprar'))         return '✅ COMPR.AR';
    if (f.includes('monitor_contratos_v2/tgn'))             return '✅ TGN (contratos)';
    if (f.includes('monitor_contratos_v2/flujo'))           return '✅ BORA+COMPR.AR+TGN';
    if (f.includes('monitor_contratos_v2'))                 return '✅ Contratos';
    if (f.includes('gob_bo_comprar_tgn'))                   return '✅ TGN';
    if (f.includes('senadores') && f.includes('nomina'))    return '✅ Senado (nómina)';
    if (f.includes('senadores') && f.includes('partido'))   return '✅ Senado (partidos)';
    if (f.includes('senadores') || f.includes('senado'))    return '✅ Senado';
    if (f.includes('monitor_legistativo'))                  return '✅ Legislativo';
    if (f.includes('justicia') && f.includes('vacantes'))   return '✅ Vacantes judiciales';
    if (f.includes('justicia'))                             return '✅ Judicial';
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
    // Agrupar por label amigable — varias fuentes técnicas pueden mapear al mismo nombre
    const agrupado = {};
    Object.entries(fuentes).forEach(([k, v]) => {
      const label = fuenteLabel(k);
      const isSint = k.includes('sintetico') || k.includes('seed') || k.includes('fallback');
      if (!agrupado[label]) agrupado[label] = { count: 0, sint: isSint, keys: [] };
      agrupado[label].count += v;
      agrupado[label].sint = agrupado[label].sint && isSint; // real si al menos una fuente es real
      agrupado[label].keys.push(k);
    });
    document.getElementById('fuentes').innerHTML = Object.entries(agrupado)
      .sort((a, b) => b[1].count - a[1].count)
      .map(([label, d]) => {
        const icon = d.sint ? '⚠️' : '✅';
        const tooltip = d.keys.join(' | ');
        return `<span style="margin-right:2rem">${icon} <code style="color:#7dd3fc" title="${tooltip}">${label}</code> &rarr; <b>${d.count}</b> organismos</span>`;
      }).join('');
  }

  function openModal()  { document.getElementById('modal-donacion').classList.add('active'); }
  function closeModal() { document.getElementById('modal-donacion').classList.remove('active'); }
  document.getElementById('modal-donacion').addEventListener('click', function(e) {
    if (e.target === this) closeModal();
  });
  function switchTab(name, btn) {
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.getElementById('tab-' + name).classList.add('active');
    btn.classList.add('active');
  }
  function cp(text) {
    navigator.clipboard.writeText(text).catch(() => {});
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
