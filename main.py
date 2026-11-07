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
  GET /grafo        red organismo ↔ principales proveedores (COMPR.AR)
  POST /refresh     regenera el CSV (protegido por REFRESH_TOKEN)

Variables de entorno:
  REFRESH_TOKEN, LEGISTATIVO_API_URL, SENADORES_API_URL, JUSTICIA_API_URL
"""

import os
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Header, Query
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
        def _num(v):
            # "sin dato" se guarda como NULL (antes se convertía en 0)
            try:
                v = float(v)
            except (TypeError, ValueError):
                return None
            return None if v != v else v

        rows = []
        for _, row in df.iterrows():
            rows.append((
                str(row.get("Organismo", "")),
                str(row.get("Area", "")),
                _num(row.get("Riesgo Financiero")),
                _num(row.get("Riesgo Contratación")),
                _num(row.get("Riesgo Operativo")),
                _num(row.get("Riesgo Datos")),
                _num(row.get("IRI (Score)")),
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
                   estado AS "Estado", fuente AS "Fuente", updated_at
            FROM monitor_iri
            WHERE fecha_datos = (SELECT MAX(fecha_datos) FROM monitor_iri)
            ORDER BY iri_score DESC
        """, conn)
        conn.close()
        if len(df) > 0:
            print(f"✅ DB: {len(df)} registros cargados desde monitor_iri")
            try:
                import json
                os.makedirs(os.path.dirname(META_PATH), exist_ok=True)
                with open(META_PATH, "w", encoding="utf-8") as f:
                    json.dump({"generado": str(df["updated_at"].max())[:19].replace(" ", "T")}, f)
            except Exception:
                pass
            return df.drop(columns=["updated_at"])
    except Exception as e:
        print(f"⚠️  DB load error: {e}")
    return None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup no bloqueante — corre motor en background thread."""
    import threading
    os.makedirs("data/processed", exist_ok=True)

    def _init_data():
        _load_grafo_db()
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

# Sin valor por defecto: antes era "dev" (y el manual lo publicaba), así que si
# la variable no estaba en Railway cualquiera podía borrar la base con /reset-db.
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN", "")
META_PATH = "data/processed/meta.json"


def _verificar_token(recibido):
    import hmac
    if not REFRESH_TOKEN or REFRESH_TOKEN == "dev":
        raise HTTPException(status_code=503, detail="Deshabilitado: falta configurar REFRESH_TOKEN")
    if not recibido or not hmac.compare_digest(str(recibido), REFRESH_TOKEN):
        raise HTTPException(status_code=401, detail="Token invalido")


def _fecha_datos():
    """Fecha y hora en que se generó el dataset que se está mostrando."""
    import json
    try:
        with open(META_PATH, encoding="utf-8") as f:
            return json.load(f).get("generado")
    except Exception:
        pass
    try:
        from datetime import datetime
        return datetime.fromtimestamp(os.path.getmtime(CSV_PATH)).isoformat(timespec="seconds")
    except Exception:
        return None

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
        "datos_actualizados": _fecha_datos(),
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
        "medio_riesgo": int(df["Estado"].astype(str).str.contains("MEDIO").sum()),
        "dato_parcial": int(df["Estado"].astype(str).str.contains("parcial").sum()),
        "bajo_riesgo":  int((df["Estado"] == "\U0001f7e2 BAJO").sum()),
        "datos_actualizados": _fecha_datos(),
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


@app.post("/reset-db")
def reset_db(x_refresh_token: str = Header(None)):
    """Limpia la DB y el CSV cacheado para forzar regeneración limpia."""
    _verificar_token(x_refresh_token)
    resultados = []
    # 1. Limpiar CSV en disco
    if os.path.exists(CSV_PATH):
        os.remove(CSV_PATH)
        resultados.append(f"CSV eliminado: {CSV_PATH}")
    else:
        resultados.append("CSV no existia en disco")
    # 2. Limpiar tabla Postgres
    conn = _get_conn()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM monitor_iri")
            filas = cur.rowcount
            conn.commit()
            conn.close()
            resultados.append(f"DB limpiada: {filas} filas borradas")
        except Exception as e:
            resultados.append(f"DB error: {e}")
    else:
        resultados.append("DB no disponible (sin DATABASE_URL)")
    return {"status": "ok", "acciones": resultados}


import threading as _threading
from datetime import datetime as _datetime

_REFRESH_LOCK = _threading.Lock()
_REFRESH_ESTADO: dict = {"estado": "sin_ejecutar"}


def _ejecutar_motor() -> dict:
    """Corre motor_analitico.py y devuelve el resultado.

    Si el motor falla o genera un dataset vacío, se restaura el CSV anterior:
    antes el CSV quedaba como lo dejara el motor y /refresh respondía 200 con
    "status": "error", así que el workflow daba por bueno un refresh roto.
    """
    import shutil, subprocess, sys
    LOG_PATH = "data/processed/refresh.log"
    os.makedirs("data/processed", exist_ok=True)
    backup = CSV_PATH + ".anterior"
    if os.path.exists(CSV_PATH):
        shutil.copy2(CSV_PATH, backup)
    # Heredar el entorno completo — asegura que TGN_AR_API_URL y CONTRATOS_AR_API_URL
    # lleguen al subprocess (en Railway los subprocesos no siempre heredan env vars)
    env = os.environ.copy()
    try:
        with open(LOG_PATH, "w", encoding="utf-8") as log_f:
            result = subprocess.run(
                [sys.executable, "motor_analitico.py"],
                stdout=log_f, stderr=log_f,
                timeout=300,
                env=env,
            )
        log_content = open(LOG_PATH, encoding="utf-8", errors="replace").read()
        try:
            filas = len(pd.read_csv(CSV_PATH)) if os.path.exists(CSV_PATH) else 0
        except pd.errors.EmptyDataError:   # CSV sin columnas = dataset vacío
            filas = 0
        if result.returncode == 0 and filas > 0:
            _save_to_db(pd.read_csv(CSV_PATH))
            _save_grafo_db()
            return {"status": "ok", "organismos": filas, "stdout": "", "stderr": log_content[-4000:]}
        motivo = (f"motor terminó con código {result.returncode}" if result.returncode != 0
                  else "el motor generó un dataset vacío")
    except subprocess.TimeoutExpired:
        motivo, log_content = "Motor timeout (>5min)", ""
    except Exception as e:
        motivo, log_content = str(e), ""
    if os.path.exists(backup):
        shutil.copy2(backup, CSV_PATH)
        motivo += " — se mantiene el dataset anterior"
    return {"status": "error", "detalle": motivo, "stdout": "", "stderr": log_content[-4000:]}


def _correr_en_segundo_plano():
    try:
        resultado = _ejecutar_motor()
    except Exception as e:  # no debería pasar: _ejecutar_motor captura todo
        resultado = {"status": "error", "detalle": str(e)}
    _REFRESH_ESTADO.update(estado=resultado["status"], fin=_datetime.now().isoformat(timespec="seconds"),
                           resultado=resultado)
    _REFRESH_LOCK.release()


@app.post("/refresh")
def refresh(x_refresh_token: str = Header(None), modo: str = "sync"):
    """Regenera el dataset.

    modo=sync (default): espera al motor y responde 200 si salió bien o 500 si
    falló. modo=async: responde 202 enseguida y el resultado se consulta en
    GET /refresh/estado. El workflow diario usa async porque Cloudflare corta
    las respuestas que tardan más de 100 s (error 524) y el motor puede tardar
    hasta 5 min.
    """
    _verificar_token(x_refresh_token)
    if not _REFRESH_LOCK.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="Ya hay un refresh en curso: consultar GET /refresh/estado")
    _REFRESH_ESTADO.clear()
    _REFRESH_ESTADO.update(estado="corriendo", inicio=_datetime.now().isoformat(timespec="seconds"))
    if modo == "async":
        _threading.Thread(target=_correr_en_segundo_plano, daemon=True).start()
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=202, content={"status": "corriendo", "consultar": "GET /refresh/estado"})
    try:
        resultado = _ejecutar_motor()
        _REFRESH_ESTADO.update(estado=resultado["status"], fin=_datetime.now().isoformat(timespec="seconds"),
                               resultado=resultado)
    finally:
        _REFRESH_LOCK.release()
    if resultado["status"] != "ok":
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=500, content=resultado)
    return resultado


@app.get("/refresh/estado")
def refresh_estado(x_refresh_token: str = Header(None)):
    """Estado del último refresh: sin_ejecutar | corriendo | ok | error."""
    _verificar_token(x_refresh_token)
    return {**_REFRESH_ESTADO, "datos_actualizados": _fecha_datos()}


# ── GRAFO ORGANISMO ↔ PROVEEDOR (COMPR.AR) ────────────────────────────────────
GRAFO_PATH = os.getenv("GRAFO_PATH", "data/processed/grafo_comprar.json")


def _save_grafo_db() -> None:
    """Guarda el grafo en Postgres: el archivo se pierde en cada redeploy."""
    conn = _get_conn()
    if not conn or not os.path.exists(GRAFO_PATH):
        return
    try:
        with open(GRAFO_PATH, encoding="utf-8") as f:
            contenido = f.read()
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS monitor_grafo "
                    "(id SERIAL PRIMARY KEY, generado TIMESTAMPTZ DEFAULT now(), contenido TEXT NOT NULL)")
        cur.execute("INSERT INTO monitor_grafo (contenido) VALUES (%s)", (contenido,))
        cur.execute("DELETE FROM monitor_grafo WHERE id NOT IN "
                    "(SELECT id FROM monitor_grafo ORDER BY id DESC LIMIT 3)")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️  DB grafo save error: {e}")


def _load_grafo_db() -> None:
    if os.path.exists(GRAFO_PATH):
        return
    conn = _get_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('monitor_grafo')")
        if cur.fetchone()[0]:
            cur.execute("SELECT contenido FROM monitor_grafo ORDER BY id DESC LIMIT 1")
            fila = cur.fetchone()
            if fila:
                os.makedirs(os.path.dirname(GRAFO_PATH) or ".", exist_ok=True)
                with open(GRAFO_PATH, "w", encoding="utf-8") as f:
                    f.write(fila[0])
                print("✅ Grafo COMPR.AR restaurado desde DB")
        conn.close()
    except Exception as e:
        print(f"⚠️  DB grafo load error: {e}")


def _filtrar_grafo(g: dict, top: int, solo_compartidos: bool) -> dict:
    """Se queda con los `top` principales proveedores de cada organismo y, si se
    pide, solo con los proveedores que trabajan con más de un organismo."""
    nodos = {n["id"]: n for n in g.get("nodos", [])}
    aristas = [a for a in g.get("aristas", []) if a["rango"] <= top]
    if solo_compartidos:
        aristas = [a for a in aristas if nodos.get(a["to"], {}).get("organismos_total", 0) > 1]
    usados = {a["from"] for a in aristas} | {a["to"] for a in aristas}
    sel = [n for i, n in nodos.items() if i in usados]
    grado = {}
    for a in aristas:
        grado[a["to"]] = grado.get(a["to"], 0) + 1
    provs = [dict(n, organismos_en_grafo=grado.get(n["id"], 0)) for n in sel if n["tipo"] == "proveedor"]
    orgs = [n for n in sel if n["tipo"] == "organismo"]
    compartidos = sorted((p for p in provs if p["organismos_en_grafo"] > 1),
                         key=lambda p: (-p["organismos_en_grafo"], -p["organismos_total"]))
    return {
        "nodos": orgs + provs, "aristas": aristas,
        "resumen": {
            "organismos": len(orgs), "proveedores": len(provs), "aristas": len(aristas),
            "compartidos": len(compartidos), "top": top, "periodo": g.get("periodo"),
            "generado": g.get("generado"), "fuente": g.get("fuente"),
            "top_compartidos": [{"id": p["id"], "label": p["label"], "cuit": p["cuit"],
                                 "organismos_en_grafo": p["organismos_en_grafo"],
                                 "organismos_total": p["organismos_total"]} for p in compartidos[:10]],
        },
    }


@app.get("/grafo")
def get_grafo(top: int = Query(5, ge=1, le=10), solo_compartidos: bool = False):
    """Red organismo ↔ principales proveedores (COMPR.AR, últimos ejercicios)."""
    import json
    if not os.path.exists(GRAFO_PATH):
        raise HTTPException(status_code=503,
                            detail="Grafo no disponible todavía: se genera con el próximo refresh")
    with open(GRAFO_PATH, encoding="utf-8") as f:
        g = json.load(f)
    return _filtrar_grafo(g, top, solo_compartidos)


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
    .badge-gray  { background: #1e293b; color: #94a3b8; }
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
    .manual-lang { background:#1e293b; border:1px solid #334155; color:#94a3b8; border-radius:4px; padding:2px 10px; font-size:0.75rem; cursor:pointer; }
    .manual-lang.activo { background:#1e3a8a; border-color:#3b82f6; color:#e2e8f0; }
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

    /* === GRAFO === */
    .grafo-layout { display: grid; grid-template-columns: minmax(0, 1fr) 300px; gap: 1rem; }
    .grafo-canvas { height: 620px; background: #0b1222; border: 1px solid #334155; border-radius: 10px; }
    .grafo-info { font-size: 0.8rem; color: #94a3b8; line-height: 1.7; margin-top: 0.4rem; }
    .grafo-info b { color: #e2e8f0; }
    .grafo-lista { list-style: none; font-size: 0.8rem; margin-top: 0.4rem; }
    .grafo-lista li { padding: 0.35rem 0; border-bottom: 1px solid #334155; cursor: pointer; color: #94a3b8; }
    .grafo-lista li:hover { color: #60a5fa; }
    @media (max-width: 900px) { .grafo-layout { grid-template-columns: 1fr; } .grafo-canvas { height: 460px; } }

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
  <button class="nav-tab" onclick="showPage('grafo',this)">🕸️ Grafo</button>
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
    <div class="card"><div class="label">&#x1F7E1; Riesgo medio (IRI 30-59 o dato parcial)</div><div class="value amber" id="m-medio">-</div></div>
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
      <span><b>Score IRI</b> = Financiero x35% + Contratacion x30% + Operativo x20% + Datos x15%. Si un componente no tiene dato (s/d) se excluye y se reponderan los demás.</span>
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
<!-- ══════════ TAB: GRAFO ══════════ -->
<div class="tab-page" id="page-grafo">
<div class="section" style="padding-top:1.2rem">
  <h2>🕸️ Grafo organismo ↔ principales proveedores (COMPR.AR <span id="g-periodo">—</span>)</h2>
  <div id="g-filtros" style="display:flex;gap:1rem;flex-wrap:wrap;align-items:center;margin-bottom:1rem">
    <label style="font-size:0.82rem;color:#94a3b8">Proveedores por organismo
      <select id="g-top" onchange="cargarGrafo()">
        <option value="3">3 principales</option>
        <option value="5" selected>5 principales</option>
        <option value="10">10 principales</option>
      </select>
    </label>
    <label style="font-size:0.82rem;color:#94a3b8;display:flex;align-items:center;gap:0.4rem">
      <input type="checkbox" id="g-compartidos" onchange="cargarGrafo()" style="padding:0"> Solo proveedores de varios organismos
    </label>
  </div>
  <div class="metrics" style="padding:0 0 1rem">
    <div class="card"><div class="label">Organismos</div><div class="value blue" id="g-org">-</div></div>
    <div class="card"><div class="label">Proveedores</div><div class="value" id="g-prov">-</div></div>
    <div class="card"><div class="label">Vínculos</div><div class="value" id="g-ari">-</div></div>
    <div class="card"><div class="label">Proveedores compartidos</div><div class="value amber" id="g-comp">-</div></div>
  </div>
  <div class="leyenda" style="margin:0 0 0.8rem">
    <span><b style="color:#3b82f6">■</b> Organismo (tamaño: monto adjudicado)</span>
    <span><b style="color:#4ade80">●</b> Proveedor de un organismo</span>
    <span><b style="color:#fbbf24">●</b> Proveedor de varios organismos del grafo</span>
    <span><b style="color:#f87171">—</b> Vínculo con contratación directa</span>
    <span>Cada nodo lleva el nombre del organismo o del proveedor · Grosor: monto del vínculo</span>
  </div>
  <div class="grafo-layout">
    <div id="g-canvas" class="grafo-canvas"><div id="loader">Cargando…</div></div>
    <div>
      <div class="card" style="margin-bottom:1rem">
        <div class="label">Detalle</div>
        <div id="g-info" class="grafo-info">Hacé clic en un nodo para ver sus datos.</div>
      </div>
      <div class="card">
        <div class="label">Proveedores de varios organismos</div>
        <ul id="g-lista" class="grafo-lista"></ul>
      </div>
    </div>
  </div>
  <p style="font-size:0.75rem;color:#64748b;margin-top:0.8rem">
    Fuente: adjudicaciones de COMPR.AR en datos.gob.ar, en pesos, de los organismos que tienen % de directas calculado.
    Cada organismo muestra solo sus principales proveedores por monto. Que un proveedor trabaje con muchos organismos
    o concentre las compras de uno es un dato descriptivo: no implica irregularidad y no entra en el IRI.
    Ver Manual → Grafo.
  </p>
</div>
</div>

<div class="tab-page" id="page-manual">
<div class="manual-wrap">
  <div style="display:flex;justify-content:flex-end;gap:6px;margin-bottom:0.4rem">
    <button type="button" class="manual-lang activo" data-lang="es" onclick="setManualLang('es')">ES</button>
    <button type="button" class="manual-lang" data-lang="en" onclick="setManualLang('en')">EN</button>
  </div>

<div id="manual-es" lang="es">
  <h2>&#x2139;&#xFE0F; &iquest;Qu&eacute; es el Monitor IRI?</h2>
  <p>El <b>Monitor de Riesgo Institucional (IRI)</b> es una herramienta acad&eacute;mica de c&oacute;digo abierto que cuantifica niveles de riesgo de corrupci&oacute;n e ineficiencia en organismos del Estado argentino. Re&uacute;ne en una sola tabla los indicadores de los otros sitios del ecosistema (Justicia, Diputados, Senado, Contratos y Tesorer&iacute;a).</p>

  <h2>&#x1F9EE; F&oacute;rmula IRI</h2>
  <div class="formula-big">IRI = R_Financiero &times; 35% &nbsp;+&nbsp; R_Contrataci&oacute;n &times; 30% &nbsp;+&nbsp; R_Operativo &times; 20% &nbsp;+&nbsp; R_Datos &times; 15%</div>
  <table class="manual-table">
    <tr><th>Componente</th><th>Peso</th><th>Descripci&oacute;n</th></tr>
    <tr><td>R_Financiero</td><td>35%</td><td>Irregularidades presupuestarias, desv&iacute;os en ejecuci&oacute;n del gasto.</td></tr>
    <tr><td>R_Contrataci&oacute;n</td><td>30%</td><td>Proporci&oacute;n de contrataciones directas sobre el total de procedimientos (ver abajo).</td></tr>
    <tr><td>R_Operativo</td><td>20%</td><td>Inasistencia legislativa, tasa de vacancia judicial, participaci&oacute;n en el Senado.</td></tr>
    <tr><td>R_Datos</td><td>15%</td><td>Calidad y disponibilidad de informaci&oacute;n p&uacute;blica.</td></tr>
  </table>
  <p><b>Componentes sin dato.</b> Si un organismo no tiene dato para un componente, ese componente no se rellena con un valor fijo: se muestra <code>s/d</code> y el IRI se calcula con los componentes medidos, repartiendo sus pesos para que sumen 100%. La columna <i>Componentes con dato</i> dice cu&aacute;ntos se usaron.</p>
  <table class="manual-table">
    <tr><th>Score IRI</th><th>Nivel de riesgo</th></tr>
    <tr><td>0 &ndash; 29</td><td>&#x1F7E2; BAJO &mdash; dentro de par&aacute;metros normales</td></tr>
    <tr><td>30 &ndash; 59</td><td>&#x1F7E1; MEDIO &mdash; requiere seguimiento</td></tr>
    <tr><td>60 &ndash; 100</td><td>&#x1F534; ALTO &mdash; alerta de riesgo institucional</td></tr>
    <tr><td>60 &ndash; 100 con un solo componente</td><td>&#x1F7E1; MEDIO (dato parcial) &mdash; no se marca ALTO con una sola medici&oacute;n</td></tr>
    <tr><td>sin componentes</td><td>&#x26AA; SIN DATOS &mdash; no se calcula IRI; va al final de la tabla y no entra en el gr&aacute;fico</td></tr>
  </table>

  <h2>&#x1F4C4; Riesgo de contrataci&oacute;n</h2>
  <p>Se calcula con las <b>adjudicaciones de COMPR.AR</b> publicadas en datos.gob.ar (archivo 2016-2026), tomando los dos ejercicios m&aacute;s recientes:</p>
  <div class="formula-big">R_Contrataci&oacute;n = contrataciones directas &divide; procedimientos adjudicados &times; 100</div>
  <ul>
    <li>Numerador y denominador salen de la misma fuente completa, as&iacute; que el porcentaje no queda sesgado.</li>
    <li>Solo se calcula para organismos con <b>20 procedimientos o m&aacute;s</b>: con pocos casos el porcentaje no es confiable (1 de 2 = 50%).</li>
    <li>La columna <i>Fuente</i> informa, adem&aacute;s, qu&eacute; parte del monto adjudicado se lleva el <b>principal proveedor</b> y cu&aacute;ntas adjudicaciones 2026 public&oacute; el organismo en el Bolet&iacute;n Oficial. La concentraci&oacute;n es informativa: no entra en el puntaje.</li>
    <li>Los organismos que licitan en el <b>Bolet&iacute;n Oficial</b> pero no aparecen en COMPR.AR (por ejemplo ARCA, Banco Naci&oacute;n o los Ministerios P&uacute;blicos) compran fuera de ese sistema: sus directas no se publican en ning&uacute;n lado. Se listan como <i>&ldquo;sin dato de directas: compra fuera de COMPR.AR&rdquo;</i>, sin porcentaje, porque un 0% ser&iacute;a enga&ntilde;oso.</li>
    <li>El Bolet&iacute;n Oficial no se usa para el porcentaje: all&iacute; se publican casi todas las licitaciones pero solo una parte de las directas.</li>
  </ul>

  <h2>🕸️ Grafo organismo ↔ proveedor</h2>
  <p>La pestaña <b>Grafo</b> muestra, para cada organismo con porcentaje de directas calculado, sus <b>principales proveedores por monto</b> en COMPR.AR durante los mismos dos ejercicios del riesgo de contratación. Cada <b>cuadrado azul</b> es un organismo y cada <b>círculo</b> es un proveedor (CUIT).</p>
  <ul>
    <li><b>Proveedores por organismo:</b> se eligen 3, 5 o 10 principales. Con más proveedores aparecen más vínculos compartidos, pero el grafo se vuelve más denso.</li>
    <li><b>Nombres:</b> cada nodo lleva el nombre del organismo o la razón social del proveedor; los proveedores compartidos se escriben más grandes. Con el zoom se leen los demás.</li>
    <li><b>Colores:</b> verde, si el proveedor está entre los principales de un solo organismo; amarillo, si lo está en varios. La línea es roja cuando al menos un procedimiento del vínculo fue una contratación directa.</li>
    <li><b>Tamaños:</b> el cuadrado crece con el monto adjudicado del organismo y la línea, con el monto del vínculo.</li>
    <li><b>Panel de detalle:</b> en un organismo muestra sus procedimientos, sus proveedores distintos y la parte del monto que se lleva el principal (el mismo dato que la columna <i>Fuente</i>). En un proveedor muestra con cuántos organismos del grafo aparece y a cuántos organismos les vendió en total, y enlaza a sus avisos 2026 en el sitio Contratos.</li>
    <li><b>"Solo proveedores de varios organismos":</b> deja solo los proveedores compartidos y los organismos que los contratan.</li>
  </ul>
  <div class="info-box warn">El grafo es descriptivo y no cambia el IRI. Un proveedor grande que vende a muchos organismos, como una aseguradora o una imprenta, aparece en el centro por la naturaleza de lo que vende, no porque haya algo irregular. Solo se usan montos en pesos, y quedan afuera los organismos con menos de 20 procedimientos y los que compran fuera de COMPR.AR.</div>
  <p>El grafo se recalcula con cada actualización diaria y se guarda en la base de datos para no perderse en un redeploy. El grafo de Contratos (BORA 2026) complementa este: allí se ven los avisos del año en curso, incluidos organismos que no usan COMPR.AR.</p>

  <h2>&#x1F5A5; C&oacute;mo usar el dashboard</h2>
  <ul>
    <li><b>Tarjetas superiores:</b> cantidad de organismos, IRI promedio y cantidad en cada nivel de riesgo.</li>
    <li><b>Gr&aacute;fico principal:</b> los 30 organismos con mayor IRI. Los que no tienen IRI no aparecen.</li>
    <li><b>Tabla:</b> se filtra por &aacute;rea, nivel de riesgo y nombre. Pasando el mouse por la columna <i>Fuente</i> se ve el detalle completo del origen del dato.</li>
    <li><b>Grafo:</b> red de organismos y sus principales proveedores (ver arriba).</li>
  </ul>

  <h2>&#x1F3D7; Repositorios integrados</h2>
  <table class="manual-table">
    <tr><th>Repositorio</th><th>Datos que aporta</th></tr>
    <tr><td>monitor (central)</td><td>FastAPI principal. Dashboard HTML con Plotly.js.</td></tr>
    <tr><td>justicia</td><td>IRA por juzgado, vacantes judiciales, magistrados.</td></tr>
    <tr><td>monitor_legistativo</td><td>NAPE, IQP, asistencia &mdash; C&aacute;mara de Diputados.</td></tr>
    <tr><td>monitor_legistativo_senadores</td><td>Participaci&oacute;n, reporte por partido &mdash; Senado.</td></tr>
    <tr><td>monitor_contratos_v2</td><td>Adjudicaciones del Bolet&iacute;n Oficial 2026; el Monitor suma COMPR.AR (datos.gob.ar).</td></tr>
    <tr><td>gob_bo_comprar_tgn</td><td>Tesorer&iacute;a General de la Naci&oacute;n Argentina.</td></tr>
  </table>

  <h2>&#x1F511; Variables de entorno</h2>
  <table class="manual-table">
    <tr><th>Variable</th><th>Descripci&oacute;n</th></tr>
    <tr><td>JUSTICIA_API_URL</td><td>URL del servicio justicia</td></tr>
    <tr><td>LEGISTATIVO_API_URL</td><td>URL del servicio monitor_legistativo</td></tr>
    <tr><td>SENADORES_API_URL</td><td>URL del servicio senadores</td></tr>
    <tr><td>CONTRATOS_AR_API_URL</td><td>URL de monitor_contratos_v2</td></tr>
    <tr><td>TGN_AR_API_URL</td><td>URL de gob_bo_comprar_tgn</td></tr>
    <tr><td>COMPRAR_HIST_URL</td><td>Archivo de adjudicaciones COMPR.AR (por defecto, el 2016-2026 de datos.gob.ar)</td></tr>
    <tr><td>COMPRAR_EJERCICIOS / COMPRAR_MIN_PROC</td><td>Ejercicios que se toman (2) y m&iacute;nimo de procedimientos por organismo (20)</td></tr>
    <tr><td>REFRESH_TOKEN</td><td>Token para <code>POST /refresh</code> y <code>/reset-db</code> (obligatorio: sin &eacute;l quedan deshabilitados)</td></tr>
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
    <tr><td>GET /grafo?top=5</td><td>Red organismo ↔ principales proveedores (COMPR.AR); <code>solo_compartidos=true</code> deja los proveedores de varios organismos</td></tr>
    <tr><td>POST /refresh</td><td>Regenera el CSV (requiere <code>X-Refresh-Token</code>)</td></tr>
    <tr><td>GET /docs</td><td>Swagger UI</td></tr>
  </table>

  <div id="relaciones">
  <h2>🕸️ Relación con los otros sitios del ecosistema</h2>
  <p>Este sitio es parte del <strong>Ecosistema de Transparencia</strong>: once monitores que se consultan entre sí. Abajo, con qué sitios intercambia datos (<em>usa →</em> = toma datos de ese sitio; <em>← lo usa</em> = ese sitio toma datos de este) y de qué fuentes oficiales se alimenta. El grafo es interactivo: hacé clic en cualquier sitio para ver su ficha.</p>
  <iframe src="https://mapa.ecosistematransparencia.com.ar/grafo?embed=1&amp;nodo=monitor" title="Relación con los otros sitios del ecosistema" loading="lazy"
          style="width:100%;height:560px;border:1px solid #cbd5e1;border-radius:10px;background:#fff"></iframe>
  <p style="font-size:.85em"><a href="https://mapa.ecosistematransparencia.com.ar/grafo" target="_blank" rel="noopener">Abrir el grafo completo ↗</a></p>
  <h3>Conexiones</h3>
  <ul><li><strong>usa → Justicia</strong>: Toma magistrados y vacantes para el riesgo financiero, operativo y de datos.</li><li><strong>usa → Diputados</strong>: Toma bloques y KPIs reales de la HCDN.</li><li><strong>usa → Senadores</strong>: Toma la nómina y la participación promedio de senadores.</li><li><strong>usa → Contratos</strong>: Toma las licitaciones 2026 del BORA para marcar a los organismos que compran fuera de COMPR.AR (“sin dato de directas”). El % de directas sale de COMPR.AR 2016-2026.</li><li><strong>← lo usa Vectores</strong> <em>(planificado)</em>: Planificado: alimentar la dimensión R_Internacional del IRI. Todavía no conectado.</li><li><strong>← lo usa MEACI</strong> <em>(planificado)</em>: Planificado: aportar alertas internacionales al IRI. Todavía no conectado.</li><li><strong>← lo usa Mapa</strong>: Muestra el IRI global (proxy /iri/*) y el estado del sitio. La validación cruzada diaria revisa de qué año es su riesgo de contratación.</li></ul>
  <h3>Fuentes oficiales</h3>
  <ul><li>Justicia (magistrados, vacantes)</li><li>Diputados (bloques, KPIs HCDN)</li><li>Senadores (nómina, participación)</li><li>COMPR.AR — adjudicaciones 2016-2026 (datos.gob.ar)</li><li>Contratos (licitaciones BORA 2026)</li></ul>
  <p style="font-size:.85em">Detalle de todas las conexiones y de cómo se verifican las cifras entre sitios: <a href="https://mapa.ecosistematransparencia.com.ar/manual#conexiones" target="_blank" rel="noopener">mapa.ecosistematransparencia.com.ar/manual#conexiones</a></p>
</div>

      <p style="margin-top:1.2rem;color:#64748b"><b>Herramienta acad&eacute;mica.</b> Los scores son indicadores algor&iacute;tmicos basados en datos p&uacute;blicos: no implican juicio de valor, acusaci&oacute;n ni determinaci&oacute;n de responsabilidad sobre ning&uacute;n organismo o persona.</p>
</div>

<div id="manual-en" lang="en" style="display:none">
  <h2>&#x2139;&#xFE0F; What is the IRI Monitor?</h2>
  <p>The <b>Institutional Risk Monitor (IRI, from its Spanish name)</b> is an open-source academic tool that quantifies levels of corruption and inefficiency risk in Argentine government bodies. It brings together in a single table the indicators of the other sites in the ecosystem (Judiciary, Chamber of Deputies, Senate, Contracts and Treasury).</p>

  <h2>&#x1F9EE; IRI formula</h2>
  <div class="formula-big">IRI = R_Financial &times; 35% &nbsp;+&nbsp; R_Procurement &times; 30% &nbsp;+&nbsp; R_Operational &times; 20% &nbsp;+&nbsp; R_Data &times; 15%</div>
  <table class="manual-table">
    <tr><th>Component</th><th>Weight</th><th>Description</th></tr>
    <tr><td>R_Financial (Financiero)</td><td>35%</td><td>Budget irregularities, deviations in spending execution.</td></tr>
    <tr><td>R_Procurement (Contrataci&oacute;n)</td><td>30%</td><td>Share of direct contracting over all procurement procedures (see below).</td></tr>
    <tr><td>R_Operational (Operativo)</td><td>20%</td><td>Legislative absenteeism, judicial vacancy rate, participation in the Senate.</td></tr>
    <tr><td>R_Data (Datos)</td><td>15%</td><td>Quality and availability of public information.</td></tr>
  </table>
  <p><b>Components without data.</b> When a body has no data for a component, that component is not filled with a fixed value: it shows <code>s/d</code> (no data) and the IRI is computed with the measured components, redistributing their weights so they add up to 100%. The <i>Componentes con dato</i> (components with data) column says how many were used.</p>
  <table class="manual-table">
    <tr><th>IRI score</th><th>Risk level</th></tr>
    <tr><td>0 &ndash; 29</td><td>&#x1F7E2; BAJO (low) &mdash; within normal parameters</td></tr>
    <tr><td>30 &ndash; 59</td><td>&#x1F7E1; MEDIO (medium) &mdash; needs follow-up</td></tr>
    <tr><td>60 &ndash; 100</td><td>&#x1F534; ALTO (high) &mdash; institutional risk alert</td></tr>
    <tr><td>60 &ndash; 100 with a single component</td><td>&#x1F7E1; MEDIO (dato parcial = partial data) &mdash; a single measurement is never marked high</td></tr>
    <tr><td>no components</td><td>&#x26AA; SIN DATOS (no data) &mdash; no IRI is computed; the body goes to the bottom of the table and is left out of the chart</td></tr>
  </table>

  <h2>&#x1F4C4; Procurement risk</h2>
  <p>It is computed from the <b>COMPR.AR contract awards</b> published on datos.gob.ar (2016-2026 file), using the two most recent fiscal years:</p>
  <div class="formula-big">R_Procurement = direct contracts &divide; awarded procedures &times; 100</div>
  <ul>
    <li>Numerator and denominator come from the same complete source, so the percentage is not biased.</li>
    <li>It is only computed for bodies with <b>20 procedures or more</b>: with few cases the percentage is unreliable (1 out of 2 = 50%).</li>
    <li>The <i>Fuente</i> (source) column also reports what share of the awarded amount goes to the <b>main supplier</b> and how many 2026 awards the body published in the Official Gazette (BORA). Concentration is informative only: it does not enter the score.</li>
    <li>Bodies that publish tenders in the <b>Official Gazette</b> but do not appear in COMPR.AR (for example ARCA, Banco Naci&oacute;n or the Public Prosecutor's and Public Defender's offices) buy outside that system: their direct contracts are not published anywhere. They are listed as <i>&ldquo;sin dato de directas: compra fuera de COMPR.AR&rdquo;</i> (no direct-contracting data: buys outside COMPR.AR), with no percentage, because 0% would be misleading.</li>
    <li>The Official Gazette is not used for the percentage: almost all tenders are published there, but only part of the direct contracts.</li>
  </ul>

  <h2>🕸️ Body ↔ supplier graph</h2>
  <p>The <b>Grafo (Graph)</b> tab shows, for each body with a computed direct-contracting percentage, its <b>main suppliers by amount</b> in COMPR.AR over the same two fiscal years used for procurement risk. Each <b>blue square</b> is a body and each <b>circle</b> is a supplier (CUIT tax ID).</p>
  <ul>
    <li><b>Suppliers per body:</b> choose the top 3, 5 or 10. More suppliers reveal more shared links but make the graph denser.</li>
    <li><b>Names:</b> every node carries the body's name or the supplier's registered name; shared suppliers are written larger. Zoom in to read the rest.</li>
    <li><b>Colours:</b> green when the supplier is among the main suppliers of a single body, yellow when it is among those of several. A line is red when at least one procedure in that link was a direct award.</li>
    <li><b>Sizes:</b> a square grows with the body's awarded amount, and a line grows with the amount of that link.</li>
    <li><b>Detail panel:</b> for a body it shows its procedures, its distinct suppliers and the share of the amount that goes to the main supplier (the same figure as the <i>Fuente</i> column). For a supplier it shows how many bodies in the graph it appears with and how many bodies it sold to in total, and it links to the supplier's 2026 notices on the Contratos site.</li>
    <li><b>"Solo proveedores de varios organismos"</b> (suppliers of several bodies only): keeps only shared suppliers and the bodies that hire them.</li>
  </ul>
  <div class="info-box warn">The graph is descriptive and does not change the IRI. A large supplier that sells to many bodies, such as an insurer or a printer, sits at the centre because of what it sells, not because anything is wrong. Only amounts in pesos are used. Bodies with fewer than 20 procedures and bodies that buy outside COMPR.AR are left out.</div>
  <p>The graph is recomputed with every daily refresh and stored in the database so that it survives a redeploy. The Contratos graph (BORA 2026) complements this one: it shows the current year's notices, including bodies that do not use COMPR.AR.</p>

  <h2>&#x1F5A5; Using the dashboard</h2>
  <ul>
    <li><b>Top cards:</b> number of bodies, average IRI and number at each risk level.</li>
    <li><b>Main chart:</b> the 30 bodies with the highest IRI. Bodies without an IRI are not shown.</li>
    <li><b>Table:</b> filter by area, risk level and name. Hovering over the <i>Fuente</i> column shows the full detail of where the data comes from.</li>
    <li><b>Grafo (Graph):</b> network of bodies and their main suppliers (see above).</li>
  </ul>

  <h2>&#x1F3D7; Integrated repositories</h2>
  <table class="manual-table">
    <tr><th>Repository</th><th>Data it provides</th></tr>
    <tr><td>monitor (central)</td><td>Main FastAPI app. HTML dashboard with Plotly.js.</td></tr>
    <tr><td>justicia</td><td>IRA per court, judicial vacancies, judges.</td></tr>
    <tr><td>monitor_legistativo</td><td>NAPE, IQP, attendance &mdash; Chamber of Deputies.</td></tr>
    <tr><td>monitor_legistativo_senadores</td><td>Participation, report by party &mdash; Senate.</td></tr>
    <tr><td>monitor_contratos_v2</td><td>2026 contract awards from the Official Gazette; the Monitor adds COMPR.AR (datos.gob.ar).</td></tr>
    <tr><td>gob_bo_comprar_tgn</td><td>National Treasury (Tesorer&iacute;a General de la Naci&oacute;n).</td></tr>
  </table>

  <h2>&#x1F511; Environment variables</h2>
  <table class="manual-table">
    <tr><th>Variable</th><th>Description</th></tr>
    <tr><td>JUSTICIA_API_URL</td><td>URL of the justicia service</td></tr>
    <tr><td>LEGISTATIVO_API_URL</td><td>URL of the monitor_legistativo service</td></tr>
    <tr><td>SENADORES_API_URL</td><td>URL of the senadores service</td></tr>
    <tr><td>CONTRATOS_AR_API_URL</td><td>URL of monitor_contratos_v2</td></tr>
    <tr><td>TGN_AR_API_URL</td><td>URL of gob_bo_comprar_tgn</td></tr>
    <tr><td>COMPRAR_HIST_URL</td><td>COMPR.AR awards file (by default, the 2016-2026 file on datos.gob.ar)</td></tr>
    <tr><td>COMPRAR_EJERCICIOS / COMPRAR_MIN_PROC</td><td>Fiscal years used (2) and minimum procedures per body (20)</td></tr>
    <tr><td>REFRESH_TOKEN</td><td>Token for <code>POST /refresh</code> and <code>/reset-db</code> (required: without it both are disabled)</td></tr>
  </table>

  <h2>&#x1F4CB; API endpoints</h2>
  <table class="manual-table">
    <tr><th>Endpoint</th><th>Description</th></tr>
    <tr><td>GET /</td><td>Redirects to /dashboard</td></tr>
    <tr><td>GET /dashboard</td><td>This dashboard</td></tr>
    <tr><td>GET /datos</td><td>Full dataset as JSON</td></tr>
    <tr><td>GET /por-area/{area}</td><td>Bodies filtered by area</td></tr>
    <tr><td>GET /top-riesgo?n=10</td><td>Top N bodies by IRI</td></tr>
    <tr><td>GET /resumen</td><td>Global statistics by area</td></tr>
    <tr><td>GET /grafo?top=5</td><td>Body ↔ main supplier network (COMPR.AR); <code>solo_compartidos=true</code> keeps only suppliers of several bodies</td></tr>
    <tr><td>POST /refresh</td><td>Regenerates the CSV (requires <code>X-Refresh-Token</code>)</td></tr>
    <tr><td>GET /docs</td><td>Swagger UI</td></tr>
  </table>

  <div id="relaciones-en">
  <h2>🕸️ How this site connects to the rest of the ecosystem</h2>
  <p>This site is part of the <strong>Transparency Ecosystem</strong>: eleven monitors that query each other. Below, which sites it exchanges data with (<em>uses →</em> = takes data from that site; <em>← used by</em> = that site takes data from this one) and which official sources feed it. The graph is interactive: click any site to see its profile.</p>
  <iframe src="https://mapa.ecosistematransparencia.com.ar/grafo?embed=1&amp;nodo=monitor&amp;lang=en" title="How this site connects to the rest of the ecosystem" loading="lazy"
          style="width:100%;height:560px;border:1px solid #cbd5e1;border-radius:10px;background:#fff"></iframe>
  <p style="font-size:.85em"><a href="https://mapa.ecosistematransparencia.com.ar/grafo?lang=en" target="_blank" rel="noopener">Open the full graph ↗</a></p>
  <h3>Connections</h3>
  <ul><li><strong>uses → Justicia</strong>: Pulls judges and vacancies for financial, operational and data risk.</li><li><strong>uses → Diputados</strong>: Pulls blocs and real HCDN KPIs.</li><li><strong>uses → Senadores</strong>: Pulls the senators roster and average participation.</li><li><strong>uses → Contratos</strong>: Pulls 2026 BORA tenders to flag agencies that buy outside COMPR.AR (“no direct-award data”). The direct-award % comes from COMPR.AR 2016-2026.</li><li><strong>← used by Vectores</strong> <em>(planned)</em>: Planned: feed the IRI R_International dimension. Not connected yet.</li><li><strong>← used by MEACI</strong> <em>(planned)</em>: Planned: contribute international alerts to the IRI. Not connected yet.</li><li><strong>← used by Mapa</strong>: Shows the global IRI (proxy /iri/*) and site status. The daily cross-validation checks the year of its procurement risk data.</li></ul>
  <h3>Official sources</h3>
  <ul><li>Judiciary (judges, vacancies)</li><li>Deputies (blocs, HCDN KPIs)</li><li>Senators (roster, participation)</li><li>COMPR.AR — 2016-2026 awards (datos.gob.ar)</li><li>Contracts (2026 BORA tenders)</li></ul>
  <p style="font-size:.85em">Every connection in detail, and how figures are cross-checked between sites: <a href="https://mapa.ecosistematransparencia.com.ar/en/manual#conexiones" target="_blank" rel="noopener">mapa.ecosistematransparencia.com.ar/en/manual#conexiones</a></p>
</div>

      <p style="margin-top:1.2rem;color:#64748b"><b>Academic tool.</b> The scores are algorithmic indicators based on public data: they imply no value judgment, accusation or finding of responsibility about any body or person.</p>
</div>

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
  // ── GRAFO ORGANISMO ↔ PROVEEDOR ──────────────────────────────────────────
  const GRAFO = { red: null, nodos: {}, lib: null, init: false };

  function escG(t) {
    return String(t ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }
  function montoG(v) {
    const n = Number(v) || 0;
    if (n >= 1e9) return '$' + (n / 1e9).toFixed(1).replace('.', ',') + ' mil M';
    if (n >= 1e6) return '$' + (n / 1e6).toFixed(1).replace('.', ',') + ' M';
    return '$' + Math.round(n).toLocaleString('es-AR');
  }
  function libGrafo() {
    if (window.vis && window.vis.Network) return Promise.resolve();
    if (!GRAFO.lib) {
      GRAFO.lib = new Promise((ok, mal) => {
        const s = document.createElement('script');
        s.src = 'https://cdnjs.cloudflare.com/ajax/libs/vis-network/9.1.9/standalone/umd/vis-network.min.js';
        s.onload = ok;
        s.onerror = () => { GRAFO.lib = null; mal(new Error('no se pudo cargar la biblioteca del grafo')); };
        document.head.appendChild(s);
      });
    }
    return GRAFO.lib;
  }

  async function cargarGrafo() {
    GRAFO.init = true;
    const cont = document.getElementById('g-canvas');
    cont.innerHTML = '<div id="loader">Cargando…</div>';
    const top = document.getElementById('g-top').value;
    const comp = document.getElementById('g-compartidos').checked;
    try {
      const [r] = await Promise.all([fetch(`/grafo?top=${top}&solo_compartidos=${comp}`), libGrafo()]);
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail || ('HTTP ' + r.status));
      dibujarGrafo(d);
    } catch (e) {
      cont.innerHTML = '<div style="padding:1.5rem;color:#f87171;font-size:0.85rem">Grafo no disponible: ' + escG(e.message) + '</div>';
    }
  }

  function dibujarGrafo(d) {
    const r = d.resumen || {};
    document.getElementById('g-periodo').textContent = r.periodo || '—';
    document.getElementById('g-org').textContent = r.organismos ?? '-';
    document.getElementById('g-prov').textContent = r.proveedores ?? '-';
    document.getElementById('g-ari').textContent = r.aristas ?? '-';
    document.getElementById('g-comp').textContent = r.compartidos ?? '-';
    const lista = document.getElementById('g-lista');
    lista.innerHTML = (r.top_compartidos || []).length
      ? r.top_compartidos.map(p => `<li data-id="${escG(p.id)}">${escG(p.label)} — <b>${p.organismos_en_grafo}</b> en el grafo (${p.organismos_total} en total)</li>`).join('')
      : '<li style="cursor:default">Ninguno con estos filtros.</li>';
    lista.querySelectorAll('li[data-id]').forEach(li => li.addEventListener('click', () => enfocarGrafo(li.dataset.id)));

    const cont = document.getElementById('g-canvas');
    if (!d.nodos.length) { cont.innerHTML = '<div style="padding:1.5rem;color:#64748b">Sin datos para estos filtros.</div>'; return; }
    GRAFO.nodos = {};
    const orgs = d.nodos.filter(n => n.tipo === 'organismo');
    const maxM = Math.max(...orgs.map(n => n.monto), 1);
    const maxA = Math.max(...d.aristas.map(a => a.monto), 1);
    const nodos = d.nodos.map(n => {
      GRAFO.nodos[n.id] = n;
      if (n.tipo === 'organismo') {
        const nom = n.label.replace(/^\d+\s*-\s*/, '');
        return { id: n.id, label: nom.length > 30 ? nom.slice(0, 29) + '…' : nom, shape: 'square',
                 size: 10 + 26 * Math.sqrt(n.monto / maxM), color: { background: '#3b82f6', border: '#93c5fd' },
                 font: { color: '#e2e8f0', size: 12 } };
      }
      const comp = n.organismos_en_grafo > 1;
      return { id: n.id, label: n.label.length > 26 ? n.label.slice(0, 25) + '…' : n.label, shape: 'dot',
               size: comp ? 6 + 3 * Math.min(n.organismos_en_grafo, 8) : 6,
               color: { background: comp ? '#fbbf24' : '#4ade80', border: comp ? '#fbbf24' : '#4ade80' },
               font: { color: comp ? '#e2e8f0' : '#94a3b8', size: comp ? 12 : 9 } };
    });
    const aristas = d.aristas.map(a => ({ from: a.from, to: a.to, width: 1 + 6 * Math.sqrt(a.monto / maxA),
      color: { color: a.directas > 0 ? '#f87171' : '#475569', opacity: 0.8 } }));
    cont.innerHTML = '';
    if (GRAFO.red) GRAFO.red.destroy();
    GRAFO.red = new vis.Network(cont, { nodes: new vis.DataSet(nodos), edges: new vis.DataSet(aristas) }, {
      physics: { solver: 'forceAtlas2Based', stabilization: { iterations: 300 } },
      interaction: { hover: true }, edges: { smooth: false },
    });
    GRAFO.red.once('stabilizationIterationsDone', () => { GRAFO.red.setOptions({ physics: false }); GRAFO.red.fit(); });
    GRAFO.red.on('click', e => { if (e.nodes.length) infoGrafo(e.nodes[0]); });
  }

  function infoGrafo(id) {
    const n = GRAFO.nodos[id];
    if (!n) return;
    const el = document.getElementById('g-info');
    if (n.tipo === 'organismo') {
      el.innerHTML = `<b>${escG(n.label)}</b><br>Procedimientos adjudicados: <b>${n.procesos}</b><br>
        Proveedores distintos: <b>${n.proveedores}</b><br>Monto adjudicado (pesos): <b>${montoG(n.monto)}</b><br>
        Principal proveedor: <b>${n.concentracion ?? '—'} %</b> del monto`;
    } else {
      el.innerHTML = `<b>${escG(n.label)}</b><br>CUIT: <code>${escG(n.cuit)}</code><br>
        Organismos en este grafo: <b>${n.organismos_en_grafo}</b><br>
        Organismos que le adjudicaron (en total): <b>${n.organismos_total}</b><br>
        <a href="https://contratos.ecosistematransparencia.com.ar/?proveedor=${encodeURIComponent(n.cuit)}" target="_blank" rel="noopener">Ver sus avisos 2026 en Contratos ↗</a>`;
    }
  }

  function enfocarGrafo(id) {
    if (!GRAFO.red || !GRAFO.nodos[id]) return;
    GRAFO.red.selectNodes([id]);
    GRAFO.red.focus(id, { scale: 1.3, animation: true });
    infoGrafo(id);
  }

  function showPage(name, btn) {
    document.querySelectorAll('.tab-page').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.nav-tab').forEach(b => b.classList.remove('active'));
    document.getElementById('page-' + name).classList.add('active');
    btn.classList.add('active');
    if (name === 'grafo' && !GRAFO.init) cargarGrafo();
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
      // Fecha de los DATOS (no la hora en que se abrió la página)
      const fd = resData.global && resData.global.datos_actualizados;
      document.getElementById('status').textContent = fd
        ? 'Datos del ' + new Date(fd).toLocaleString('es-AR', {dateStyle: 'short', timeStyle: 'short'})
        : 'Fecha de los datos no disponible';
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
    if ((estado || '').includes('SIN DATOS')) return 'badge badge-gray';
    return 'badge badge-green';
  }

  // IRI vacío (ningún componente medido, p. ej. compra fuera de COMPR.AR) → -1
  // para ordenarlo al final; nunca se muestra ni se grafica como 0.
  const iriNum = d => (d['IRI (Score)'] === '' || d['IRI (Score)'] == null || isNaN(+d['IRI (Score)'])) ? -1 : +d['IRI (Score)'];

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
    if (f.includes('fuera de COMPR.AR'))                    return '⚪ BORA (fuera de COMPR.AR)';
    if (f.includes('monitor_contratos') && f.includes('COMPR.AR')) return '✅ COMPR.AR';
    if (f.includes('gob_bo_comprar_tgn'))                   return '✅ TGN';
    if (f.includes('senadores') && f.includes('nomina'))    return '✅ Senado (nómina)';
    if (f.includes('senadores') && f.includes('partido'))   return '✅ Senado (partidos)';
    if (f.includes('senadores') || f.includes('senado'))    return '✅ Senado';
    if (f.includes('monitor_legistativo'))                  return '✅ Legislativo';
    if (f.includes('justicia') && f.includes('vacantes'))   return '✅ Vacantes judiciales';
    if (f.includes('justicia'))                             return '✅ Judicial';
    return f.split('/')[0] || '-';
  }

  // Componente sin dato → "s/d" (antes se mostraba un valor fijo como si fuera medido)
  function sd(v) {
    return (v === '' || v === null || v === undefined || isNaN(+v))
      ? '<span style="color:#64748b" title="Sin dato para este organismo: no entra en el IRI">s/d</span>'
      : (+v).toFixed(0);
  }

  // Idioma del manual (ES/EN); se recuerda en el navegador si se puede
  function setManualLang(lang) {
    document.getElementById('manual-es').style.display = lang === 'en' ? 'none' : '';
    document.getElementById('manual-en').style.display = lang === 'en' ? '' : 'none';
    document.querySelectorAll('.manual-lang').forEach(b => b.classList.toggle('activo', b.dataset.lang === lang));
    try { localStorage.setItem('manualLang', lang); } catch (e) {}
  }
  try { if (localStorage.getItem('manualLang') === 'en' || new URLSearchParams(location.search).get('lang') === 'en') setManualLang('en'); } catch (e) {}

  function renderTable() {
    const rows = filterData().sort((a,b) => iriNum(b) - iriNum(a));
    document.getElementById('table-body').innerHTML = rows.map(d => `
      <tr>
        <td><span class="${badgeClass(d.Estado)}">${d.Estado || '-'}</span></td>
        <td><b>${d.Organismo}</b></td>
        <td style="color:#94a3b8">${d.Area}</td>
        <td style="color:${iriNum(d) < 0 ? '#64748b' : iriColor(iriNum(d))};font-weight:700;font-size:1rem">${iriNum(d) < 0 ? sd('') : iriNum(d).toFixed(1)}</td>
        <td>${sd(d['Riesgo Financiero'])}</td>
        <td>${sd(d['Riesgo Contratación'])}</td>
        <td>${sd(d['Riesgo Operativo'])}</td>
        <td>${sd(d['Riesgo Datos'])}</td>
        <td title="${d.Fuente || ''}">${fuenteLabel(d.Fuente)}</td>
      </tr>`).join('');
    document.getElementById('table-count').textContent =
      rows.length + ' de ' + allData.length + ' organismos mostrados';
  }

  function renderMainBar(data) {
    const top = data.filter(d => iriNum(d) >= 0).sort((a,b) => iriNum(b) - iriNum(a)).slice(0,30).reverse();
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
