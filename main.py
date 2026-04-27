"""
main.py — FastAPI del Monitor IRI
==================================
Endpoints:
  GET /                    → health check
  GET /datos               → dataset completo
  GET /por-area/{area}     → organismos filtrados por área
  GET /top-riesgo          → top N organismos de mayor IRI
  GET /resumen             → estadísticas globales por área
  POST /refresh            → regenera el CSV (protegido por REFRESH_TOKEN)

Variables de entorno:
  REFRESH_TOKEN            → token para /refresh (default: "dev")
  LEGISTATIVO_API_URL      → URL Railway de monitor_legistativo (opcional)
  JUSTICIA_API_URL         → URL Railway de justicia (opcional)
"""

import os
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
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
            detail="Dataset no disponible. Ejecutá: python src/motor_analitico.py"
        )
    return pd.read_csv(CSV_PATH)


def _df_to_records(df: pd.DataFrame) -> list:
    return df.fillna("").to_dict(orient="records")


# ── Endpoints ─────────────────────────────────────────────────────────────────

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
    }


@app.get("/datos")
def get_datos(area: str = None, estado: str = None):
    """
    Devuelve el dataset completo.
    Filtros opcionales por query param: ?area=Poder+Judicial&estado=🔴+ALTO
    """
    df = _load_df()
    if area:
        df = df[df["Area"].str.contains(area, case=False, na=False)]
    if estado:
        df = df[df["Estado"].str.contains(estado, case=False, na=False)]
    return {
        "total": len(df),
        "datos": _df_to_records(df),
    }


@app.get("/por-area/{area}")
def get_por_area(area: str):
    """Organismos de un área específica, ordenados por IRI descendente."""
    df = _load_df()
    df_area = df[df["Area"].str.contains(area, case=False, na=False)]
    if df_area.empty:
        areas_disponibles = df["Area"].unique().tolist()
        raise HTTPException(
            status_code=404,
            detail=f"Área '{area}' no encontrada. Disponibles: {areas_disponibles}"
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
    """Top N organismos con mayor score IRI."""
    if n > 50:
        n = 50
    df = _load_df()
    top = df.nlargest(n, "IRI (Score)")
    return {
        "n": len(top),
        "organismos": _df_to_records(top),
    }


@app.get("/resumen")
def get_resumen():
    """Estadísticas globales y por área."""
    df = _load_df()

    global_stats = {
        "total_organismos": len(df),
        "iri_promedio_global": round(df["IRI (Score)"].mean(), 2),
        "iri_max": round(df["IRI (Score)"].max(), 2),
        "iri_min": round(df["IRI (Score)"].min(), 2),
        "alto_riesgo":  int((df["Estado"] == "🔴 ALTO").sum()),
        "medio_riesgo": int((df["Estado"] == "🟡 MEDIO").sum()),
        "bajo_riesgo":  int((df["Estado"] == "🟢 BAJO").sum()),
    }

    por_area = (
        df.groupby("Area")
        .agg(
            organismos=("Organismo", "count"),
            iri_promedio=("IRI (Score)", "mean"),
            iri_max=("IRI (Score)", "max"),
        )
        .round(2)
        .reset_index()
        .sort_values("iri_promedio", ascending=False)
        .to_dict(orient="records")
    )

    fuentes = df["Fuente"].value_counts().to_dict() if "Fuente" in df.columns else {}

    return {
        "global": global_stats,
        "por_area": por_area,
        "fuentes_de_datos": fuentes,
    }


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
