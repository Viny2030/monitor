"""
app/dashboard.py — Dashboard Streamlit del Monitor IRI
=======================================================
Consume la FastAPI del monitor central y muestra:
  - Métricas globales (IRI promedio, organismos en rojo/amarillo/verde)
  - Filtros por área y nivel de riesgo
  - Tabla coloreada con todos los organismos
  - Gráfico de barras horizontal coloreado por Estado
  - Top 5 mayor riesgo
"""

import streamlit as st
import pandas as pd
import requests

st.set_page_config(
    page_title="Monitor IRI — Argentina",
    page_icon="🚦",
    layout="wide",
)

# ── Config ────────────────────────────────────────────────────────────────────
API_URL = "http://127.0.0.1:8000"

COLORES_ESTADO = {
    "🔴 ALTO":  "#ef4444",
    "🟡 MEDIO": "#f59e0b",
    "🟢 BAJO":  "#22c55e",
}


# ── Carga de datos ────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def cargar_datos() -> pd.DataFrame | None:
    try:
        r = requests.get(f"{API_URL}/datos", timeout=10)
        if r.status_code == 200:
            return pd.DataFrame(r.json()["datos"])
    except Exception as e:
        st.error(f"No se pudo conectar con la API: {e}")
    return None


@st.cache_data(ttl=300)
def cargar_resumen() -> dict | None:
    try:
        r = requests.get(f"{API_URL}/resumen", timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


# ── Sidebar ───────────────────────────────────────────────────────────────────

st.sidebar.image("https://raw.githubusercontent.com/Viny2030/monitor/main/app/logo.png",
                 use_container_width=True, caption="")

st.sidebar.title("🚦 Monitor IRI")
st.sidebar.caption("Semáforo Institucional Argentina")
st.sidebar.markdown("---")

st.sidebar.markdown("**Repos conectados:**")
st.sidebar.markdown("- ⚖️ [justicia](https://github.com/Viny2030/justicia)")
st.sidebar.markdown("- 🏛️ [monitor_legistativo](https://github.com/Viny2030/monitor_legistativo)")
st.sidebar.markdown("- 🏛️ [monitor_legistativo_senadores](https://github.com/Viny2030/monitor_legistativo_senadores)")
st.sidebar.markdown("---")

if st.sidebar.button("🔄 Actualizar datos"):
    st.cache_data.clear()
    st.rerun()

# ── Header ────────────────────────────────────────────────────────────────────

st.title("🚦 Monitor de Riesgo Institucional (IRI)")
st.caption("Argentina · Semáforo de integridad pública · v2.0")

# ── Cargar datos ──────────────────────────────────────────────────────────────

df_raw      = cargar_datos()
resumen     = cargar_resumen()

if df_raw is None:
    st.warning(
        "⚠️ La API no está disponible. "
        "Levantala con: `uvicorn main:app --reload`  "
        "y generá el dataset con: `python src/motor_analitico.py`"
    )
    st.stop()

# ── Métricas globales ─────────────────────────────────────────────────────────

g = resumen["global"] if resumen else {}

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Organismos monitoreados", g.get("total_organismos", len(df_raw)))
col2.metric("IRI Promedio", f"{g.get('iri_promedio_global', df_raw['IRI (Score)'].mean()):.1f}")
col3.metric("🔴 Alto riesgo",  g.get("alto_riesgo",  (df_raw["Estado"] == "🔴 ALTO").sum()))
col4.metric("🟡 Riesgo medio", g.get("medio_riesgo", (df_raw["Estado"] == "🟡 MEDIO").sum()))
col5.metric("🟢 Bajo riesgo",  g.get("bajo_riesgo",  (df_raw["Estado"] == "🟢 BAJO").sum()))

st.markdown("---")

# ── Filtros ───────────────────────────────────────────────────────────────────

col_f1, col_f2 = st.columns(2)
with col_f1:
    areas_disp = sorted(df_raw["Area"].dropna().unique())
    areas_sel  = st.multiselect("Filtrar por Área", areas_disp, default=areas_disp)
with col_f2:
    estados_disp = ["🔴 ALTO", "🟡 MEDIO", "🟢 BAJO"]
    estados_sel  = st.multiselect("Filtrar por Estado", estados_disp, default=estados_disp)

df = df_raw[
    df_raw["Area"].isin(areas_sel) &
    df_raw["Estado"].isin(estados_sel)
].copy()

st.caption(f"{len(df)} organismos mostrados de {len(df_raw)} totales")

# ── Gráfico de barras coloreado ───────────────────────────────────────────────

st.subheader("📊 Score IRI por Organismo")

df_chart = df.sort_values("IRI (Score)", ascending=True).tail(30)
df_chart["color"] = df_chart["Estado"].map(COLORES_ESTADO)

import plotly.express as px

fig = px.bar(
    df_chart,
    x="IRI (Score)",
    y="Organismo",
    orientation="h",
    color="Estado",
    color_discrete_map=COLORES_ESTADO,
    hover_data=["Area", "Riesgo Financiero", "Riesgo Contratación",
                "Riesgo Operativo", "Riesgo Datos"],
    height=max(400, len(df_chart) * 22),
    labels={"IRI (Score)": "Score IRI (0-100)", "Organismo": ""},
)
fig.update_layout(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    legend_title_text="Nivel de Riesgo",
    xaxis=dict(range=[0, 100]),
)
fig.add_vline(x=60, line_dash="dot", line_color="#ef4444", annotation_text="Umbral Alto")
fig.add_vline(x=30, line_dash="dot", line_color="#f59e0b", annotation_text="Umbral Medio")

st.plotly_chart(fig, use_container_width=True)

# ── Tabla detallada ───────────────────────────────────────────────────────────

st.subheader("📋 Tabla de Organismos")

cols_mostrar = [c for c in [
    "Estado", "Organismo", "Area", "IRI (Score)",
    "Riesgo Financiero", "Riesgo Contratación", "Riesgo Operativo", "Riesgo Datos", "Fuente"
] if c in df.columns]

df_tabla = df[cols_mostrar].sort_values("IRI (Score)", ascending=False).reset_index(drop=True)


def color_estado(val):
    colores = {"🔴 ALTO": "background-color:#fee2e2", "🟡 MEDIO": "background-color:#fef3c7", "🟢 BAJO": "background-color:#dcfce7"}
    return colores.get(val, "")


def color_iri(val):
    try:
        v = float(val)
        if v >= 60: return "color:#dc2626; font-weight:bold"
        if v >= 30: return "color:#d97706; font-weight:bold"
        return "color:#16a34a; font-weight:bold"
    except Exception:
        return ""


styled = df_tabla.style.applymap(color_estado, subset=["Estado"])
if "IRI (Score)" in df_tabla.columns:
    styled = styled.applymap(color_iri, subset=["IRI (Score)"])

st.dataframe(styled, use_container_width=True, height=400)

# ── Resumen por área ──────────────────────────────────────────────────────────

if resumen and resumen.get("por_area"):
    st.subheader("📁 Resumen por Área")
    df_area = pd.DataFrame(resumen["por_area"])
    df_area.columns = ["Área", "Organismos", "IRI Promedio", "IRI Máximo"]
    st.dataframe(df_area, use_container_width=True)

# ── Fuentes de datos ──────────────────────────────────────────────────────────

if resumen and resumen.get("fuentes_de_datos"):
    with st.expander("🔎 Trazabilidad — Fuentes de datos"):
        for fuente, n in resumen["fuentes_de_datos"].items():
            st.write(f"- `{fuente}` → **{n}** organismos")
        st.caption(
            "Los datos de justicia y legislativo son reales (repos Viny2030). "
            "Los organismos ejecutivos usan síntesis reproducible (seed 44) "
            "hasta conectar compr.ar y datos.gob.ar."
        )

# ── Footer ────────────────────────────────────────────────────────────────────

st.markdown("---")
st.caption(
    "Monitor IRI v2.0 · Ph.D. Vicente Humberto Monteverde · "
    "[github.com/Viny2030/monitor](https://github.com/Viny2030/monitor)"
)
