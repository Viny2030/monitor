import streamlit as st
import pandas as pd

st.set_page_config(page_title="Monitor IRI Nacional", layout="wide")
st.title("🚦 Monitor de Riesgo Institucional - Argentina")

# Cargar el dataset con nombres reales
try:
    df = pd.read_csv("data/processed/monitor_completo.csv")
except Exception:
    st.error("⚠️ Ejecutá primero 'python src/motor_analitico.py'")
    st.stop()

# Sidebar: Filtros por Área
st.sidebar.header("Filtros de Auditoría")
areas_sel = st.sidebar.multiselect("Seleccionar Áreas", df['Area'].unique(), default=df['Area'].unique())

# Filtrar datos
df_filtrado = df[df['Area'].isin(areas_sel)]

# Métricas
c1, c2, c3 = st.columns(3)
c1.metric("Organismos", len(df_filtrado))
c2.metric("Riesgo Promedio", f"{df_filtrado['IRI (Score)'].mean():.1f}")
c3.metric("Alertas Rojas", len(df_filtrado[df_filtrado['Estado'] == '🔴 ALTO']))

st.subheader("Nivel de Riesgo por Organismo (Ranking)")
st.bar_chart(df_filtrado.set_index('Organismo')['IRI (Score)'])

st.subheader("📋 Detalle de Alertas por Área")
st.dataframe(df_filtrado, use_container_width=True)
