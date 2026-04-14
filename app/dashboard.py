import streamlit as st
import pandas as pd

st.set_page_config(page_title="Monitor IRI Nacional", layout="wide")
st.title("🚦 Monitor de Riesgo Institucional - 200 Organismos")

# CARGA DINÁMICA: Lee el CSV de 200 organismos
try:
    df = pd.read_csv("data/processed/monitor_completo.csv")
except:
    st.error("⚠️ Error: Ejecutá 'python src/motor_analitico.py' primero.")
    st.stop()

# Filtro por Áreas en el Sidebar
st.sidebar.header("Filtros de Auditoría")
areas_disponibles = df['Area'].unique().tolist()
areas_sel = st.sidebar.multiselect("Seleccionar Áreas", areas_disponibles, default=areas_disponibles)

# Filtrar y mostrar los 200
df_filtrado = df[df['Area'].isin(areas_sel)]

col1, col2, col3 = st.columns(3)
col1.metric("Total Organismos", len(df_filtrado))
col2.metric("Riesgo Promedio", f"{df_filtrado['IRI (Score)'].mean():.1f}")
col3.metric("Alertas Rojas", len(df_filtrado[df_filtrado['Estado'] == '🔴 ALTO']))

st.subheader("Nivel de Riesgo por Organismo (Top 50)")
st.bar_chart(df_filtrado.set_index('Organismo')['IRI (Score)'].head(50))

st.subheader("📋 Detalle Completo de Alertas")
st.dataframe(df_filtrado, use_container_width=True)
