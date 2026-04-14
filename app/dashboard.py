import streamlit as st
import pandas as pd

st.set_page_config(page_title="Monitor de Riesgo Institucional", page_icon="🚦")

st.title("🚦 Semáforo de Riesgo Institucional (IRI)")
st.markdown("---")

# Simulación de datos basada en el modelo de riesgo
# IRI = (Financiero * 0.35) + (Contratación * 0.30) + (Operativo * 0.20) + (Datos * 0.15)
data = {
    'Organismo': ['Ministerio de Economía', 'Obras Públicas', 'Salud', 'Educación'],
    'Riesgo Financiero': [20, 80, 45, 10],
    'Riesgo Contratación': [15, 90, 50, 15],
    'IRI (Score)': [18.5, 85.0, 47.5, 12.5],
    'Estado': ['🟢 BAJO', '🔴 ALTO', '🟡 MEDIO', '🟢 BAJO']
}
df = pd.DataFrame(data)

# Sidebar para filtros
st.sidebar.header("Filtros de Auditoría")
st.sidebar.selectbox("Seleccionar Organismo", df['Organismo'].unique())

# Visualización Principal
st.subheader("Visualización de Riesgo por Organismo")
st.bar_chart(df.set_index('Organismo')['IRI (Score)'])

st.subheader("Detalle de Alertas")
st.table(df)

st.info("Nota: Los niveles de riesgo se calculan cruzando datos del Boletín Oficial y Compr.ar.")
