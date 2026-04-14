import streamlit as st
import pandas as pd

# Configuración de página
st.set_page_config(page_title="Monitor IRI Nacional", layout="wide")
st.title("🚦 Monitor de Riesgo Institucional - Argentina")

# 1. Cargar el dataset
try:
    df_raw = pd.read_csv("data/processed/monitor_completo.csv")
    
    # --- LIMPIEZA DE DUPLICADOS ---
    # Agrupamos por Organismo y Área para eliminar repetidos (como las sedes)
    # Tomamos el promedio (mean) de los scores de riesgo
    df = df_raw.groupby(['Organismo', 'Area'], as_index=False).agg({
        'Riesgo Financiero': 'mean',
        'Riesgo Contratación': 'mean',
        'IRI (Score)': 'mean'
    })

    # Recalculamos el Estado basado en el promedio obtenido
    def definir_estado(score):
        if score > 70: return '🔴 ALTO'
        if score > 40: return '🟡 MEDIO'
        return '🟢 BAJO'
    
    df['Estado'] = df['IRI (Score)'].apply(definir_estado)
    # ------------------------------

except Exception:
    st.error("⚠️ No se encontró el dataset. Ejecutá primero: 'python src/motor_analitico.py'")
    st.stop()

# 2. Sidebar: Filtros
st.sidebar.header("Filtros de Auditoría")
areas_sel = st.sidebar.multiselect(
    "Filtrar por Áreas Económicas", 
    options=sorted(df['Area'].unique()), 
    default=df['Area'].unique()
)

# Aplicar Filtro
df_filtrado = df[df['Area'].isin(areas_sel)].sort_values('IRI (Score)', ascending=False)

# 3. Métricas Principales
c1, c2, c3 = st.columns(3)
with c1:
    st.metric("Total Organismos", len(df_filtrado))
with c2:
    promedio = df_filtrado['IRI (Score)'].mean()
    st.metric("Riesgo Promedio", f"{promedio:.1f}")
with c3:
    alertas = len(df_filtrado[df_filtrado['Estado'] == '🔴 ALTO'])
    st.metric("Alertas Críticas", alertas, delta_color="inverse")

st.markdown("---")

# 4. Visualización Gráfica (Top 20 para mayor claridad)
st.subheader("📊 Ranking de Riesgo (Top 20 Organismos)")
# Usamos un gráfico horizontal para que los nombres se lean perfectamente
top_20 = df_filtrado.head(20)
st.bar_chart(data=top_20.set_index('Organismo')['IRI (Score)'], horizontal=True)

# 5. Tabla Detallada
st.subheader("📋 Listado Detallado de Organismos")
st.dataframe(
    df_filtrado[['Organismo', 'Area', 'IRI (Score)', 'Estado']], 
    use_container_width=True,
    hide_index=True
)

# Botón de descarga
csv = df_filtrado.to_csv(index=False).encode('utf-8')
st.download_button("📥 Descargar Reporte CSV", csv, "reporte_monitor_iri.csv", "text/csv")
