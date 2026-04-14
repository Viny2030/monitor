import streamlit as st
import pandas as pd
import requests

st.set_page_config(page_title="Monitor IRI Nacional", layout="wide")
st.title("🚦 Monitor de Riesgo Institucional (vía API)")

# 1. Configuración de la URL de la API
API_URL = "http://127.0.0.1:8000/datos"

@st.cache_data # Para no saturar la API en cada click
def cargar_datos_desde_api():
    try:
        response = requests.get(API_URL)
        if response.status_code == 200:
            data = response.json()
            return pd.DataFrame(data)
        else:
            st.error(f"Error en la API: {response.status_code}")
            return None
    except Exception as e:
        st.error(f"No se pudo conectar con la API: {e}")
        return None

# Intentar obtener los datos
df_raw = cargar_datos_desde_api()

if df_raw is not None:
    # --- PROCESAMIENTO (Igual que antes pero sobre datos de API) ---
    df = df_raw.groupby(['Organismo', 'Area'], as_index=False).agg({
        'Riesgo Financiero': 'mean',
        'Riesgo Contratación': 'mean',
        'IRI (Score)': 'mean'
    })
    
    # ... (el resto de tu lógica de filtros y métricas se mantiene igual) ...
    st.success("✅ Datos sincronizados en tiempo real con la API")
    
    # Filtros y visualización
    areas_sel = st.sidebar.multiselect("Áreas", df['Area'].unique(), default=df['Area'].unique())
    df_filtrado = df[df['Area'].isin(areas_sel)]
    
    st.metric("Riesgo Promedio", f"{df_filtrado['IRI (Score)'].mean():.2f}")
    st.bar_chart(df_filtrado.set_index('Organismo')['IRI (Score)'], horizontal=True)
else:
    st.warning("⚠️ Asegurante de que la API esté corriendo en la otra terminal (uvicorn main:app)")
