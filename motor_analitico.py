"""
src/motor_analitico.py
======================
Motor analítico del Monitor IRI.

Antes: generaba 200 organismos con np.random.randint() — datos sin sentido.
Ahora: consume datos REALES desde connector.py (justicia + monitor_legistativo)
       con fallback sintético reproducible (seed fija) si los repos no responden.

Uso:
    python src/motor_analitico.py
    → genera data/processed/monitor_completo.csv
"""

import os
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Importar connector (mismo directorio — raíz del proyecto)
from connector import build_monitor_completo

OUT_DIR = "data/processed"
OUT_FILE = os.path.join(OUT_DIR, "monitor_completo.csv")


def generar_datos_reales():
    """
    Genera el dataset completo del Monitor IRI usando datos reales
    de los repos justicia y monitor_legistativo.

    Retorna el DataFrame generado y lo guarda en data/processed/monitor_completo.csv
    """
    log.info("=" * 60)
    log.info("  MOTOR IRI — Iniciando generación de dataset")
    log.info("=" * 60)

    # ── Construir dataset desde repos reales ──────────────────────────────────
    df = build_monitor_completo()

    # ── Renombrar columnas para compatibilidad con la API (main.py) ───────────
    # La API y el dashboard esperan: Organismo, Area, Riesgo Financiero,
    # Riesgo Contratación, IRI (Score), Estado
    col_map = {
        "Riesgo Financiero":   "Riesgo Financiero",
        "Riesgo Contratación": "Riesgo Contratación",
        "Riesgo Operativo":    "Riesgo Operativo",
        "Riesgo Datos":        "Riesgo Datos",
    }
    # Asegurar que existen todas las columnas esperadas
    for col in ["Organismo", "Area", "IRI (Score)", "Estado", "Fuente"]:
        if col not in df.columns:
            df[col] = ""

    # ── Guardar CSV ───────────────────────────────────────────────────────────
    os.makedirs(OUT_DIR, exist_ok=True)
    df.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")

    # ── Resumen ───────────────────────────────────────────────────────────────
    log.info("")
    log.info(f"✅ Dataset guardado → {OUT_FILE}")
    log.info(f"   Total organismos : {len(df)}")
    log.info(f"   🔴 ALTO          : {(df['Estado'] == '🔴 ALTO').sum()}")
    log.info(f"   🟡 MEDIO         : {(df['Estado'] == '🟡 MEDIO').sum()}")
    log.info(f"   🟢 BAJO          : {(df['Estado'] == '🟢 BAJO').sum()}")
    log.info("")
    log.info("Fuentes de datos:")
    for fuente, n in df["Fuente"].value_counts().items():
        log.info(f"   {n:>4} organismos  ←  {fuente}")
    log.info("")
    log.info("Top 5 mayor riesgo IRI:")
    top5 = df.nlargest(5, "IRI (Score)")[["Organismo", "IRI (Score)", "Estado", "Area"]]
    for _, r in top5.iterrows():
        log.info(f"   {r['Estado']}  {r['IRI (Score)']:>6.2f}  {r['Organismo']}")
    log.info("=" * 60)

    return df


if __name__ == "__main__":
    generar_datos_reales()
