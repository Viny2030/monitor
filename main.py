from fastapi import FastAPI, HTTPException
import pandas as pd
import os

app = FastAPI(title="Monitor IRI API")

@app.get("/")
def read_root():
    return {"status": "OK", "proyecto": "Monitor IRI"}

@app.get("/datos")
def get_datos():
    ruta_csv = "data/processed/monitor_completo.csv"
    if not os.path.exists(ruta_csv):
        raise HTTPException(status_code=404, detail="Archivo CSV no encontrado. Ejecute el motor analítico.")
    df = pd.read_csv(ruta_csv)
    return df.to_dict(orient="records")
