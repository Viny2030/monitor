import pandas as pd
import numpy as np

def clasificar_area(nombre):
    if any(x in nombre for x in ['Ministerio', 'Secretaría']): return "Administración Central"
    if any(x in nombre for x in ['Vialidad', 'Trenes', 'AySA', 'Obras']): return "Infraestructura"
    if any(x in nombre for x in ['Universidad', 'CONICET', 'CNEA']): return "Educación y Ciencia"
    if any(x in nombre for x in ['SIGEN', 'AGN', 'Anticorrupción', 'Justicia']): return "Control y Justicia"
    return "Empresas y Otros"

def generar_datos_masivos():
    organismos = [f"Organismo {i}" for i in range(1, 201)]
    organismos[0:5] = ["Ministerio de Economía", "Vialidad Nacional", "CONICET", "SIGEN", "Aerolíneas Argentinas"]
    
    data = []
    for org in organismos:
        area = clasificar_area(org)
        r_fin, r_con, r_ope, r_dat = np.random.randint(0, 100, 4)
        # IRI = (Financiero*0.35) + (Contratación*0.30) + (Operativo*0.20) + (Datos*0.15)
        iri = (r_fin * 0.35) + (r_con * 0.30) + (r_ope * 0.20) + (r_dat * 0.15)
        estado = '🔴 ALTO' if iri > 60 else ('🟡 MEDIO' if iri > 30 else '🟢 BAJO')
        data.append([org, area, r_fin, r_con, iri, estado])
    
    df = pd.DataFrame(data, columns=['Organismo', 'Area', 'Riesgo Financiero', 'Riesgo Contratación', 'IRI (Score)', 'Estado'])
    df.to_csv("data/processed/monitor_completo.csv", index=False)
    print("✅ Dataset de 200 organismos generado en data/processed/monitor_completo.csv")

if __name__ == "__main__":
    generar_datos_masivos()
