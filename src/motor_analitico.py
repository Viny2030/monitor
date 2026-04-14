import pandas as pd

def calcular_iri(r_fin, r_con, r_ope, r_dat):
    # IRI = (Financiero * 0.35) + (Contratación * 0.30) + (Operativo * 0.20) + (Datos * 0.15) [cite: 272]
    return (r_fin * 0.35 + r_con * 0.30 + r_ope * 0.20 + r_dat * 0.15)

if __name__ == "__main__":
    print("🧠 Ejecutando lógica de Scoring IRI...")
    score = calcular_iri(50, 40, 30, 20)
    print(f"✅ Score de prueba generado: {score}")
