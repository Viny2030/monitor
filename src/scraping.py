import requests
import pandas as pd
from bs4 import BeautifulSoup

def scrap_boletin():
    url = "https://www.boletinoficial.gob.ar/seccion/primera"
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        return [{"fuente": "boletin", "texto": item.get_text(strip=True)} for item in soup.find_all("article")]
    except Exception as e:
        print(f"Error: {e}")
        return []

if __name__ == "__main__":
    print("📥 Extrayendo datos del Boletín Oficial...")
    data = scrap_boletin()
    print(f"✅ Proceso finalizado. Registros encontrados: {len(data)}")
