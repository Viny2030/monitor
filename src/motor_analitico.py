"""
src/motor_analitico.py — DEPRECADO

Esta era una versión antigua del motor (datos sintéticos sin seed, sin las
columnas Riesgo Operativo / Riesgo Datos / Fuente que requieren main.py y el
dashboard). Se reemplaza por un wrapper que delega al motor real en la raíz
del proyecto, para evitar que una ejecución accidental de este script genere
un CSV incompatible con la API.

Uso correcto: python motor_analitico.py (en la raíz del repo).
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from motor_analitico import generar_datos_reales  # noqa: E402

if __name__ == "__main__":
    generar_datos_reales()
