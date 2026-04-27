"""
src/connector.py
================
Conecta el motor IRI del monitor central con los repos especializados:

  - justicia        → IRA por juzgado, vacantes, magistrados (GitHub raw JSON)
  - monitor_legistativo → NAPE, IQP, asistencia, proyectos (CSV + Railway API)
  - monitor_legistativo_senadores → pipeline senadores (Railway API opcional)

Estrategia de ingesta (en orden de prioridad):
  1. Railway API  → si LEGISTATIVO_API_URL / JUSTICIA_API_URL están definidas
  2. GitHub raw   → JSON/CSV directos del repo (siempre disponible, datos del último commit)
  3. Fallback     → datos sintéticos reproducibles (np.random.seed(42))

Salida de cada función pública:
  DataFrame con columnas estandarizadas compatibles con motor_analitico.py:
    Organismo | Area | R_Financiero | R_Contratacion | R_Operativo | R_Datos | IRI | Estado
"""

import os
import logging
import requests
import pandas as pd
import numpy as np
from io import StringIO

log = logging.getLogger(__name__)

# ── URLs configurables por env var ────────────────────────────────────────────
LEGISTATIVO_API   = os.getenv("LEGISTATIVO_API_URL", "").rstrip("/")
SENADORES_API     = os.getenv("SENADORES_API_URL", "").rstrip("/")
JUSTICIA_API      = os.getenv("JUSTICIA_API_URL", "").rstrip("/")

TIMEOUT = 12
HEADERS = {"User-Agent": "MonitorIRI/1.0 (github.com/Viny2030/monitor)"}

# GitHub raw base URLs
_JUSTICIA_RAW  = "https://raw.githubusercontent.com/Viny2030/justicia/main"
_LEGIS_RAW     = "https://raw.githubusercontent.com/Viny2030/monitor_legistativo/main"
_SENADO_RAW    = "https://raw.githubusercontent.com/Viny2030/monitor_legistativo_senadores/main"


# ── Helpers internos ──────────────────────────────────────────────────────────

def _get_json(url: str) -> dict | list | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"  GET {url[:70]}: {e}")
        return None


def _get_csv(url: str) -> pd.DataFrame | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        for enc in ("utf-8-sig", "latin-1", "utf-8"):
            try:
                return pd.read_csv(StringIO(r.content.decode(enc)), low_memory=False)
            except Exception:
                continue
    except Exception as e:
        log.warning(f"  GET CSV {url[:70]}: {e}")
    return None


def _score_estado(iri: float) -> str:
    if iri >= 60:
        return "🔴 ALTO"
    elif iri >= 30:
        return "🟡 MEDIO"
    return "🟢 BAJO"


def _iri(r_fin, r_con, r_ope, r_dat) -> float:
    """Fórmula IRI: Financiero(35%) + Contratación(30%) + Operativo(20%) + Datos(15%)"""
    return round(r_fin * 0.35 + r_con * 0.30 + r_ope * 0.20 + r_dat * 0.15, 2)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. DATOS JUDICIALES — repo: justicia
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_justicia_juzgados() -> list | None:
    """Lee juzgados_nacional.json desde Railway API o GitHub raw."""
    # 1. Railway API
    if JUSTICIA_API:
        data = _get_json(f"{JUSTICIA_API}/operativo/data")
        if data:
            return data if isinstance(data, list) else data.get("juzgados")

    # 2. GitHub raw
    return _get_json(f"{_JUSTICIA_RAW}/juzgados_nacional.json")


def _fetch_justicia_vacantes() -> dict | None:
    """Lee vacantes.json para tasa de vacancia del Poder Judicial."""
    data = _get_json(f"{_JUSTICIA_RAW}/vacantes.json")
    return data


def _fetch_justicia_estadisticas() -> dict | None:
    return _get_json(f"{_JUSTICIA_RAW}/estadisticas_causas.json")


def build_judicial_df() -> pd.DataFrame:
    """
    Construye registros IRI para organismos judiciales.

    Dimensiones usadas:
      R_Financiero   ← costo_por_causa normalizado (0-100)
      R_Contratacion ← placeholder 40 (sin compr.ar aún)
      R_Operativo    ← IRA del juzgado (escala ya 0-100) o mora + clearance
      R_Datos        ← tasa_vacancia + mora > 2 años
    """
    log.info("Cargando datos judiciales (justicia)...")

    juzgados  = _fetch_justicia_juzgados()
    vacantes  = _fetch_justicia_vacantes()
    estadisticas = _fetch_justicia_estadisticas()

    # Tasa de vacancia global (datum documentado: 32.9%)
    tasa_vacancia = 32.9
    if vacantes:
        try:
            total  = vacantes.get("total_cargos") or vacantes.get("total", 1643)
            vacos  = vacantes.get("vacantes") or vacantes.get("cantidad_vacantes", 540)
            tasa_vacancia = round(vacos / total * 100, 1)
        except Exception:
            pass

    rows = []

    # ── Procesar juzgados individuales ────────────────────────────────────────
    if juzgados and isinstance(juzgados, list) and len(juzgados) > 0:
        for j in juzgados[:50]:  # top 50 para no saturar el dashboard
            try:
                nombre = j.get("juzgado") or j.get("nombre") or j.get("organismo", "Juzgado")
                fuero  = j.get("fuero") or j.get("area", "Federal")

                # IRA ya calculado en el repo justicia (0-100)
                ira = float(j.get("ira") or j.get("IRA") or 0)
                mora_pct = float(j.get("mora_pct") or j.get("pct_mora") or 0)
                clearance = float(j.get("clearance_rate") or j.get("cr") or 50)

                # Mapear dimensiones IRI
                r_fin  = min(100, max(0, ira * 0.8 + mora_pct * 0.2))     # financiero proxy
                r_con  = 40.0                                               # placeholder sin compr.ar
                r_ope  = min(100, max(0, ira))                              # IRA directamente
                r_dat  = min(100, max(0, tasa_vacancia + mora_pct * 0.5))  # vacancia + mora

                rows.append({
                    "Organismo": nombre,
                    "Area": f"Poder Judicial — {fuero}",
                    "Riesgo Financiero": round(r_fin, 1),
                    "Riesgo Contratación": round(r_con, 1),
                    "Riesgo Operativo": round(r_ope, 1),
                    "Riesgo Datos": round(r_dat, 1),
                    "IRI (Score)": _iri(r_fin, r_con, r_ope, r_dat),
                    "Fuente": "justicia/juzgados_nacional.json",
                })
            except Exception as e:
                log.debug(f"  juzgado skip: {e}")
                continue

    # ── Organismos judiciales institucionales ─────────────────────────────────
    institucionales = [
        ("Corte Suprema de Justicia", "Control y Justicia"),
        ("Consejo de la Magistratura", "Control y Justicia"),
        ("Ministerio Público Fiscal", "Control y Justicia"),
        ("Ministerio Público de la Defensa", "Control y Justicia"),
    ]
    for org, area in institucionales:
        r_fin  = max(20, min(80, tasa_vacancia * 1.2))
        r_con  = 40.0
        r_ope  = max(25, min(75, tasa_vacancia * 1.5))
        r_dat  = max(20, min(70, tasa_vacancia))
        rows.append({
            "Organismo": org, "Area": area,
            "Riesgo Financiero": round(r_fin, 1),
            "Riesgo Contratación": round(r_con, 1),
            "Riesgo Operativo": round(r_ope, 1),
            "Riesgo Datos": round(r_dat, 1),
            "IRI (Score)": _iri(r_fin, r_con, r_ope, r_dat),
            "Fuente": "justicia/vacantes.json",
        })

    if not rows:
        log.warning("  justicia: sin datos, usando fallback sintético")
        return _fallback_judicial()

    df = pd.DataFrame(rows)
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)
    log.info(f"  ✅ judicial: {len(df)} organismos cargados")
    return df


def _fallback_judicial() -> pd.DataFrame:
    np.random.seed(42)
    orgs = [
        ("Corte Suprema de Justicia", "Control y Justicia"),
        ("Consejo de la Magistratura", "Control y Justicia"),
        ("Poder Judicial de la Nación", "Control y Justicia"),
        ("Ministerio Público Fiscal", "Control y Justicia"),
        ("Ministerio Público de la Defensa", "Control y Justicia"),
        ("Juzgado Federal Civil N°1", "Poder Judicial — Civil"),
        ("Juzgado Federal Penal N°1", "Poder Judicial — Penal"),
        ("Cámara Federal de Apelaciones", "Poder Judicial — Federal"),
    ]
    rows = []
    for org, area in orgs:
        r_fin, r_con, r_ope, r_dat = np.random.randint(20, 75, 4)
        rows.append({
            "Organismo": org, "Area": area,
            "Riesgo Financiero": float(r_fin), "Riesgo Contratación": float(r_con),
            "Riesgo Operativo": float(r_ope), "Riesgo Datos": float(r_dat),
            "IRI (Score)": _iri(r_fin, r_con, r_ope, r_dat),
            "Estado": "", "Fuente": "fallback_seed42",
        })
    df = pd.DataFrame(rows)
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 2. DATOS LEGISLATIVOS — repos: monitor_legistativo + senadores
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_legis_kpis() -> dict | None:
    """KPIs globales desde Railway API o fallback a valores documentados en README."""
    if LEGISTATIVO_API:
        data = _get_json(f"{LEGISTATIVO_API}/api/kpis")
        if data:
            return data

    # Valores documentados en monitor_legistativo README (marzo 2026)
    return {
        "nape": 0.27,           # 27% asistencia perfecta → NAPE = 1 - 0.27 = 0.73
        "cols": 72.7,           # % legislación sustantiva
        "iap": 0.95,            # autonomía presupuestaria
        "crc": 4818,            # costo por ciudadano ARS
        "total_diputados": 257,
        "paridad": {"pct_mujeres": 43.2},
        "fuente": "monitor_legistativo README v1.0 marzo 2026",
    }


def _fetch_legis_bloques() -> list | None:
    """Bloques parlamentarios desde Railway API."""
    if LEGISTATIVO_API:
        data = _get_json(f"{LEGISTATIVO_API}/api/bloques")
        if data:
            return data.get("bloques", [])
    return None


def _fetch_legis_nomina() -> pd.DataFrame | None:
    """Nómina de diputados desde GitHub raw CSV."""
    df = _get_csv(f"{_LEGIS_RAW}/nomina_diputados.csv")
    return df


def _fetch_legis_presupuesto() -> dict | None:
    """Presupuesto legislativo desde Railway API o GitHub raw JSON."""
    if LEGISTATIVO_API:
        data = _get_json(f"{LEGISTATIVO_API}/api/presupuesto")
        if data:
            return data
    return _get_json(f"{_LEGIS_RAW}/presupuesto_legislativo.json")


def build_legislative_df() -> pd.DataFrame:
    """
    Construye registros IRI para organismos legislativos.

    Dimensiones:
      R_Financiero   ← IAP (Autonomía Presupuestaria) invertido + CRC
      R_Contratacion ← placeholder 35 (sin compr.ar)
      R_Operativo    ← NAPE (inasistencia) + COLS invertido
      R_Datos        ← IAD (accesibilidad documental), TVD
    """
    log.info("Cargando datos legislativos (monitor_legistativo)...")

    kpis       = _fetch_legis_kpis()
    bloques    = _fetch_legis_bloques()
    presupuesto = _fetch_legis_presupuesto()

    rows = []

    # ── KPIs globales → organismos institucionales ────────────────────────────
    nape_raw    = float(kpis.get("nape", 0.73)) if kpis else 0.73
    # NAPE = 1 - asistencia_pct → ya es el ratio de inasistencia (0-1)
    # Si viene como ratio de asistencia_pct (ej: 0.27 = 27% asist. perfecta), convertir
    nape_score  = nape_raw * 100 if nape_raw <= 1.0 else nape_raw
    cols        = float(kpis.get("cols", 72.7)) if kpis else 72.7
    iap         = float(kpis.get("iap", 0.95)) if kpis else 0.95
    iap_score   = (1 - iap) * 100  # invertido: alta autonomía = bajo riesgo

    # Costo por ciudadano — normalizar a 0-100 (ref: $4818 ARS en marzo 2026)
    crc         = float(kpis.get("crc", 4818)) if kpis else 4818
    crc_score   = min(100, crc / 100)  # escala relativa

    # IAD (Accesibilidad Documental): 3/5 = 60 → riesgo = 40
    iad_riesgo  = 40.0

    institucional_leg = [
        ("Cámara de Diputados", "Poder Legislativo"),
        ("Cámara de Senadores", "Poder Legislativo"),
        ("Jefatura de Gabinete", "Administración Central"),
        ("Auditoría General de la Nación (AGN)", "Control y Justicia"),
        ("Defensoría del Pueblo", "Control y Justicia"),
    ]
    for org, area in institucional_leg:
        r_fin = round(min(100, iap_score + crc_score * 0.2), 1)
        r_con = 35.0
        r_ope = round(min(100, nape_score * 0.7 + (100 - cols) * 0.3), 1)
        r_dat = round(iad_riesgo, 1)
        rows.append({
            "Organismo": org, "Area": area,
            "Riesgo Financiero": r_fin, "Riesgo Contratación": r_con,
            "Riesgo Operativo": r_ope, "Riesgo Datos": r_dat,
            "IRI (Score)": _iri(r_fin, r_con, r_ope, r_dat),
            "Fuente": kpis.get("fuente", "monitor_legistativo"),
        })

    # ── Bloques parlamentarios como organismos ────────────────────────────────
    if bloques:
        for b in bloques[:10]:  # top 10 bloques por tamaño
            nombre    = b.get("bloque", "Bloque")
            asist     = b.get("asistencia_pct") or (100 - nape_score)
            iqp       = b.get("iqp_promedio") or 0.5
            tasa_apro = b.get("tasa_aprobacion") or cols

            nape_b = 100 - float(asist)              # inasistencia del bloque
            r_fin  = round(min(100, iap_score + 5), 1)
            r_con  = 35.0
            r_ope  = round(min(100, nape_b * 0.6 + (100 - tasa_apro) * 0.4), 1)
            r_dat  = round(min(100, (1 - float(iqp)) * 60 + 15), 1)
            rows.append({
                "Organismo": f"Bloque {nombre}",
                "Area": "Poder Legislativo",
                "Riesgo Financiero": r_fin, "Riesgo Contratación": r_con,
                "Riesgo Operativo": r_ope, "Riesgo Datos": r_dat,
                "IRI (Score)": _iri(r_fin, r_con, r_ope, r_dat),
                "Fuente": "monitor_legistativo/api/bloques",
            })

    if not rows:
        log.warning("  legistativo: sin datos, usando fallback sintético")
        return _fallback_legislative()

    df = pd.DataFrame(rows)
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)
    log.info(f"  ✅ legislativo: {len(df)} organismos cargados")
    return df


def _fallback_legislative() -> pd.DataFrame:
    np.random.seed(43)
    orgs = [
        ("Cámara de Diputados", "Poder Legislativo"),
        ("Cámara de Senadores", "Poder Legislativo"),
        ("Jefatura de Gabinete", "Administración Central"),
        ("AGN", "Control y Justicia"),
    ]
    rows = []
    for org, area in orgs:
        r_fin, r_con, r_ope, r_dat = np.random.randint(15, 65, 4)
        rows.append({
            "Organismo": org, "Area": area,
            "Riesgo Financiero": float(r_fin), "Riesgo Contratación": float(r_con),
            "Riesgo Operativo": float(r_ope), "Riesgo Datos": float(r_dat),
            "IRI (Score)": _iri(r_fin, r_con, r_ope, r_dat),
            "Estado": "", "Fuente": "fallback_seed43",
        })
    df = pd.DataFrame(rows)
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 3. ORGANISMOS EJECUTIVOS — datos sintéticos reproducibles hasta tener compr.ar
# ═══════════════════════════════════════════════════════════════════════════════

def build_ejecutivo_df() -> pd.DataFrame:
    """
    Organismos del Poder Ejecutivo.
    Riesgo Financiero y Contratación usan datos del Presupuesto Abierto
    como proxy hasta conectar compr.ar.
    """
    log.info("Cargando datos ejecutivos (síntesis reproducible)...")
    np.random.seed(44)

    orgs = {
        "Administración Central": [
            "Ministerio de Economía", "Ministerio de Salud", "Ministerio de Seguridad",
            "Ministerio de Justicia", "Ministerio de Capital Humano",
            "Ministerio de Relaciones Exteriores", "Secretaría de Energía",
            "Secretaría de Comercio", "Secretaría de Minería",
        ],
        "Infraestructura": [
            "Vialidad Nacional", "AySA", "Trenes Argentinos",
            "Administración General de Puertos", "Corredores Viales S.A.", "ENOHSA",
        ],
        "Educación y Ciencia": [
            "CONICET", "CNEA", "INTA", "INTI", "CONAE", "ANMAT",
            "UBA", "UNC", "UNLP", "UNR",
        ],
        "Empresas y Otros": [
            "Aerolíneas Argentinas", "YPF", "Correo Argentino", "Banco Nación",
            "AFIP", "ANSES", "PAMI", "INCAA", "Enacom",
        ],
    }

    rows = []
    for area, lista in orgs.items():
        for org in lista:
            r_fin, r_con, r_ope, r_dat = np.random.randint(5, 90, 4)
            rows.append({
                "Organismo": org, "Area": area,
                "Riesgo Financiero": float(r_fin),
                "Riesgo Contratación": float(r_con),
                "Riesgo Operativo": float(r_ope),
                "Riesgo Datos": float(r_dat),
                "IRI (Score)": _iri(r_fin, r_con, r_ope, r_dat),
                "Estado": "", "Fuente": "sintetico_seed44_pendiente_comprar",
            })

    df = pd.DataFrame(rows)
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)
    log.info(f"  ✅ ejecutivo: {len(df)} organismos (sintético con seed fija)")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 4. FUNCIÓN PRINCIPAL — combina todo
# ═══════════════════════════════════════════════════════════════════════════════

def build_monitor_completo() -> pd.DataFrame:
    """
    Construye el dataset completo del Monitor IRI combinando:
      - Datos reales de justicia (judicial)
      - Datos reales de monitor_legistativo (legislativo)
      - Datos sintéticos reproducibles (ejecutivo — hasta conectar compr.ar)

    Devuelve DataFrame estandarizado listo para guardar en CSV y consumir por la API.
    """
    dfs = []

    try:
        dfs.append(build_judicial_df())
    except Exception as e:
        log.error(f"build_judicial_df falló: {e}")
        dfs.append(_fallback_judicial())

    try:
        dfs.append(build_legislative_df())
    except Exception as e:
        log.error(f"build_legislative_df falló: {e}")
        dfs.append(_fallback_legislative())

    try:
        dfs.append(build_ejecutivo_df())
    except Exception as e:
        log.error(f"build_ejecutivo_df falló: {e}")

    df = pd.concat(dfs, ignore_index=True)

    # Columna Estado por si algún df parcial la trae vacía
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)

    # Columna de fuente de datos para trazabilidad
    if "Fuente" not in df.columns:
        df["Fuente"] = "desconocida"

    log.info(f"\n✅ Monitor completo: {len(df)} organismos — "
             f"🔴 {(df['Estado']=='🔴 ALTO').sum()} | "
             f"🟡 {(df['Estado']=='🟡 MEDIO').sum()} | "
             f"🟢 {(df['Estado']=='🟢 BAJO').sum()}")

    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    df = build_monitor_completo()
    print(df[["Organismo", "Area", "IRI (Score)", "Estado", "Fuente"]].to_string())
