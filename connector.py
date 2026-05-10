"""
connector.py
================
Conecta el motor IRI del monitor central con los repos especializados:

  - justicia                         → IRA por juzgado, vacantes, magistrados
  - monitor_legistativo              → NAPE, IQP, asistencia, proyectos (Diputados)
  - monitor_legistativo_senadores    → participation_pct, reporte por partido
  - monitor_contratos_v2             → BORA + COMPR.AR + TGN Argentina (Ejecutivo AR)
  - gob_bo_comprar_tgn               → COMPR.AR + TGN Argentina — Tesorería General de la Nación

Estrategia de ingesta (en orden de prioridad):
  1. Railway API  → si la env var correspondiente está definida
  2. GitHub raw   → JSON/CSV directos del repo (siempre disponible)
  3. Fallback     → datos sintéticos reproducibles (np.random.seed fija)

Variables de entorno:
  LEGISTATIVO_API_URL
  SENADORES_API_URL
  JUSTICIA_API_URL
  CONTRATOS_AR_API_URL   ← monitor_contratos_v2 en Railway
  TGN_AR_API_URL         ← gob_bo_comprar_tgn en Railway (Tesorería General de la Nación AR)
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
SENADORES_API     = os.getenv("SENADORES_API_URL",   "").rstrip("/")
JUSTICIA_API      = os.getenv("JUSTICIA_API_URL",    "").rstrip("/")
CONTRATOS_AR_API  = os.getenv("CONTRATOS_AR_API_URL","").rstrip("/")
TGN_AR_API        = os.getenv("TGN_AR_API_URL",      "").rstrip("/")

TIMEOUT = 12
HEADERS = {"User-Agent": "MonitorIRI/1.0 (github.com/Viny2030/monitor)"}

# GitHub raw base URLs
_JUSTICIA_RAW  = "https://raw.githubusercontent.com/Viny2030/justicia/main"
_LEGIS_RAW     = "https://raw.githubusercontent.com/Viny2030/monitor_legistativo/main"
_SENADO_RAW    = "https://raw.githubusercontent.com/Viny2030/monitor_legistativo_senadores/main"

# Auto-descubrimiento lazy: se ejecuta solo cuando se necesita, no al importar
import datetime as _dt

def _find_latest_senado_csv(base_url: str, prefix: str, days_back: int = 60) -> str:
    """Prueba fechas desde hoy hacia atrás hasta encontrar un CSV disponible."""
    today = _dt.date.today()
    for delta in range(days_back):
        fecha = (today - _dt.timedelta(days=delta)).strftime("%Y-%m-%d")
        url = f"{base_url}/data/{prefix}{fecha}.csv"
        try:
            r = requests.head(url, timeout=6, headers=HEADERS)
            if r.status_code == 200:
                log.info(f"  CSV encontrado: {prefix}{fecha}.csv")
                return url
        except Exception:
            continue
    log.warning(f"  No se encontró CSV para {prefix} — usando fallback 2026-05-03")
    return f"{base_url}/data/{prefix}2026-05-03.csv"

# Las URLs se resuelven en runtime, no en import-time
_SENADO_CSV_NOMINA  = None  # se inicializa en build_senado_df()
_SENADO_CSV_PARTIDO = None  # se inicializa en build_senado_partido_df()


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


def _col_find(df: pd.DataFrame, keywords: list) -> str | None:
    """Busca la primera columna cuyos nombre contenga alguna de las keywords."""
    for kw in keywords:
        for c in df.columns:
            if kw in c.lower():
                return c
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# 1. DATOS JUDICIALES — repo: justicia
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_justicia_juzgados() -> list | None:
    if JUSTICIA_API:
        data = _get_json(f"{JUSTICIA_API}/operativo/data")
        if data:
            return data if isinstance(data, list) else data.get("juzgados")
    return _get_json(f"{_JUSTICIA_RAW}/juzgados_nacional.json")


def _fetch_justicia_vacantes() -> dict | None:
    return _get_json(f"{_JUSTICIA_RAW}/vacantes.json")


def build_judicial_df() -> pd.DataFrame:
    log.info("Cargando datos judiciales (justicia)...")

    juzgados = _fetch_justicia_juzgados()
    vacantes = _fetch_justicia_vacantes()

    tasa_vacancia = 32.9
    if vacantes:
        try:
            total = vacantes.get("total_cargos") or vacantes.get("total", 1643)
            vacos = vacantes.get("vacantes") or vacantes.get("cantidad_vacantes", 540)
            tasa_vacancia = round(vacos / total * 100, 1)
        except Exception:
            pass

    rows = []

    if juzgados and isinstance(juzgados, list):
        for j in juzgados[:50]:
            try:
                nombre    = j.get("juzgado") or j.get("nombre") or j.get("organismo", "Juzgado")
                fuero     = j.get("fuero") or j.get("area", "Federal")
                ira       = float(j.get("ira") or j.get("IRA") or 0)
                mora_pct  = float(j.get("mora_pct") or j.get("pct_mora") or 0)

                r_fin = min(100, max(0, ira * 0.8 + mora_pct * 0.2))
                # r_con varía por fuero: penal tiene menos discrecionalidad
                # en compras que civil o federal
                fuero_lower = str(fuero).lower()
                if "penal" in fuero_lower:
                    r_con = 35.0
                elif "civil" in fuero_lower:
                    r_con = 38.0
                elif "federal" in fuero_lower:
                    r_con = 42.0
                else:
                    r_con = 40.0
                r_ope = min(100, max(0, ira))
                r_dat = min(100, max(0, tasa_vacancia + mora_pct * 0.5))

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

    institucionales = [
        ("Corte Suprema de Justicia",          "Control y Justicia"),
        ("Consejo de la Magistratura",          "Control y Justicia"),
        ("Ministerio Público Fiscal",           "Control y Justicia"),
        ("Ministerio Público de la Defensa",    "Control y Justicia"),
    ]
    for org, area in institucionales:
        r_fin = max(20, min(80, tasa_vacancia * 1.2))
        r_con = 40.0
        r_ope = max(25, min(75, tasa_vacancia * 1.5))
        r_dat = max(20, min(70, tasa_vacancia))
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
        ("Corte Suprema de Justicia",        "Control y Justicia"),
        ("Consejo de la Magistratura",        "Control y Justicia"),
        ("Poder Judicial de la Nación",       "Control y Justicia"),
        ("Ministerio Público Fiscal",         "Control y Justicia"),
        ("Ministerio Público de la Defensa",  "Control y Justicia"),
        ("Juzgado Federal Civil N°1",         "Poder Judicial — Civil"),
        ("Juzgado Federal Penal N°1",         "Poder Judicial — Penal"),
        ("Cámara Federal de Apelaciones",     "Poder Judicial — Federal"),
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
# 2. DATOS LEGISLATIVOS — repo: monitor_legistativo (Diputados)
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_legis_kpis() -> dict | None:
    if LEGISTATIVO_API:
        data = _get_json(f"{LEGISTATIVO_API}/api/kpis")
        if data:
            return data
    return {
        "nape": 0.27, "cols": 72.7, "iap": 0.95, "crc": 4818,
        "total_diputados": 257,
        "fuente": "monitor_legistativo README v1.0 marzo 2026",
    }


def _fetch_legis_bloques() -> list | None:
    if LEGISTATIVO_API:
        data = _get_json(f"{LEGISTATIVO_API}/api/bloques")
        if data:
            return data.get("bloques", [])
    return None


def build_legislative_df() -> pd.DataFrame:
    log.info("Cargando datos legislativos (monitor_legistativo — Diputados)...")

    kpis    = _fetch_legis_kpis()
    bloques = _fetch_legis_bloques()

    rows = []

    nape_raw  = float(kpis.get("nape", 0.73)) if kpis else 0.73
    nape_score = nape_raw * 100 if nape_raw <= 1.0 else nape_raw
    cols      = float(kpis.get("cols", 72.7)) if kpis else 72.7
    iap       = float(kpis.get("iap", 0.95))  if kpis else 0.95
    iap_score = (1 - iap) * 100
    crc       = float(kpis.get("crc", 4818))  if kpis else 4818
    crc_score = min(100, crc / 100)
    iad_riesgo = 40.0

    institucional_leg = [
        ("Cámara de Diputados",                "Poder Legislativo"),
        ("Jefatura de Gabinete",               "Administración Central"),
        ("Auditoría General de la Nación (AGN)","Control y Justicia"),
        ("Defensoría del Pueblo",              "Control y Justicia"),
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
            "Fuente": kpis.get("fuente", "monitor_legistativo") if kpis else "monitor_legistativo",
        })

    if bloques:
        for b in bloques[:10]:
            nombre    = b.get("bloque", "Bloque")
            asist     = b.get("asistencia_pct") or (100 - nape_score)
            iqp       = b.get("iqp_promedio") or 0.5
            tasa_apro = b.get("tasa_aprobacion") or cols

            nape_b = 100 - float(asist)
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
    log.info(f"  ✅ legislativo (diputados): {len(df)} organismos cargados")
    return df


def _fallback_legislative() -> pd.DataFrame:
    np.random.seed(43)
    orgs = [
        ("Cámara de Diputados", "Poder Legislativo"),
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
# 3. DATOS SENADORES — repo: monitor_legistativo_senadores
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_senado_nomina() -> list | None:
    if SENADORES_API:
        data = _get_json(f"{SENADORES_API}/senado/senadores")
        if data and data.get("ok"):
            senadores = data.get("senadores", [])
            if senadores:
                return senadores

    url = _find_latest_senado_csv(_SENADO_RAW, "senadores_")
    df = _get_csv(url)
    if df is not None and not df.empty:
        return df.to_dict(orient="records")
    return None


def _fetch_senado_partidos() -> list | None:
    if SENADORES_API:
        data = _get_json(f"{SENADORES_API}/senado/reporte-partido")
        if data and data.get("ok"):
            partidos = data.get("partidos", [])
            if partidos:
                return partidos

    url = _find_latest_senado_csv(_SENADO_RAW, "reporte_partido_senado_")
    df = _get_csv(url)
    if df is not None and not df.empty:
        return df.to_dict(orient="records")
    return None


def build_senado_df() -> pd.DataFrame:
    log.info("Cargando datos senatoriales (monitor_legistativo_senadores)...")

    nomina   = _fetch_senado_nomina()
    partidos = _fetch_senado_partidos()

    rows = []
    participation_avg = None
    total_ausencias = total_votos = 0

    if nomina:
        partic_vals = []
        for s in nomina:
            p = s.get("participation_pct")
            try:
                if p is not None:
                    partic_vals.append(float(p))
            except (ValueError, TypeError):
                pass
            try:
                total_ausencias += int(s.get("ausencias") or 0)
                total_votos     += int(s.get("votos_total") or 0)
            except (ValueError, TypeError):
                pass
        if partic_vals:
            participation_avg = round(sum(partic_vals) / len(partic_vals), 1)

    if participation_avg is None:
        participation_avg = 72.0
        log.warning("  senado: sin participation_pct, usando default 72%")

    inasistencia_global = round(100 - participation_avg, 1)
    r_fin_cam = 35.0
    r_con_cam = 35.0
    r_ope_cam = round(min(100, inasistencia_global * 1.1), 1)
    r_dat_cam = 40.0

    rows.append({
        "Organismo": "Cámara de Senadores",
        "Area": "Poder Legislativo",
        "Riesgo Financiero": r_fin_cam,
        "Riesgo Contratación": r_con_cam,
        "Riesgo Operativo": r_ope_cam,
        "Riesgo Datos": r_dat_cam,
        "IRI (Score)": _iri(r_fin_cam, r_con_cam, r_ope_cam, r_dat_cam),
        "Fuente": "senadores/nomina_real — participation_pct promedio",
    })

    if partidos:
        def _bancas(p):
            for k in ("bancas", "total_bancas", "cantidad"):
                try:
                    return int(p.get(k, 0))
                except (ValueError, TypeError):
                    pass
            return 0

        for p in sorted(partidos, key=_bancas, reverse=True)[:8]:
            partido = (p.get("partido_normalizado") or p.get("partido")
                       or p.get("bloque") or "Bloque")
            bancas  = _bancas(p)
            part_p  = None
            for k in ("participation_pct", "participacion_prom", "asistencia_pct"):
                try:
                    v = p.get(k)
                    if v is not None:
                        part_p = float(v)
                        break
                except (ValueError, TypeError):
                    pass
            if part_p is None:
                part_p = participation_avg
            inasist_p = round(100 - part_p, 1)
            r_ope = round(min(100, inasist_p * 1.1), 1)
            # Si ninguna clave de participación existe en el dict del partido,
            # el dato viene del promedio global — lo indicamos en Fuente
            tiene_dato_propio = any(
                p.get(k) is not None
                for k in ("participation_pct", "participacion_prom", "asistencia_pct")
            )
            fuente_partido = (
                "senadores/reporte_partido_senado CSV"
                if tiene_dato_propio
                else "senadores/participation_avg_global"
            )
            rows.append({
                "Organismo": f"Senado — {partido} ({bancas} bancas)",
                "Area": "Poder Legislativo",
                "Riesgo Financiero": 35.0, "Riesgo Contratación": 35.0,
                "Riesgo Operativo": r_ope, "Riesgo Datos": 40.0,
                "IRI (Score)": _iri(35.0, 35.0, r_ope, 40.0),
                "Fuente": fuente_partido,
            })

    if not rows:
        log.warning("  senado: sin datos, usando fallback sintético")
        return _fallback_senado()

    df = pd.DataFrame(rows)
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)
    log.info(f"  ✅ senado: {len(df)} organismos (participation_avg={participation_avg}%)")
    return df


def _fallback_senado() -> pd.DataFrame:
    np.random.seed(45)
    orgs = [
        ("Cámara de Senadores",                        "Poder Legislativo"),
        ("Senado — Unión por la Patria (33 bancas)",   "Poder Legislativo"),
        ("Senado — La Libertad Avanza (7 bancas)",     "Poder Legislativo"),
        ("Senado — PRO (6 bancas)",                    "Poder Legislativo"),
    ]
    rows = []
    for org, area in orgs:
        r_fin, r_con, r_ope, r_dat = np.random.randint(15, 60, 4)
        rows.append({
            "Organismo": org, "Area": area,
            "Riesgo Financiero": float(r_fin), "Riesgo Contratación": float(r_con),
            "Riesgo Operativo": float(r_ope), "Riesgo Datos": float(r_dat),
            "IRI (Score)": _iri(r_fin, r_con, r_ope, r_dat),
            "Estado": "", "Fuente": "fallback_seed45",
        })
    df = pd.DataFrame(rows)
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 4. CONTRATOS ARGENTINA — repo: monitor_contratos_v2
#    Reemplaza los 34 organismos ejecutivos sintéticos con datos reales
#    BORA + COMPR.AR + TGN cruzados
# ═══════════════════════════════════════════════════════════════════════════════

def build_contratos_ar_df() -> pd.DataFrame:
    """
    Construye registros IRI para organismos del Ejecutivo argentino
    a partir de datos reales de monitor_contratos_v2.

    Dimensiones IRI:
      R_Financiero   ← indice_fenomeno_corruptivo promedio del flujo BORA→TGN
      R_Contratacion ← % contrataciones directas vs licitaciones (COMPR.AR)
      R_Operativo    ← placeholder 40 (sin datos operativos del ejecutivo)
      R_Datos        ← 25 (BORA + COMPR.AR = datos relativamente accesibles)
    """
    log.info("Cargando contratos Argentina (monitor_contratos_v2)...")

    if not CONTRATOS_AR_API:
        log.warning("  CONTRATOS_AR_API_URL no definida — usando fallback sintético")
        return build_ejecutivo_df()

    data = _get_json(f"{CONTRATOS_AR_API}/api/licitaciones/datos")

    if not data or data.get("sin_datos"):
        log.warning("  contratos AR: sin datos disponibles — usando fallback")
        return build_ejecutivo_df()

    rows = []

    # ── Métricas globales del flujo ───────────────────────────────────────────
    flujo = data.get("flujo", [])
    r_fin_global = 50.0
    r_con_global = 50.0

    if flujo:
        indices    = []
        alto_count = 0
        for f in flujo:
            try:
                idx = float(f.get("indice_fenomeno_corruptivo", 0))
                indices.append(idx)
            except (ValueError, TypeError):
                pass
            if str(f.get("nivel_riesgo_teorico", "")).lower() == "alto":
                alto_count += 1
        if indices:
            r_fin_global = round(min(100, sum(indices) / len(indices)), 1)
        if flujo:
            r_con_global = round(min(100, alto_count / len(flujo) * 100), 1)

    # ── Organismos desde COMPR.AR ─────────────────────────────────────────────
    comprar = data.get("comprar", [])
    if comprar:
        df_comp  = pd.DataFrame(comprar)
        col_org  = _col_find(df_comp, ["organismo", "unidad", "jurisdiccion", "entidad"])
        col_tipo = _col_find(df_comp, ["tipo", "proceso", "modalidad"])

        if col_org:
            for org, grp in list(df_comp.groupby(col_org))[:25]:
                org_str = str(org).strip()
                if not org_str or org_str in ("", "nan", "n/a", "N/A"):
                    continue
                total    = len(grp)
                directos = 0
                if col_tipo:
                    directos = grp[col_tipo].astype(str).str.upper().str.contains(
                        "DIRECT|CONTRAT", na=False).sum()
                pct_directos_c = (directos / total * 100) if total > 0 else r_con_global
                r_con = round(min(100, pct_directos_c * 1.5), 1)
                rows.append({
                    "Organismo": org_str[:80],
                    "Area": "Administración Central",
                    "Riesgo Financiero": r_fin_global,
                    "Riesgo Contratación": r_con,
                    "Riesgo Operativo": 40.0,
                    "Riesgo Datos": 25.0,
                    "IRI (Score)": _iri(r_fin_global, r_con, 40.0, 25.0),
                    "Fuente": "monitor_contratos_v2/comprar",
                })

    # ── Fallback a TGN si COMPR.AR no tiene organismos ───────────────────────
    if not rows:
        tgn = data.get("tgn", [])
        if tgn:
            df_tgn  = pd.DataFrame(tgn)
            col_jur = _col_find(df_tgn, ["jurisdiccion", "entidad", "organismo", "unidad"])
            if col_jur:
                for jur, _ in list(df_tgn.groupby(col_jur))[:20]:
                    jur_str = str(jur).strip()
                    if not jur_str or jur_str in ("", "nan", "n/a"):
                        continue
                    rows.append({
                        "Organismo": jur_str[:80],
                        "Area": "Administración Central",
                        "Riesgo Financiero": r_fin_global,
                        "Riesgo Contratación": r_con_global,
                        "Riesgo Operativo": 40.0,
                        "Riesgo Datos": 25.0,
                        "IRI (Score)": _iri(r_fin_global, r_con_global, 40.0, 25.0),
                        "Fuente": "monitor_contratos_v2/tgn",
                    })

    # ── Fila global si no hay desglose por organismo ──────────────────────────
    if not rows and flujo:
        totales = data.get("totales", {})
        rows.append({
            "Organismo": "Ejecutivo Nacional (agregado BORA+COMPR.AR+TGN)",
            "Area": "Administración Central",
            "Riesgo Financiero": r_fin_global,
            "Riesgo Contratación": r_con_global,
            "Riesgo Operativo": 40.0,
            "Riesgo Datos": 25.0,
            "IRI (Score)": _iri(r_fin_global, r_con_global, 40.0, 25.0),
            "Fuente": f"monitor_contratos_v2/flujo ({totales.get('flujo',0)} procesos)",
        })

    if not rows:
        log.warning("  contratos AR: sin organismos — usando fallback sintético")
        return build_ejecutivo_df()

    df = pd.DataFrame(rows)
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)
    log.info(f"  ✅ contratos AR: {len(df)} organismos (datos reales BORA+COMPR.AR+TGN)")
    return df


# ── Fallback ejecutivo argentino (seed 44, se mantiene como respaldo) ─────────
def build_ejecutivo_df() -> pd.DataFrame:
    """Datos sintéticos reproducibles — usado como fallback si monitor_contratos_v2 no responde."""
    log.info("  ⚠️  ejecutivo AR: usando datos sintéticos seed 44")
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
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 5. TESORERÍA GENERAL DE LA NACIÓN — repo: gob_bo_comprar_tgn
#    Ejecutivo argentino: ejecución presupuestaria TGN + COMPR.AR cruzados
# ═══════════════════════════════════════════════════════════════════════════════

def build_tgn_df() -> pd.DataFrame:
    """
    Construye registros IRI para organismos del Ejecutivo argentino
    a partir de datos reales de gob_bo_comprar_tgn (Tesorería General de la Nación).

    Dimensiones IRI:
      R_Financiero   ← indice_fenomeno_corruptivo promedio (flujo TGN→COMPR.AR)
      R_Contratacion ← % procesos directos vs licitaciones
      R_Operativo    ← placeholder 42
      R_Datos        ← 30 (TGN publica datos de ejecución presupuestaria)
    """
    log.info("Cargando datos TGN Argentina (gob_bo_comprar_tgn — Tesorería General de la Nación)...")

    if not TGN_AR_API:
        log.warning("  TGN_AR_API_URL no definida — usando fallback sintético")
        return _fallback_tgn()

    data = _get_json(f"{TGN_AR_API}/api/licitaciones/datos")

    if not data or data.get("sin_datos"):
        log.warning("  TGN AR: sin datos — usando fallback")
        return _fallback_tgn()

    rows = []

    # ── Métricas globales del flujo ───────────────────────────────────────────
    flujo = data.get("flujo", [])
    r_fin_global = 50.0
    r_con_global = 45.0

    if flujo:
        indices = []
        alto_count = 0
        for f in flujo:
            try:
                idx = float(f.get("indice_fenomeno_corruptivo", 0))
                indices.append(idx)
            except (ValueError, TypeError):
                pass
            if str(f.get("nivel_riesgo_teorico", "")).lower() == "alto":
                alto_count += 1
        if indices:
            r_fin_global = round(min(100, sum(indices) / len(indices)), 1)
        if flujo:
            r_con_global = round(min(100, alto_count / len(flujo) * 100), 1)

    # ── Organismos desde datos COMPR.AR / TGN ejecución ──────────────────────
    source_data = data.get("comprar", []) or data.get("tgn", [])
    if source_data:
        df_src  = pd.DataFrame(source_data)
        col_org = _col_find(df_src, ["organismo", "jurisdiccion", "entidad", "unidad", "ministerio"])
        col_tipo = _col_find(df_src, ["tipo", "modalidad", "proceso"])

        if col_org:
            for org, grp in list(df_src.groupby(col_org))[:20]:
                org_str = str(org).strip()
                if not org_str or org_str in ("", "nan", "n/a"):
                    continue
                total    = len(grp)
                directos = 0
                if col_tipo:
                    directos = grp[col_tipo].astype(str).str.upper().str.contains(
                        "DIRECT|CONTRAT|MENOR", na=False).sum()
                pct_directos_t = (directos / total * 100) if total > 0 else r_con_global
                r_con = round(min(100, pct_directos_t * 1.3), 1)
                rows.append({
                    "Organismo": org_str[:80],
                    "Area": "Tesorería General de la Nación",
                    "Riesgo Financiero": r_fin_global,
                    "Riesgo Contratación": r_con,
                    "Riesgo Operativo": 42.0,
                    "Riesgo Datos": 30.0,
                    "IRI (Score)": _iri(r_fin_global, r_con, 42.0, 30.0),
                    "Fuente": "gob_bo_comprar_tgn/real",
                })

    # ── Fila global si no hay desglose ────────────────────────────────────────
    if not rows and flujo:
        rows.append({
            "Organismo": "Ejecutivo Nacional — TGN (agregado ejecución presupuestaria)",
            "Area": "Tesorería General de la Nación",
            "Riesgo Financiero": r_fin_global,
            "Riesgo Contratación": r_con_global,
            "Riesgo Operativo": 42.0,
            "Riesgo Datos": 30.0,
            "IRI (Score)": _iri(r_fin_global, r_con_global, 42.0, 30.0),
            "Fuente": f"gob_bo_comprar_tgn/flujo ({len(flujo)} procesos)",
        })

    if not rows:
        log.warning("  TGN AR: sin organismos — usando fallback sintético")
        return _fallback_tgn()

    df = pd.DataFrame(rows)
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)
    log.info(f"  ✅ TGN AR: {len(df)} organismos cargados (datos reales ejecución presupuestaria)")
    return df


def _fallback_tgn() -> pd.DataFrame:
    np.random.seed(46)
    orgs = [
        "Ministerio de Economía",
        "Ministerio de Salud",
        "Ministerio de Capital Humano",
        "Ministerio de Seguridad",
        "Ministerio de Infraestructura",
        "Ministerio de Relaciones Exteriores",
        "Secretaría de Hacienda",
        "Secretaría de Finanzas",
        "Vialidad Nacional",
        "ANSES",
    ]
    rows = []
    for org in orgs:
        r_fin, r_con, r_ope, r_dat = np.random.randint(20, 80, 4)
        rows.append({
            "Organismo": org,
            "Area": "Tesorería General de la Nación",
            "Riesgo Financiero": float(r_fin),
            "Riesgo Contratación": float(r_con),
            "Riesgo Operativo": float(r_ope),
            "Riesgo Datos": float(r_dat),
            "IRI (Score)": _iri(r_fin, r_con, r_ope, r_dat),
            "Estado": "", "Fuente": "fallback_seed46_tgn_ar",
        })
    df = pd.DataFrame(rows)
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 6. FUNCIÓN PRINCIPAL — combina todo
# ═══════════════════════════════════════════════════════════════════════════════

def build_monitor_completo() -> pd.DataFrame:
    """
    Construye el dataset completo del Monitor IRI combinando:
      - Datos reales de justicia            (Poder Judicial AR)
      - Datos reales de monitor_legistativo (Diputados AR)
      - Datos reales de senadores           (Senado AR)
      - Datos reales de monitor_contratos_v2 (Ejecutivo AR — BORA+COMPR.AR+TGN)
      - Datos reales de gob_bo_comprar_tgn  (Tesorería General de la Nación AR)

    Fallback sintético reproducible (seed fija) si algún repo no responde.
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
        dfs.append(build_senado_df())
    except Exception as e:
        log.error(f"build_senado_df falló: {e}")
        dfs.append(_fallback_senado())

    try:
        dfs.append(build_contratos_ar_df())
    except Exception as e:
        log.error(f"build_contratos_ar_df falló: {e}")
        dfs.append(build_ejecutivo_df())

    try:
        dfs.append(build_tgn_df())
    except Exception as e:
        log.error(f"build_tgn_df falló: {e}")
        dfs.append(_fallback_tgn())

    df = pd.concat(dfs, ignore_index=True)
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)

    if "Fuente" not in df.columns:
        df["Fuente"] = "desconocida"

    log.info(
        f"\n✅ Monitor completo: {len(df)} organismos — "
        f"🔴 {(df['Estado'] == '🔴 ALTO').sum()} | "
        f"🟡 {(df['Estado'] == '🟡 MEDIO').sum()} | "
        f"🟢 {(df['Estado'] == '🟢 BAJO').sum()}"
    )
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    df = build_monitor_completo()
    print(df[["Organismo", "Area", "IRI (Score)", "Estado", "Fuente"]].to_string())
