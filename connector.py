"""
connector.py
================
Conecta el motor IRI del monitor central con los repos especializados:
- justicia → IRA por juzgado, vacantes, magistrados
- monitor_legistativo → NAPE, IQP, asistencia, proyectos (Diputados)
- monitor_legistativo_senadores → participation_pct, reporte por partido
- monitor_contratos_v2 → BORA + COMPR.AR + TGN Argentina (Ejecutivo AR)
- gob_bo_comprar_tgn → COMPR.AR + TGN Argentina — Tesorería General de la Nación

Estrategia de ingesta (en orden de prioridad):
1. Railway API → si la env var correspondiente está definida
2. GitHub raw → JSON/CSV directos del repo (siempre disponible)
3. Si ninguna fuente real responde, esa sección se omite del dataset — no se
   generan datos sintéticos.

Variables de entorno:
  LEGISTATIVO_API_URL
  SENADORES_API_URL
  JUSTICIA_API_URL
  CONTRATOS_AR_API_URL  ← monitor_contratos_v2 en Railway
  TGN_AR_API_URL        ← gob_bo_comprar_tgn en Railway (Tesorería General de la Nación AR)
"""

import json
import os
import re
import logging
import requests
import pandas as pd
from io import StringIO

log = logging.getLogger(__name__)

LEGISTATIVO_API  = os.getenv("LEGISTATIVO_API_URL", "").rstrip("/")
SENADORES_API    = os.getenv("SENADORES_API_URL",   "").rstrip("/")
JUSTICIA_API     = os.getenv("JUSTICIA_API_URL",    "").rstrip("/")
CONTRATOS_AR_API = os.getenv("CONTRATOS_AR_API_URL","").rstrip("/")
TGN_AR_API       = os.getenv("TGN_AR_API_URL",      "").rstrip("/")

TIMEOUT = 12
HEADERS = {"User-Agent": "MonitorIRI/1.0 (github.com/Viny2030/monitor)"}

_JUSTICIA_RAW = "https://raw.githubusercontent.com/Viny2030/justicia/main"
_LEGIS_RAW    = "https://raw.githubusercontent.com/Viny2030/monitor_legistativo/main"
_SENADO_RAW   = "https://raw.githubusercontent.com/Viny2030/monitor_legistativo_senadores/main"

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

_SENADO_CSV_NOMINA   = None
_SENADO_CSV_PARTIDO  = None

def _get_json(url: str, timeout: int = TIMEOUT) -> dict | list | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
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
    if iri is None or iri != iri:
        return "⚪ SIN DATOS"
    if iri >= 60:
        return "🔴 ALTO"
    elif iri >= 30:
        return "🟡 MEDIO"
    return "🟢 BAJO"

_PESOS = (0.35, 0.30, 0.20, 0.15)  # Financiero, Contratación, Operativo, Datos


def _iri(r_fin, r_con, r_ope, r_dat) -> float:
    """Fórmula IRI: Financiero(35%) + Contratación(30%) + Operativo(20%) + Datos(15%).

    Un componente en None significa "sin dato" para ese organismo. Antes se
    rellenaba con constantes (40, 35, 25…) que pesaban hasta el 65% del índice
    sin medir nada; ahora el IRI se calcula con los componentes que sí tienen
    dato, reponderando sus pesos para que sumen 100%."""
    vals = [(v, w) for v, w in zip((r_fin, r_con, r_ope, r_dat), _PESOS)
            if v is not None and v == v]
    if not vals:
        return None
    return round(sum(v * w for v, w in vals) / sum(w for _, w in vals), 2)


def _arreglar_texto(t) -> str:
    """'FundaciÃ³n' → 'Fundación' (UTF-8 leído como Latin-1 en la fuente)."""
    t = str(t)
    if "Ã" in t or "Â" in t:
        try:
            return t.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
    return t

def _col_find(df: pd.DataFrame, keywords: list) -> str | None:
    """Busca la primera columna cuyo nombre contenga alguna de las keywords."""
    for kw in keywords:
        for c in df.columns:
            if kw in c.lower():
                return c
    return None

_MONITOR_COLUMNS = [
    "Organismo", "Area", "Riesgo Financiero", "Riesgo Contratación",
    "Riesgo Operativo", "Riesgo Datos", "IRI (Score)", "Estado", "Fuente",
]

def _empty_monitor_df() -> pd.DataFrame:
    """
    DataFrame vacío con el esquema del monitor.

    Se usa cuando una fuente real no responde: en vez de rellenar el hueco
    con datos sintéticos (np.random con seed fija), esos organismos
    simplemente no aparecen en el dataset. Menos datos, pero todos reales.
    """
    return pd.DataFrame(columns=_MONITOR_COLUMNS)

# ═══════════════════════════════════════════════════════════════════════════════
# 1. DATOS JUDICIALES — repo: justicia
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_justicia_magistrados() -> list | None:
    if JUSTICIA_API:
        data = _get_json(f"{JUSTICIA_API}/operativo/data")
        if data:
            return data if isinstance(data, list) else data.get("magistrados")
    return _get_json(f"{_JUSTICIA_RAW}/magistrados.json")


def _fetch_justicia_vacantes_lista() -> list | None:
    data = _get_json(f"{_JUSTICIA_RAW}/vacantes.json")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("vacantes", [])
    return None


def build_judicial_df() -> pd.DataFrame:
    log.info("Cargando datos judiciales (justicia)...")

    magistrados       = _fetch_justicia_magistrados()
    vacantes_lista    = _fetch_justicia_vacantes_lista()

    tasa_vacancia_global = 32.9
    # magistrados.json ya trae los cargos vacantes (campo "vacante"=True): la
    # tasa es vacantes / cargos. Antes se sumaban otra vez las vacantes al
    # total (383/1385 = 27,7% en lugar de 383/1002 = 38,2%).
    con_flag = bool(magistrados) and isinstance(magistrados, list) and "vacante" in magistrados[0]
    if con_flag:
        total_vac = sum(1 for m in magistrados if m.get("vacante") in (True, "True", "true", 1))
        tasa_vacancia_global = round(total_vac / len(magistrados) * 100, 1)
        log.info(f"  Tasa vacancia (campo vacante): {tasa_vacancia_global}% ({total_vac}/{len(magistrados)})")
    elif vacantes_lista and isinstance(vacantes_lista, list) and len(vacantes_lista) > 0:
        total_vacantes = len(vacantes_lista)
        if magistrados and isinstance(magistrados, list):
            total_cargos = len(magistrados)
            if total_cargos > 0:
                tasa_vacancia_global = round(
                    total_vacantes / (total_cargos + total_vacantes) * 100, 1
                )
        log.info(f"  Tasa vacancia calculada: {tasa_vacancia_global}% ({total_vacantes} vacantes)")

    vacantes_por_camara: dict[str, int] = {}
    if vacantes_lista:
        for v in vacantes_lista:
            camara = (v.get("camara") or v.get("organo_nombre") or "").strip()
            try:
                camara = camara.encode("latin-1").decode("utf-8")
            except Exception:
                pass
            if camara:
                vacantes_por_camara[camara] = vacantes_por_camara.get(camara, 0) + 1

    rows = []

    if magistrados and isinstance(magistrados, list):
        df_mag = pd.DataFrame(magistrados)

        camara_col = None
        for c in ["camara", "organo_nombre", "tribunal", "juzgado"]:
            if c in df_mag.columns:
                camara_col = c
                break

        if camara_col:
            for camara_raw, grp in df_mag.groupby(camara_col):
                camara = str(camara_raw).strip()
                if not camara or camara in ("", "nan"):
                    continue
                try:
                    camara_clean = camara.encode("latin-1").decode("utf-8")
                except Exception:
                    camara_clean = camara

                total_en_camara   = len(grp)
                if "vacante" in grp.columns:
                    vacantes_camara    = int(grp["vacante"].astype(str).str.lower().isin(["true", "1"]).sum())
                    total_con_vacantes = total_en_camara      # las vacantes ya están en grp
                else:
                    vacantes_camara    = vacantes_por_camara.get(camara_clean, 0)
                    total_con_vacantes = total_en_camara + vacantes_camara
                tasa_vac_camara   = round(
                    vacantes_camara / total_con_vacantes * 100, 1
                ) if total_con_vacantes > 0 else tasa_vacancia_global

                tipo_j = ""
                if "tipo_justicia" in grp.columns:
                    tipo_j = grp["tipo_justicia"].iloc[0] if len(grp) > 0 else ""
                    try:
                        tipo_j = str(tipo_j).encode("latin-1").decode("utf-8")
                    except Exception:
                        tipo_j = str(tipo_j)

                area = (
                    "Poder Judicial — Federal"
                    if "federal" in tipo_j.lower()
                    else "Poder Judicial — Nacional"
                )

                r_fin = round(min(100, max(10, tasa_vac_camara * 1.5)), 1)
                r_con = None  # sin datos de contrataciones por cámara (antes: 40 fijo)
                r_ope = round(min(100, max(10, tasa_vac_camara * 1.2)), 1)
                r_dat = round(min(100, max(15, tasa_vac_camara * 0.8 + 10)), 1)

                rows.append({
                    "Organismo":           camara_clean[:90],
                    "Area":                area,
                    "Riesgo Financiero":   r_fin,
                    "Riesgo Contratación": r_con,
                    "Riesgo Operativo":    r_ope,
                    "Riesgo Datos":        r_dat,
                    "IRI (Score)":         _iri(r_fin, r_con, r_ope, r_dat),
                    "Fuente":              "justicia/magistrados.json",
                })

            log.info(f"  Cámaras/tribunales procesados: {len(rows)}")

    institucionales = [
        ("Corte Suprema de Justicia",        "Control y Justicia"),
        ("Consejo de la Magistratura",        "Control y Justicia"),
        ("Ministerio Público Fiscal",         "Control y Justicia"),
        ("Ministerio Público de la Defensa",  "Control y Justicia"),
    ]
    for org, area in institucionales:
        r_fin = round(max(20, min(80, tasa_vacancia_global * 1.2)), 1)
        r_con = None  # sin datos de contrataciones (antes: 40 fijo)
        r_ope = round(max(25, min(75, tasa_vacancia_global * 1.5)), 1)
        r_dat = round(max(20, min(70, tasa_vacancia_global)), 1)
        rows.append({
            "Organismo":           org,
            "Area":                area,
            "Riesgo Financiero":   r_fin,
            "Riesgo Contratación": r_con,
            "Riesgo Operativo":    r_ope,
            "Riesgo Datos":        r_dat,
            "IRI (Score)":         _iri(r_fin, r_con, r_ope, r_dat),
            "Fuente":              "justicia/vacantes.json",
        })

    if not rows:
        log.warning("  justicia: sin datos reales disponibles — se omite del dataset (sin fallback sintético)")
        return _empty_monitor_df()

    df = pd.DataFrame(rows)
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)
    log.info(
        f"  ✅ judicial: {len(df)} organismos cargados "
        f"(vacancia global: {tasa_vacancia_global}%)"
    )
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 2. DATOS LEGISLATIVOS — repo: monitor_legistativo (Diputados)
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_legis_kpis() -> dict | None:
    if LEGISTATIVO_API:
        data = _get_json(f"{LEGISTATIVO_API}/api/kpis")
        if data and data.get("total_diputados"):
            nape_raw = float(data.get("nape", 0.0))
            nape_score = max(5.0, round(nape_raw * 100, 1))

            cols_raw = data.get("cols")
            cols_riesgo = round(100 - float(cols_raw), 1) if cols_raw is not None else None

            iap_raw = data.get("iap")
            iap_score = round((1 - float(iap_raw)) * 100, 1) if iap_raw is not None else None

            iqp = data.get("iqp_global")
            iqp_riesgo = round((1 - float(iqp)) * 60 + 15, 1) if iqp is not None else None

            return {
                "nape":            nape_score,
                "cols":            cols_riesgo,
                "iap":             iap_score,
                "iqp_global":      iqp_riesgo,
                "total_diputados": int(data.get("total_diputados", 257)),
                "fuente":          "monitor_legistativo/api/kpis (datos reales HCDN)",
                "_raw":            data,
            }
    # Sin API no se inventan valores: antes devolvía un "fallback histórico"
    # fijo (nape 27, cols 27.3, iap 5, iqp 40) que se mostraba como dato.
    return None

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
    rows    = []

    if not kpis:
        log.warning("  legistativo: API sin respuesta — se omite del dataset (sin valores inventados)")
        return _empty_monitor_df()
    nape_score  = kpis.get("nape")
    cols_riesgo = kpis.get("cols")
    iap_score   = kpis.get("iap")
    iqp_riesgo  = kpis.get("iqp_global")
    fuente_kpis = kpis.get("fuente", "monitor_legistativo")

    def _ope(nape, cols):
        """Operativo = 70% inasistencia (NAPE) + 30% no-aprobación (COLS); si
        falta uno, se usa el que hay."""
        partes = [(v, w) for v, w in ((nape, 0.7), (cols, 0.3)) if v is not None]
        return round(min(100, sum(v * w for v, w in partes) / sum(w for _, w in partes)), 1) if partes else None

    log.info(f"  KPIs legistativo: nape_score={nape_score} cols_riesgo={cols_riesgo} "
             f"iap_score={iap_score} iqp_riesgo={iqp_riesgo}")

    iad_riesgo = iqp_riesgo

    institucional_leg = [
        ("Cámara de Diputados",                    "Poder Legislativo"),
        ("Jefatura de Gabinete",                   "Administración Central"),
        ("Auditoría General de la Nación (AGN)",   "Control y Justicia"),
        ("Defensoría del Pueblo",                  "Control y Justicia"),
    ]
    for org, area in institucional_leg:
        r_fin = round(min(100, iap_score), 1) if iap_score is not None else None
        r_con = None  # sin datos de contrataciones (antes: 35 fijo)
        r_ope = _ope(nape_score, cols_riesgo)
        r_dat = round(iad_riesgo, 1) if iad_riesgo is not None else None
        rows.append({
            "Organismo": org, "Area": area,
            "Riesgo Financiero": r_fin, "Riesgo Contratación": r_con,
            "Riesgo Operativo":  r_ope, "Riesgo Datos":        r_dat,
            "IRI (Score)": _iri(r_fin, r_con, r_ope, r_dat),
            "Fuente": fuente_kpis,
        })

    if bloques:
        for b in bloques[:10]:
            nombre    = b.get("bloque", "Bloque")
            asist_pct = b.get("asistencia_pct")
            iqp_b     = b.get("iqp_promedio")
            tasa_apro = b.get("tasa_aprobacion")

            if asist_pct is not None:
                nape_b = round(max(0, 100 - float(asist_pct)), 1)
            else:
                nape_b = nape_score

            if tasa_apro is not None:
                col_b = round(max(0, 100 - float(tasa_apro)), 1)
            else:
                col_b = cols_riesgo

            if iqp_b is not None:
                dat_b = round(min(100, (1 - float(iqp_b)) * 60 + 15), 1)
            else:
                dat_b = iqp_riesgo

            r_fin = round(min(100, iap_score), 1) if iap_score is not None else None
            r_con = None  # sin datos de contrataciones (antes: 35 fijo)
            partes = [(v, w) for v, w in ((nape_b, 0.6), (col_b, 0.4)) if v is not None]
            r_ope = round(min(100, sum(v * w for v, w in partes) / sum(w for _, w in partes)), 1) if partes else None
            r_dat = round(dat_b, 1) if dat_b is not None else None
            rows.append({
                "Organismo": f"Bloque {nombre}",
                "Area": "Poder Legislativo",
                "Riesgo Financiero": r_fin, "Riesgo Contratación": r_con,
                "Riesgo Operativo":  r_ope, "Riesgo Datos":        r_dat,
                "IRI (Score)": _iri(r_fin, r_con, r_ope, r_dat),
                "Fuente": "monitor_legistativo/api/bloques",
            })

    if not rows:
        log.warning("  legistativo: sin datos reales disponibles — se omite del dataset (sin fallback sintético)")
        return _empty_monitor_df()

    df = pd.DataFrame(rows)
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)
    log.info(f"  ✅ legislativo (diputados): {len(df)} organismos cargados")
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
    df  = _get_csv(url)
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
    df  = _get_csv(url)
    if df is not None and not df.empty:
        return df.to_dict(orient="records")
    return None

def build_senado_df() -> pd.DataFrame:
    log.info("Cargando datos senatoriales (monitor_legistativo_senadores)...")
    nomina   = _fetch_senado_nomina()
    partidos = _fetch_senado_partidos()
    rows = []

    participation_avg  = None
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
        log.warning("  senado: sin participation_pct — Cámara sin dato (antes se usaba 72% fijo)")
        inasistencia_global = None
    else:
        inasistencia_global = round(100 - participation_avg, 1)
    # Sólo hay dato de participación: financiero, contratación y datos quedan
    # "sin dato" (antes 35, 35 y 40 fijos = 80% del índice).
    r_fin_cam = None
    r_con_cam = None
    r_ope_cam = round(min(100, inasistencia_global * 1.1), 1) if inasistencia_global is not None else None
    r_dat_cam = None
    rows.append({
        "Organismo": "Cámara de Senadores", "Area": "Poder Legislativo",
        "Riesgo Financiero": r_fin_cam, "Riesgo Contratación": r_con_cam,
        "Riesgo Operativo":  r_ope_cam, "Riesgo Datos":        r_dat_cam,
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
            partido = (
                p.get("partido_normalizado") or p.get("partido")
                or p.get("bloque") or "Bloque"
            )
            bancas = _bancas(p)
            part_p = None
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
            r_ope = round(min(100, (100 - part_p) * 1.1), 1) if part_p is not None else None
            rows.append({
                "Organismo": f"Senado — {partido} ({bancas} bancas)",
                "Area": "Poder Legislativo",
                "Riesgo Financiero": None, "Riesgo Contratación": None,
                "Riesgo Operativo":  r_ope, "Riesgo Datos":        None,
                "IRI (Score)": _iri(None, None, r_ope, None),
                "Fuente": "senadores/reporte_partido_senado CSV",
            })

    if not rows:
        log.warning("  senado: sin datos reales disponibles — se omite del dataset (sin fallback sintético)")
        return _empty_monitor_df()

    df = pd.DataFrame(rows)
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)
    log.info(f"  ✅ senado: {len(df)} organismos (participation_avg={participation_avg}%)")
    return df

# ═══════════════════════════════════════════════════════════════════════════════
# 4. CONTRATOS ARGENTINA — repo: monitor_contratos_v2
# ═══════════════════════════════════════════════════════════════════════════════


def _organismos_compras(registros: list, patron_directa: str, area: str, fuente: str,
                        n: int = 25) -> list[dict]:
    """Un renglón por organismo comprador con el % de procesos que son
    contratación directa (u otros procedimientos sin competencia abierta).

    Cambios respecto de la versión anterior:
      * se toman los n organismos con MÁS procesos (antes: los n primeros por
        número de código, es decir, al azar);
      * el % de directas se usa tal cual (antes se multiplicaba por 1,5 y
        quedaban 11 de 25 organismos clavados en 100);
      * se arreglan los nombres con caracteres rotos y se unifican los
        duplicados por código SAF;
      * financiero, operativo y datos quedan "sin dato": eran constantes
        (40/25 o 42/30) y el índice de la fuente vale 1 en todos los casos.
    """
    df = pd.DataFrame(registros)
    col_org  = _col_find(df, ["organismo", "jurisdiccion", "entidad", "unidad", "ministerio"])
    col_tipo = _col_find(df, ["tipo", "modalidad", "proceso"])
    if not col_org or not col_tipo:
        return []
    df = df[df[col_org].notna()].copy()
    df["_org"] = df[col_org].map(_arreglar_texto).str.strip()
    df = df[~df["_org"].isin(["", "nan", "n/a", "N/A"])]
    # Unificar por código SAF ("202 - Instituto …"): mismo código = mismo organismo
    df["_clave"] = df["_org"].str.extract(r"^(\d+)\s*-", expand=False).fillna(df["_org"])
    nombres = df.groupby("_clave")["_org"].agg(lambda x: x.value_counts().index[0])
    df["_directa"] = df[col_tipo].map(_arreglar_texto).str.upper().str.contains(patron_directa, na=False)
    anios = pd.to_numeric(df.get("Ejercicio"), errors="coerce").dropna() if "Ejercicio" in df else pd.Series(dtype=float)
    periodo = (f" {int(anios.min())}" + (f"-{int(anios.max())}" if anios.max() != anios.min() else "")) if len(anios) else ""
    rows = []
    for clave, grp in sorted(df.groupby("_clave"), key=lambda kv: len(kv[1]), reverse=True)[:n]:
        r_con = round(grp["_directa"].mean() * 100, 1)
        rows.append({
            "Organismo": nombres[clave][:80], "Area": area,
            "Riesgo Financiero": None, "Riesgo Contratación": r_con,
            "Riesgo Operativo": None, "Riesgo Datos": None,
            "IRI (Score)": _iri(None, r_con, None, None),
            "Fuente": f"{fuente} (COMPR.AR{periodo}, {len(grp)} procesos)",
        })
    return rows

COMPRAR_HIST_URL = os.getenv(
    "COMPRAR_HIST_URL",
    "https://infra.datos.gob.ar/catalog/jgm/dataset/4/distribution/4.22/download/Adjudicaciones.csv",
)
# Ejercicios que se toman del archivo (los N más recientes) y mínimo de
# procedimientos para calcular el % de directas: con pocos procedimientos el
# porcentaje no es confiable (1 de 2 = 50 %).
COMPRAR_EJERCICIOS = int(os.getenv("COMPRAR_EJERCICIOS", "2"))
COMPRAR_MIN_PROC = int(os.getenv("COMPRAR_MIN_PROC", "20"))


def _norm_org(t: str) -> str:
    import unicodedata
    t = re.sub(r"^\s*\d+\s*-\s*", "", _arreglar_texto(t))           # "375 - Gendarmeria" -> "Gendarmeria"
    t = "".join(c for c in unicodedata.normalize("NFD", t.upper()) if unicodedata.category(c) != "Mn")
    t = re.sub(r"[^A-Z ]", " ", t)
    stop = {"NACIONAL", "ARGENTINA", "ARGENTINO", "DE", "DEL", "LA", "EL", "LOS", "LAS", "Y", "GENERAL"}
    return " ".join(w for w in t.split() if len(w) > 2 and w not in stop)


# Lo que el BORA agrega después del nombre del organismo en la sección tercera
# ("SERVICIO PENITENCIARIO FEDERAL Solicitudes de Provisión").
_SUFIJOS_BORA = re.compile(
    r"\s+(Solicitud(es)?\s+de\s+Provisi|Listado\s+[ÓO]rdenes|Contrataciones\s+(Directas|Varias)|"
    r"Licitaciones\s+P[úu]blicas).*$", re.IGNORECASE)

# Organismos que en COMPR.AR figuran con el nombre de su servicio administrativo
# (código SAF) y no con el nombre que usa el BORA.
_ALIAS_SAF = {"POLICIA FEDERAL": "326", "SERVICIO PENITENCIARIO FEDERAL": "331"}


def _actividad_bora_2026(base_url: str) -> dict:
    """{nombre normalizado: {"nombre", "avisos", "licitaciones", "directas"}} con
    las adjudicaciones 2026 publicadas en el BORA que acumula monitor_contratos
    (/api/contratos)."""
    filas, offset = [], 0
    while base_url:
        # la primera consulta puede tardar: monitor_contratos lee todos los reportes
        data = _get_json(f"{base_url}/api/contratos?limit=1000&offset={offset}", timeout=90)
        if not data or not isinstance(data, dict):
            break
        lote = data.get("data", [])
        filas.extend(lote)
        offset += len(lote)
        if not lote or offset >= int(data.get("total", 0) or 0):
            break
    if not filas:
        return {}
    df = pd.DataFrame(filas)
    if "organismo_contratante" not in df or "tipo_proceso_bora" not in df:
        return {}
    if "cancelada" in df:
        df = df[~df["cancelada"].astype(str).str.lower().isin(["true", "1"])]
    if "link_bora" in df:
        con_link = df["link_bora"].astype(str).str.strip() != ""
        df = pd.concat([df[con_link].drop_duplicates("link_bora"), df[~con_link]])
    df["_nombre"] = (df["organismo_contratante"].astype(str).str.split(" - ").str[0]
                     .str.replace(_SUFIJOS_BORA, "", regex=True).str.strip())
    df = df[~df["_nombre"].str.upper().isin(["", "NAN", "NOTA ACLARATORIA"])]
    df["_org"] = df["_nombre"].map(_norm_org)
    tipo = df["tipo_proceso_bora"].astype(str).str.lower()
    df["_dir"] = tipo.str.contains("directa")
    df["_lic"] = tipo.str.contains("licitaci|concurso")
    return {o: {"nombre": g["_nombre"].value_counts().index[0], "avisos": len(g),
                "licitaciones": int(g["_lic"].sum()), "directas": int(g["_dir"].sum())}
            for o, g in df.groupby("_org") if o}


_GENERICAS = {"ESTADO", "MAYOR", "MINISTERIO", "SECRETARIA", "COMANDO", "SERVICIO", "REPUBLICA"}


def _match_bora(nombre: str, actividad: dict):
    n = set(_norm_org(nombre).split())
    if not n:
        return None
    mejor, puntaje = None, 0
    for org, val in actividad.items():
        o = set(org.split())
        comunes = len(n & o)
        # todas las palabras del nombre más corto tienen que estar en el otro, y lo
        # que sobra en el más largo solo pueden ser palabras genéricas
        # ("ESTADO MAYOR GENERAL DEL EJERCITO" = "EJERCITO ARGENTINO", pero
        # "SUPERINTENDENCIA DE BIENESTAR DE LA POLICIA FEDERAL" != "POLICIA FEDERAL")
        sobra = (n | o) - (n & o)
        if comunes and comunes == min(len(n), len(o)) and sobra <= _GENERICAS and comunes > puntaje:
            mejor, puntaje = val, comunes
    return mejor


def _leer_comprar_hist() -> pd.DataFrame | None:
    """Adjudicaciones COMPR.AR (datos.gob.ar), una fila por procedimiento y
    proveedor. Acepta el archivo 2016-2026 (columnas con guion bajo) y los
    anuales anteriores (columnas con espacios)."""
    try:
        r = requests.get(COMPRAR_HIST_URL, headers=HEADERS, timeout=300)
        r.raise_for_status()
        df = pd.read_csv(StringIO(r.content.decode("utf-8-sig", errors="replace")),
                         low_memory=False, dtype=str)
    except Exception as e:
        log.warning(f"  COMPR.AR: no se pudo descargar ({e})")
        return None
    df.columns = [c.strip().replace(" ", "_") for c in df.columns]
    df = df.rename(columns={"Número_Procedimiento": "Numero_Proceso"})
    req = {"Numero_Proceso", "Descripcion_SAF", "Tipo_de_Procedimiento", "Ejercicio"}
    if not req.issubset(df.columns):
        log.warning(f"  COMPR.AR: columnas inesperadas {list(df.columns)[:8]}")
        return None
    df["Ejercicio"] = pd.to_numeric(df["Ejercicio"], errors="coerce")
    return df


def _concentracion_proveedor(filas: pd.DataFrame) -> float | None:
    """% del monto adjudicado en pesos que se lleva el principal proveedor."""
    if "CUIT" not in filas or "Monto" not in filas:
        return None
    f = filas
    if "Moneda" in f:
        f = f[f["Moneda"].astype(str).str.contains("peso", case=False, na=False)]
    monto = pd.to_numeric(f["Monto"], errors="coerce")
    por_prov = monto.groupby(f["CUIT"]).sum()
    total = por_prov.sum()
    return round(por_prov.max() / total * 100, 1) if total > 0 else None


GRAFO_PATH = os.getenv("GRAFO_PATH", "data/processed/grafo_comprar.json")
GRAFO_TOP = 10  # proveedores por organismo que se guardan; la API muestra hasta este número


def _grafo_comprar(df: pd.DataFrame, nombres: pd.Series, safs: list, periodo: str) -> dict | None:
    """
    Red organismo ↔ proveedor de COMPR.AR para la pestaña Grafo.

    `df` son las adjudicaciones de los ejercicios usados para el riesgo (una fila
    por procedimiento y proveedor, con _saf). Se toman solo los organismos con
    % de directas calculado (`safs`) y los montos en pesos. Por organismo se
    guardan sus GRAFO_TOP principales proveedores por monto; de cada proveedor
    se cuenta además con cuántos de esos organismos trabaja en total (no solo
    entre los principales). Todo es informativo: nada de esto entra en el IRI.
    """
    if "CUIT" not in df or "Monto" not in df or not safs:
        return None
    d = df[df["_saf"].isin(safs)].copy()
    if "Moneda" in d:
        d = d[d["Moneda"].astype(str).str.contains("peso", case=False, na=False)]
    d["CUIT"] = d["CUIT"].astype(str).str.strip()
    d = d[d["CUIT"].str.replace(r"\D", "", regex=True).str.len() == 11]
    if d.empty:
        return None
    d["_monto"] = pd.to_numeric(d["Monto"], errors="coerce").fillna(0.0)
    d["_directa"] = (d["Tipo_de_Procedimiento"].map(_arreglar_texto)
                     .str.upper().str.contains("DIRECT", na=False))
    col_nom = "Descripcion_Proveedor" if "Descripcion_Proveedor" in d else None

    pares = d.groupby(["_saf", "CUIT"]).agg(
        monto=("_monto", "sum"), procesos=("Numero_Proceso", "nunique")).reset_index()
    directas = (d[d["_directa"]].groupby(["_saf", "CUIT"])["Numero_Proceso"].nunique()
                .rename("directas"))
    pares = pares.join(directas, on=["_saf", "CUIT"]).fillna({"directas": 0})
    org_por_prov = pares.groupby("CUIT")["_saf"].nunique()
    pares = pares.sort_values(["_saf", "monto"], ascending=[True, False])
    pares["rango"] = pares.groupby("_saf").cumcount() + 1
    top = pares[pares["rango"] <= GRAFO_TOP]

    nodos = []
    for saf, g in pares.groupby("_saf"):
        total = g["monto"].sum()
        nodos.append({
            "id": f"o:{saf}", "tipo": "organismo", "label": str(nombres.get(saf, saf))[:80],
            "monto": float(total), "proveedores": int(len(g)),
            "procesos": int(d.loc[d["_saf"] == saf, "Numero_Proceso"].nunique()),
            "concentracion": round(float(g["monto"].max() / total) * 100, 1) if total > 0 else None,
        })
    for cuit, g in top.groupby("CUIT"):
        nombre = cuit
        if col_nom:
            ns = d.loc[d["CUIT"] == cuit, col_nom].dropna().astype(str).str.strip()
            if len(ns):
                nombre = ns.value_counts().index[0]
        nodos.append({
            "id": f"p:{cuit}", "tipo": "proveedor", "label": _arreglar_texto(nombre)[:60], "cuit": cuit,
            "organismos_total": int(org_por_prov[cuit]),
        })
    cols = ["_saf", "CUIT", "rango", "monto", "procesos", "directas"]
    aristas = [{"from": f"o:{saf}", "to": f"p:{cuit}", "rango": int(rango), "monto": float(monto),
                "procesos": int(procesos), "directas": int(directas)}
               for saf, cuit, rango, monto, procesos, directas in top[cols].itertuples(index=False, name=None)]
    return {
        "formato": "grafo/1", "fuente": "COMPR.AR Adjudicaciones (datos.gob.ar 4.22)",
        "periodo": periodo, "generado": _dt.datetime.now().isoformat(timespec="seconds"),
        "top_guardado": GRAFO_TOP, "nodos": nodos, "aristas": aristas,
    }


def _guardar_grafo(grafo: dict | None) -> None:
    if not grafo:
        return
    try:
        os.makedirs(os.path.dirname(GRAFO_PATH) or ".", exist_ok=True)
        with open(GRAFO_PATH, "w", encoding="utf-8") as f:
            json.dump(grafo, f, ensure_ascii=False)
        log.info(f"  grafo COMPR.AR: {sum(n['tipo'] == 'organismo' for n in grafo['nodos'])} organismos, "
                 f"{len(grafo['aristas'])} vínculos → {GRAFO_PATH}")
    except Exception as e:
        log.warning(f"  grafo COMPR.AR: no se pudo guardar ({e})")


def _organismos_comprar(actividad: dict, area: str = "Administración Central") -> list[dict]:
    """
    Riesgo de contratación por organismo, en dos capas:

    1. Organismos que compran por COMPR.AR: % de contrataciones directas sobre el
       total de sus procedimientos adjudicados en los últimos ejercicios del
       archivo de datos.gob.ar (numerador y denominador de la misma fuente
       completa). Solo con COMPRAR_MIN_PROC procedimientos o más. En la fuente
       se informa también la concentración en el principal proveedor y la
       actividad 2026 publicada en el BORA.
    2. Organismos que publican licitaciones en el BORA pero no aparecen en
       COMPR.AR: compran fuera del sistema y sus directas no se ven en ningún
       lado. Se listan "sin dato de directas", sin inventar un porcentaje (un
       0 % sería engañoso), con la cantidad de licitaciones del BORA.

    El BORA no sirve para el % de directas: las licitaciones se publican casi
    todas, pero la mayoría de las directas solo figura en COMPR.AR. Tampoco
    para la concentración: dos de cada tres avisos no traen el adjudicatario.
    """
    df = _leer_comprar_hist()
    if df is None or df.empty:
        return []
    ejercicios = sorted(df["Ejercicio"].dropna().unique())[-COMPRAR_EJERCICIOS:]
    df = df[df["Ejercicio"].isin(ejercicios)].copy()
    df["_org"] = df["Descripcion_SAF"].map(_arreglar_texto).str.strip()
    df["_saf"] = df["_org"].str.extract(r"^(\d+)\s*-", expand=False).fillna(df["_org"])
    nombres = df.groupby("_saf")["_org"].agg(lambda x: x.value_counts().index[0])
    procs = df.drop_duplicates("Numero_Proceso").copy()
    procs["_directa"] = (procs["Tipo_de_Procedimiento"].map(_arreglar_texto)
                         .str.upper().str.contains("DIRECT", na=False))
    periodo = f"{int(ejercicios[0])}" + (f"-{int(ejercicios[-1])}" if len(ejercicios) > 1 else "")

    # Organismos del BORA que tienen SAF en COMPR.AR (con cualquier cantidad de procedimientos)
    saf_por_nombre = {_norm_org(nombres[k]): k for k in nombres.index}
    bora_saf, en_comprar = {}, set()
    for org, act in actividad.items():
        k = _ALIAS_SAF.get(org) or _match_bora(act["nombre"], saf_por_nombre)
        if k in nombres.index:
            bora_saf.setdefault(k, []).append(act)
            en_comprar.add(org)

    rows = []
    for saf, grp in procs.groupby("_saf"):
        if len(grp) < COMPRAR_MIN_PROC:
            continue
        r_con = round(grp["_directa"].mean() * 100, 1)
        conc = _concentracion_proveedor(df[df["_saf"] == saf])
        fuente = (f"monitor_contratos · COMPR.AR {periodo}: {len(grp)} procedimientos, "
                  f"{int(grp['_directa'].sum())} directas")
        if conc is not None:
            fuente += f" · principal proveedor: {conc} % del monto"
        acts = bora_saf.get(saf, [])
        fuente += (f" · BORA 2026: {sum(a['avisos'] for a in acts)} adjudicaciones "
                   f"({sum(a['directas'] for a in acts)} directas)" if acts
                   else " · BORA 2026: sin adjudicaciones publicadas")
        rows.append({
            "Organismo": nombres[saf][:80], "Area": area,
            "Riesgo Financiero": None, "Riesgo Contratación": r_con,
            "Riesgo Operativo": None, "Riesgo Datos": None,
            "IRI (Score)": _iri(None, r_con, None, None),
            "Fuente": fuente,
        })

    try:
        safs_con_dato = [saf for saf, grp in procs.groupby("_saf") if len(grp) >= COMPRAR_MIN_PROC]
        _guardar_grafo(_grafo_comprar(df, nombres, safs_con_dato, periodo))
    except Exception as e:  # el grafo es accesorio: nunca debe tumbar el riesgo
        log.warning(f"  grafo COMPR.AR: no se pudo calcular ({e})")

    # Control cruzado: licitan en el BORA y no figuran en COMPR.AR
    for org, act in sorted(actividad.items(), key=lambda kv: -kv[1]["licitaciones"]):
        if org in en_comprar or act["licitaciones"] == 0:
            continue
        rows.append({
            "Organismo": act["nombre"][:80], "Area": area,
            "Riesgo Financiero": None, "Riesgo Contratación": None,
            "Riesgo Operativo": None, "Riesgo Datos": None,
            "IRI (Score)": None,
            "Fuente": (f"monitor_contratos · BORA 2026: {act['licitaciones']} licitaciones · "
                       f"sin dato de directas: compra fuera de COMPR.AR"),
        })
    return rows


def build_contratos_ar_df() -> pd.DataFrame:
    log.info("Cargando contratos Argentina (monitor_contratos)...")

    if not CONTRATOS_AR_API:
        log.warning("  CONTRATOS_AR_API_URL no definida — se omite del dataset (sin fallback sintético)")
        return _empty_monitor_df()

    rows_hist = _organismos_comprar(_actividad_bora_2026(CONTRATOS_AR_API))
    if rows_hist:
        df = pd.DataFrame(rows_hist)
        df["Estado"] = df["IRI (Score)"].apply(_score_estado)
        con_dato = int(df["Riesgo Contratación"].notna().sum())
        log.info(f"  ✅ contratos AR: {con_dato} organismos con % de directas (COMPR.AR) "
                 f"+ {len(df) - con_dato} que compran fuera de COMPR.AR (BORA 2026)")
        return df
    log.warning("  contratos AR: COMPR.AR no disponible — se usa /api/licitaciones/datos")

    data = _get_json(f"{CONTRATOS_AR_API}/api/licitaciones/datos")
    if not data or data.get("sin_datos"):
        log.warning("  contratos AR: sin datos disponibles — se omite del dataset (sin fallback sintético)")
        return _empty_monitor_df()

    rows = []
    flujo        = data.get("flujo", [])
    r_fin_global = 50.0
    r_con_global = 50.0

    if flujo:
        indices   = []
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

    comprar = data.get("comprar", [])
    if comprar:
        rows = _organismos_compras(comprar, r"DIRECT", "Administración Central",
                                   "monitor_contratos_v2/comprar")

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
                        "Organismo": _arreglar_texto(jur_str)[:80], "Area": "Administración Central",
                        "Riesgo Financiero": None, "Riesgo Contratación": r_con_global,
                        "Riesgo Operativo":  None, "Riesgo Datos":        None,
                        "IRI (Score)": _iri(None, r_con_global, None, None),
                        "Fuente": "monitor_contratos_v2/tgn",
                    })

    if not rows and flujo:
        totales = data.get("totales", {})
        rows.append({
            "Organismo": "Ejecutivo Nacional (agregado BORA+COMPR.AR+TGN)",
            "Area": "Administración Central",
            "Riesgo Financiero": None, "Riesgo Contratación": r_con_global,
            "Riesgo Operativo":  None, "Riesgo Datos":        None,
            "IRI (Score)": _iri(None, r_con_global, None, None),
            "Fuente": f"monitor_contratos_v2/flujo ({totales.get('flujo', 0)} procesos)",
        })

    if not rows:
        log.warning("  contratos AR: sin organismos — se omite del dataset (sin fallback sintético)")
        return _empty_monitor_df()

    df = pd.DataFrame(rows)
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)
    log.info(f"  ✅ contratos AR: {len(df)} organismos (datos reales BORA+COMPR.AR+TGN)")
    return df

# ═══════════════════════════════════════════════════════════════════════════════
# 5. TESORERÍA GENERAL DE LA NACIÓN — repo: gob_bo_comprar_tgn
# ═══════════════════════════════════════════════════════════════════════════════

def build_tgn_df() -> pd.DataFrame:
    log.info("Cargando datos TGN Argentina (gob_bo_comprar_tgn — Tesorería General de la Nación)...")

    if not TGN_AR_API:
        log.warning("  TGN_AR_API_URL no definida — se omite del dataset (sin fallback sintético)")
        return _empty_monitor_df()

    data = _get_json(f"{TGN_AR_API}/api/licitaciones/datos")
    if not data or data.get("sin_datos"):
        log.warning("  TGN AR: sin datos — se omite del dataset (sin fallback sintético)")
        return _empty_monitor_df()

    rows = []
    flujo        = data.get("flujo", [])
    r_fin_global = 50.0
    r_con_global = 45.0

    if flujo:
        indices   = []
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

    source_data = data.get("comprar", []) or data.get("tgn", [])
    if source_data:
        rows = _organismos_compras(source_data, r"DIRECT|MENOR", "Tesorería General de la Nación",
                                   "gob_bo_comprar_tgn/real", n=20)

    if not rows and flujo:
        rows.append({
            "Organismo": "Ejecutivo Nacional — TGN (agregado ejecución presupuestaria)",
            "Area": "Tesorería General de la Nación",
            "Riesgo Financiero": None, "Riesgo Contratación": r_con_global,
            "Riesgo Operativo":  None, "Riesgo Datos":        None,
            "IRI (Score)": _iri(None, r_con_global, None, None),
            "Fuente": f"gob_bo_comprar_tgn/flujo ({len(flujo)} procesos)",
        })

    if not rows:
        log.warning("  TGN AR: sin organismos — se omite del dataset (sin fallback sintético)")
        return _empty_monitor_df()

    df = pd.DataFrame(rows)
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)
    log.info(f"  ✅ TGN AR: {len(df)} organismos cargados (datos reales ejecución presupuestaria)")
    return df

# ═══════════════════════════════════════════════════════════════════════════════
# 6. FUNCIÓN PRINCIPAL — combina todo
# ═══════════════════════════════════════════════════════════════════════════════

def build_monitor_completo() -> pd.DataFrame:
    """
    Construye el dataset completo del Monitor IRI combinando:
    - Datos reales de justicia (Poder Judicial AR)
    - Datos reales de monitor_legistativo (Diputados AR)
    - Datos reales de senadores (Senado AR)
    - Datos reales de monitor_contratos_v2 (Ejecutivo AR — BORA+COMPR.AR+TGN)
    - Datos reales de gob_bo_comprar_tgn (Tesorería General de la Nación AR)

    Si algún repo no responde, esa sección se omite del dataset. No se genera
    ningún dato sintético: mejor un dataset más chico que uno con números
    fabricados que aparenten ser reales.
    """
    dfs = []

    try:
        dfs.append(build_judicial_df())
    except Exception as e:
        log.error(f"build_judicial_df falló: {e} — se omite del dataset (sin fallback sintético)")
        dfs.append(_empty_monitor_df())

    try:
        dfs.append(build_legislative_df())
    except Exception as e:
        log.error(f"build_legislative_df falló: {e} — se omite del dataset (sin fallback sintético)")
        dfs.append(_empty_monitor_df())

    try:
        dfs.append(build_senado_df())
    except Exception as e:
        log.error(f"build_senado_df falló: {e} — se omite del dataset (sin fallback sintético)")
        dfs.append(_empty_monitor_df())

    try:
        dfs.append(build_contratos_ar_df())
    except Exception as e:
        log.error(f"build_contratos_ar_df falló: {e} — se omite del dataset (sin fallback sintético)")
        dfs.append(_empty_monitor_df())

    try:
        dfs.append(build_tgn_df())
    except Exception as e:
        log.error(f"build_tgn_df falló: {e} — se omite del dataset (sin fallback sintético)")
        dfs.append(_empty_monitor_df())

    dfs_con_datos = [d for d in dfs if not d.empty]
    if not dfs_con_datos:
        log.error("Ninguna fuente real respondió — dataset vacío (sin fallback sintético)")
        return _empty_monitor_df()

    df = pd.concat(dfs_con_datos, ignore_index=True)
    componentes = ["Riesgo Financiero", "Riesgo Contratación", "Riesgo Operativo", "Riesgo Datos"]
    df["Componentes con dato"] = df[componentes].notna().sum(axis=1)
    df["Estado"] = df["IRI (Score)"].apply(_score_estado)
    # Con un solo componente medido no se marca ALTO: queda en MEDIO como dato
    # parcial (p. ej. organismos cuyo IRI es sólo el % de contrataciones directas).
    parcial = (df["Componentes con dato"] < 2) & df["Estado"].str.contains("ALTO", na=False)
    df.loc[parcial, "Estado"] = "🟡 MEDIO (dato parcial)"
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
