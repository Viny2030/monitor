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

COMPRAR_2020_URL = os.getenv(
    "COMPRAR_HIST_URL",
    "https://infra.datos.gob.ar/catalog/jgm/dataset/4/distribution/4.20/download/adjudicaciones-2020.csv",
)


def _norm_org(t: str) -> str:
    import unicodedata
    t = re.sub(r"^\s*\d+\s*-\s*", "", _arreglar_texto(t))           # "375 - Gendarmeria" -> "Gendarmeria"
    t = "".join(c for c in unicodedata.normalize("NFD", t.upper()) if unicodedata.category(c) != "Mn")
    t = re.sub(r"[^A-Z ]", " ", t)
    stop = {"NACIONAL", "ARGENTINA", "ARGENTINO", "DE", "DEL", "LA", "EL", "LOS", "LAS", "Y", "GENERAL"}
    return " ".join(w for w in t.split() if len(w) > 2 and w not in stop)


def _actividad_bora_2026(base_url: str) -> dict:
    """{nombre normalizado: (adjudicaciones, directas)} con las adjudicaciones 2026
    publicadas en el BORA que acumula monitor_contratos (/api/contratos)."""
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
    df["_org"] = df["organismo_contratante"].astype(str).str.split(" - ").str[0].map(_norm_org)
    df["_dir"] = df["tipo_proceso_bora"].astype(str).str.lower().str.contains("directa")
    return {o: (len(g), int(g["_dir"].sum())) for o, g in df.groupby("_org") if o}


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


def _organismos_comprar_2020(actividad: dict, area: str = "Administración Central", n: int = 25) -> list[dict]:
    """
    % de contrataciones directas por organismo con las adjudicaciones de COMPR.AR 2020
    (datos.gob.ar, siempre la misma fuente). Se cuenta cada procedimiento una vez
    (el archivo trae una fila por proveedor/renglón).

    Antes esta área dependía de lo que mandara /api/licitaciones/datos: el archivo
    2020 solo los días en que fallaba el portal Comprar y 12 aperturas próximas el
    resto, así que cambiaba de base día a día. En la fuente se agrega, como
    referencia, la actividad 2026 publicada en el BORA (que subestima las directas:
    la mayoría no se publica en el Boletín).
    """
    try:
        r = requests.get(COMPRAR_2020_URL, headers=HEADERS, timeout=120)
        r.raise_for_status()
        df = pd.read_csv(StringIO(r.content.decode("utf-8-sig", errors="replace")), low_memory=False)
    except Exception as e:
        log.warning(f"  COMPR.AR 2020: no se pudo descargar ({e})")
        return []
    req = {"Número Procedimiento", "Descripcion SAF", "Tipo de Procedimiento"}
    if not req.issubset(df.columns):
        log.warning(f"  COMPR.AR 2020: columnas inesperadas {list(df.columns)[:8]}")
        return []
    df = df.drop_duplicates("Número Procedimiento")
    df = df.rename(columns={"Descripcion SAF": "organismo", "Tipo de Procedimiento": "tipo"})
    rows = _organismos_compras(df.to_dict("records"), r"DIRECT", area, "COMPR.AR", n=n)
    for row in rows:
        row["Fuente"] = row["Fuente"].replace("COMPR.AR (COMPR.AR", "COMPR.AR").replace(" procesos)", " procedimientos")
        m = _match_bora(row["Organismo"], actividad)
        row["Fuente"] += (f" · BORA 2026: {m[0]} adjudicaciones ({m[1]} directas)" if m
                          else " · BORA 2026: sin adjudicaciones publicadas")
        row["Fuente"] = "monitor_contratos · " + row["Fuente"]
    return rows


def build_contratos_ar_df() -> pd.DataFrame:
    log.info("Cargando contratos Argentina (monitor_contratos)...")

    if not CONTRATOS_AR_API:
        log.warning("  CONTRATOS_AR_API_URL no definida — se omite del dataset (sin fallback sintético)")
        return _empty_monitor_df()

    rows_hist = _organismos_comprar_2020(_actividad_bora_2026(CONTRATOS_AR_API))
    if rows_hist:
        df = pd.DataFrame(rows_hist)
        df["Estado"] = df["IRI (Score)"].apply(_score_estado)
        log.info(f"  ✅ contratos AR: {len(df)} organismos (COMPR.AR 2020 + actividad BORA 2026)")
        return df
    log.warning("  contratos AR: COMPR.AR 2020 no disponible — se usa /api/licitaciones/datos")

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
