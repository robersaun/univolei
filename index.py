# UniVolei Live Scout – index.py (versão completa e estável)
from __future__ import annotations
from pathlib import Path
import re
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib as mpl
import streamlit.components.v1 as components
import html
import numpy as np
import time as _time
from datetime import date, datetime
import json
from db_duck import ensure_db as duck_ensure, replace_all as duck_replace
import logging, os
import gsheets_sync
from io import BytesIO
from parser_free import parse_line
import html as html_mod
from string import Template
import datetime as _dt, pandas as _pd, json as _json
import configparser, os as _os
from pathlib import Path as _P
from db_excel import save_all
import pandas as _pd
import datetime as _dt

from db_excel import (

    init_or_load, save_all, add_set,
    append_rally, last_open_match, finalize_match
)   
# =========================
# [1] CONFIGURAÇÃO INICIAL (dinâmica e segura)
# =========================
BASE_DIR = Path(__file__).parent.resolve()
DEFAULT_XLSX = BASE_DIR / "volei_base_dados.xlsx"
DEFAULT_DUCK = BASE_DIR / "volei_base_dados.dv"

CONFIG = {"online": {}, "backup": {}, "secrets": {}, "gcp": {}}  # <- inclui gcp aqui
_cfg_path = BASE_DIR / "config.ini"
BACKUP_DIR = BASE_DIR / "backups"
LOGS_DIR   = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# IMPORTANTE: GRAVÇÕES -> Frequência de persistência por destino
# =========================
# Frequência de persistência por destino (pode ir logo após carregar o config.ini)
SYNC_CFG = {
    # rápido e offline: por lance + checkpoints
    "duck":    {"rally", "set_open", "set_close", "match_close", "manual"},
    # arquivo para interoperabilidade: só checkpoints (ou manual)
    "xlsx":    {"set_open", "set_close", "match_close", "manual"},
    # online (lento): só checkpoints (ou manual)
    "gsheets": {"set_open", "set_close", "match_close", "manual"},
}



# =========================
# Config + Estilos
# =========================
st.set_page_config(page_title="Vôlei Scout – UniVolei", layout="wide", initial_sidebar_state="collapsed")

# =========================
# DEBUG: console + UI
# =========================
DEBUG_PRINTS = False  # se False, somem os blocos de debug da UI também
def debug_print(*args, **kwargs):
    if DEBUG_PRINTS:
        print("[UV-DEBUG]", *args, **kwargs, flush=True)
def show_debug_ui() -> bool:
    return bool(DEBUG_PRINTS)

# =========================
# --- Debug helper (safe) ---
# =========================
def dbg_print(*args, **kwargs):
    """Prints to Streamlit only if st.session_state.get('debug', False) is True; logs always if _logger exists."""
    try:
        if 'debug' in st.session_state and st.session_state.get('debug'):
            try:
                st.write(*args, **kwargs)
            except Exception:
                pass
        try:
            _logger  # type: ignore
            _logger.debug(" ".join(str(a) for a in args))
        except Exception:
            pass
    except Exception:
        pass
st.sidebar.caption(f"Excel: {st.session_state.get('db_path','(definir)')}")
st.sidebar.caption(f"DuckDB: {st.session_state.get('duck_path','(definir)')}")
st.sidebar.caption(f"Último salvamento: {st.session_state.get('last_save_at','-')}")
try:
    st.sidebar.caption(f"Webhook: {'on' if CONFIG['online'].get('webhook_url') else 'off'}")
except Exception:
    pass

# =========================
# ===== Mini painel de latência =====
# =========================

def _perf_begin(reason: str):
    return {
        "ts": _dt.datetime.now().isoformat(timespec="seconds"),
        "reason": reason,
        "steps": [],       # [{"label":"XLSX","ms":12.3}, ...]
        "statuses": [],    # mesmas strings que você já mostra
        "total_ms": 0.0,
    }

def _perf_step(perf: dict, label: str, t0: float):
    ms = (_time.perf_counter() - t0) * 1000.0
    perf["steps"].append({"label": label, "ms": round(ms, 1)})

def _perf_commit(perf: dict, statuses: list[str]):
    perf["statuses"] = statuses
    perf["total_ms"] = round(sum(s["ms"] for s in perf["steps"]), 1)
    lst = st.session_state.setdefault("perf_logs", [])
    lst.append(perf)
    # mantém só os últimos 60
    if len(lst) > 60:
        st.session_state["perf_logs"] = lst[-60:]

def _render_latency_panel(max_rows: int = 12):
    import pandas as pd
    logs = st.session_state.get("perf_logs", [])[-max_rows:]
    if not logs:
        st.caption("Sem medições ainda.")
        return

    rows = []
    for p in logs:
        row = {
            "Quando": p["ts"][-8:],     # HH:MM:SS
            "Motivo": p["reason"],
            "Total (ms)": p.get("total_ms", 0.0),
        }
        for s in p.get("steps", []):
            row[s["label"] + " (ms)"] = s["ms"]
        row["Status"] = " | ".join(p.get("statuses", []))
        rows.append(row)

    df = pd.DataFrame(rows)
    df = df.iloc[::-1].reset_index(drop=True)  # mais recente primeiro

    # Sempre renderizar via HTML para evitar import do pyarrow
    try:
        display_dataframe(df, height=220, use_container_width=True)
    except Exception:
        # Fallback 100% HTML (sem Arrow)
        try:
            html_table = df.to_html(index=False, escape=False)
        except Exception:
            html_table = "<em>Falha ao renderizar tabela.</em>"
        st.markdown(
            f"<div style='max-height:220px; overflow:auto'>{html_table}</div>",
            unsafe_allow_html=True
        )

    # Médias (últimos 20)
    tail = st.session_state.get("perf_logs", [])[-20:]
    if not tail:
        return
    agg = {}
    for p in tail:
        for s in p.get("steps", []):
            agg.setdefault(s["label"], []).append(s["ms"])
    if agg:
        meds = [f"{k}: {round(sum(v)/len(v),1)} ms" for k, v in agg.items()]
        st.caption("Médias (últimos 20): " + " · ".join(meds))


# =========================
# Carrega config.ini 
# =========================
if _cfg_path.exists():
    # Carrega config.ini (sem interpolation para não quebrar com % no JSON)
    _cp = configparser.ConfigParser(interpolation=None)
    _cp.read(_cfg_path, encoding="utf-8")
    # atualiza todas as seções presentes, criando chaves conforme necessário
    for sec in _cp.sections():
        CONFIG.setdefault(sec, {}).update({k: v for k, v in _cp.items(sec)})
# =========================
# Config GOOGLE_DRIVE
# =========================
if _cfg_path.exists():
    _cp = configparser.ConfigParser(interpolation=None); _cp.read(_cfg_path, encoding="utf-8")
    for sec in ("online","backup","secrets","gcp"):
        if _cp.has_section(sec):
            CONFIG[sec].update({k: v for k,v in _cp.items(sec)})


# >>> Fixos solicitados por você (podem ser sobrescritos por env/secrets se quiser no futuro)
GOOGLE_DRIVE_ROOT_FOLDER_URL = "https://drive.google.com/drive/folders/10PDkcUb4yGhrEmiNKwNo7mzZBGJIN_5r"
GOOGLE_SHEETS_SPREADSHEET_ID = "1FLBTjIMAgQjGM76XbNZT3U_lIDGUsWQbea2QCmdXbYI"
# >>> Valores devem vir SOMENTE do config.ini

# >>> SE QUISER PUXAR DO CONFIG.INI ->> MAS ESTAVA DANDO ERRO AO SUBIR NO STREAMLIT, GIT, ETC...
#GOOGLE_DRIVE_ROOT_FOLDER_URL   = CONFIG["backup"].get("drive_folder_url", "")
#GOOGLE_SHEETS_SPREADSHEET_ID   = CONFIG["online"].get("gsheet_id", "")
BACKUP_DIRS = [BACKUP_DIR]  # mantém o comportamento local

# Propaga para CONFIG (sem ENV/secrets; apenas reafirma o que veio do config.ini)
CONFIG["online"]["webhook_url"]      = CONFIG["online"].get("webhook_url", "")
CONFIG["online"]["gsheet_id"]        = CONFIG["online"].get("gsheet_id", "")
CONFIG["backup"]["drive_folder_url"] = CONFIG["backup"].get("drive_folder_url", "")


def _normalize_gsheet_id(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    # Se vier a URL completa de Planilhas, extrai o ID
    if "spreadsheets/d/" in s:
        return s.split("spreadsheets/d/")[1].split("/")[0]
    # Se vier uma pasta/Doc/Slide, invalida
    if "/drive/folders/" in s or "/document/d/" in s or "/presentation/d/" in s:
        return ""
    # Caso contrário, assume que já é um ID
    return s


# Sidebar de conferência
try:
    st.sidebar.caption(
        f"Config: webhook={'on' if CONFIG['online'].get('webhook_url') else 'off'} | "
        f"gsheets_id={CONFIG['online'].get('gsheet_id','unset')[:8]}..."
    )
    if CONFIG["backup"].get("drive_folder_url"):
        st.sidebar.caption(f"Drive folder URL: {CONFIG['backup']['drive_folder_url']}")
except Exception:
    pass

def _get_gspread_client():
    """
    Retorna um cliente gspread autenticado, tentando nesta ordem:
    A) st.secrets["gcp_service_account"]
    B) GOOGLE_APPLICATION_CREDENTIALS (path para .json)
    C) config.ini -> [gcp] credentials_mode=(path|inline)
    """
    try:
        import gspread
    except Exception:
        return None

    from google.oauth2.service_account import Credentials
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # === A) st.secrets ===
    try:
        import json
        if "gcp_service_account" in st.secrets:
            sa_info = st.secrets["gcp_service_account"]
            if isinstance(sa_info, str):
                sa_info = json.loads(sa_info)
            creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
            return gspread.authorize(creds)
    except Exception as e:
        try:
            _logger.warning(f"gspread via st.secrets falhou: {e}")
        except Exception:
            pass

        # === B) GOOGLE_APPLICATION_CREDENTIALS ===
    try:
        import os
        cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
        if cred_path:
            creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
            return gspread.authorize(creds)
    except Exception as e:
        _logger.warning(f"gspread via GOOGLE_APPLICATION_CREDENTIALS falhou: {e}")


    # === C) config.ini ===
    try:
        gcp_cfg = CONFIG.get("gcp", {})
        mode = (gcp_cfg.get("credentials_mode") or "").strip().lower()
        if mode == "path":
            cpath = (gcp_cfg.get("credentials_path") or "").strip()
            if cpath:
                creds = Credentials.from_service_account_file(cpath, scopes=scopes)
                return gspread.authorize(creds)
        elif mode == "inline":
            import json
            inline = gcp_cfg.get("inline_json")
            if inline:
                sa_info = json.loads(inline)
                creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
                return gspread.authorize(creds)
    except Exception as e:
        try:
            _logger.warning(f"gspread via config.ini falhou: {e}")
        except Exception:
            pass

    return None


def _persist_to_gsheets(frames, reason: str) -> str | None:
    """
    Sincroniza frames -> Google Sheets (uma aba por frame) usando gspread.
    Respeita CONFIG['online']['gsheet_id'].
    Se não houver gspread/credenciais, retorna string de erro (para log) e
    o fluxo seguirá para o fallback (Webhook) no _persist_all.
    """
    raw_id = (CONFIG["online"].get("gsheet_id") or "").strip()
    spreadsheet_id = _normalize_gsheet_id(raw_id)
    if not spreadsheet_id:
        return ("GSHEETS: erro — o gsheet_id aponta para um item que NÃO é uma planilha do Google Sheets "
                "(pode ser Doc/Slide/Pasta/XLSX). Use o ID de uma planilha nativa: /spreadsheets/d/<ID>/edit")

    # 1) Tenta cliente gspread
    gc = _get_gspread_client()
    if gc is None:
        return "GSHEETS: erro (gspread/credenciais ausentes)"

    try:
        sh = gc.open_by_key(spreadsheet_id)
    except Exception as e:
        msg = str(e)
        if "This operation is not supported for this document" in msg:
            return ("GSHEETS: erro — o gsheet_id aponta para um item que NÃO é uma "
                    "planilha do Google Sheets (pode ser Doc/Slide/Pasta/XLSX). "
                    "Use o ID de uma planilha nativa: /spreadsheets/d/<ID>/edit")
        return f"GSHEETS: erro ao abrir planilha ({e!s})"

    # 2) Para cada frame (DataFrame), cria/limpa a worksheet e escreve tudo
    try:
        for tab_name, df in frames.items():
            if not isinstance(df, _pd.DataFrame):
                continue

            # Normaliza nome de aba (Google Sheets tem limites de 100 chars e proíbe alguns caracteres)
            ws_title = str(tab_name)[:95].replace("/", "_").replace("\\", "_").replace(":", " ")

            # Tenta abrir a worksheet; se não existir, cria
            try:
                try:
                    ws = sh.worksheet(ws_title)
                except Exception:
                    ws = sh.add_worksheet(title=ws_title, rows=max(1000, len(df) + 10), cols=max(26, len(df.columns) + 5))

                # Limpa a worksheet (melhor para evitar sujeira de tamanhos diferentes)
                ws.clear()

                # Prepara valores: cabeçalho + dados
                values = [list(map(str, df.columns.tolist()))]
                if not df.empty:
                    values += df.astype(object).where(_pd.notna(df), "").astype(str).values.tolist()

                # Redimensiona a planilha (para evitar erro de range)
                # Ajuste mínimo para evitar "exceeded grid limits"
                rows_needed = max(100, len(values) + 5)
                cols_needed = max(26, len(values[0]) if values else 1)
                try:
                    ws.resize(rows=rows_needed, cols=cols_needed)
                except Exception:
                    pass  # alguns ambientes não permitem resize frequente; segue com update

                # Escreve a partir de A1
                ws.update("A1", values, value_input_option="RAW")

            except Exception as e:
                return f"GSHEETS: erro na aba '{ws_title}' ({e!s})"

        return f"GSHEETS: ok (reason={reason}) -> {spreadsheet_id}"

    except Exception as e:
        return f"GSHEETS: erro geral ({e!s})"

# =========================
# Logs
# =========================
def _uv2_log(msg):
    print(f"[UV2] {msg}", flush=True)

_logger = logging.getLogger("uv_persist")
if not _logger.handlers:
    _logger.setLevel(logging.INFO)
    fh = logging.FileHandler(LOGS_DIR / "uv_saves.log", encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh.setFormatter(fmt)
    _logger.addHandler(fh)

with st.sidebar.expander("Debug de salvamento", expanded=False):
    for item in st.session_state.get("dbg_prints", [])[-5:]:
        st.write(f"• {item['ts']} — {item['reason']}")
        for s in item["status"]:
            st.caption(s)

with st.sidebar.expander("⏱️ Latência de salvamento", expanded=False):
    _render_latency_panel(max_rows=12)

# =========================
# CSS
# =========================
def load_css(filename: str = "univolei.css"):
    for css_path in (BASE_DIR / filename, Path.cwd() / filename):
        if css_path.exists():
            st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)
            return str(css_path)  # caminho efetivamente usado
    print(f"[load_css] CSS não encontrado: {filename} | BASE_DIR={BASE_DIR} | CWD={Path.cwd()}")
    return None
_css_path = load_css("univolei.css")


# === UV2 GAME MODE INJECT (auto) — functional version ===
def _flag_param_true(val):
    if val is None:
        return False
    if isinstance(val, (list, tuple)) and val:
        val = val[0]
    return str(val).lower() in ("1","true","on","yes","y","sim","s")

def _uv2_read_file(path: Path) -> str:
    try:
        return Path(path).read_text(encoding="utf-8")
    except Exception:
        return ""

def _uv2_build_html() -> str:
    # EMBEDDED layout: não lê mais uv2_game.html/css do disco
    css_txt = """/* ===== Base 100% branco, sem “respiradores” entre componentes ===== */
:root{
  --pad:6px;       /* padding interno mínimo */
  --gap:0px;       /* sem espaçamento entre blocos */
  --pill-r:9999px;
  --fg:#222;       /* texto neutro */
  --bd:#dfe3ea;    /* borda leve */
  --bg:#ffffff;    /* fundo branco absoluto */
  --muted:#777;
  --danger:#b91c1c;
  --danger-soft:#fae3e3;
}

*{ box-sizing:border-box; }
html,body{ height:100%; }
body.uv2-body{
  margin:0; color:var(--fg); background:#fff;            /* TUDO branco */
  font:400 13px/1.18 "Inter", system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
}

/* App ocupa a viewport inteira (sem rolagem) */
#uv2-app{
  height:100svh;
  display:grid;
  grid-template-rows:
    min-content   /* header */
    min-content   /* controles (1 linha) */
    min-content   /* jogadoras */
    min-content   /* atalhos */
    min-content   /* placar */
    1fr;          /* quadra */
  gap:var(--gap);                 /* **sem** espaço entre os cards */
  padding:0;                      /* borda da tela colada */
}

/* Cartões brancos, super enxutos */
.uv2-card{
  background:var(--bg);
  border:1px solid var(--bd);
  border-radius:6px;
  padding:var(--pad);
}

/* ===== Título / badge ===== */
.badge{
  display:inline-flex; align-items:center; gap:6px;
  padding:2px 8px; border:1px solid var(--bd); border-radius:var(--pill-r);
  background:#fff; font-weight:800; font-size:clamp(11px, 3.0vw, 16px);
}
.badge .x{ font-weight:900; }
.badge .date{ font-weight:600; color:var(--muted); }

/* Header: título + toggle (neutro) */
#uv2-header{
  display:flex; align-items:center; justify-content:space-between; gap:6px;
  padding:4px var(--pad);
}

/* Toggle SEM verde (cinza quando ligado) */
.uv2-toggle{ display:inline-flex; align-items:center; gap:6px; cursor:pointer; user-select:none; }
.uv2-toggle input{ display:none; }
.uv2-toggle .knob{
  width:38px; height:22px; border-radius:9999px; position:relative;
  background:#e6e8ec; border:1px solid #cfd6e0; display:inline-block;
}
.uv2-toggle .knob::after{
  content:""; position:absolute; top:2px; left:2px; width:16px; height:16px; border-radius:50%;
  background:#fff; border:1px solid #cfd6e0; transition:.16s;
}
.uv2-toggle input:checked + .knob{ background:#d9def2; border-color:#c6cbe0; } /* tom frio neutro */
.uv2-toggle input:checked + .knob::after{ transform:translateX(16px); }
.uv2-toggle .lbl{ color:#111; font-weight:800; }

/* ===== Controles: 1 LINHA ===== */
#uv2-controls{ padding:4px var(--pad); }
/* CONTROLES: Resultado | Posição em 2 colunas (1 linha) */
.uv2-ctrl-row{
  display:grid !important;
  grid-template-columns: 1fr 1fr !important; /* 2 colunas lado a lado */
  align-items:center !important;
  gap:6px !important;
}
.uv2-field{ display:flex; align-items:center; gap:8px; min-width:0; }
.uv2-legend{ font-weight:800; margin:0; white-space:nowrap; }
.uv2-radio{ display:flex; gap:10px; flex-wrap:nowrap; }
.uv2-radio label span{ white-space:nowrap; }

.uv2-radio input{ accent-color:#111; }

/* ===== Jogadoras ===== */
#uv2-players .uv2-legend{ margin-bottom:4px; }
#uv2-players .uv2-grid{
  display:grid; grid-template-columns:repeat(12,minmax(0,1fr));
  gap:4px;
}
.pill{
  display:inline-flex; align-items:center; justify-content:center;
  height:24px; padding:0 8px; border-radius:var(--pill-r);
  border:1px solid var(--bd); background:#fff; font-weight:700;
}
.pill.adv{ background:#efe8ff; border-color:#d6c8ff; }

/* ===== Atalhos ===== */
#uv2-shortcuts .uv2-legend{ margin-bottom:4px; }
#uv2-shortcuts .uv2-row{ display:flex; flex-wrap:wrap; gap:4px; }
#uv2-shortcuts .pill{ height:24px; padding:0 8px; }
.pill.danger{ background:var(--danger); color:#fff; border-color:#7f1d1d; }
.pill.danger.soft{ background:var(--danger-soft); color:#222; border-color:#e8b4b4; }

/* ===== Placar ===== */
#uv2-score{ padding:4px var(--pad); }
#uv2-score .teams{
  display:grid; grid-template-columns:1fr auto 1fr; align-items:end; gap:4px;
}
#uv2-score .team{ text-align:center; }
#uv2-score .name{ font-size:clamp(11px,2.8vw,14px); font-weight:900; }
#uv2-score .points{ font-size:clamp(26px,9vw,48px); font-weight:900; line-height:1; }
#uv2-score .x{ font-size:clamp(14px,5.6vw,24px); font-weight:900; align-self:center; }
#uv2-score .sets{ text-align:center; font-weight:800; margin-top:2px; font-size:clamp(10px,2.7vw,13px); }

/* ===== Quadra (ocupa o restante) ===== */
#uv2-court{ display:grid; grid-template-rows:min-content 1fr min-content; gap:2px; }
.uv2-court-head,.uv2-court-foot{
  text-align:center; font-weight:800; font-size:clamp(11px,2.9vw,14px);
}
.uv2-court-box{
  position:relative; border:2px solid #111827; border-radius:6px;
  background:#f3a23a;                /* laranja (sem verde) */
  height:100%; min-height:160px;
}
.uv2-net{
  position:absolute; left:6%; right:6%; top:50%; transform:translateY(-50%);
  height:6px; background:
    repeating-linear-gradient(90deg,#111827 0 8px, transparent 8px 14px),
    linear-gradient(#111827,#111827);
  background-size:auto, 100% 2px; background-position:0 50%, 0 50%;
  background-repeat:repeat,no-repeat;
}
.uv2-dot{
  position:absolute; left:26%; top:58%;
  width:10px; height:10px; border-radius:50%; background:#1d4ed8; box-shadow:0 0 0 2px #0f172a40;
}

/* ===== Responsivo: manter 1 tela ===== */
@media (max-width:480px){
  .badge{ padding:2px 8px; }
  #uv2-players .uv2-grid{ grid-template-columns:repeat(12,minmax(0,1fr)); }
  #uv2-players .pill.adv{ grid-column:span 2; }
}

/* Larguras maiores centralizam sem criar espaços entre blocos */
@media (min-width:481px){
  #uv2-app{ max-width:520px; margin:0 auto; }
}

/* FORÇA Resultado | Posição em UMA linha (2 colunas) */
#uv2-controls .uv2-ctrl-row{
  display: grid !important;
  grid-template-columns: minmax(0,1fr) minmax(0,1fr) !important;
  column-gap: 6px !important;
  row-gap: 0 !important;
  align-items: center !important;
  width: 100% !important;
}
#uv2-controls .uv2-ctrl-row > .uv2-field{ min-width: 0 !important; }

#uv2-controls .uv2-legend{ white-space: nowrap !important; }
#uv2-controls .uv2-radio{ display:flex !important; flex-wrap: nowrap !important; gap:10px !important; }
#uv2-controls .uv2-radio label span{ white-space: nowrap !important; }
"""
    html_tpl = """<!doctype html><html><head><meta charset='utf-8'></head><body><div id='app'></div></body></html>"""
    # Se o HTML original referenciava o css via <link>, injeta inline
    html_tpl = html_tpl.replace('<link rel="stylesheet" href="uv2_game.css" />', '<style>$CSS</style>')
    if '$CSS' not in html_tpl:
        # garante css inline no <head>
        if '</head>' in html_tpl:
            html_tpl = html_tpl.replace('</head>', '<style>$CSS</style></head>')
        else:
            html_tpl = '<!doctype html><html><head><meta charset="utf-8"><style>$CSS</style></head>' + html_tpl
    # --- valores dinâmicos ---
    frames = st.session_state.get("frames", {})
    def _n(v, d=""):
        try: return str(v) if v is not None else d
        except Exception: return d
    try:
        home_name = team_name_by_id(frames, 1)
        away_name = team_name_by_id(frames, 2)
    except Exception:
        home_name = "Nós"; away_name = "Adversário"
    try:
        date_str = datetime.today().strftime("%d/%m/%Y")
    except Exception:
        date_str = ""
    try:
        mid = st.session_state.get("match_id")
        sn  = int(st.session_state.get("set_number") or 1)
        if mid is not None:
            df_cur = current_set_df(frames, mid, sn)
            hp, ap = set_score_from_df(df_cur)
            hs, as_ = update_sets_score_and_match(frames, mid)
        else:
            hp = ap = 0; hs = as_ = 0; sn = 1
    except Exception:
        hp = ap = 0; hs = as_ = 0; sn = 1
    from string import Template
    tpl = Template(html_tpl)
    html_doc = tpl.safe_substitute({
        "CSS": css_txt,
        "home": _n(home_name, "Nós"),
        "away": _n(away_name, "Adversário"),
        "date": _n(date_str, ""),
        "home_pts": _n(hp, "0"),
        "away_pts": _n(ap, "0"),
        "home_sets": _n(hs, "0"),
        "away_sets": _n(as_, "0"),
        "set_atual": _n(sn, "1"),
    })
    if "</head>" in html_doc:
        html_doc = html_doc.replace("</head>", "<script>console.log('[UV2] inline embutido');</script></head>")
    return html_doc


def _paint_adv_rede_buttons():
    components.html("""
    <script>
    (function(){
      // colapsa o iframe utilitário
      try{
        const f = window.frameElement;
        if (f){
          f.classList.add('uv-collapse');
          f.style.height='0px'; f.style.minHeight='0px';
          const ec = f.closest('.element-container');
          if(ec){
            ec.style.margin='0'; ec.style.padding='0'; ec.style.height='0'; ec.style.minHeight='0';
          }
          const p = f.parentElement;
          if(p){ p.style.margin='0'; p.style.padding='0'; p.style.height='0'; p.style.minHeight='0'; }
        }
      }catch(e){}
      // pinta botões
      function paint(){
        var doc;
        try{
          doc = (window.parent && window.parent.document) ? window.parent.document : document;
        }catch(e){
          doc = document;
        }
        const map = [
          {text:'adv',  bg:'rgba(255,0,255,0.20)', border:'rgba(160,0,160,0.55)'},
          {text:'rede', bg:'rgba(220,50,50,0.18)', border:'rgba(160,20,20,0.55)'},
          {text:'refazer rally', bg:'#b91c1c', border:'#7f1d1d'}
        ];
        const btns = Array.from(doc.querySelectorAll('button'));
        btns.forEach(b=>{
          const t = (b.textContent || '').trim().toLowerCase();
          map.forEach(m=>{
            if(t === m.text){
              b.style.background = m.bg;
              b.style.border = '1px solid ' + m.border;
              b.style.color = '#fff';
              b.style.fontWeight = '700';
              b.style.boxShadow = 'none';
            }
          });
        });
      }
      paint(); setTimeout(paint, 50); setTimeout(paint, 250);
    })();
    </script>
    """, height=0, scrolling=False)

# ---- onde você lê o html da automação de espaçamento
st.markdown("""
<script>
(function(){
  const N = 6; // botões por linha (troque para 5 se quiser 5 por linha)
  function addRowClassByTitle(substr, cls){
    // legacy: procura títulos em Markdown

    // Encontra o título (ex.: "Jogadoras", "Atalhos")
    const md = [...document.querySelectorAll('div[data-testid="stMarkdownContainer"]')]
      .find(el => (el.innerText || '').toLowerCase().includes(substr.toLowerCase()));
    if(!md) return false;
    // Pega o bloco de colunas logo após o título e marca com a classe
    let sib = md.parentElement;
    for(let i=0;i<30 && sib;i++){
      sib = sib.nextElementSibling;
      if(sib && sib.matches('div[data-testid="stHorizontalBlock"]')){
        sib.classList.add(cls);
        return true;
      }
    }
    return false;
  }
  function addRowClassByAny(substr, cls){
    substr = (substr || "").toLowerCase();

    // 1) tenta como título markdown (reaproveita sua função)
    if (addRowClassByTitle(substr, cls)) return true;

    // 2) fallback: procura o texto em contêineres comuns do Streamlit
    const nodes = [
      ...document.querySelectorAll(
        'div[data-testid="stMarkdownContainer"], ' +
        'div[data-testid="stVerticalBlock"], ' +
        'div[data-testid="stHorizontalBlock"]'
      )
    ];

    const anchor = nodes.find(el => (el.innerText || '').toLowerCase().includes(substr));
    if (!anchor) return false;

    // 3) a partir desse elemento, acha a próxima “linha” horizontal e aplica a classe
    let sib = anchor;
    for (let i = 0; i < 30 && sib; i++){
      sib = sib.nextElementSibling;
      if (sib && sib.matches('div[data-testid="stHorizontalBlock"]')){
        sib.classList.add(cls);
        return true;
      }
    }
    return false;
  }
  function forceGrid(){
    // Seleciona o stHorizontalBlock das duas linhas, mesmo que haja wrappers
    const rows = document.querySelectorAll(
      '.gm-players-row[data-testid="stHorizontalBlock"], .gm-players-row div[data-testid="stHorizontalBlock"],' +
      '.gm-quick-row[data-testid="stHorizontalBlock"],  .gm-quick-row  div[data-testid="stHorizontalBlock"]'
    );
    rows.forEach(row=>{
      // Força o container a ser um grid flex com wrap
      row.style.setProperty('display','flex','important');
      row.style.setProperty('flex-wrap','wrap','important');
      row.style.setProperty('gap','6px','important');
      row.style.setProperty('align-items','stretch','important');
      // Cada coluna ocupa 1/N da largura
      row.querySelectorAll('div[data-testid="column"]').forEach(col=>{
        col.style.setProperty('flex', `0 0 calc(100%/${N})`, 'important');
        col.style.setProperty('width', `calc(100%/${N})`, 'important');
        col.style.setProperty('max-width', `calc(100%/${N})`, 'important');
        col.style.setProperty('min-width', '0', 'important');
        col.style.setProperty('box-sizing', 'border-box', 'important');
      });
      // Botão preenche a célula (pílula)
      row.querySelectorAll('.stButton > button').forEach(btn=>{
        btn.style.setProperty('width','100%','important');
        btn.style.setProperty('display','inline-flex','important');
        btn.style.setProperty('align-items','center','important');
        btn.style.setProperty('justify-content','center','important');
        btn.style.setProperty('border-radius','9999px','important');
      });
    });
  }
  function apply(){
    const ok0 = addRowClassByAny('Resultado', 'gm-result-row');
    const ok1 = addRowClassByTitle('Jogadoras', 'gm-players-row');
    const ok2 = addRowClassByTitle('Atalhos',   'gm-quick-row');
    const ok3 = addRowClassByAny('Jogadora', 'gm-postcourt-row');
    forceGrid();
    console.log('[uv] grid fix', {ok0, ok1, ok2, ok3});
  }
  // Debounce para re-renders do Streamlit
  let t = null;
  const run = () => { clearTimeout(t); t = setTimeout(apply, 60); };
  if (document.readyState !== 'loading') run();
  else document.addEventListener('DOMContentLoaded', run);
  new MutationObserver(run).observe(document.body, {childList:true, subtree:true});
})();
(function(){
  if (window.__uvGridObs) return;       // <<< evita registrar de novo
  window.__uvGridObs = true;
  const N = 6;
  /* resto do seu script... */
  let t = null;
  const run = () => { clearTimeout(t); t = setTimeout(apply, 60); };
  if (document.readyState !== 'loading') run();
  else document.addEventListener('DOMContentLoaded', run);
  const mo = new MutationObserver(run);
  mo.observe(document.body, {childList:true, subtree:true});
  window.__uvGridMO = mo;               // opcional, guarda referência
})();

(function(){
  if (window.__uvSquashObs) return;     // <<< evita múltiplas inscrições
  window.__uvSquashObs = true;
  function squash(id){ /* ... */ }
  function run(){ ['div5','div6','div7'].forEach(squash); }
  run();
  const mo = new MutationObserver(run);
  mo.observe(document.body,{childList:true,subtree:true});
  window.__uvSquashMO = mo;             // opcional
})();
</script>

""", unsafe_allow_html=True)
# === CSS anti-gap para QUALQUER iframe de components.html ===
st.markdown("""
<style>
  /* Enxuga espaçamento geral */
  [data-testid="stVerticalBlock"] > div { margin-bottom: .20rem !important; }
  div[data-testid="stMarkdownContainer"] > p:empty { margin:0 !important; padding:0 !important; }
  /* ===== Anti-gap agressivo para iframes de components.html ===== */
  /* Streamlit clássico */
  .element-container:has(> iframe[title^="streamlit-component"]) {
    margin: 0 !important;
    padding: 0 !important;
  }
  iframe[title^="streamlit-component"]{
    display: block !important;
    width: 100% !important;
    border: 0 !important;
    background: transparent !important;
  }
  /* Streamlit recente (testids) */
  div[data-testid="stVerticalBlock"] > div:has(> iframe[title^='streamlit-component']){
    margin: 0 !important; padding: 0 !important;
  }
  div[data-testid="stHorizontalBlock"] > div:has(> iframe[title^='streamlit-component']){
    margin: 0 !important; padding: 0 !important;
  }
  /* Quando o iframe utilitário estiver marcado como 'uv-collapse', zera TUDO */
  iframe.uv-collapse {
    display: none !important;
    height: 0 !important;
    min-height: 0 !important;
  }
  .element-container:has(> iframe.uv-collapse){
    margin: 0 !important; padding: 0 !important; height: 0 !important; min-height: 0 !important;
  }
  div[data-testid="stVerticalBlock"] > div:has(> iframe.uv-collapse),
  div[data-testid="stHorizontalBlock"] > div:has(> iframe.uv-collapse){
    margin: 0 !important; padding: 0 !important; height: 0 !important; min-height: 0 !important;
  }
  /* Seus estilos mobile */
  .uv-mobile-only { display:none; margin:0 !important; padding:0 !important; }
  @media (max-width: 480px){ .uv-mobile-only { display:block; } }
  .uv-row { display:flex; flex-wrap:wrap; gap:6px; margin:0; }
  .uv-btn {
    display:inline-block; padding:8px 10px; border-radius:10px; border:1px solid rgba(0,0,0,0.25);
    font-weight:700; cursor:pointer; background:rgba(240,240,240,.9);
  }
  .uv-btn.adv  { background:rgba(255,0,255,0.20); border-color:rgba(160,0,160,0.55); color:#fff; }
  .uv-btn.rede { background:rgba(220,50,50,0.18); border-color:rgba(160,20,20,0.55); color:#fff; }
</style>
""", unsafe_allow_html=True)
# =========================
# Figuras compactas
# =========================
SMALL_RC = {
    "figure.dpi": 110,
    "axes.titlesize": 8,
    "axes.labelsize": 7,
    "xtick.labelsize": 7,
    "ytick.labelsize": 7,
    "legend.fontsize": 7,
}
mpl.rcParams.update(SMALL_RC)
def small_fig(w=2.8, h=1.25):
    fig, ax = plt.subplots(figsize=(w, h), dpi=110)
    ax.grid(True, alpha=0.15)
    for side in ("top", "right"): ax.spines[side].set_visible(False)
    ax.margins(x=0.02)
    ax.tick_params(length=2.5, width=0.6, pad=1.5)
    return fig, ax
def trim_ax(ax, xlabel="", ylabel="", legend=False, max_xticks=6, max_yticks=5):
    from matplotlib.ticker import MaxNLocator
    if xlabel: ax.set_xlabel(xlabel, fontsize=7, labelpad=1.5)
    if ylabel: ax.set_ylabel(ylabel, fontsize=7, labelpad=1.5)
    ax.xaxis.set_major_locator(MaxNLocator(nbins=max_xticks, integer=True))
    ax.yaxis.set_major_locator(MaxNLocator(nbins=max_yticks, integer=True))
    if not legend and ax.get_legend(): ax.get_legend().remove()
    ax.get_figure().tight_layout(pad=0.15)
    return ax.get_figure()
def bar_chart_safe(obj, title=None, rotate_xticks=0):
    """Bar chart sem Altair/Arrow/pyarrow. Aceita Series (1 série) ou DataFrame (agrupado)."""
    import numpy as np
    import pandas as pd

    fig, ax = small_fig(3.6, 1.6)

    if isinstance(obj, pd.Series):
        s = obj.fillna(0)
        x = np.arange(len(s))
        ax.bar(x, s.values)
        ax.set_xticks(x)
        ax.set_xticklabels([str(i) for i in s.index], rotation=rotate_xticks, ha="right", fontsize=7)
    else:  # DataFrame
        df = obj.fillna(0)
        idx = list(df.index)
        cols = list(df.columns)
        x = np.arange(len(idx))
        n = max(1, len(cols))
        width = min(0.8, 0.8 / n)
        for i, c in enumerate(cols):
            ax.bar(x + i*width, df[c].values, width=width, label=str(c))
        ax.set_xticks(x + width*(n-1)/2)
        ax.set_xticklabels([str(i) for i in idx], rotation=rotate_xticks, ha="right", fontsize=7)
        ax.legend(loc="best", fontsize=7)

    st.pyplot(trim_ax(ax, legend=True), use_container_width=True)

# =========================
# DataFrame HTML
# =========================
def display_dataframe(df, height=None, use_container_width=False, extra_class: str = ""):
    if df is None or len(df) == 0:
        st.write("_Sem dados._"); return
    classes = ('custom-table ' + extra_class).strip()
    html_table = df.to_html(classes=classes, index=False, escape=False)

    height_css = f"{int(height)}px" if isinstance(height, (int, float)) else "auto"
    width_css = "100%" if use_container_width else "auto"

    styled_html = f"""
    <div style="overflow:auto; height:{height_css}; width:{width_css};">
        {html_table}
    </div>
    """
    st.markdown(styled_html, unsafe_allow_html=True)

# =========================
# Estado/Base
# =========================
DEFAULT_DB = str(BASE_DIR / "volei_base_dados.xlsx")
if "db_path" not in st.session_state: st.session_state.db_path = DEFAULT_DB
if "frames" not in st.session_state: st.session_state.frames = init_or_load(Path(st.session_state.db_path))

if "duck_path" not in st.session_state: st.session_state.duck_path = str(DEFAULT_DUCK)
if "match_id" not in st.session_state: st.session_state.match_id = None
if "set_number" not in st.session_state: st.session_state.set_number = None
if "auto_close" not in st.session_state: st.session_state.auto_close = True
if "graph_filter" not in st.session_state: st.session_state.graph_filter = "Ambos"
st.session_state.setdefault("data_rev", 0)
# auxiliares
st.session_state.setdefault("q_side", "Nós")
st.session_state.setdefault("q_result", "Acerto")
st.session_state.setdefault("q_action", "d")
st.session_state.setdefault("q_position", "Frente")
st.session_state.setdefault("last_selected_player", None)
st.session_state.setdefault("show_cadastro", False)
st.session_state.setdefault("show_tutorial", False)
st.session_state.setdefault("show_config_team", False)
st.session_state.setdefault("line_input_text", "")
st.session_state.setdefault("perf_logs", [])
# Heatmap / clique
st.session_state.setdefault("last_court_click", None)   # {"x":float,"y":float,"ts":int}
st.session_state.setdefault("heatmap_debug", True)
st.session_state.setdefault("show_heat_numbers", False)
# garantias de estado
st.session_state.setdefault("game_mode", False)
st.session_state.setdefault("player_label_mode", "Número")
st.session_state.setdefault("btn_label_mode", "Número")
st.session_state.setdefault("_do_rerun_after", False)
# =========== Debug/prints (em memória) ===========
st.session_state.setdefault("dbg_prints", [])


# =========================
# [2] Offline: Journal append-only (NDJSON) por lance
# =========================

def _journal_write(frames, reason: str):
    """Escreve 1 linha NDJSON por lance: journal/YYYYMMDD_match_<match_id>.ndjson"""
    try:
        mid = st.session_state.get("match_id")
        if not mid:
            return
        jdir = BASE_DIR / "journal"; jdir.mkdir(parents=True, exist_ok=True)
        ts = _dt.datetime.now()
        jpath = jdir / f"{ts.strftime('%Y%m%d')}_match_{mid}.ndjson"
        rl = frames.get("rallies", _pd.DataFrame())
        row = rl.iloc[-1].to_dict() if not rl.empty else {}
        payload = {
            "ts": _dt.datetime.now().isoformat(timespec="seconds"),
            "reason": reason,
            "match_id": mid,
            "set_number": st.session_state.get("set_number"),
            "rally": row,
        }
        with open(jpath, "a", encoding="utf-8") as f:
            f.write(_json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception as e:
        try:
            _logger.warning(f"journal falhou: {e}")
        except Exception:
            pass

# =========================
# [3] Online: Webhook (Apps Script) a cada lance
# =========================
def _persist_to_webhook(frames, reason: str) -> str|None:
    """POST no Web App (Apps Script). Precisa de CONFIG['online']['webhook_url']."""
    try:
        url = CONFIG["online"].get("webhook_url","").strip()
        if not url:
            return None
        import requests, pandas as pd
        rl = frames.get("rallies", pd.DataFrame())
        if rl is None or rl.empty:
            data_rows, cols = [], []
        else:
            cols = list(rl.columns)
            data_rows = rl.astype(object).where(pd.notna(rl), "").values.tolist()
        payload = {
            "sheet": "rallies",
            "columns": cols,
            "rows": data_rows,
            "reason": reason,
            "match_id": st.session_state.get("match_id"),
            "set_number": st.session_state.get("set_number"),
        }
        r = requests.post(url, json=payload, timeout=6)
        return "WEBHOOK: ok" if r.ok else f"WEBHOOK: http {r.status_code}"
    except Exception as e:
        try:
            _logger.warning(f"Webhook falhou: {e}")
        except Exception:
            pass
        return None


def _persist_all(frames, reason: str = "rally"):
    """Salva Excel sempre; DuckDB e Google Sheets quando possível.
       Backups timestampados apenas em 'set_close'/'match_close' (evita 1 arquivo por ponto)."""

    statuses = []  # coleta mensagens para print/log
    _perf = _perf_begin(reason)  # <<< inicia medição desse ciclo

    # 1) Excel principal (sempre)
    t = _time.perf_counter()
    try:
        save_all(_P(st.session_state.db_path), frames)
        statuses.append(f"XLSX: ok -> {st.session_state.db_path}")
    except Exception as e:
        msg = f"XLSX: erro {e!s}"
        statuses.append(msg)
        _logger.exception(f"Excel principal falhou: {e}")
    _perf_step(_perf, "XLSX", t)

    # 2) Backup Excel (somente em set/match) — local ./backups
    if reason in ("set_close","match_close"):
        t = _time.perf_counter()
        try:
            ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            bkp_dir = BASE_DIR / "backups"
            bkp_dir.mkdir(parents=True, exist_ok=True)
            bkp = bkp_dir / f"volei_base_dados_{ts}.xlsx"
            save_all(bkp, frames)
            statuses.append(f"BACKUP local: ok -> {bkp}")
        except Exception as e:
            msg = f"BACKUP local: erro {e!s}"
            statuses.append(msg)
            _logger.warning(msg)
        _perf_step(_perf, "BACKUP", t)

    # 3) DuckDB (se disponível)
    t = _time.perf_counter()
    try:
        try:
            import duckdb as _duck
            duck_path = st.session_state.get("duck_path", DEFAULT_DUCK)
            con = _duck.connect(duck_path)
            for tname, df in frames.items():
                if isinstance(df, _pd.DataFrame):
                    con.register("df_tmp", df)
                    con.execute(f"CREATE OR REPLACE TABLE {tname} AS SELECT * FROM df_tmp")
                    con.unregister("df_tmp")
            con.execute("CHECKPOINT")
            con.close()
            statuses.append(f"DUCKDB: ok -> {duck_path}")
        except Exception as e:
            msg = f"DUCKDB: erro {e!s}"
            statuses.append(msg)
            _logger.error(msg)
    except Exception as e:
        msg = f"DUCK bloco: erro {e!s}"
        statuses.append(msg)
        _logger.error(msg)
    _perf_step(_perf, "DUCKDB", t)

    # 4) Google Sheets (se credenciais existirem)
    t = _time.perf_counter()
    ok_gs = False
    try:
        gs_status = _persist_to_gsheets(frames, reason)
        if gs_status:
            statuses.append(gs_status); _logger.info(gs_status)
            ok_gs = str(gs_status).startswith("GSHEETS: ok")
        else:
            statuses.append("GSHEETS: skip (sem gsheet_id)")
    except Exception as e:
        statuses.append(f"GSHEETS: falhou {e!s}")
        _logger.error(statuses[-1])
    finally:
        _perf_step(_perf, "GSHEETS", t)

    # fallback para Webhook quando GS não persistiu
    if not ok_gs:
        t_wb = _time.perf_counter()
        try:
            wb = _persist_to_webhook(frames, reason)
            if wb:
                statuses.append(wb); _logger.info(wb)
        except Exception as e2:
            statuses.append(f"Webhook bloco falhou: {e2!s}")
            _logger.warning(statuses[-1])
        _perf_step(_perf, "WEBHOOK", t_wb)
    else:
        _perf_step(_perf, "GSHEETS", t)

    # 5) Log final + prints na UI
    try:
        r = int(frames.get("rallies", _pd.DataFrame()).shape[0])
        s = int(frames.get("sets", _pd.DataFrame()).shape[0])
        m = int(frames.get("amistosos", _pd.DataFrame()).shape[0])
    except Exception:
        r = s = m = -1

    duck_show = st.session_state.get("duck_path", "(sem duck_path)")
    st.session_state['last_save_at'] = _dt.datetime.now().strftime("%H:%M:%S")

    _logger.info(
        f"SALVO | reason={reason} | xlsx={st.session_state.db_path} | dv={duck_show} "
        f"| rows(r/s/m)={r}/{s}/{m} | {' | '.join(statuses)}"
    )

    st.session_state['dbg_prints'].append({
        "ts": _dt.datetime.now().isoformat(timespec="seconds"),
        "reason": reason,
        "rows": {"rallies": r, "sets": s, "amistosos": m},
        "status": statuses,
    })

    try:
        st.write("💾 Persistência:", "; ".join(statuses))
    except Exception:
        pass

    # <<< encerra medição e guarda
    _perf_commit(_perf, statuses)

    return



# =========================
# Captura de clique via query param
# =========================
def _uv_handle_court_click():
    try:
        payload = st.query_params.get("uv_click", None)
    except Exception:
        payload = None
    if not payload:
        return
    try:
        xs, ys = payload.split(",")[:2]
        x = float(xs); y = float(ys)  # normalizado [0..1]
        st.session_state["last_court_click"] = {"x": x, "y": y, "ts": int(_time.time())}
        dbg_print(f"Clique capturado: x={x:.4f}, y={y:.4f} (0..1).")
        try:
            del st.query_params["uv_click"]
        except Exception:
            pass
    except Exception as e:
        dbg_print(f"Falha lendo uv_click: {e}")
        try:
            del st.query_params["uv_click"]
        except Exception:
            pass
_uv_handle_court_click()
# Fechar tutorial via query-param
def _handle_tutorial_qp():
    try:
        uv = st.query_params.get("uv_tut", None)
    except Exception:
        uv = None
    if uv is not None:
        st.session_state["show_tutorial"] = False
        try:
            del st.query_params["uv_tut"]
        except Exception:
            pass
_handle_tutorial_qp()
frames = st.session_state.frames
# =========================
# Normalização jogadoras
# =========================
def _normalize_jogadoras_df(df_in: pd.DataFrame) -> pd.DataFrame:
    if df_in is None or df_in.empty:
        return pd.DataFrame(columns=["team_id", "player_number", "player_name", "position"])
    df = df_in.copy(); rename_map = {}
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in ["team_id", "id_time", "time_id", "equipe_id", "id_equipe", "idteam", "time"]:
        if cand in cols_lower: rename_map[cols_lower[cand]] = "team_id"; break
    for cand in ["player_number", "numero", "número", "num", "nro", "jogadora_numero", "dorsal"]:
        if cand in cols_lower: rename_map[cols_lower[cand]] = "player_number"; break
    for cand in ["player_name", "nome", "jogadora", "athlete", "atleta", "name"]:
        if cand in cols_lower: rename_map[cols_lower[cand]] = "player_name"; break
    for cand in ["position", "posicao", "posição", "pos", "role", "função", "funcao"]:
        if cand in cols_lower: rename_map[cols_lower[cand]] = "position"; break
    if rename_map: df = df.rename(columns=rename_map)
    for c in ["team_id","player_number","player_name","position"]:
        if c not in df.columns: df[c] = None
    df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce")
    df["player_number"] = pd.to_numeric(df["player_number"], errors="coerce")
    df["position"] = df["position"].astype(str).str.strip().str.lower()
    df["player_name"] = df["player_name"].astype(str).str.strip()
    return df
if "jogadoras" in frames:
    frames["jogadoras"] = _normalize_jogadoras_df(frames["jogadoras"])
    st.session_state.frames = frames
# =========================
# Helpers DB/lógica
# =========================
OUR_TEAM_ID = 1
# Ações
ACT_MAP = {
    "d": "Diagonal","l": "Paralela","m": "Meio","lob": "Largada","seg": "Segunda",
    "pi": "Pipe","re": "Recepção","b": "Bloqueio","sa": "Saque","rede": "Rede"
}
REVERSE_ACT_MAP = {v: k for k, v in ACT_MAP.items()}
ACTION_CODE_TO_NAME = {
    "d": "DIAGONAL","l": "LINHA","m": "MEIO","lob": "LOB","seg": "SEGUNDA",
    "pi": "PIPE","re": "RECEPÇÃO","b": "BLOQUEIO","sa": "SAQUE","rede": "REDE"
}
ACTION_SYNONYM_TO_NAME = {
    "diagonal":"DIAGONAL","diag":"DIAGONAL",
    "linha":"LINHA","paralela":"LINHA",
    "meio":"MEIO",
    "largada":"LOB","lob":"LOB",
    "segunda":"SEGUNDA","seg":"SEGUNDA",
    "pipe":"PIPE","pi":"PIPE",
    "recepcao":"RECEPÇÃO","recepção":"RECEPÇÃO","re":"RECEPÇÃO",
    "bloqueio":"BLOQUEIO","bloq":"BLOQUEIO","b":"BLOQUEIO",
    "saque":"SAQUE","sa":"SAQUE",
    "rede":"REDE"
}
ATTACK_ACTIONS = ["DIAGONAL","LINHA","PIPE","SEGUNDA","LOB","MEIO"]
def team_name_by_id(fr: dict, team_id: int | None) -> str:
    eq = fr.get("equipes", pd.DataFrame())
    if eq.empty or team_id is None: return "Equipe"
    eq = eq.copy(); eq["team_id"] = pd.to_numeric(eq["team_id"], errors="coerce")
    row = eq.loc[eq["team_id"] == int(team_id)]
    return str(row.iloc[0]["team_name"]) if not row.empty else f"Equipe {int(team_id)}"
def current_set_df(fr: dict, match_id: int, set_number: int) -> pd.DataFrame:
    rl = fr["rallies"]
    return rl[(rl["match_id"] == match_id) & (rl["set_number"] == set_number)].copy().sort_values("rally_no")
def set_score_from_df(df: pd.DataFrame) -> tuple[int, int]:
    if df.empty: return 0, 0
    last = df.iloc[-1]
    return int(last["score_home"]), int(last["score_away"])
def update_sets_score_and_match(fr: dict, match_id: int) -> tuple[int,int]:
    sets_df = fr["sets"]; mt = fr["amistosos"]
    sets_m = sets_df[sets_df["match_id"] == match_id]
    home_sets = int((sets_m["winner_team_id"] == 1).sum())
    away_sets = int((sets_m["winner_team_id"] == 2).sum())
    mt_mask = (mt["match_id"] == match_id)
    mt.loc[mt_mask, "home_sets"] = home_sets; mt.loc[mt_mask, "away_sets"] = away_sets
    fr["amistosos"] = mt
    return home_sets, away_sets

def _apply_set_winner_and_proceed(home_pts: int, away_pts: int):
    frames = st.session_state.frames
    match_id = st.session_state.match_id
    set_number = st.session_state.set_number
    winner_id = 1 if home_pts > away_pts else 2

    stf = frames["sets"]
    mask = (stf["match_id"] == match_id) & (stf["set_number"] == set_number)
    stf.loc[mask, "winner_team_id"] = winner_id
    frames["sets"] = stf

    home_sets, away_sets = update_sets_score_and_match(frames, match_id)
    save_all(Path(st.session_state.db_path), frames)

    # Journal por lance (append-only)
    try:
        _journal_write(frames, reason="set_close")
    except Exception as e:
        try:
            _logger.warning(f"journal hook falhou: {e}")
        except Exception:
            pass

    if home_sets >= 3 or away_sets >= 3:
        try:
            finalize_match(frames, match_id)
        except Exception:
            pass
        try:
            mt = frames.get("amistosos", pd.DataFrame())
            mt.loc[mt["match_id"] == match_id, "is_closed"] = True
            mt.loc[mt["match_id"] == match_id, "closed_at"] = datetime.now().isoformat(timespec="seconds")
            frames["amistosos"] = mt
            _persist_all(frames, reason='match_close')
        except Exception:
            pass
        st.success(f"Set {set_number} encerrado ({home_pts} x {away_pts}). Partida finalizada: {home_sets} x {away_sets} em sets.")
        st.session_state.match_id = None; st.session_state.set_number = None
    else:
        st.session_state.set_number = int(set_number) + 1
        add_set(frames, match_id=match_id, set_number=st.session_state.set_number)
        _persist_all(frames, reason='set_open')
        st.success(f"Set {set_number} encerrado ({home_pts} x {away_pts}). Novo set: {st.session_state.set_number}")


def auto_close_set_if_needed() -> None:
    if not st.session_state.auto_close: return
    frames = st.session_state.frames
    match_id = st.session_state.match_id
    set_number = st.session_state.set_number
    if match_id is None or set_number is None: return
    df_cur = current_set_df(frames, match_id, set_number)
    if df_cur.empty: return
    home_pts, away_pts = set_score_from_df(df_cur)
    target = 15 if int(set_number) == 5 else 25
    diff = abs(home_pts - away_pts)
    if (home_pts >= target or away_pts >= target) and diff >= 2:
        _apply_set_winner_and_proceed(home_pts, away_pts)
def recompute_set_score_fields(fr: dict, match_id: int, set_number: int):
    rl = fr["rallies"]
    sub = rl[(rl["match_id"]==match_id) & (rl["set_number"]==set_number)].copy().sort_values("rally_no")
    home = away = 0; rows = []
    for _, r in sub.iterrows():
        who = r["who_scored"]
        if who == "NOS": home += 1
        elif who == "ADV": away += 1
        r["score_home"] = home; r["score_away"] = away; rows.append(r)
    rl = rl[~((rl["match_id"]==match_id) & (rl["set_number"]==set_number))]
    if rows: rl = pd.concat([rl, pd.DataFrame(rows)], ignore_index=True)
    fr["rallies"] = rl
    stf = fr["sets"]; mask = (stf["match_id"]==match_id) & (stf["set_number"]==set_number)
    stf.loc[mask, "home_points"] = home; stf.loc[mask, "away_points"] = away
    fr["sets"] = stf
def undo_last_rally_current_set():
    fr = st.session_state.frames
    match_id = st.session_state.match_id
    set_number = st.session_state.set_number
    rl = fr["rallies"]
    sub = rl[(rl["match_id"]==match_id) & (rl["set_number"]==set_number)].copy().sort_values("rally_no")
    if sub.empty:
        st.warning("Não há rallies para desfazer neste set."); return
    last_row = sub.iloc[-1]
    last_rally_id = last_row["rally_id"]
    rl = rl[rl["rally_id"] != last_rally_id]
    fr["rallies"] = rl
    if len(sub) >= 2:
        prev = sub.iloc[-2]
        hp, ap = int(prev["score_home"]), int(prev["score_away"])
    else:
        hp, ap = 0, 0
    stf = fr["sets"]
    mask = (stf["match_id"]==match_id) & (stf["set_number"]==set_number)
    stf.loc[mask, "home_points"] = hp; stf.loc[mask, "away_points"] = ap
    fr["sets"] = stf
    save_all(Path(st.session_state.db_path), fr)
    st.session_state.data_rev += 1
    dbg_print(f"Desfeito rally_id={last_rally_id}. Placar {hp}-{ap}.")
# ===== who_scored e ação =====
def _fix_who_scored_from_raw_and_row(raw_line: str, row: dict) -> dict:
    try:
        tokens = raw_line.strip().split()
        if not tokens: return row
        prefix = tokens[0]  # "1" (Nós executa) ou "0" (Adv executa)
        is_error = tokens[-1].lower() == "e"
        if prefix == "1":
            row["who_scored"] = "ADV" if is_error else "NOS"
        elif prefix == "0":
            row["who_scored"] = "NOS" if is_error else "ADV"
        row["result"] = "ERRO" if is_error else "PONTO"
    except Exception:
        pass
    return row
def _normalize_action_in_row(row: dict) -> dict:
    a = str(row.get("action", "") or "").strip().lower()
    if not a:
        raw = str(row.get("raw_text", "")).strip()
        toks = raw.split()
        if toks:
            last = toks[-1].lower()
            if last == "e" and len(toks) >= 2:
                a = toks[-2].lower()
            else:
                a = last
    if a in ACTION_CODE_TO_NAME:
        name = ACTION_CODE_TO_NAME[a]
    elif a in ACTION_SYNONYM_TO_NAME:
        name = ACTION_SYNONYM_TO_NAME[a]
    else:
        name = str(row.get("action", "") or "").strip().upper()
        if name in ("", "NA", "NONE"): name = ""
    row["action"] = name
    return row
def _fast_apply_scores_to_row(row: dict):
    frames_local = st.session_state.frames
    mid, sn = st.session_state.match_id, st.session_state.set_number
    df_cur = current_set_df(frames_local, mid, sn)
    if df_cur.empty:
        home, away = 0, 0
    else:
        last = df_cur.iloc[-1]
        home, away = int(last["score_home"]), int(last["score_away"])
    if row.get("who_scored") == "NOS": home += 1
    elif row.get("who_scored") == "ADV": away += 1
    row["score_home"] = home; row["score_away"] = away
    return row
# ==== CLICK MAPA (compat) ====
def _capture_court_click_from_query():
    try:
        params = dict(st.query_params)
        if "uvx" in params and "uvy" in params:
            x = float(params.get("uvx")); y = float(params.get("uvy"))
            ts = int(params.get("uvt", "0") or 0)
            st.session_state["last_court_click"] = {"x": x, "y": y, "ts": ts}
            dbg_print(f"Clique (uvx/uvy) recebido: x={x:.4f}, y={y:.4f}")
            newp = {k: v for k, v in dict(st.query_params).items() if not k.startswith("uv")}
            st.query_params.from_dict(newp)
    except Exception as e:
        dbg_print(f"Falha ao ler uvx/uvy: {e}")
_capture_court_click_from_query()
# >>> Persistir Frente/Fundo mesmo que DB ignore colunas soltas
def _persist_fb_on_last_rally(fb_upper: str):
    try:
        fr = st.session_state.frames
        mid = st.session_state.match_id
        sn  = st.session_state.set_number
        rl = fr.get("rallies", pd.DataFrame())
        sub = rl[(rl["match_id"]==mid) & (rl["set_number"]==sn)]
        if sub.empty: return
        last_id = sub.iloc[-1]["rally_id"]
        for col in ["position_zone","pos_fb","frente_fundo"]:
            rl.loc[rl["rally_id"]==last_id, col] = fb_upper
        fr["rallies"] = rl
        st.session_state.frames = fr
        save_all(Path(st.session_state.db_path), fr)
        dbg_print(f"Persistido Frente/Fundo='{fb_upper}' no rally_id={last_id}.")
    except Exception as e:
        dbg_print(f"Falha ao persistir Frente/Fundo: {e}")
        
def quick_register_line(raw_line: str):
    if not raw_line.strip():
        dbg_print("Linha vazia ignorada."); return

    row = parse_line(raw_line)
    row = _fix_who_scored_from_raw_and_row(raw_line, row)
    row = _normalize_action_in_row(row)

    fb = str(st.session_state.get("q_position","Frente")).strip().upper()
    row["position_zone"] = fb

    row = _fast_apply_scores_to_row(row)

    last_click = st.session_state.get("last_court_click")
    if last_click and isinstance(last_click, dict):
        row["court_x"] = float(last_click.get("x", 0.0))
        row["court_y"] = float(last_click.get("y", 0.0))
        st.session_state["last_court_click"] = None

    append_rally(
        st.session_state.frames,
        match_id=st.session_state.match_id,
        set_number=st.session_state.set_number,
        row=row
    )

    _persist_fb_on_last_rally(fb)

    # journal append-only por lance (segurança offline extra)
    try:
        _journal_write(st.session_state.frames, reason="rally")
    except Exception:
        pass

    _persist_all(st.session_state.frames, reason='rally')

    st.session_state.data_rev += 1
    auto_close_set_if_needed()

    dbg_print(
        f"REGISTRO: raw='{raw_line}' -> action='{row.get('action')}', result='{row.get('result')}', "
        f"who_scored='{row.get('who_scored')}', player={row.get('player_number')}, "
        f"pos={row.get('position_zone')}, placar={row.get('score_home')}-{row.get('score_away')}"
    )


def quick_register_click(side: str, number: int | None, action: str, is_error: bool):
    prefix = "1" if side == "NOS" else "0"
    num = f"{number}" if number is not None else ""
    line = f"{prefix} {num} {action}{' e' if is_error else ''}".strip()
    quick_register_line(line)
def resolve_our_roster_numbers(frames: dict) -> list[int]:
    jg = frames.get("jogadoras", pd.DataFrame()).copy()
    if jg.empty: return []
    for col in ["team_id","player_number"]:
        if col in jg.columns: jg[col] = pd.to_numeric(jg[col], errors="coerce")
    ours = jg[jg["team_id"] == OUR_TEAM_ID].dropna(subset=["player_number"]).sort_values("player_number")
    return ours["player_number"].astype(int).unique().tolist()
def roster_for_ui(frames: dict) -> list[dict]:
    jg = frames.get("jogadoras", pd.DataFrame()).copy()
    if jg.empty: return []
    for col in ["team_id","player_number"]:
        if col in jg.columns: jg[col] = pd.to_numeric(jg[col], errors="coerce")
    ours = jg[(jg["team_id"] == OUR_TEAM_ID) & (~jg["player_number"].isna())].copy()
    if ours.empty: return []
    ours["player_number"] = ours["player_number"].astype(int)
    ours["player_name"] = ours["player_name"].astype(str)
    ours = ours.sort_values("player_number")
    return ours[["player_number","player_name"]].rename(
        columns={"player_number":"number","player_name":"name"}
    ).to_dict("records")
def player_name_by_number(frames: dict, number: int | None) -> str:
    if number is None: return ""
    jg = frames.get("jogadoras", pd.DataFrame())
    if jg is None or jg.empty: return ""
    row = jg[(pd.to_numeric(jg["team_id"], errors="coerce")==OUR_TEAM_ID) &
             (pd.to_numeric(jg["player_number"], errors="coerce")==int(number))]
    return (str(row.iloc[0]["player_name"]) if not row.empty else "")
# central de registro
def register_current(number: int | None = None, action: str | None = None):
    side_code = "NOS" if st.session_state.get("q_side", "Nós") == "Nós" else "ADV"
    is_err = (st.session_state.get("q_result", "Acerto") == "Erro")
    act = action if action is not None else st.session_state.get("q_action", "d")
    num_val = number if number is not None else st.session_state.get("last_selected_player", None)
    if num_val is None:
        raw = st.session_state.get("line_input_text", "")
        m = re.findall(r"\b(\d{1,2})\b", raw)
        num_val = int(m[-1]) if m else None
    if str(act).lower() == "rede":
        is_err = True
    dbg_print(f"register_current: side={side_code}, num={num_val}, action={act}, is_err={is_err}, pos={st.session_state.get('q_position')}")
    quick_register_click(side_code, num_val, act, is_err)
# ========= HEATMAP – POSICIONAMENTO + ANTICOLISÃO =========
FRONT_Y = {"opp": 44.0, "our": 56.0}
BACK_Y  = {"opp":  8.0, "our": 92.0}
def _y_net_touch(half: str) -> float:
    return 49.0 if half == "opp" else 51.0
def _x_for_action(act: str) -> float:
    if act in ("MEIO","PIPE","SEGUNDA","SAQUE","REDE","BLOQUEIO","LOB"):
        return 50.0
    if act == "DIAGONAL": return 28.0
    if act == "LINHA":    return 82.0
    return 50.0
def build_heat_points(df: pd.DataFrame,
                      selected_players: list[int] | None,
                      include_success: bool,
                      include_errors: bool,
                      include_adv_points: bool,
                      include_adv_errors: bool,
                      return_debug: bool = False):
    if df is None or df.empty:
        empty_dbg = pd.DataFrame(columns=["rally_no","player_number","action_u","res_u","who_u","used_x","used_y","origem","cor"])
        return ([], [], [], [], empty_dbg) if return_debug else ([], [], [], [])
    def _norm_action(a: str) -> str:
        a = (a or "").strip().upper()
        if a in ("M",): return "MEIO"
        if a in ("D",): return "DIAGONAL"
        if a in ("L","PARALELA"): return "LINHA"
        if a in ("LOB","LARGADA"): return "LOB"
        if a in ("PI","PIPE"): return "PIPE"
        if a in ("SEG","SEGUNDA"): return "SEGUNDA"
        if a in ("RE","RECEPCAO","RECEPÇÃO"): return "RECEPÇÃO"
        if a in ("SA","SAQUE"): return "SAQUE"
        if a in ("B","BLOQ","BLOQUEIO"): return "BLOQUEIO"
        if a in ("REDE",): return "REDE"
        return a
    FB_COLS = ["position_zone","pos_fb","frente_fundo","frente_fundo_sel","posicao_fb","posicao","pos","zona_fb","zona"]
    def _row_fb(r) -> str | None:
        for c in FB_COLS:
            if c in r and pd.notna(r[c]):
                v = str(r[c]).strip().upper()
                if v in ("FRENTE","F","FR","FRONTAL","ATAQUE"): return "FRENTE"
                if v in ("FUNDO","B","U","BACK","TRAS","TRÁS","DEFESA"): return "FUNDO"
        return None
    df0 = df.copy()
    df0["action_u"] = df0.get("action", "").astype(str).str.strip().str.upper()
    df0["who_u"]    = df0.get("who_scored", "").astype(str).str.strip().str.upper()
    df0["res_u"]    = df0.get("result", "").astype(str).str.strip().str.upper()
    if "player_number" in df0.columns:
        df0["player_number"] = pd.to_numeric(df0["player_number"], errors="coerce")
    df_nos = df0.copy()
    if selected_players is not None and "player_number" in df_nos.columns:
        df_nos["player_number"] = df_nos["player_number"].astype("Int64")
        sel = pd.Series(selected_players, dtype="Int64")
        df_nos = df_nos[df_nos["player_number"].isin(sel) | df_nos["player_number"].isna()]
    actions_ok = {"MEIO","M","DIAGONAL","D","LINHA","PARALELA","L","LOB","LARGADA","PIPE","PI",
                  "SEGUNDA","SEG","RECEPÇÃO","RECEPCAO","RE","BLOQUEIO","B","BLOQ","SAQUE","SA","REDE"}
    succ_pts: list[dict] = []
    err_pts:  list[dict] = []
    adv_pts:  list[dict] = []
    adv_err_pts: list[dict] = []
    dbg_rows = []
    OCC = set()
    STEP = 2.0
    def _cell(x,y):
        return (int(round(x/STEP)), int(round(y/STEP)))
    def _find_free(x, y):
        cx, cy = _cell(x,y)
        if (cx,cy) not in OCC: return x,y,(cx,cy)
        k = 1
        while k < 14:
            for dx in range(-k,k+1):
                for dy in range(-k,k+1):
                    if abs(dx)!=k and abs(dy)!=k: continue
                    nx, ny = cx+dx, cy+dy
                    if (nx,ny) in OCC: continue
                    X = max(0.0, min(100.0, nx*STEP))
                    Y = max(0.0, min(100.0, ny*STEP))
                    OCC.add((nx,ny))
                    return X, Y, (nx,ny)
            k += 1
        return x, y, (cx,cy)
    def _eff_y(half: str, fb: str | None, act: str) -> float:
        if act in ("BLOQUEIO","REDE"): return _y_net_touch(half)
        if fb == "FRENTE": return FRONT_Y[half]
        if fb == "FUNDO":  return BACK_Y[half]
        return FRONT_Y[half]
    def _add_point(lst: list, x: float, y: float, color_tag: str, label: str | None, dbg_row: list):
        X, Y, cell = _find_free(x, y)
        OCC.add(cell)
        lst.append({"x": X, "y": Y, "label": label})
        if return_debug:
            d = dbg_row.copy(); d[5] = X; d[6] = Y; d[8] = color_tag
            dbg_rows.append(d)
    def _infer_point(r, half: str, color_tag: str, bucket: list, label: str | None):
        act = _norm_action(r.get("action_u",""))
        fb  = _row_fb(r)
        cx, cy = r.get("court_x"), r.get("court_y")
        if pd.notna(cx) and pd.notna(cy):
            x_use = float(cx)*100 if 0<=cx<=1 else float(cx)
            y_use = float(cy)*100 if 0<=cy<=1 else float(cy)
            _add_point(bucket, x_use, y_use, color_tag, label,
                       [r.get("rally_no"), r.get("player_number"), act, r.get("res_u"), r.get("who_u"),
                        x_use, y_use, f"clique fb={fb or '—'} half={half}", color_tag])
            return
        eff_half = "our" if act == "RECEPÇÃO" else half
        if act in ("BLOQUEIO","REDE"):
            y_rule = _eff_y(eff_half, fb, act)
            idx = len(bucket)
            x_rule = 10.0 + (idx % 16) * (80.0/15.0)
        else:
            x_rule = _x_for_action(act)
            y_rule = _eff_y(eff_half, fb, act)
        origem_txt = f"fb={fb or '—'} half={eff_half} ruleY={y_rule:.1f}"
        _add_point(bucket, x_rule, y_rule, color_tag, label,
                   [r.get("rally_no"), r.get("player_number"), act, r.get("res_u"), r.get("who_u"),
                    x_rule, y_rule, origem_txt, color_tag])
    if include_success:
        srows = df_nos[(df_nos["who_u"] == "NOS") & (df_nos["res_u"] == "PONTO") & (df_nos["action_u"].isin(actions_ok))]
        for _, r in srows.iterrows():
            lbl = str(int(r["player_number"])) if pd.notna(r.get("player_number")) else None
            _infer_point(r, half="opp", color_tag="nos_ok", bucket=succ_pts, label=lbl)
    if include_errors:
        erows = df_nos[(df_nos["who_u"] == "ADV") & (df_nos["res_u"] == "ERRO") & (df_nos["action_u"].isin(actions_ok))]
        for _, r in erows.iterrows():
            lbl = str(int(r["player_number"])) if pd.notna(r.get("player_number")) else None
            _infer_point(r, half="our", color_tag="nos_err", bucket=err_pts, label=lbl)
    if include_adv_points:
        arows = df0[(df0["who_u"] == "ADV") & (df0["res_u"] == "PONTO") & (df0["action_u"].isin(actions_ok))]
        for _, r in arows.iterrows():
            _infer_point(r, half="our", color_tag="adv_ok", bucket=adv_pts, label=None)
    if include_adv_errors:
        aerr = df0[(df0["who_u"] == "NOS") & (df0["res_u"] == "ERRO") & (df0["action_u"].isin(actions_ok))]
        for _, r in aerr.iterrows():
            _infer_point(r, half="opp", color_tag="adv_err", bucket=adv_err_pts, label=None)
    if return_debug:
        dbg = pd.DataFrame(dbg_rows, columns=["rally_no","player_number","action_u","res_u","who_u","used_x","used_y","origem","cor"])
        return succ_pts, err_pts, adv_pts, adv_err_pts, dbg
    else:
        return succ_pts, err_pts, adv_pts, adv_err_pts
# =========================
# QUADRA HTML
# =========================
def render_court_html(pts_success, pts_errors, pts_adv=None, pts_adv_err=None, enable_click=False, key="set", show_numbers=False):
    def _norm(points):
        out = []
        for it in points or []:
            if isinstance(it, dict):
                x = float(it.get("x", 0)); y = float(it.get("y", 0)); lab = it.get("label")
            elif isinstance(it, (list, tuple)) and len(it) >= 2:
                x = float(it[0]); y = float(it[1]); lab = None
            else:
                continue
            if 0.0 <= x <= 1.0 and 0.0 <= y <= 1.0:
                x *= 100.0; y *= 100.0
            out.append((max(0.0, min(100.0, x)), max(0.0, min(100.0, y)), lab))
        return out
    S = _norm(pts_success)
    E = _norm(pts_errors)
    A = _norm(pts_adv or [])
    AE = _norm(pts_adv_err or [])
    container_id = f"uv-court-{key}"
    DOT_PX = 14      # era 18
    FONT_PX = 10     # era 11
    def _dot_html(x, y, bg, border, text=None):
        label_html = ""
        if show_numbers and text:
            label_html = (
                f"<div style='position:absolute; inset:0; display:flex; align-items:center; justify-content:center; "
                f"font-size:{FONT_PX}px; color:#fff; font-weight:700;'>{html.escape(str(text))}</div>"
            )
        return (
            f"<div style='left:{x}%; top:{y}%; width:{DOT_PX}px; height:{DOT_PX}px; position:absolute;"
            f"background:{bg}; border:1px solid {border}; border-radius:50%;"
            f"transform:translate(-50%,-50%); z-index:4;'>{label_html}</div>"
        )
    dots_html = []
    # NÓS
    for x,y,lab in S:  dots_html.append(_dot_html(x, y, "rgba(30,144,255,0.92)", "rgba(20,90,200,0.95)", lab))  # azul
    for x,y,lab in E:  dots_html.append(_dot_html(x, y, "rgba(220,50,50,0.92)", "rgba(160,20,20,0.95)", lab))   # vermelho
    # ADV
    for x,y,lab in A:  dots_html.append(_dot_html(x, y, "rgba(255,0,255,0.92)", "rgba(160,0,160,0.95)", lab or "ADV"))    # magenta
    for x,y,lab in AE: dots_html.append(_dot_html(x, y, "rgba(128,0,128,0.92)", "rgba(90,0,110,0.95)",  lab or "ADV"))    # roxo
    click_js = ""
    if enable_click:
        click_js = f"""
        (function(){{
          const containerId = {json.dumps(container_id)};
          const root = document.getElementById(containerId);
          if (!root) return;
          root.addEventListener('click', function(e){{
            const rect = root.getBoundingClientRect();
            const x = (e.clientX - rect.left) / rect.width;
            const y = (e.clientY - rect.top) / rect.height;
            try {{
              const params = new URLSearchParams(window.parent.location.search || "");
              params.set('uv_click', x.toFixed(4) + ',' + y.toFixed(4));
              const newUrl = window.parent.location.pathname + '?' + params.toString() + window.parent.location.hash;
              window.parent.history.replaceState({{}}, '', newUrl);
              window.parent.location.reload();
            }} catch(err) {{
              const params = new URLSearchParams(window.location.search || "");
              params.set('uv_click', x.toFixed(4) + ',' + y.toFixed(4));
              const newUrl = window.location.pathname + '?' + params.toString() + window.location.hash;
              window.history.replaceState({{}}, '', newUrl);
              window.location.reload();
            }}
          }});
        }})();
        """
    adv_lbl = "ADV"
    try:
        fr = st.session_state.frames
        mid = st.session_state.match_id
        mt = fr.get("amistosos", pd.DataFrame())
        row = mt.loc[mt["match_id"] == mid]
        if not row.empty:
            away_id = int(row.iloc[0]["away_team_id"])
            adv_lbl = team_name_by_id(fr, away_id)
    except Exception:
        pass
    adv_lbl_esc = html_mod.escape(str(adv_lbl))
    html_block = f"""
    <div style="width:100%; text-align:center; font-weight:700; margin-bottom:6px;">{adv_lbl_esc}</div>
    <div id="{container_id}" style="background:#FFA94D; border:2px solid #333; position:relative; width:100%; height:320px; border-radius:6px;">
      <!-- REDE -->
      <div style="
           position:absolute; left:0; top:calc(50% - 8px); width:100%; height:16px;
           background:repeating-linear-gradient(90deg, rgba(255,255,255,0.95) 0 12px, rgba(0,0,0,0.12) 12px 14px);
           border-top:2px solid #111; border-bottom:2px solid #111; z-index:2; opacity:.95; "></div>
      <div style="position:absolute; left:0; top:50%; width:100%; height:2px; background:#111; z-index:3;"></div>
      <!-- Linhas de ataque (3m) -->
      <div style="position:absolute; left:0; top:33.333%; width:100%; height:1px; background:rgba(0,0,0,.30); z-index:1;"></div>
      <div style="position:absolute; left:0; top:66.666%; width:100%; height:1px; background:rgba(0,0,0,.30); z-index:1;"></div>
      {''.join(dots_html)}
    </div>
    <div style="width:100%; text-align:center; font-weight:700; margin-top:12px; margin-bottom:22px;">UNIVÔLEI</div>
    <script>{click_js}</script>
    """
    components.html(html_block, height=468, scrolling=False)
# =========================
# Abertura de partida
# =========================
def _list_open_matches(frames: dict) -> list[int]:
    mt = frames.get("amistosos", pd.DataFrame())
    if mt.empty: return []
    if "is_closed" in mt.columns:
        mt = mt[~mt["is_closed"].fillna(False).astype(bool)]
    return [int(x) for x in pd.to_numeric(mt["match_id"], errors="coerce").dropna().astype(int).tolist()]
open_mid = last_open_match(frames)
if st.session_state.match_id is None:
    open_list = _list_open_matches(frames)
    if len(open_list) == 1:
        st.session_state.match_id = int(open_list[0])
    elif len(open_list) > 1:
        with st.container():
            st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
            st.subheader("🟢 Jogos em aberto")
            opts = []
            mt = frames.get("amistosos", pd.DataFrame())
            for mid in sorted(open_list, reverse=True):
                row = mt.loc[mt["match_id"]==mid]
                if row.empty: continue
                away_id = int(row.iloc[0]["away_team_id"])
                away_name = team_name_by_id(frames, away_id)
                opts.append((f"Jogo #{mid} vs {away_name} — {row.iloc[0]['date']}", mid))
            label = [o[0] for o in opts]
            val = [o[1] for o in opts]
            pick = st.selectbox("Selecione o jogo para carregar:", options=val, format_func=lambda v: label[val.index(v)])
            c1, c2 = st.columns([1,1])
            with c1:
                if st.button("Carregar jogo", use_container_width=True):
                    st.session_state.match_id = int(pick); st.session_state.set_number = 1
                    st.session_state._do_rerun_after = True
            with c2:
                if st.button("Fechar", use_container_width=True):
                    st.session_state._do_rerun_after = True
            st.markdown('</div>', unsafe_allow_html=True)
        if st.session_state._do_rerun_after:
            st.session_state._do_rerun_after = False
            st.rerun()
        st.stop()
    elif open_mid:
        st.session_state.match_id = int(open_mid)
# set atual
if st.session_state.match_id is not None and st.session_state.set_number is None:
    sets_m = frames["sets"]
    if not sets_m.empty and (sets_m["match_id"] == st.session_state.match_id).any():
        st.session_state.set_number = int(sets_m[sets_m["match_id"] == st.session_state.match_id]["set_number"].max())
    else:
        st.session_state.set_number = 1
home_name = away_name = date_str = ""
if st.session_state.match_id is not None:
    mt = frames["amistosos"]
    mrow = mt.loc[mt["match_id"] == st.session_state.match_id].iloc[0]
    home_name = team_name_by_id(frames, OUR_TEAM_ID)
    away_name = team_name_by_id(frames, int(mrow["away_team_id"]))
    date_str = str(mrow["date"])
# =========================
# Navegação: Histórico
# =========================
def _find_hist_page():
    pages_dir = BASE_DIR / "pages"
    if pages_dir.exists():
        for p in sorted(pages_dir.iterdir()):
            name = p.name.lower()
            if "histor" in name:
                return p
    return None
def _go_hist():
    """Abre a página de Histórico (em /pages) com logs na UI e no console."""
    try:
        hp = _find_hist_page()
    except Exception as e:
        st.error(f"❌ Erro ao localizar a página de histórico: {e}")
        print("[HIST-ERR] _find_hist_page falhou:", e, flush=True)
        return
    if hp is None:
        st.warning("Página de histórico não encontrada em /pages.")
        print("[HIST-NONE] Nenhum arquivo de histórico encontrado em /pages", flush=True)
        return
    # Caminho relativo correto para páginas dentro de /pages (sem usar barra invertida na f-string)
    rel = ("pages/" + hp.name).replace("\\\\", "/")
    st.info("🔎 Abrindo Histórico via switch_page → " + rel)
    print("[HIST-SP] tentando st.switch_page('" + rel + "')", flush=True)
    # 1) switch_page
    try:
        if hasattr(st, "switch_page"):
            st.switch_page(rel)
            print("[HIST-SP-OK] switch_page executado para " + rel, flush=True)
            return
        else:
            st.warning("switch_page() não disponível nesta versão do Streamlit.")
            print("[HIST-SP-MISS] switch_page indisponível", flush=True)
    except Exception as e:
        st.warning(f"⚠️ Falha no switch_page('{rel}'): {e}")
        print("[HIST-SP-FAIL]", e, flush=True)
    # 2) Fallback: rota direta /historico (igual à barra lateral)
    try:
        st.info("🔁 Tentando fallback via rota absoluta /historico")
        components.html(
            """
            <script>
            (function(){
              try{
                var loc = (window.parent && window.parent.location) ? window.parent.location : window.location;
                var target = (loc.origin || "") + "/historico";
                console.warn("[HIST-JS] Redirecionando para", target);
                loc.href = target;
              }catch(e){
                console.error("[HIST-JS] Falha no redirecionamento:", e);
                try{ window.location.href = "/historico"; }catch(_){}
              }
            })();
            </script>
            """,
            height=0, scrolling=False
        )
        return
    except Exception as e:
        st.warning("⚠️ Falha no fallback JS para /historico: " + str(e))
        print("[HIST-FB-FAIL]", e, flush=True)
    # 3) Último fallback: link clicável (relativo)
    st.markdown(
        "**Abra o histórico aqui:** [Abrir Histórico](" + rel + ")  \n"
        "Se não abrir, use a barra lateral do Streamlit."
    )
    print("[HIST-LINK] Exibido link para " + rel, flush=True)

# =========================
# Barra do sistema
# =========================
with st.container():
    bar1, bar2, bar3 = st.columns([1.6, 2.5, 3.2])
    with bar1:
        if home_name and away_name:
            st.markdown(
                (
                    f'<div class="badge"><b>{home_name}</b> &nbsp; X &nbsp; <b>{away_name}</b> — '
                    f'{(datetime.strptime(date_str, "%Y-%m-%d") if isinstance(date_str, str) else date_str).strftime("%d/%m/%Y")}'
                    f'</div>'
                ),
                unsafe_allow_html=True
            )
    with bar2:
        st.session_state.game_mode = st.toggle("🎮 Modo Jogo", value=st.session_state.game_mode, key="game_mode_toggle")        
    with bar3:
        if not st.session_state.game_mode:
            st.session_state.auto_close = st.toggle("Auto 25/15+2", value=st.session_state.auto_close, key="auto_close_toggle")
    st.markdown('</div>', unsafe_allow_html=True)
# rerun pós-callbacks
if st.session_state._do_rerun_after:
    st.session_state._do_rerun_after = False
    st.rerun()

#st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
    
# =========================
# Topo (Time, Jogo, Tutorial, Histórico)
# =========================
top1, top2, top3, top4 = st.columns([2.5, 1, 1, 1])
with top1:
    if not st.session_state.game_mode:
        st.button("⚙️ Time", use_container_width=True, key="top_config_team_btn",
                on_click=lambda: st.session_state.__setitem__("show_config_team", True))
with top2:
    if not st.session_state.game_mode:
        st.button("🆕 Jogo", use_container_width=True, key="top_new_game_btn",
            on_click=lambda: st.session_state.__setitem__("show_cadastro", True))
with top3:
    if not st.session_state.game_mode:
        st.button("📘 Tutorial", use_container_width=True, key="top_tutorial_btn",
            on_click=lambda: st.session_state.__setitem__("show_tutorial", True))
with top4:
    # Abrir Histórico (link direto — evita issues com switch_page)
    if not st.session_state.game_mode:
        st.markdown(
            '<a href="/historico" target="_self" style="display:block;text-align:center;padding:.4rem .6rem;border:1px solid rgba(49,51,63,.2);border-radius:.5rem;font-weight:600;">🗂️ Histórico</a>',
            unsafe_allow_html=True
        )

# =========================
# Sets
# =========================
if not st.session_state.game_mode:
    top5, top6, top7, top8, top9, top10 = st.columns([0.5, 1, 1, 1, 1, 1])
    sets_df = frames.get("sets", pd.DataFrame())
    sets_match_all = sets_df.loc[sets_df.get("match_id", pd.Series(dtype="Int64")) == st.session_state.match_id].sort_values("set_number")
    sel_vals = sets_match_all["set_number"].tolist() if not sets_match_all.empty else [1]
    with top5:
        st.markdown("<div class='uv-inline-label'>Opções do Set e da Partida:</div>", unsafe_allow_html=True)
    with top6:
        set_picked = st.selectbox("Set:", sel_vals, label_visibility="collapsed", key="set_select")
    def _reopen_set():
        frames_local = st.session_state.frames
        stf = frames_local["sets"]
        mask = (stf["match_id"]==st.session_state.match_id) & (stf["set_number"]==int(set_picked))
        if mask.any():
            stf.loc[mask, "winner_team_id"] = np.nan
            frames_local["sets"] = stf
            save_all(Path(st.session_state.db_path), frames_local)
        st.session_state.set_number = int(set_picked)
        st.session_state.frames = frames_local
        st.session_state.data_rev += 1
        st.success(f"Set {set_picked} reaberto.")
    def _close_set():
        frames_local = st.session_state.frames
        df_cur = current_set_df(frames_local, st.session_state.match_id, int(set_picked))
        if df_cur.empty: 
            st.warning("Sem rallies neste set.")
            return
        hp, ap = set_score_from_df(df_cur)
        if hp == ap: 
            st.warning("Empate — defina o set antes.")
            return
        st.session_state.set_number = int(set_picked)
        _apply_set_winner_and_proceed(hp, ap)
        st.session_state.data_rev += 1
    with top7:
        st.button("🔓 Reabrir Set", use_container_width=True, key="reopen_btn", on_click=_reopen_set)
        st.markdown('</div>', unsafe_allow_html=True)
    with top8:
        st.button("✅ Fechar Set", use_container_width=True, key="close_set_btn", on_click=_close_set)
        st.markdown('</div>', unsafe_allow_html=True)
    with top9:
        def _remove_empty_set():
                frames_local = st.session_state.frames
                stf = frames_local["sets"]; rl = frames_local["rallies"]; mid = st.session_state.match_id
                sets_m = stf[stf["match_id"]==mid]
                if sets_m.empty: 
                    st.warning("Sem sets cadastrados.")
                    return
                max_set = int(sets_m["set_number"].max())
                sub = rl[(rl["match_id"]==mid) & (rl["set_number"]==max_set)]
                if not sub.empty: 
                    st.warning(f"O Set {max_set} tem rallies e não será removido.")
                    return
                stf = stf[~((stf["match_id"]==mid) & (stf["set_number"]==max_set))]; frames_local["sets"] = stf
                save_all(Path(st.session_state.db_path), frames_local)
                st.success(f"Set {max_set} removido.")
                st.session_state.frames = frames_local
                st.session_state.data_rev += 1
                st.markdown('<div class="btn-xxs">', unsafe_allow_html=True)
        st.button("🗑️ Remover Set Vazio", use_container_width=True, key="remove_empty_set_btn", on_click=_remove_empty_set)
        st.markdown('</div>', unsafe_allow_html=True)
    ########   
    # Finalizar partida direto
 # Finalizar partida direto
    def _finalizar_partida():
        if st.session_state.match_id is None: 
            return

        mid = st.session_state.match_id

        try:
            finalize_match(st.session_state.frames, mid)
        except Exception:
            pass

        # Força is_closed na tabela e salva local
        try:
            frames_local = st.session_state.frames
            mt = frames_local.get("amistosos", pd.DataFrame())
            mt.loc[mt["match_id"] == mid, "is_closed"] = True
            mt.loc[mt["match_id"] == mid, "closed_at"] = datetime.now().isoformat(timespec="seconds")
            frames_local["amistosos"] = mt
            save_all(Path(st.session_state.db_path), frames_local)
            st.session_state.frames = frames_local

            # >>> NOVO: checkpoint de fim de partida (inclui GSheets conforme SYNC_CFG)
            _persist_all(st.session_state.frames, reason='match_close')

        except Exception:
            pass

        st.success("Partida finalizada.")
        st.session_state.match_id = None
        st.session_state.set_number = None
        st.session_state._do_rerun_after = True
    with top10:
        st.button("🏁 Finalizar Partida", use_container_width=True, on_click=_finalizar_partida)
        


# =========================
# Modais (Config/Tutorial/Cadastro)
# =========================
if st.session_state.get("show_config_team", False):
    st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
    col_title, col_close = st.columns([4, 1])
    with col_title: st.subheader("⚙️ Nosso Time e Jogadoras")
    with col_close:
        st.button("❌ Fechar", key="close_config_top_btn", on_click=lambda: st.session_state.__setitem__("show_config_team", False))
    st.markdown("**Nome do Nosso Time**")
    current_team_name = team_name_by_id(frames, OUR_TEAM_ID)
    new_team_name = st.text_input("Nome do time:", value=current_team_name, key="team_name_input")
    # download de template (xlsx + csv)
    def _download_template():
        cols = ["team_id","player_number","player_name","position"]
        df = pd.DataFrame(columns=cols)
        bio = BytesIO()
        try:
            with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="jogadoras")
            data = bio.getvalue()
            st.download_button("⬇️ Baixar modelo Excel (jogadoras.xlsx)", data=data, file_name="jogadoras_template.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception:
            st.info("Não consegui gerar XLSX aqui. Baixe como CSV e abra no Excel.")
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Baixar modelo CSV (jogadoras.csv)", data=csv, file_name="jogadoras_template.csv", mime="text/csv")
    _download_template()
    def _save_team_name():
        if "equipes" in frames:
            equipes = frames["equipes"]; mask = equipes["team_id"] == OUR_TEAM_ID
            if mask.any(): equipes.loc[mask, "team_name"] = new_team_name
            else:
                new_team = pd.DataFrame({"team_id":[OUR_TEAM_ID], "team_name":[new_team_name]})
                equipes = pd.concat([equipes, new_team], ignore_index=True)
            frames["equipes"] = equipes; _persist_all(frames, reason='generic')
            st.session_state.show_config_team = False
    st.button("💾 Salvar Nome do Time", key="save_team_name_btn", on_click=_save_team_name)
    st.markdown("---"); st.subheader("👥 Jogadoras")
    jogadoras_df = frames.get("jogadoras", pd.DataFrame())
    our_players = jogadoras_df[jogadoras_df["team_id"] == OUR_TEAM_ID].copy()
    if not our_players.empty:
        st.markdown("**Cadastradas**")
        display_df = our_players[["player_number", "player_name", "position"]].copy()
        display_df.columns = ["Número", "Nome", "Posição"]; display_dataframe(display_df, height=140)
        st.markdown("**Excluir**")
        players_to_delete = our_players["player_number"].astype(str) + " - " + our_players["player_name"]
        player_to_delete = st.selectbox("Escolha:", players_to_delete.tolist(), key="delete_player_select")
        def _delete_player():
            if player_to_delete:
                player_num = int(player_to_delete.split(" - ")[0])
                jog_df = frames["jogadoras"]
                jog_df = jog_df[~((jog_df["team_id"] == OUR_TEAM_ID) & (jog_df["player_number"] == player_num))]
                frames["jogadoras"] = jog_df; _persist_all(frames, reason='generic')
        st.button("🗑️ Excluir", key="delete_player_btn", on_click=_delete_player)
    st.markdown("---"); st.subheader("➕ Adicionar")
    c1, c2, c3 = st.columns(3)
    with c1: new_number = st.number_input("Número:", min_value=1, max_value=99, key="new_player_number")
    with c2: new_name = st.text_input("Nome:", key="new_player_name")
    with c3: new_position = st.selectbox("Posição:", ["oposto","levantador","central","ponteiro","líbero"], key="new_player_position")
    def _add_player():
        if new_name.strip():
            new_player = pd.DataFrame({"team_id":[OUR_TEAM_ID],"player_number":[new_number],"player_name":[new_name],"position":[new_position]})
            if "jogadoras" in frames:
                jog_df = frames["jogadoras"]
                jog_df = jog_df[~((jog_df["team_id"] == OUR_TEAM_ID) & (jog_df["player_number"] == new_number))]
                jog_df = pd.concat([jog_df, new_player], ignore_index=True)
                frames["jogadoras"] = jog_df
            else:
                frames["jogadoras"] = new_player
            _persist_all(frames, reason='generic')
        else:
            st.warning("Digite um nome.")
    st.button("➕ Adicionar Jogadora", key="add_player_btn", on_click=_add_player)
    st.markdown('</div>', unsafe_allow_html=True)
# Tutorial modal
if st.session_state.get("show_tutorial", False):
    try:
        html_path = BASE_DIR / "tutorial_scout.html"
        if html_path.exists():
            html_content = html_path.read_text(encoding="utf-8")
            components.html(
                f"""
                <div id='uv-modal' style='position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%);
                 width: 90vw; height: 85vh; background-color: white; z-index: 1000;
                 border: 2px solid #ccc; border-radius: 10px; overflow: hidden;'>
                    <button id='uv-close'
                         style='position:absolute; top:10px; right:10px; z-index:1001; background:#ff4b4b; color:white;
                                border:none; border-radius:50%; width:30px; height:30px; cursor:pointer; font-weight:bold;'>X</button>
                    <iframe srcdoc='{html_mod.escape(html_content)}' style='width:100%; height:100%; border:none; margin-top:40px;'></iframe>
                </div>
                <script>
                  (function(){{
                    const btn = document.getElementById('uv-close');
                    btn.addEventListener('click', function(){{
                      try {{
                        const params = new URLSearchParams(window.parent.location.search || "");
                        params.set('uv_tut','off');
                        const newUrl = window.parent.location.pathname + '?' + params.toString() + window.parent.location.hash;
                        window.parent.history.replaceState({{}}, '', newUrl);
                        window.parent.location.reload();
                      }} catch (e) {{
                        const params = new URLSearchParams(window.location.search || "");
                        params.set('uv_tut','off');
                        const newUrl = window.location.pathname + '?' + params.toString() + window.location.hash;
                        window.history.replaceState({{}}, '', newUrl);
                        window.location.reload();
                      }}
                    }});
                  }})();
                </script>
                """,
                height=900, scrolling=True
            )
        else:
            st.error("Arquivo de tutorial não encontrado.")
    except Exception as e:
        st.error(f"Não consegui abrir o tutorial: {e}")
    st.button("❌ Fechar Tutorial", key="close_tutorial_btn",
              on_click=lambda: st.session_state.__setitem__("show_tutorial", False))
# =========================
# Cadastro rápido / NOVO JOGO
# =========================
def _get_or_create_team_id_by_name(frames: dict, name: str) -> int:
    name_norm = str(name).strip()
    if not name_norm:
        return 2
    eq = frames.get("equipes", pd.DataFrame())
    if not eq.empty:
        hit = eq[eq["team_name"].astype(str).str.strip().str.lower() == name_norm.lower()]
        if not hit.empty:
            return int(hit.iloc[0]["team_id"])
        next_id = int(pd.to_numeric(eq["team_id"], errors="coerce").max() or 1) + 1
        if next_id == OUR_TEAM_ID: next_id += 1
        eq = pd.concat([eq, pd.DataFrame([{"team_id": next_id, "team_name": name_norm}])], ignore_index=True)
    else:
        next_id = 2 if OUR_TEAM_ID == 1 else 1
        eq = pd.DataFrame([{"team_id": next_id, "team_name": name_norm}])
    frames["equipes"] = eq
    return int(next_id)
def _create_new_match(opp_name: str, dt: date):
    frames_local = st.session_state.frames
    mt = frames_local.get("amistosos", pd.DataFrame())
    if mt.empty:
        mt = pd.DataFrame(columns=["match_id","away_team_id","date","home_sets","away_sets"])
        next_mid = 1
    else:
        next_mid = int(pd.to_numeric(mt["match_id"], errors="coerce").max() or 0) + 1
    away_id = _get_or_create_team_id_by_name(frames_local, opp_name or "Adversário")
    new_row = {"match_id": next_mid, "away_team_id": away_id, "date": str(dt), "home_sets": 0, "away_sets": 0}
    mt = pd.concat([mt, pd.DataFrame([new_row])], ignore_index=True)
    frames_local["amistosos"] = mt
    add_set(frames_local, match_id=next_mid, set_number=1)
    save_all(Path(st.session_state.db_path), frames_local)
    st.session_state.frames = frames_local
    st.session_state.match_id = next_mid
    st.session_state.set_number = 1
    st.session_state.show_cadastro = False
    st.success(f"Novo jogo criado: {team_name_by_id(frames_local, OUR_TEAM_ID)} x {opp_name or team_name_by_id(frames_local, away_id)}")
if (st.session_state.match_id is None or st.session_state.show_cadastro) and not st.session_state.show_config_team:
    with st.container():
        st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
        st.subheader("🆕 Novo Jogo")
        cgj1, cgj2 = st.columns([2, 1])
        with cgj1: opp_name = st.text_input("Adversário:", key="new_game_opponent", value="")
        with cgj2: game_date = st.date_input("Data:", value=date.today(), key="new_game_date")
        cgjb1, cgjb2 = st.columns([1,1])
        with cgjb1:
            st.button("Criar Jogo", key="create_game_btn",
                      on_click=lambda: _create_new_match(st.session_state.get("new_game_opponent","").strip(), st.session_state.get("new_game_date", date.today())),
                      use_container_width=True)
        with cgjb2:
            st.button("Fechar", key="close_new_game_btn",
                      on_click=lambda: st.session_state.__setitem__("show_cadastro", False),
                      use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    st.stop()

# =========================
# PLACAR (top) – visível fora do Modo Jogo
if not st.session_state.game_mode:
    # =========================
    with st.container():
        st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
        frames = st.session_state.frames
        df_set = current_set_df(frames, st.session_state.match_id, st.session_state.set_number)
        home_pts, away_pts = set_score_from_df(df_set)
        stf = frames["sets"]; sm = stf[stf["match_id"] == st.session_state.match_id]
        home_sets_w = int((sm["winner_team_id"] == 1).sum()); away_sets_w = int((sm["winner_team_id"] == 2).sum())
        st.markdown('<div class="gm-score-row">', unsafe_allow_html=True)
        pc1, pc2, pc3, pc4 = st.columns([1.1, .8, 1.1, 2.2])
        with pc1:
            st.markdown(f"<div class='score-box'><div class='score-team'>{home_name}</div><div class='score-points'>{home_pts}</div></div>", unsafe_allow_html=True)
        with pc2:
            st.markdown("<div class='score-box'><div class='score-x'>×</div></div>", unsafe_allow_html=True)
        with pc3:
            st.markdown(f"<div class='score-box'><div class='score-team'>{away_name}</div><div class='score-points'>{away_pts}</div></div>", unsafe_allow_html=True)
        with pc4:
            st.markdown(f"<div class='set-summary'>Sets: <b>{home_sets_w}</b> × <b>{away_sets_w}</b> &nbsp;|&nbsp; Set atual: <b>{st.session_state.set_number}</b></div>", unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
     
# =========================
# MODO JOGO
# =========================
if st.session_state.game_mode:
    with st.container():
        st.markdown('<div id=\"uv-game-mode\">', unsafe_allow_html=True)
        #DEIXEI COMENTADO -> NAO RETIRAR ESTAS LINHAS, HABILITAR SE NECESSÁRIO DEPOIS!
        #st.markdown('<div class="sectionCard game-mode-container">', unsafe_allow_html=True)
        #st.subheader("🎮 Modo Jogo")
        # Linha compacta
        st.markdown('<div id="div2" class="gm-row">', unsafe_allow_html=True)
        cR, cP, cM = st.columns([1.1, 1.1, 1.6])
        with cR:
            st.markdown("**Resultado**")
            st.session_state.q_result = st.radio(
                "", ["Acerto", "Erro"], horizontal=True,
                index=["Acerto", "Erro"].index(st.session_state.q_result),
                key="gm_q_result", label_visibility="collapsed"
            )
        with cP:
            st.markdown("**Posição**")
            st.session_state.q_position = st.radio(
                "", ["Frente", "Fundo"], horizontal=True,
                index=["Frente", "Fundo"].index(st.session_state.q_position),
                key="gm_q_position", label_visibility="collapsed"
            )
        with cM:
            if not st.session_state.game_mode:
                st.markdown("**Mostrar botões por**")
                st.session_state.player_label_mode = st.radio(
                    "", ["Número", "Nome"], horizontal=True,
                    index=["Número", "Nome"].index(st.session_state.player_label_mode),
                    key="player_label_mode_gm", label_visibility="collapsed"
                )
         # Linha de botões de jogadoras + ADV
        st.markdown('</div>', unsafe_allow_html=True)  # close div2
        st.markdown('<div id="div3" class="gm-row">', unsafe_allow_html=True)
        st.markdown('<div class="gm-players-row">', unsafe_allow_html=True)
        st.markdown("**Jogadoras (toque rápido define lado = Nós)**")
        nums = resolve_our_roster_numbers(st.session_state.frames)
        name_map = {r["number"]: r["name"] for r in roster_for_ui(st.session_state.frames)}
        if nums:
            num_cols = 12 if st.session_state.player_label_mode == "Número" else 4
            jcols = st.columns(num_cols)
            for i, n in enumerate(nums):
                label_txt = str(n) if st.session_state.player_label_mode == "Número" else (name_map.get(n) or str(n))
                with jcols[i % num_cols]:
                    st.button(
                        f"{label_txt}",
                        key=f"gm_pill_{n}",
                        on_click=lambda n=n: (
                            st.session_state.__setitem__("last_selected_player", n),
                            st.session_state.__setitem__("q_side", "Nós")
                        ),
                        use_container_width=True
                    )
            with jcols[(len(nums)) % num_cols]:
                st.button(
                    "ADV", key="gm_adv_btn",
                    on_click=lambda: st.session_state.__setitem__("q_side", "Adv"),
                    use_container_width=True
                )
            _paint_adv_rede_buttons()
        else:
            st.caption("Sem jogadoras")
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)  # close div3

        # >>> MOBILE (VISUAL) – Jogadoras + ADV (SEM IFRAME)
        if False:
            try:
                _labels_players = []
                for _n in nums or []:
                    _lab = str(_n) if st.session_state.player_label_mode == "Número" else (name_map.get(_n) or str(_n))
                    _labels_players.append(_lab)
                import html as _html
                _btns = []
                for _lab in _labels_players:
                    _lab_esc = _html.escape(str(_lab))
                    _btns.append(f"<button class='uv-btn' onclick='uvMobClick(\"{_lab_esc}\")'>{_lab_esc}</button>")
                _btns.append("<button class='uv-btn adv' onclick='uvMobClick(\"ADV\")'>ADV</button>")
                components.html(
                    """
                    <div class="uv-mobile-only" style="margin:0;">
                      <div class="uv-row" id="gm-mob-players" style="margin:4px 0 4px 0;">
                    """ + "".join(_btns) + """
                      </div>
                    </div>
                    <script>
                    (function(){
                      function uvMobClick(txt){
                        try{
                          var doc;
                          try{
                            doc = (window.parent && window.parent.document) ? window.parent.document : document;
                          }catch(e){
                            doc = document;
                          }
                          const t = (txt||'').toString().trim();
                          const btns = Array.from(doc.querySelectorAll('button'));
                          const target = btns.find(b => (b.textContent||'').trim() === t);
                          if(target) target.click();
                        }catch(e){ console.log('uvMobClick error', e); }
                      }
                      if(typeof window.uvMobClick!=='function'){ window.uvMobClick = uvMobClick; }
                    })();
                    </script>
                    """,
                    height=0, scrolling=False
                )
            except Exception:
                pass
        # >>> FIM MOBILE (VISUAL) – Jogadoras + ADV (SEM IFRAME)
        # Atalhos
        st.markdown('<div id="div4" class="gm-row">', unsafe_allow_html=True)
        st.markdown('<div class="gm-quick-row">', unsafe_allow_html=True)
        st.markdown("**Atalhos**")
        atalho_specs = [
            ("d",    "Diag"),
            ("l",    "Par"),
            ("m",    "Meio"),
            ("lob",  "Lob"),
            ("seg",  "Seg"),
            ("pi",   "Pipe"),
            ("re",   "Recep"),
            ("b",    "Bloq"),
            ("sa",   "Saque"),
            ("rede", "Rede"),
        ]
        acols = st.columns(12)
        for i, (code, label) in enumerate(atalho_specs):
            with acols[i % len(acols)]:
                st.button(
                    label, key=f"gm_quick_{code}",
                    on_click=lambda code=code: register_current(action=code),
                    use_container_width=True
                )
            # Inserir "Refazer Rally" imediatamente após 'Rede' (mesma linha/grade)
            if code == "rede":
                with acols[(i+1) % len(acols)]:
                    st.button(
                        "Refazer Rally",
                        key="gm_quick_refazer",
                        on_click=undo_last_rally_current_set,
                        use_container_width=True
                    )
        
        _paint_adv_rede_buttons()
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)  # close div4

        # >>> MOBILE (VISUAL) – Atalhos (SEM IFRAME)
        if False:
            try:
                import html as _html
                _btns2 = []
                for _code, _label in atalho_specs:
                    _lab_esc = _html.escape(str(_label))
                    cls = "uv-btn rede" if str(_label).strip().lower()=="rede" else "uv-btn"
                    _btns2.append(f"<button class='{cls}' onclick='uvMobClick(\"{_lab_esc}\")'>{_lab_esc}</button>")
                components.html(
                    """
                    <div class="uv-mobile-only" style="margin:0;">
                      <div class="uv-row" id="gm-mob-quick" style="margin:4px 0 6px 0;">
                    """ + "".join(_btns2) + """
                      </div>
                    </div>
                    <script>
                    (function(){
                      if(typeof window.uvMobClick!=='function'){
                        window.uvMobClick = function(txt){
                          try{
                            var doc;
                            try{
                              doc = (window.parent && window.parent.document) ? window.parent.document : document;
                            }catch(e){
                              doc = document;
                            }
                            const t = (txt||'').toString().trim();
                            const btns = Array.from(doc.querySelectorAll('button'));
                            const target = btns.find(b => (b.textContent||'').trim() === t);
                            if(target) target.click();
                          }catch(e){ console.log('uvMobClick error', e); }
                        };
                      }
                    })();
                    </script>
                    """,
                    height=0, scrolling=False
                )
            except Exception:
                pass
        # >>> FIM MOBILE (VISUAL) – Atalhos (SEM IFRAME)
        # ===== Encerramento da UI do Modo Jogo para esconder o restante =====
        
        # --- Quadra (Modo Jogo) exibida logo antes do encerramento ---
    # --- LINHA DO PLACAR + QUADRA + FILTROS (MODO JOGO) — ULTRA COMPACT (sem :has) ---
    # --- LINHA DO PLACAR + QUADRA + FILTROS (MODO JOGO) — ULTRA COMPACT (sem :has) ---
    try:
        # --- Linha do placar (texto auxiliar acima do placar) ---
        _df_for_score = df_hm if 'df_hm' in locals() else current_set_df(
            st.session_state.frames, st.session_state.match_id, st.session_state.set_number
        )
        _home_pts, _away_pts = set_score_from_df(_df_for_score)
        _set_raw = st.session_state.get("set_number"); _setn = 1
        if _set_raw is not None:
            _s = str(_set_raw).strip()
            if _s.isdigit(): _setn = int(_s)
        st.markdown(
            f"<div class='gm-preline'><strong>Set {_setn} — Placar: {_home_pts} x {_away_pts}</strong></div>",
            unsafe_allow_html=True
        )
        df_hm = current_set_df(st.session_state.frames, st.session_state.match_id, st.session_state.set_number)
    except Exception:
        df_hm = None

    show_success_gm = bool(st.session_state.get("gm_show_succ", True))
    show_errors_gm  = bool(st.session_state.get("gm_show_err", True))
    show_adv_pts_gm = bool(st.session_state.get("gm_show_adv_ok", True))
    show_adv_errs_gm= bool(st.session_state.get("gm_show_adv_err", True))
    _picked = st.session_state.get("gm_players_filter", "Todas")
    _sel_players_gm = None if _picked == "Todas" else [_picked]

    pts_succ, pts_errs, pts_adv, pts_adv_err = build_heat_points(
        df_hm,
        selected_players=_sel_players_gm,
        include_success=show_success_gm,
        include_errors=show_errors_gm,
        include_adv_points=show_adv_pts_gm,
        include_adv_errors=show_adv_errs_gm,
        return_debug=False
    )

    # === P L A C A R  (imediatamente acima da quadra) ===
    st.markdown('<div id="div5" style="margin:0;padding:0;">', unsafe_allow_html=True)
    st.markdown('<div class="gm-score-row">', unsafe_allow_html=True)

    frames = st.session_state.frames
    df_set = current_set_df(frames, st.session_state.match_id, st.session_state.set_number)
    home_pts, away_pts = set_score_from_df(df_set)
    stf = frames["sets"]; sm = stf[stf["match_id"] == st.session_state.match_id]
    home_sets_w = int((sm["winner_team_id"] == 1).sum()); away_sets_w = int((sm["winner_team_id"] == 2).sum())

    sc1, sc2, sc3, sc4 = st.columns([1.1, .8, 1.1, 2.2])
    with sc1:
        st.markdown(
            f"<div class='score-box'><div class='score-team'>{html_mod.escape(home_name or 'Nós')}</div><div class='score-points'>{home_pts}</div></div>",
            unsafe_allow_html=True
        )
    with sc2:
        st.markdown("<div class='score-box'><div class='score-x'>×</div></div>", unsafe_allow_html=True)
    with sc3:
        st.markdown(
            f"<div class='score-box'><div class='score-team'>{away_name}</div><div class='score-points'>{away_pts}</div></div>",
            unsafe_allow_html=True
        )
    with sc4:
        st.markdown(
            f"<div class='set-summary'>Sets: <b>{home_sets_w}</b> × <b>{away_sets_w}</b>  |  Set atual: <b>{st.session_state.set_number}</b></div>",
            unsafe_allow_html=True
        )
    st.markdown('</div>', unsafe_allow_html=True)  # fecha .gm-score-row
    st.markdown('</div>', unsafe_allow_html=True)  # fecha #div5

    # === Q U A D R A ===
    st.markdown('<div id="div6" style="margin:0;padding:0;">', unsafe_allow_html=True)
    render_court_html(
        pts_succ, pts_errs, pts_adv, pts_adv_err,
        enable_click=True, key="gm", show_numbers=st.session_state.show_heat_numbers
    )
    st.markdown('</div>', unsafe_allow_html=True)  # fecha #div6

    # === F I L T R O S  abaixo da quadra ===
    if not st.session_state.game_mode:
        st.markdown('<div id="div7" style="margin:0;padding:0;">', unsafe_allow_html=True)
        f1, f2, f3, f4, f5, f6 = st.columns([1.0, 1.0, 1.0, 1.2, 1.2, 1.2])
        with f1: 
            nums_all = resolve_our_roster_numbers(st.session_state.frames)
            player_opts = ["Todas"] + nums_all
            c1, c2 = st.columns([0.40, 0.60])
            with c1:
                st.markdown("<div class='uv-inline-label'>Jogadora</div>", unsafe_allow_html=True)
            with c2:
                picked = st.selectbox("", options=player_opts, index=0, key="hm_players_filter_main", label_visibility="collapsed")
            sel_players = None if picked == "Todas" else [picked]

        with f2: 
            st.session_state.show_heat_numbers = st.checkbox(
                "Mostrar número/ADV nas bolinhas",
                value=st.session_state.show_heat_numbers, key="hm_show_numbers_main"
            )
        with f3: show_success   = st.checkbox("Nossos acertos", value=True, key="hm_show_succ_main")        
        with f4: show_errors    = st.checkbox("Nossos erros",   value=True, key="hm_show_err_main")
        with f5: show_adv_pts   = st.checkbox("ADV acertos",    value=True, key="hm_show_adv_ok_main")
        with f6: show_adv_err   = st.checkbox("ADV erros",      value=True, key="hm_show_adv_err_main")
        st.markdown('</div>', unsafe_allow_html=True)  # fecha #div7

    # --- SCRIPT: “amassa” os wrappers do Streamlit ao redor de #div5/#div6/#div7 (sem :has) ---
    components.html("""
    <script>
    (function(){
    function squash(id){
        var el = document.getElementById(id);
        if(!el) return false;
        var wrap = el.closest('.element-container');
        if(wrap){
        wrap.classList.add('uv-squash');
        wrap.style.margin='0'; wrap.style.padding='0'; wrap.style.minHeight='0';
        var inner = wrap.querySelector(':scope > div');
        if(inner){ inner.style.margin='0'; inner.style.padding='0'; inner.style.minHeight='0'; }
        if(wrap.previousElementSibling){ wrap.previousElementSibling.style.marginBottom='0'; }
        if(wrap.nextElementSibling)    { wrap.nextElementSibling.style.marginTop='0'; }
        }
        return true;
    }
    function run(){ ['div5','div6','div7'].forEach(squash); }
    run();
    new MutationObserver(run).observe(document.body,{childList:true,subtree:true});
    })();
    </script>
    """, height=0, scrolling=False)

    st.stop()



# =========================
# Painel principal
# =========================
with st.container():
    frames = st.session_state.frames
    df_set = current_set_df(frames, st.session_state.match_id, st.session_state.set_number)
    left, right = st.columns([1.25, 1.0])
    # -------- ESQUERDA --------
    with left:
        bar4, bar5 = st.columns([1.6, 2.5])
        with bar4:
            st.markdown("**🎯 Registrar Rally**")
            def on_submit_text_main():
                raw = st.session_state.get("line_input_text", "").strip()
                if not raw:
                    return
                quick_register_line(raw)
                st.session_state["line_input_text"] = ""
                st.session_state["q_side"] = "Nós"
                st.session_state["q_result"] = "Acerto"
                st.session_state["q_action"] = "d"
                st.session_state["q_position"] = "Frente"
            st.text_input(
                "Digite código:", key="line_input_text",
                placeholder="Ex: 1 9 d", label_visibility="collapsed",
                on_change=on_submit_text_main
            )
            def _cb_register_main():
                register_current()
                st.session_state["line_input_text"] = ""
            c_reg, c_undo = st.columns([1, 1])
        with bar5:
            if not st.session_state.game_mode:
                st.session_state.graph_filter = st.radio("Filtro Gráficos:", options=["Nós","Adversário","Ambos"],
                    horizontal=True, index=["Nós","Adversário","Ambos"].index(st.session_state.graph_filter), key="graph_filter_radio")
        with c_reg:
            st.button("Registrar", use_container_width=True, key="btn_register_main", on_click=_cb_register_main)
        with c_undo:
            st.button("↩️ Desfazer", use_container_width=True, key="btn_undo_main", on_click=undo_last_rally_current_set)
        # Seleções rápidas (lado/resultado/posição/ação)
        s1, s2, s3, s4 = st.columns([1.0, 1.0, 1.0, 1.6])
        with s1:
            st.markdown("**Lado**")
            st.session_state.q_side = st.radio(
                "", ["Nós", "Adv"], horizontal=True,
                index=["Nós", "Adv"].index(st.session_state.q_side),
                key="main_q_side", label_visibility="collapsed"
            )
        with s2:
            st.markdown("**Resultado**")
            st.session_state.q_result = st.radio(
                "", ["Acerto", "Erro"], horizontal=True,
                index=["Acerto", "Erro"].index(st.session_state.q_result),
                key="main_q_result", label_visibility="collapsed"
            )
        with s3:
            st.markdown("**Posição**")
            st.session_state.q_position = st.radio(
                "", ["Frente", "Fundo"], horizontal=True,
                index=["Frente", "Fundo"].index(st.session_state.q_position),
                key="main_q_position", label_visibility="collapsed"
            )
        with s4:
            st.markdown("**Ação**")
            action_options = list(ACT_MAP.values())
            current_action = ACT_MAP.get(st.session_state.q_action, "Diagonal")
            def _on_action_change_main():
                sel = st.session_state.get("q_action_select_main")
                st.session_state["q_action"] = REVERSE_ACT_MAP.get(sel, "d")
            st.selectbox(
                "", action_options, index=action_options.index(current_action),
                key="q_action_select_main", on_change=_on_action_change_main,
                label_visibility="collapsed"
            )
        st.markdown("---")
        st.markdown("**Jogadoras**")
        nums = resolve_our_roster_numbers(st.session_state.frames)
        name_map = {r["number"]: r["name"] for r in roster_for_ui(st.session_state.frames)}
        if nums:
            btn_mode = st.session_state.get("btn_label_mode", "Número")
            num_cols = 12 if btn_mode == "Número" else 4
            pcols = st.columns(num_cols)
            for i, n in enumerate(nums):
                label_txt = str(n) if btn_mode == "Número" else (name_map.get(n) or str(n))
                with pcols[i % num_cols]:
                    st.button(
                        f"{label_txt}", key=f"main_pill_{n}", use_container_width=True,
                        on_click=lambda n=n: (
                            st.session_state.__setitem__("last_selected_player", n),
                            st.session_state.__setitem__("q_side", "Nós")
                        )
                    )
            with pcols[(len(nums)) % num_cols]:
                st.button(
                    "ADV", key="main_adv_btn", use_container_width=True,
                    on_click=lambda: st.session_state.__setitem__("q_side", "Adv")
                )
            _paint_adv_rede_buttons()
        else:
            st.caption("Sem jogadoras cadastradas para o nosso time.")
        # Atalhos
        st.markdown('<div class="gm-quick-row">', unsafe_allow_html=True)
        st.markdown("**Atalhos**")
        atalho_specs = [
            ("d",    "Diag"),
            ("l",    "Par"),
            ("m",    "Meio"),
            ("lob",  "Lob"),
            ("seg",  "Seg"),
            ("pi",   "Pipe"),
            ("re",   "Recep"),
            ("b",    "Bloq"),
            ("sa",   "Saque"),
            ("rede", "Rede"),
        ]
        acols = st.columns(12)
        for i, (code, label) in enumerate(atalho_specs):
            with acols[i % len(acols)]:
                st.button(
                    label, key=f"main_quick_{code}",
                    on_click=lambda code=code: register_current(action=code),
                    use_container_width=True
                )
        _paint_adv_rede_buttons()
        st.markdown("---")
        st.markdown("**🗺️ Mapa de Calor (clique para marcar o local do ataque)**")
        # Filtros do mapa de calor
        f1, f2, f3, f4, f5, f6 = st.columns([1.0, 1.0, 1.0, 1.2, 1.2, 1.2])
        with f1: 
            nums_all = resolve_our_roster_numbers(st.session_state.frames)
            player_opts = ["Todas"] + nums_all
            c1, c2 = st.columns([0.40, 0.60])
        with c1:
            st.markdown("<div class='uv-inline-label'>Jogadora</div>", unsafe_allow_html=True)
        with c2:
            picked = st.selectbox("", options=player_opts, index=0, key="hm_players_filter_main", label_visibility="collapsed")
        sel_players = None if picked == "Todas" else [picked]

        with f2: show_success   = st.checkbox("Nossos acertos", value=True, key="hm_show_succ_main")    
        with f3: show_errors    = st.checkbox("Nossos erros",   value=True, key="hm_show_err_main")
        with f4: show_adv_pts   = st.checkbox("ADV acertos",    value=True, key="hm_show_adv_ok_main")
        with f5: show_adv_err   = st.checkbox("ADV erros",      value=True, key="hm_show_adv_err_main")
        with f6:
            st.session_state.show_heat_numbers = st.checkbox(
                "Mostrar número/ADV nas bolinhas",
                value=st.session_state.show_heat_numbers, key="hm_show_numbers_main"
            )
            
        df_hm = current_set_df(st.session_state.frames, st.session_state.match_id, st.session_state.set_number)
        pts_succ, pts_errs, pts_adv, pts_adv_err, dbg_hm = build_heat_points(
            df_hm,
            selected_players=sel_players,
            include_success=show_success,
            include_errors=show_errors,
            include_adv_points=show_adv_pts,
            include_adv_errors=show_adv_err,
            return_debug=True
        )
        render_court_html(
            pts_succ, pts_errs, pts_adv, pts_adv_err,
            enable_click=True, key="main", show_numbers=st.session_state.show_heat_numbers
        )
        if show_debug_ui():
            with st.expander("🔎 Debug Heatmap (Painel Principal)"):
                st.write(
                    f"Acertos (azul): **{len(pts_succ)}**  |  Erros (vermelho): **{len(pts_errs)}**  |  "
                    f"ADV acertos (magenta): **{len(pts_adv)}**  |  ADV erros (roxo): **{len(pts_adv_err)}**"
                )
                if not dbg_hm.empty:
                    view = dbg_hm[["rally_no","player_number","action_u","res_u","who_u","used_x","used_y","origem","cor"]].tail(30)
                    display_dataframe(view, height=220, use_container_width=True)
                else:
                    st.write("_Sem registros elegíveis._")
    # -------- DIREITA --------
    with right:
        st.markdown("**📜 Últimos rallies (set atual)**")
        if df_set is not None and not df_set.empty:
            cols_show = []
            for c in ["rally_no","player_number","action","result","who_scored","score_home","score_away"]:
                if c in df_set.columns: cols_show.append(c)
            preview = df_set.sort_values("rally_no").tail(15)[cols_show].copy()
            preview.rename(columns={
                "rally_no":"#",
                "player_number":"Jog",
                "action":"Ação",
                "result":"Resultado",
                "who_scored":"Quem",
                "score_home":"H",
                "score_away":"A",
            }, inplace=True)
            display_dataframe(preview, height=260, use_container_width=True)
        else:
            st.caption("_Sem rallies no set atual._")
        # Resumo rápido por ação (nossos pontos/erros) — com proteção total ao 'Ação'
        def _norm_cols_for_summary(df):
            d = df.copy()
            for col in ["action","result","who_scored"]:
                if col in d.columns:
                    d[col] = d[col].astype(str).str.strip().str.upper()
            return d
        if df_set is not None and not df_set.empty:
            dfx = _norm_cols_for_summary(df_set)
            mask_pts = (dfx["who_scored"]=="NOS") & (dfx["result"]=="PONTO")
            mask_err = (dfx["who_scored"]=="ADV") & (dfx["result"]=="ERRO")
            counts_pts = dfx.loc[mask_pts, "action"].value_counts().rename("Pontos")
            counts_err = dfx.loc[mask_err, "action"].value_counts().rename("Erros")
            by_action = (
                pd.concat([counts_pts, counts_err], axis=1)
                .fillna(0).astype(int).reset_index().rename(columns={"index": "Ação"})
            )
            # proteção contra ausência de coluna após reset/rename
            if "Ação" not in by_action.columns and len(by_action.columns) >= 1:
                firstcol = by_action.columns[0]
                by_action = by_action.rename(columns={firstcol: "Ação"})
            # ordenação segura
            if "Ação" in by_action.columns and not by_action.empty:
                by_action = by_action.sort_values(by="Ação", kind="stable")
            cols_disp = [c for c in ["Ação","Pontos","Erros"] if c in by_action.columns]
            display_dataframe(by_action[cols_disp], height=200, use_container_width=True)
        # ========= GRÁFICOS E TABELAS RÁPIDAS DO SET (ACRESCENTADOS) =========
        st.markdown("---")
        st.markdown("**📈 Placar (evolução no set)**")
        if df_set is not None and not df_set.empty:
            fig3, ax3 = small_fig(3.4, 1.4)
            from matplotlib.ticker import MaxNLocator
            ax3.xaxis.set_major_locator(MaxNLocator(integer=True))
            ax3.yaxis.set_major_locator(MaxNLocator(integer=True))
            ax3.plot(df_set["rally_no"], df_set["score_home"], marker="o", markersize=2.6, linewidth=1.0, label=home_name or "Nós")
            ax3.plot(df_set["rally_no"], df_set["score_away"], marker="o", markersize=2.6, linewidth=1.0, label=away_name or "Adv")
            ax3.set_xlabel("Rally"); ax3.set_ylabel("Pontos")
            ax3.legend(loc="best", fontsize=7)
            st.pyplot(trim_ax(ax3, legend=True), use_container_width=True)
        # Pontos (Nossos)
        st.markdown("**🏅 Pontos (Nossos)**")
        if df_set is not None and not df_set.empty:
            dfx = _norm_cols_for_summary(df_set)
            mask_pts = (dfx["who_scored"]=="NOS") & (dfx["result"]=="PONTO")
            tbl_pontos = (
                dfx.loc[mask_pts]
                   .assign(Jog=lambda x: pd.to_numeric(x["player_number"], errors="coerce").astype("Int64"))
                   .groupby("Jog", dropna=False).size().rename("Pontos").reset_index()
                   .sort_values(["Pontos","Jog"], ascending=[False, True])
            )
            display_dataframe(tbl_pontos, height=160, use_container_width=True)
        # Erros (Nossos)
        st.markdown("**⚠️ Erros (Nossos)**")
        if df_set is not None and not df_set.empty:
            dfx = _norm_cols_for_summary(df_set)
            mask_err = (dfx["who_scored"]=="ADV") & (dfx["result"]=="ERRO")
            tbl_erros = (
                dfx.loc[mask_err]
                   .assign(Jog=lambda x: pd.to_numeric(x["player_number"], errors="coerce").astype("Int64"))
                   .groupby("Jog", dropna=False).size().rename("Erros").reset_index()
                   .sort_values(["Erros","Jog"], ascending=[False, True])
            )
            display_dataframe(tbl_erros, height=160, use_container_width=True)
        # Histórico (sequência de rallies) - set atual inteiro (compacto)
        st.markdown("**🕒 Histórico (sequência de rallies)**")
        # ---- KPIs adicionais e gráficos ----
        st.markdown("---")
        st.subheader("📊 KPIs e Análises do Set")
        if df_set is not None and not df_set.empty:
            dfA = df_set.copy()
            if "result" in dfA.columns:
                dfA["result"] = dfA["result"].astype(str).str.upper()
            if "who_scored" in dfA.columns:
                dfA["who_scored"] = dfA["who_scored"].astype(str).str.upper()

            mask_pontos = (dfA["who_scored"] == "NOS") & (dfA["result"] == "PONTO")
            mask_erros  = (dfA["who_scored"] == "ADV") & (dfA["result"] == "ERRO")

            # Top jogadoras – Pontos (NOS)
            if "player_number" in dfA.columns:
                sc_p = (dfA[mask_pontos]
                        .groupby("player_number").size().sort_values(ascending=False).head(10))
                er_p = (dfA[mask_erros]
                        .groupby("player_number").size().sort_values(ascending=False).head(10))
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown("**Top jogadoras – Pontos (NOS)**")
                    bar_chart_safe(sc_p)
                with c2:
                    st.markdown("**Top jogadoras – Erros (NOS)**")
                    bar_chart_safe(er_p)

            # Fundamentos com mais acertos/erros (NOS e ADV)
            if "action" in dfA.columns:
                c3, c4 = st.columns(2)
                with c3:
                    st.markdown("**Fundamentos – Acertos (NOS vs ADV)**")
                    # Definir ok_mask para filtrar ações válidas
                    ok_mask = dfA["action"].notna() & (dfA["action"] != "")
                    f_ok = (dfA[ok_mask]
                            .groupby(["action","who_scored"]).size()
                            .unstack(fill_value=0))
                    bar_chart_safe(f_ok, rotate_xticks=30)
                with c4:
                    st.markdown("**Fundamentos – Erros (NOS vs ADV)**")
                    # Usar a mesma máscara já definida
                    f_er = (dfA[ok_mask]
                            .groupby(["action","who_scored"]).size()
                            .unstack(fill_value=0))
                    bar_chart_safe(f_er, rotate_xticks=30)

            # Mapa simples por posição/região (contagem)
            if "position" in dfA.columns:
                st.markdown("**Distribuição por região (conteúdo bruto)**")
                pos_ct = dfA.groupby(["position","who_scored"]).size().unstack(fill_value=0)
                bar_chart_safe(pos_ct, rotate_xticks=30)

        if df_set is not None and not df_set.empty:
            hist = df_set.copy()
            cols_hist = []
            for c in ["rally_no","player_number","action","result","who_scored","score_home","score_away"]:
                if c in hist.columns: cols_hist.append(c)
            hist = hist[cols_hist].rename(columns={
                "rally_no":"#","player_number":"Jog","action":"Ação",
                "result":"Resultado","who_scored":"Quem","score_home":"H","score_away":"A"
            })
            display_dataframe(hist, height=220, use_container_width=True)
        if show_debug_ui() and st.session_state.get("dbg_prints"):
            st.markdown("---")
            st.markdown("**🧰 Debug (logs recentes)**")
st.markdown(f"_arquivo: {LOGS_DIR / 'uv_saves.log'}_")
st.code("\\n".join(st.session_state["dbg_prints"][-40:]), language="text")
st.markdown('</div>', unsafe_allow_html=True)


st.markdown(f"_arquivo: {LOGS_DIR / 'uv_saves.log'}_")
try:
    _items = st.session_state.get("dbg_prints", [])[-40:]
    lines = []
    for it in _items:
        ts = it.get("ts", "-")
        reason = it.get("reason", "-")
        status = " | ".join(map(str, it.get("status", [])))
        lines.append(f"{ts} — {reason} | {status}")
    if lines:
        st.code("\n".join(lines), language="text")
except Exception:
    pass


# =========================
# Boot para Render
# =========================
if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 10000))
    if not st.session_state.get("_boot_rerun_done", False):
        st.session_state["_boot_rerun_done"] = True
        st.rerun()

    # Google Sheets (opcional)
    try:
        if gsheets_sync.is_enabled():
            status = gsheets_sync.sync_all(frames)
            _logger.info(status)
    except Exception as e:
        try:
            _logger.error(f"GSHEETS falhou: {e}")
        except Exception:
            pass
