from __future__ import annotations
from pathlib import Path
import re
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib as mpl
import streamlit.components.v1 as components
import html
import numpy as np
import time as _time
from datetime import date, datetime
import pandas as pd
import pandas as _pd_real  # não conflitar com _pd já usado
from db_duck import ensure_db as duck_ensure, replace_all as duck_replace
import logging, os
import gsheets_sync
from io import BytesIO
from parser_free import parse_line
from string import Template
import datetime as _dt, pandas as _pd, json as _json
import configparser, os as _os
from pathlib import Path as _P
import duckdb as _duck
import os
import gspread
import base64
from pathlib import Path
import base64, mimetypes, os
from google.oauth2.service_account import Credentials
from db_excel import (
    init_or_load, save_all, add_set,
    append_rally, last_open_match, finalize_match
)   
    
# --- UV Excel engine monkey-patch (garante engine ao abrir Excel) ---
def _uv_pick_engine(_path_str: str):
    _s = str(_path_str).lower()
    if _s.endswith(('.xlsx','.xlsm','.xltx','.xltm')): return 'openpyxl'
    if _s.endswith('.xls'): return 'xlrd'
    if _s.endswith('.xlsb'): return 'pyxlsb'
    if _s.endswith('.ods'): return 'odf'
    return None

_pd_ExcelFile_original = _pd_real.ExcelFile
def _pd_ExcelFile_patched(path, *args, **kwargs):
    if 'engine' not in kwargs:
        _eng = _uv_pick_engine(path)
        if _eng: kwargs['engine'] = _eng
    return _pd_ExcelFile_original(path, *args, **kwargs)
_pd_real.ExcelFile = _pd_ExcelFile_patched
# --------------------------------------------------------------------
# === UV STATE & ACTIONS (aplicado aos botões reais) ===
def uv_init_state():
    st.session_state.setdefault('uv_active_player', None)      # número ativo
    st.session_state.setdefault('uv_player_state', {})         # {num:'neutral'|'ok'|'err'}
    st.session_state.setdefault('uv_adv_state', 'neutral')     # 'neutral'|'ok'|'err'
    st.session_state.setdefault('uv_last_action', None)
    st.session_state.setdefault('q_result', st.session_state.get('q_result', 'Acerto'))

def uv_set_player(n:int):
    uv_init_state()
    cur = st.session_state['uv_active_player']
    states = st.session_state['uv_player_state']
    if cur is None or cur != n:
        if cur is not None:
            states[cur] = 'neutral'
        st.session_state['uv_active_player'] = n
        states[n] = 'ok'  # 1º clique -> OK
    else:
        prev = states.get(n, 'neutral')
        states[n] = 'err' if prev == 'ok' else 'ok'
    # integrações já usadas
    st.session_state['last_selected_player'] = st.session_state['uv_active_player']
    st.session_state['q_side'] = 'Nós'

def uv_toggle_adv():
    uv_init_state()
    st.session_state['uv_adv_state'] = 'err' if st.session_state['uv_adv_state'] == 'ok' else 'ok'
    st.session_state['q_side'] = 'Adv'

def uv_reset_all():
    st.session_state['uv_active_player'] = None
    st.session_state['uv_player_state'] = {}
    st.session_state['uv_adv_state'] = 'neutral'
    st.session_state['uv_last_action'] = None

def uv_apply_result_from_state():
    """Ajusta q_result conforme jogadora ativa (prioridade) ou ADV."""
    uv_init_state()
    player = st.session_state['uv_active_player']
    pstate = st.session_state['uv_player_state'].get(player or -1, 'neutral')
    adv_state = st.session_state['uv_adv_state']
    if player is not None and pstate in ('ok','err'):
        st.session_state['q_result'] = 'Acerto' if pstate == 'ok' else 'Erro'
    elif adv_state in ('ok','err'):
        st.session_state['q_result'] = 'Acerto' if adv_state == 'ok' else 'Erro'
    # senão, deixa como está

def gm_quick_click(code: str):
    """Wrapper para ações rápidas reais: aplica resultado via estado e registra."""
    uv_apply_result_from_state()
    register_current(action=code)

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
BACKUP_DIRS = [BACKUP_DIR]  # mantém o comportamento local

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
SAVE_TO_GOOGLE = True  # ou False para desativar
QTD_PONTOS_SALVAR_GOOGLE = 5  # a cada N pontos (rallies) salva no Google; 0 desativa
os.environ["STREAMLIT_SERVER_FILE_WATCHER"] = "false"
# =========================# =========================# =========================
#SALVAMENTOS ATUAIS -> NA _persist_all
# 1) Salva Excel sempre; DuckDB e Google Sheets quando possível.
    # 2) Backup Excel (**** somente em set/match **** ) — local ./backups
    # 3) DuckDB (se disponível)
    # 4) Google Sheets (condicional por flag e intervalo de pontos)
    # 5) Log final + prints na UI Log final + prints na UI
#VER ESSE # [2] Offline: Journal append-only (NDJSON) por lance
# =========================
# =========================
# DEBUG: console + UI
# =========================
DEBUG_PRINTS = False  # se False, somem os blocos de debug da UI também
_logger = logging.getLogger("uv_persist")
_logger.addHandler(logging.NullHandler())  # não emite nada por padrão, apenas se DEBUG_PRINTS for True

def debug_print(*args, **kwargs):
    if DEBUG_PRINTS:
        print("[UV-DEBUG]", *args, **kwargs, flush=True)
        _logger = logging.getLogger("uv_persist")    
        st.session_state.setdefault("QTD_PONTOS_SALVAR_GOOGLE", QTD_PONTOS_SALVAR_GOOGLE)

    # Adicione temporariamente no sidebar para testar
    if st.sidebar.button("Testar Autenticação Google"):
        debug_google_auth()
        
    # Chame esta função em algum lugar para testar
    debug_google_auth()
    if st.sidebar.button("🔧 Testar Google Sheets"):
        if debug_google_auth():
            st.success("✅ Google Sheets configurado corretamente!")
        else:
            st.error("❌ Problema na configuração do Google Sheets")

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
        
        st.sidebar.caption(f"Excel: {st.session_state.get('db_path','(definir)')}")
        st.sidebar.caption(f"DuckDB: {st.session_state.get('duck_path','(definir)')}")
        st.sidebar.caption(f"Último salvamento: {st.session_state.get('last_save_at','-')}")
        try:
            st.sidebar.caption(f"Webhook: {'on' if CONFIG['online'].get('webhook_url') else 'off'}")
        except Exception:
            pass

def show_debug_ui() -> bool:
    return bool(DEBUG_PRINTS)
# =========================
# --- Debug helper (safe) ---
# =========================
def dbg_print(*args, **kwargs):
    """Prints to Streamlit only if st.session_state.get('debug', False) is True; logs always if _logger exists."""
    if DEBUG_PRINTS:
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

def _perf_commit(perf, statuses):
    perf["statuses"] = list(statuses)
    perf["ts"] = _dt.datetime.now().strftime("%H:%M:%S")

    lst = st.session_state.get("perf_logs", [])
    lst.append(perf)
    if len(lst) > 60:
        lst = lst[-60:]
    st.session_state["perf_logs"] = lst

def _render_latency_panel(max_rows: int = 12):
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
        display_dataframe(df, height=220, width='stretch')
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
    #_cp = configparser.ConfigParser(interpolation=None)
    _cp = configparser.ConfigParser(interpolation=None); _cp.read(_cfg_path, encoding="utf-8")
    _cp.read(_cfg_path, encoding="utf-8")
    # atualiza todas as seções presentes, criando chaves conforme necessário
    for sec in _cp.sections():
        CONFIG.setdefault(sec, {}).update({k: v for k, v in _cp.items(sec)})
    for sec in ("online","backup","secrets","gcp"):
        if _cp.has_section(sec):
            CONFIG[sec].update({k: v for k,v in _cp.items(sec)})
# =========================
# Config GOOGLE_DRIVE
# =========================
# >>> Fixos solicitados por você (podem ser sobrescritos por env/secrets se quiser no futuro)
GOOGLE_DRIVE_ROOT_FOLDER_URL = "https://drive.google.com/drive/folders/10PDkcUb4yGhrEmiNKwNo7mzZBGJIN_5r"
GOOGLE_SHEETS_SPREADSHEET_ID = "1FLBTjIMAgQjGM76XbNZT3U_lIDGUsWQbea2QCmdXbYI"
GOOGLE_WEBHOOK_URL = ""
# >>> Valores devem vir SOMENTE do config.ini

# >>> SE QUISER PUXAR DO CONFIG.INI ->> MAS ESTAVA DANDO ERRO AO SUBIR NO STREAMLIT, GIT, ETC...
#GOOGLE_DRIVE_ROOT_FOLDER_URL   = CONFIG["backup"].get("drive_folder_url", "")
#GOOGLE_SHEETS_SPREADSHEET_ID   = CONFIG["online"].get("gsheet_id", "")

# Propaga para CONFIG (sem ENV/secrets; apenas reafirma o que veio do config.ini)
CONFIG["online"]["webhook_url"]      = GOOGLE_WEBHOOK_URL#CONFIG["online"].get("webhook_url", "")
CONFIG["online"]["gsheet_id"]        = GOOGLE_SHEETS_SPREADSHEET_ID#CONFIG["online"].get("gsheet_id", "")
CONFIG["backup"]["drive_folder_url"] = GOOGLE_DRIVE_ROOT_FOLDER_URL#CONFIG["backup"].get("drive_folder_url", "")

# =========================
# Config + Estilos
# =========================
st.set_page_config(page_title="Vôlei Scout – UniVolei", layout="wide", initial_sidebar_state="collapsed")

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

# =========================
# Chamadas GOOGLE_DRIVE
# =========================
def _normalize_gsheet_id(raw_id):
    """
    Normaliza ID da planilha - versão corrigida e mais permissiva
    """
    if not raw_id:
        return ""
    
    s = str(raw_id).strip()
    print(f"🔍 [NORMALIZE DEBUG] Input: '{s}'")
    
    # Se for URL completa, extrai o ID
    if "spreadsheets/d/" in s:
        parts = s.split("spreadsheets/d/")
        if len(parts) > 1:
            id_part = parts[1].split("/")[0]
            print(f"🔍 [NORMALIZE DEBUG] Extraído de URL: '{id_part}'")
            
            # Verificação básica de ID válido
            if id_part and len(id_part) >= 10:
                return id_part
    
    # Se já parece um ID (mais permissivo)
    if s and len(s) >= 10:
        # Remove caracteres problemáticos mas mantém o ID
        clean_id = ''.join(c for c in s if c.isalnum() or c in '_-')
        if clean_id:
            print(f"🔍 [NORMALIZE DEBUG] ID limpo: '{clean_id}'")
            return clean_id
    
    print(f"🔍 [NORMALIZE DEBUG] ID inválido, retornando vazio")
    return ""

def _get_spreadsheet_id():
    """
    Obtém o ID da planilha - PRIORIZA a variável fixa
    """
    # PRIMEIRO tenta a variável fixa
    fixed_id = GOOGLE_SHEETS_SPREADSHEET_ID
    if fixed_id and len(fixed_id) >= 10:
        normalized = _normalize_gsheet_id(fixed_id)
        if normalized:
            print(f"🔍 [GET_ID] Usando ID FIXO: {normalized}")
            return normalized
    
    # DEPOIS tenta outras fontes (fallback)
    try:
        # Streamlit secrets
        if hasattr(st, 'secrets'):
            if 'gsheet_id' in st.secrets:
                id_from_secrets = _normalize_gsheet_id(st.secrets['gsheet_id'])
                if id_from_secrets:
                    print(f"🔍 [GET_ID] Usando Secrets: {id_from_secrets}")
                    return id_from_secrets
            if 'online' in st.secrets and 'gsheet_id' in st.secrets['online']:
                id_from_secrets = _normalize_gsheet_id(st.secrets['online']['gsheet_id'])
                if id_from_secrets:
                    print(f"🔍 [GET_ID] Usando Secrets online: {id_from_secrets}")
                    return id_from_secrets
        
        # config.ini
        from pathlib import Path
        import configparser
        
        config_path = Path(__file__).parent / "config.ini"
        if config_path.exists():
            config = configparser.ConfigParser()
            config.read(config_path)
            if config.has_section('online'):
                gsheet_id = config.get('online', 'gsheet_id', fallback='').strip()
                id_from_config = _normalize_gsheet_id(gsheet_id)
                if id_from_config:
                    print(f"🔍 [GET_ID] Usando config.ini: {id_from_config}")
                    return id_from_config
                
    except Exception as e:
        print(f"🔍 [GET_ID] Erro ao obter ID de outras fontes: {e}")
    
    print("🔍 [GET_ID] Nenhum ID válido encontrado")
    return None

def debug_gsheet_validation():
    """
    Função temporária para debug
    """
    spreadsheet_id = _get_spreadsheet_id()
    print(f"🔍 [DEBUG] ID obtido: '{spreadsheet_id}'")
    print(f"🔍 [DEBUG] Tipo: {type(spreadsheet_id)}")
    print(f"🔍 [DEBUG] Tamanho: {len(spreadsheet_id) if spreadsheet_id else 0}")
    
    # Testa a normalização
    normalized = _normalize_gsheet_id(spreadsheet_id)
    print(f"🔍 [DEBUG] Normalizado: '{normalized}'")
    
    return normalized

def sync_all(frames):
    """
    Sincroniza todos os frames com Google Sheets - versão corrigida
    Retorna string de status.
    """
    try:
        if not is_enabled():
            return "Google Sheets não habilitado"
        
        spreadsheet_id = _get_spreadsheet_id()
        
        # DEBUG - Log para verificar o ID
        print(f"🔍 [GSHEETS DEBUG] ID obtido: '{spreadsheet_id}'")
        
        if not spreadsheet_id:
            return "ID da planilha não configurado"
        
        # Verificação mais permissiva do ID
        if len(spreadsheet_id) < 10 or not all(c.isalnum() or c in '_-' for c in spreadsheet_id):
            return f"GSHEETS: ID inválido - '{spreadsheet_id}'"
        
        print(f"🔍 [GSHEETS DEBUG] ID validado: {spreadsheet_id}")
        
        client = _get_gspread_client()
        if not client:
            return "GSHEETS: erro (gspread/credenciais ausentes)"

        # Tenta abrir a planilha
        try:
            print(f"🔍 [GSHEETS DEBUG] Tentando abrir planilha...")
            sh = client.open_by_key(spreadsheet_id)
            print(f"🔍 [GSHEETS DEBUG] Planilha aberta: {sh.title}")
        except Exception as e:
            error_msg = str(e)
            print(f"🔍 [GSHEETS DEBUG] Erro ao abrir: {error_msg}")
            
            if "This operation is not supported for this document" in error_msg:
                return ("GSHEETS: erro — o gsheet_id aponta para um item que NÃO é uma "
                        "planilha do Google Sheets (pode ser Doc/Slide/Pasta/XLSX). "
                        "Use o ID de uma planilha nativa: /spreadsheets/d/<ID>/edit")
            elif "not found" in error_msg.lower() or "Unable to open spreadsheet" in error_msg:
                return f"GSHEETS: erro — planilha não encontrada. Verifique o ID: {spreadsheet_id}"
            else:
                return f"GSHEETS: erro ao abrir planilha ({error_msg})"

        # Sincroniza cada frame
        success_count = 0
        error_messages = []
        
        for tab_name, df in frames.items():
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            try:
                # Normaliza nome da aba
                ws_title = str(tab_name)[:95].replace("/", "_").replace("\\", "_").replace(":", " ")
                print(f"🔍 [GSHEETS DEBUG] Processando aba: {ws_title}")

                # Tenta abrir a worksheet; se não existir, cria
                try:
                    ws = sh.worksheet(ws_title)
                    print(f"🔍 [GSHEETS DEBUG] Aba existente: {ws_title}")
                except Exception:
                    print(f"🔍 [GSHEETS DEBUG] Criando nova aba: {ws_title}")
                    ws = sh.add_worksheet(
                        title=ws_title, 
                        rows=max(1000, len(df) + 100), 
                        cols=max(26, len(df.columns) + 10)
                    )

                # Limpa a worksheet completamente
                ws.clear()
                print(f"🔍 [GSHEETS DEBUG] Aba limpa: {ws_title}")

                # Prepara valores: cabeçalho + dados
                values = [df.columns.tolist()]  # Cabeçalho
                if not df.empty:
                    # Converte dados para string, tratando NaN
                    df_strings = df.astype(object).where(pd.notna(df), "").astype(str)
                    values.extend(df_strings.values.tolist())
                
                print(f"🔍 [GSHEETS DEBUG] Dados preparados: {len(values)} linhas, {len(values[0]) if values else 0} colunas")

                # Ajusta tamanho se a API permitir
                try:
                    rows_needed = max(100, len(values) + 50)
                    cols_needed = max(26, len(values[0]) if values else 10)
                    ws.resize(rows=rows_needed, cols=cols_needed)
                    print(f"🔍 [GSHEETS DEBUG] Tamanho ajustado: {rows_needed} rows, {cols_needed} cols")
                except Exception as resize_error:
                    print(f"🔍 [GSHEETS DEBUG] Não foi possível ajustar tamanho: {resize_error}")

                # Envia dados para o Google Sheets
                if values:
                    print(f"🔍 [GSHEETS DEBUG] Enviando dados...")
                    ws.update("A1", values, value_input_option="RAW")
                    print(f"🔍 [GSHEETS DEBUG] Dados enviados com sucesso")
                
                success_count += 1
                print(f"🔍 [GSHEETS DEBUG] ✅ Aba {ws_title} sincronizada")

            except Exception as e:
                error_msg = f"Erro na aba '{ws_title}': {str(e)}"
                error_messages.append(error_msg)
                print(f"🔍 [GSHEETS DEBUG] ❌ {error_msg}")

        # Prepara resultado final
        if success_count > 0 and not error_messages:
            result = f"GSHEETS: ok ({success_count} abas) -> {spreadsheet_id}"
        elif success_count > 0 and error_messages:
            result = f"GSHEETS: parcial ({success_count} ok, {len(error_messages)} erros) -> {spreadsheet_id}. Erros: {'; '.join(error_messages[:3])}"
        else:
            result = f"GSHEETS: falha total -> {spreadsheet_id}. Erros: {'; '.join(error_messages[:3])}"
        
        print(f"🔍 [GSHEETS DEBUG] Resultado final: {result}")
        return result

    except Exception as e:
        error_msg = f"GSHEETS: erro geral ({str(e)})"
        print(f"🔍 [GSHEETS DEBUG] ❌ ERRO GERAL: {error_msg}")
        return error_msg
    
def _get_gspread_client():
    """
    Retorna cliente gspread autenticado - PRIORIZA Streamlit Secrets
    """
    try:        
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        # 1) PRIMEIRO: Streamlit Secrets (funciona no Streamlit Cloud)
        try:
            if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
                sa_info = dict(st.secrets["gcp_service_account"])
                print("🔍 [AUTH] Usando Streamlit Secrets")
                creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
                return gspread.authorize(creds)
        except Exception as e:
            print(f"🔍 [AUTH] Erro com Streamlit Secrets: {e}")

        # 2) SEGUNDO: Variável de ambiente (fallback)
        try:
            cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
            if cred_path and os.path.exists(cred_path):
                print(f"🔍 [AUTH] Usando variável de ambiente: {cred_path}")
                creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
                return gspread.authorize(creds)
        except Exception as e:
            print(f"🔍 [AUTH] Erro com variável de ambiente: {e}")

        # 3) TERCEIRO: config.ini (último recurso)
        try:
            gcp_cfg = CONFIG.get("gcp", {})
            mode = (gcp_cfg.get("credentials_mode") or "").strip().lower()
            if mode == "path":
                cpath = (gcp_cfg.get("credentials_path") or "").strip()
                if cpath and os.path.exists(cpath):
                    print(f"🔍 [AUTH] Usando config.ini path: {cpath}")
                    creds = Credentials.from_service_account_file(cpath, scopes=scopes)
                    return gspread.authorize(creds)
            elif mode == "inline":
                inline = gcp_cfg.get("inline_json")
                if inline:
                    print("🔍 [AUTH] Usando config.ini inline JSON")
                    sa_info = _json.loads(inline)
                    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
                    return gspread.authorize(creds)
        except Exception as e:
            print(f"🔍 [AUTH] Erro com config.ini: {e}")

        print("🔍 [AUTH] Nenhum método de autenticação funcionou")
        return None
        
    except Exception as e:
        print(f"🔍 [AUTH] Erro geral: {e}")
        return None

def debug_google_auth():
    """Debug detalhado da autenticação Google"""
    try:
        st.sidebar.subheader("🔧 Debug Google Auth")
        
        # Verifica secrets
        if hasattr(st, 'secrets'):
            st.sidebar.write("✅ Streamlit Secrets disponível")
            
            if 'gcp_service_account' in st.secrets:
                sa = st.secrets['gcp_service_account']
                st.sidebar.write(f"✅ Service Account encontrado")
                st.sidebar.write(f"📧 Client Email: {sa.get('client_email', 'Não encontrado')}")
                st.sidebar.write(f"🆔 Project ID: {sa.get('project_id', 'Não encontrado')}")
                
                # Verifica private_key
                pk = sa.get('private_key', '')
                if pk:
                    st.sidebar.write(f"🔑 Private Key: {len(pk)} caracteres")
                    if 'BEGIN PRIVATE KEY' in pk:
                        st.sidebar.write("✅ Formato da private_key parece correto")
                    else:
                        st.sidebar.write("❌ Formato da private_key pode estar errado")
            else:
                st.sidebar.write("❌ gcp_service_account não encontrado")
        else:
            st.sidebar.write("❌ Streamlit Secrets não disponível")
        
        # Testa autenticação
        client = _get_gspread_client()
        if client:
            st.sidebar.success("✅ Autenticação Google Sheets: SUCESSO")
            return True
        else:
            st.sidebar.error("❌ Autenticação Google Sheets: FALHOU")
            return False
            
    except Exception as e:
        st.sidebar.error(f"❌ Erro no debug: {e}")
        return False
    
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
# Quadra BTNS
# =========================
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

      // pinta botões por texto
      function paint(){
        var doc;
        try{
          doc = (window.parent && window.parent.document) ? window.parent.document : document;
        }catch(e){
          doc = document;
        }
        const map = [
          {text:'adv',        bg:'rgba(255,0,255,0.20)', border:'rgba(160,0,160,0.55)'},
          {text:'rede',       bg:'rgba(220,50,50,0.18)', border:'rgba(160,20,20,0.55)'},
          {text:'refazer rally', bg:'#b91c1c', border:'#7f1d1d'},

          // <<< NOVO: desfazer igual ao "rede" (fundo vermelho)
          {text:'desfazer',   bg:'rgba(220,50,50,0.18)', border:'rgba(160,20,20,0.55)'},
          {text:'↩️Desfazer',   bg:'rgba(220,50,50,0.18)', border:'rgba(160,20,20,0.55)'},
          {text:'↩️',         bg:'rgba(220,50,50,0.18)', border:'rgba(160,20,20,0.55)'},
          {text:'↩',          bg:'rgba(220,50,50,0.18)', border:'rgba(160,20,20,0.55)'}
        ];
        const btns = Array.from(doc.querySelectorAll('button'));
        btns.forEach(b=>{
          const t = (b.textContent || '').trim().toLowerCase();
          map.forEach(m=>{
            if (t === m.text){
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

# ---- Chamada inicial:  onde você lê o html da automação de espaçamento
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
# === Chamada inicial: CSS anti-gap para QUALQUER iframe de components.html ===
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
# Chamada inicial: Figuras compactas
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
def small_fig(w=3.2, h=1.8):
    # >>> CORREÇÃO: Garantir dimensões mínimas seguras
    w = max(0.1, w)  # Mínimo de 0.1 em vez de 1.0
    h = max(0.1, h)  # Mínimo de 0.1 em vez de 1.0
    
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
    fig, ax = small_fig(4.0, 2.0)

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
def display_dataframe(df, height=None, width='content', extra_class: str = ""):
    if df is None or len(df) == 0:
        st.write("_Sem dados._"); return
    classes = ('custom-table ' + extra_class).strip()
    html_table = df.to_html(classes=classes, index=False, escape=False)

    height_css = f"{int(height)}px" if isinstance(height, (int, float)) else "auto"
    width_css = "100%" if width == 'stretch' else "auto"  # CORREÇÃO AQUI

    styled_html = f"""
    <div style="overflow:auto; height:{height_css}; width:{width_css};">
        {html_table}
    </div>
    """
    st.markdown(styled_html, unsafe_allow_html=True)

# =========================
# Chamada inicial: Variáveis Estado/Base
# =========================
DEFAULT_DB = str(BASE_DIR / "volei_base_dados.xlsx")
if "db_path" not in st.session_state: st.session_state.db_path = DEFAULT_DB
if "frames" not in st.session_state:
    # --- Guard robusto p/ carregar Excel base ---
    import zipfile as _zip
    try:
        _dbp = Path(st.session_state.db_path)
        if not _dbp.exists():
            st.error(f"Arquivo Excel base não encontrado: {_dbp}")
            st.stop()
        if _dbp.is_dir():
            st.error(f"O caminho do Excel aponta para uma pasta: {_dbp}")
            st.stop()
        if _dbp.suffix.lower() not in (".xlsx",".xlsm",".xltx",".xltm",".xls",".xlsb",".ods"):
            st.error(f"O caminho do Excel não parece válido: {_dbp.name}")
            st.info("Use um arquivo .xlsx/.xls/.xlsb/.ods e garanta que o engine correspondente está instalado (ex.: openpyxl para .xlsx).")
            st.stop()
        # Tenta carregar normalmente
        st.session_state.frames = init_or_load(_dbp)
        st.session_state["db_path_in_use"] = str(_dbp)
    except (ValueError, ImportError, _zip.BadZipFile) as _e:
        _msg = str(_e)
        # Se engine ausente
        if "Missing optional dependency 'openpyxl'" in _msg or "Excel file format cannot be determined" in _msg:
            st.error("Falha ao abrir o Excel base: falta engine. Para .xlsx, adicione 'openpyxl' ao requirements.txt.")
            st.stop()
        # Se arquivo corrompido/inválido (.xlsx precisa ser um ZIP válido)
        _need_backup = isinstance(_e, _zip.BadZipFile) or "File is not a zip file" in _msg
        if _need_backup:
            # Tenta restaurar a partir do backup mais recente
            _bdir = Path(st.session_state.get("backups_dir", "backups"))
            if _bdir.exists():
                _cands = sorted(_bdir.glarg("*.xlsx"), reverse=True)
            else:
                _cands = []
            _restored = False
            for _bf in _cands:
                try:
                    _frames = init_or_load(_bf)
                    st.session_state.frames = _frames
                    st.session_state["db_path_in_use"] = str(_bf)
                    st.warning(f"Arquivo base parece corrompido ({_dbp.name}). Carregado backup: {_bf.name}")
                    _restored = True
                    break
                except Exception:
                    pass
            if not _restored:
                st.error("Arquivo Excel base inválido/corrompido e nenhum backup válido encontrado. Substitua o arquivo base ou verifique permissões.")
                st.stop()
        else:
            # Outros erros são propagados
            raise




if "duck_path" not in st.session_state: st.session_state.duck_path = str(DEFAULT_DUCK)
if "match_id" not in st.session_state: st.session_state.match_id = None
if "set_number" not in st.session_state: st.session_state.set_number = None
if "auto_close" not in st.session_state: st.session_state.auto_close = True
if "graph_filter" not in st.session_state: st.session_state.graph_filter = "Ambos"
st.session_state.setdefault("data_rev", 0)
options = ["Quem Sacou"]  # ou ["Front", "Back"], etc.
st.session_state.setdefault("quemsacou", options[0])
# auxiliares
st.session_state.setdefault("q_side", "Nós")
st.session_state.setdefault("q_result", "Acerto")
st.session_state.setdefault("q_action", "d")
st.session_state.setdefault("q_position", "Front")
st.session_state.setdefault("last_selected_player", None)
st.session_state.setdefault("show_cadastro", False)
st.session_state.setdefault("show_tutorial", False)
st.session_state.setdefault("show_config_team", False)
st.session_state.setdefault("line_input_text", "")
st.session_state.setdefault("perf_logs", [])
# Heatmap / clique
st.session_state.setdefault("last_court_click", None)   # {"x":float,"y":float,"ts":int}
st.session_state.setdefault("heatmap_debug", True)
st.session_state.setdefault("show_heat_numbers", True)
# garantias de estado
st.session_state.setdefault("game_mode", False)
st.session_state.setdefault("player_label_mode", "Número")
st.session_state.setdefault("btn_label_mode", "Número")
st.session_state.setdefault("_do_rerun_after", False)
# =========== Debug/prints (em memória) ===========
st.session_state.setdefault("dbg_prints", [])

# === Chamada inicial:  garante estado de jogadoras/ADV antes de renderizar UI ===
uv_init_state()
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

# =========================
# Pre-Loader overlay central (texto no meio da tela) 
# =========================
class _UvOverlayPreloader:
    """
    Context manager para exibir um overlay de "processando" sem bagunçar a UI.
    Uso: with _UvOverlayPreloader("Salvando..."): ...operações...
    """
    def __init__(self, message: str = "Processando..."):
        self.message = message
        self._ph = None  # placeholder para permitir limpar o overlay sem depender de rerun

    def __enter__(self):
        import streamlit as st
        st.session_state["_uv__overlay_active"] = True
        self._ph = st.empty()  # <<< render controlado

        overlay_css = """
        <style>
        .uv-preloader-backdrop {
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0,0,0,0.35);
            z-index: 999999;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .uv-preloader-card {
            background: #fff;
            border-radius: 14px;
            padding: 22px 28px;
            box-shadow: 0 10px 30px rgba(0,0,0,.25);
            font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji";
            text-align: center;
            max-width: 420px;
        }
        .uv-spinner {
            width: 28px; height: 28px;
            border: 3px solid #e3e3e3; border-top-color: #4a90e2;
            border-radius: 50%;
            animation: uvspin 0.9s linear infinite;
            margin: 0 auto 12px auto;
        }
        @keyframes uvspin { to { transform: rotate(360deg); } }
        </style>
        """
        overlay_html = f'''
        <div class="uv-preloader-backdrop">
          <div class="uv-preloader-card">
            <div class="uv-spinner"></div>
            <div><strong>{self.message}</strong></div>
          </div>
        </div>
        '''
        # Renderiza o overlay dentro do placeholder
        self._ph.markdown(overlay_css + overlay_html, unsafe_allow_html=True)
        return self

    def __exit__(self, exc_type, exc, tb):
        import streamlit as st
        st.session_state["_uv__overlay_active"] = False
        try:
            if self._ph is not None:
                self._ph.empty()  # <<< remove o overlay imediatamente, sem precisar de rerun
        except Exception:
            pass
        return False

# UniVolei Live Scout – index.py (versão completa e estável)

def uv_preloader(kind: str = "gs") -> _UvOverlayPreloader:
    print("@@@ FN uv_preloader kind ", kind)

    if DEBUG_PRINTS:
        """
        kind:
        - 'gs'          -> 'Salvando informações...'
        - 'set_close'   -> 'Fechar Set...'
        - 'set_open'    -> 'Fechar Set...'
        - 'match_close' -> 'Finalizar partida...'
        - outro         -> 'Processando...'
        """

    if kind in ("set_close", "set_open"):
        msg = "Fechando Set..."
    elif kind == "match_close":
        msg = "Finalizando partida..."
    elif kind == "gs":
        msg = "Salvando informações..."
    else:
        msg = "Processando..."
    return _UvOverlayPreloader(msg)

# =========================
# “Excel principal (sempre)” — grava o XLSX no caminho de st.session_state.db_path (por padrão algo como BASE_DIR/volei_base_dados.xlsx).
# Backup Excel ao fechar set/partida — gera um arquivo com timestamp em ./backups/volei_base_dados_YYYYMMDD_HHMMSS.xlsx.
# DuckDB — atualiza as tabelas no arquivo .duckdb.
# Google Sheets — quando a flag estiver habilitada
# =========================
def _persist_all(frames, reason: str = "rally"):
    # 1) Salva Excel sempre; DuckDB e Google Sheets quando possível.
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

    # 2) Backup Excel (**** somente em set/match **** ) — local ./backups
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

    # 4) Google Sheets (condicional por flag e intervalo de pontos)
    if SAVE_TO_GOOGLE:
        should_gs = False
        # calcula rcount e N uma vez
        try:
            rcount = int(frames.get("rallies", _pd.DataFrame()).shape[0])
        except Exception:
            rcount = 0
        N = int(st.session_state.get("QTD_PONTOS_SALVAR_GOOGLE", QTD_PONTOS_SALVAR_GOOGLE))
        print("********** CHAMA QTD_PONTOS_SALVAR_GOOGLE ", QTD_PONTOS_SALVAR_GOOGLE , " reason = ",reason)
        if reason in ("set_open", "set_close", "match_close", "manual"):
            should_gs = True
        elif reason == "rally":
            if N > 0 and rcount > 0 and (rcount % N == 0):
                should_gs = True
        print("********** reason ", reason , " N = ",N, " should_gs ",should_gs)
        if should_gs:
            # Define a mensagem do preloader conforme o motivo
            if reason in ("set_open", "set_close"):
                kind = "set_close"          # mostra "Fechar Set..."
            elif reason == "match_close":
                kind = "match_close"        # mostra "Finalizar partida..."
            else:
                kind = "gs"                 # "Salvando informações..." (ex.: a cada N pontos)

            print("********** if should_gs ", should_gs , " kind = ",kind)
        
            # Mostra o spinner durante a sincronização com o Google Sheets
            with uv_preloader(kind):
                salva_google(frames, reason, statuses, _perf)

            # Anexa contador ao último status do GS, se houver
            if statuses and statuses[-1].startswith("GSHEETS:"):
                statuses[-1] = f"{statuses[-1]} | pontos={rcount}/{N if N>0 else '-'}"
        else:
            statuses.append(
                f"GSHEETS: skip (aguardando intervalo de pontos) | pontos={rcount}/{N if N>0 else '-'}"
            )
    else:
        statuses.append("GSHEETS: skip (desativado por flag)")

    # 5) Log final + prints na UI Log final + prints na UI
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

    if DEBUG_PRINTS:
        try:
            st.write("💾 Persistência:", "; ".join(statuses))
        except Exception:
            pass

    # <<< encerra medição e guarda
    _perf_commit(_perf, statuses)

    return

def salva_google(frames=None, reason="manual", statuses=None, _perf=None):
    """
    Compatível com chamadas antigas (sem args) e novas (com frames, reason, statuses, _perf).
    - Se frames/statuses/_perf vierem None, a função cuida de obter/criar o necessário.
    - Registra métricas no _perf e faz fallback para webhook se GS falhar.
    """
    # Defaults para compatibilidade
    if frames is None:
        frames = st.session_state.get("frames", {})
    created_perf = False
    if _perf is None:
        _perf = _perf_begin(f"gs:{reason}")
        created_perf = True
    if statuses is None:
        statuses = []

    t = _time.perf_counter()
    ok_gs = False
    try:
        gs_status = _persist_to_gsheets(frames, reason)
        if gs_status:
            statuses.append(gs_status)
            _logger.info(gs_status)
            ok_gs = str(gs_status).startswith("GSHEETS: ok")
        else:
            statuses.append("GSHEETS: skip (sem gsheet_id)")
    except Exception as e:
        statuses.append(f"GSHEETS: falhou {e!s}")
        _logger.error(statuses[-1])
    finally:
        _perf_step(_perf, "GSHEETS", t)

        # Fallback: webhook se Google não persistiu
        if not ok_gs:
            t_wb = _time.perf_counter()
            try:
                wb = _persist_to_webhook(frames, reason)
                if wb:
                    statuses.append(wb)
                    _logger.info(wb)
            except Exception as e2:
                statuses.append(f"Webhook bloco falhou: {e2!s}")
                _logger.warning(statuses[-1])
            _perf_step(_perf, "WEBHOOK", t_wb)

        # Se criamos o _perf aqui (call antiga), também finalizamos aqui
        if created_perf:
            _perf_commit(_perf, statuses)

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

#Chamada inicial: tutorial e frames 
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
#Chamada inicial: Normalização jogadoras
if "jogadoras" in frames:
    frames["jogadoras"] = _normalize_jogadoras_df(frames["jogadoras"])
    st.session_state.frames = frames
# =========================
# HChamada inicial: elpers DB/lógica
# =========================
OUR_TEAM_ID = 1
# Ações
ACTION_CODE_TO_NAME = {
    "d": "DIAGONAL","l": "LINHA","m": "MEIO","larg": "LOB","seg": "SEGUNDA",
    "pi": "PIPE","re": "RECEPÇÃO","b": "BLOQUEIO","sa": "SAQUE","rede": "REDE"
}
ACTION_SYNONYM_TO_NAME = {
    "diagonal":"DIAGONAL","diag":"DIAGONAL",
    "linha":"LINHA","paralela":"LINHA",
    "meio":"MEIO",
    "largada":"LOB","larg":"LOB",
    "segunda":"SEGUNDA","seg":"SEGUNDA",
    "pipe":"PIPE","pi":"PIPE",
    "recepcao":"RECEPÇÃO","recepção":"RECEPÇÃO","re":"RECEPÇÃO",
    "bloqueio":"BLOQUEIO","bloq":"BLOQUEIO","b":"BLOQUEIO",
    "saque":"SAQUE","sa":"SAQUE",
    "rede":"REDE"
}
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
    set_number = int(st.session_state.set_number)
    winner_id = 1 if home_pts > away_pts else 2

    # Marca vencedor do set atual (e garante os pontos finais registrados na linha do set)
    stf = frames["sets"]
    mask = (stf["match_id"] == match_id) & (stf["set_number"] == set_number)
    stf.loc[mask, "winner_team_id"] = winner_id
    if "home_points" in stf.columns and "away_points" in stf.columns:
        stf.loc[mask, "home_points"] = int(home_pts)
        stf.loc[mask, "away_points"] = int(away_pts)
    frames["sets"] = stf

    # Atualiza sets da partida e persiste fechamento do set atual
    home_sets, away_sets = update_sets_score_and_match(frames, match_id)
    save_all(Path(st.session_state.db_path), frames)

    # (melhor esforço) journal
    try:
        _journal_write(frames, reason="set_close")
    except Exception:
        try:
            _logger.warning("journal hook falhou em set_close")
        except Exception:
            pass

    # Se a partida encerrou em sets (ex.: 3x0, 3x1, 3x2)
    if home_sets >= 3 or away_sets >= 3:
        # Oferece snapshot local apenas deste jogo **antes** de finalizar a partida
        try:
            uv_snapshot_prompt(kind="match_close_auto", match_id=match_id)
        except Exception:
            pass

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
        return

    # Caso contrário, abre próximo set (ou o 5º com alvo 15)
    next_set = set_number + 1
    try:
        # Garante que o próximo set exista
        stf = frames["sets"]
        exists = ((stf["match_id"] == match_id) & (stf["set_number"] == next_set)).any()
        if not exists:
            add_set(frames, match_id=match_id, set_number=next_set)
            stf = frames["sets"]  # recarrega após add_set

        # Zera explicitamente o placar do novo set e limpa winner
        sel_next = (stf["match_id"] == match_id) & (stf["set_number"] == next_set)
        if "home_points" in stf.columns:
            stf.loc[sel_next, "home_points"] = 0
        if "away_points" in stf.columns:
            stf.loc[sel_next, "away_points"] = 0
        if "winner_team_id" in stf.columns:
            stf.loc[sel_next, "winner_team_id"] = pd.NA
        frames["sets"] = stf

        # Remove quaisquer rallies residuais do próximo set (se houver)
        rl = frames.get("rallies", pd.DataFrame())
        if isinstance(rl, pd.DataFrame) and not rl.empty:
            rl = rl[~((rl["match_id"] == match_id) & (rl["set_number"] == next_set))]
            frames["rallies"] = rl
    except Exception:
        pass

    # Atualiza o set corrente para o novo set ANTES de persistir e re-renderizar
    st.session_state.set_number = next_set

    # Persiste abertura do set e força rerender
    _persist_all(frames, reason='set_open')
    st.session_state.frames = frames
    st.session_state.data_rev = st.session_state.get("data_rev", 0) + 1

    st.success(f"Set {set_number} encerrado ({home_pts} x {away_pts}). Novo set: {next_set} (placar reiniciado).")

def auto_close_set_if_needed() -> None:
    """
    Fecha automaticamente o set quando atingir o alvo (25; ou 15 no 5º) e diferença >= 2.
    Mantém o prompt do snapshot, mas não bloqueia o fechamento.
    Não fecha novamente se o set já tiver winner marcado.
    """
    try:
        if not st.session_state.get("auto_close", True):
            return

        frames = st.session_state.frames
        match_id = st.session_state.match_id
        set_number = int(st.session_state.set_number if st.session_state.set_number is not None else 1)
        if match_id is None or set_number is None:
            return

        # Pontuação atual do set
        df_cur = current_set_df(frames, match_id, set_number)
        if df_cur.empty:
            return
        home_pts, away_pts = set_score_from_df(df_cur)

        # Já está fechado?
        stf = frames.get("sets")
        if stf is not None and "winner_team_id" in stf.columns:
            already = ((stf["match_id"] == match_id) &
                       (stf["set_number"] == set_number) &
                       (stf["winner_team_id"].notna())).any()
            if already:
                return

        # Alvo: 25 (sets 1–4) ou 15 (set 5)
        target = 15 if set_number == 5 else 25
        diff = abs(home_pts - away_pts)

        # Regra: atingiu/alcançou o alvo e diferença >= 2  -> fecha
        if (home_pts >= target or away_pts >= target) and diff >= 2:
            # Oferece snapshot do jogo ATUAL (não bloqueia)
            try:
                uv_snapshot_prompt(kind="set_close_auto", match_id=match_id)
            except Exception:
                pass

            # Fecha o set agora
            _apply_set_winner_and_proceed(home_pts, away_pts)

    except Exception as e:
        if st.session_state.get("DEBUG_PRINTS", False):
            st.warning(f"[auto_close_set_if_needed] erro: {e}")
        
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
    set_number = int(st.session_state.set_number)
    rl = fr["rallies"]
    stf = fr["sets"]

    # Subconjunto do set atual, em ordem
    sub = rl[(rl["match_id"] == match_id) & (rl["set_number"] == set_number)].copy().sort_values("rally_no")

    if sub.empty:
        # Mesmo sem rallies, se o set atual estiver 0x0 e existir set anterior, reabrimos o anterior.
        mask_cur = (stf["match_id"] == match_id) & (stf["set_number"] == set_number)
        if mask_cur.any():
            row = stf[mask_cur].iloc[0]
            hp0 = int(row.get("home_points", 0))
            ap0 = int(row.get("away_points", 0))
            if hp0 == 0 and ap0 == 0 and set_number > 1:
                _reopen_set()  # reabre set anterior
                st.session_state.set_number = set_number - 1
                st.session_state.frames = fr
                st.session_state.data_rev += 1
                st.success(f"Set {set_number-1} reaberto (desfazer com set atual 0x0).")
                return
        st.warning("Não há rallies para desfazer neste set.")
        return

    # Remove o último rally do set atual
    last_row = sub.iloc[-1]
    last_rally_id = last_row["rally_id"]
    rl = rl[rl["rally_id"] != last_rally_id]
    fr["rallies"] = rl

    # Define placar do set após remoção
    if len(sub) >= 2:
        prev = sub.iloc[-2]
        hp, ap = int(prev.get("score_home", 0)), int(prev.get("score_away", 0))
    else:
        # Ficou sem rallies no set
        hp, ap = 0, 0

    # Atualiza placar do set atual
    mask_set = (stf["match_id"] == match_id) & (stf["set_number"] == set_number)
    if mask_set.any():
        stf.loc[mask_set, "home_points"] = hp
        stf.loc[mask_set, "away_points"] = ap
        # Se havia winner/is_closed marcados por engano, mantemos como aberto sem vencedor
        if "winner_team_id" in stf.columns:
            stf.loc[mask_set, "winner_team_id"] = pd.NA
        if "is_closed" in stf.columns:
            stf.loc[mask_set, "is_closed"] = False
        fr["sets"] = stf

    # Persiste
    save_all(Path(st.session_state.db_path), fr)
    st.session_state.frames = fr
    st.session_state.data_rev += 1
    dbg_print(f"Desfeito rally_id={last_rally_id}. Placar {hp}-{ap} no set {set_number}.")

    # ---- NOVO: se o set atual ficou 0x0 e existe set anterior, reabra o anterior
    if hp == 0 and ap == 0 and set_number > 1:
        _reopen_set()
        st.session_state.set_number = set_number - 1
        save_all(Path(st.session_state.db_path), fr)
        st.session_state.frames = fr
        st.session_state.data_rev += 1
        st.success(f"Set {set_number-1} reaberto (set atual ficou 0x0).")

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

# >>> Persistir Front/Back mesmo que DB ignore colunas soltas
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
        dbg_print(f"Persistido Front/Back='{fb_upper}' no rally_id={last_id}.")
    except Exception as e:
        dbg_print(f"Falha ao persistir Front/Back: {e}")
        
def quick_register_line(raw_line: str):
    if not raw_line.strip():
        dbg_print("Linha vazia ignorada."); return

    row = parse_line(raw_line)
    row = _fix_who_scored_from_raw_and_row(raw_line, row)
    row = _normalize_action_in_row(row)

    fb = str(st.session_state.get("q_position","Front")).strip().upper()
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

# =========================
# QUADRA HTML
# =========================
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
    adv_lbl_esc = html.escape(str(adv_lbl))
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
# Abertura inicial de partida
# =========================
def _list_open_matches(frames: dict) -> list[int]:
    mt = frames.get("amistosos", pd.DataFrame())
    if mt.empty: return []
    if "is_closed" in mt.columns:
        mt = mt[~mt["is_closed"].fillna(False).astype(bool)]
    return [int(x) for x in pd.to_numeric(mt["match_id"], errors="coerce").dropna().astype(int).tolist()]
open_mid = last_open_match(frames)

# =========================
# Chamada inicial: Verificação de jogos em aberto
# =========================
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
#  Chamada inicial: Barra do sistema título jogo e Modo jogo
# =========================
with st.container():
    # --- Espaçamento no topo (responsivo) ---
    st.markdown(
        """
        <div id="uv-top-spacer"></div>
        <style>
        /* desktop */
        #uv-top-spacer { height: 50px; }
        /* mobile */
        @media (max-width: 900px) {
            #uv-top-spacer { height: 40px; }
        }
        </style>
        """,
        unsafe_allow_html=True
    )

 #   if not st.session_state.game_mode:
 #       st.markdown("**Existe um jogo em aberto**")

    bar1, bar2, bar3 = st.columns([1.6, 2.5, 2.0])
    with bar1:
        stf = frames["sets"]; sm = stf[stf["match_id"] == st.session_state.match_id]
        home_sets_w = int((sm["winner_team_id"] == 1).sum())
        away_sets_w = int((sm["winner_team_id"] == 2).sum())
        st.markdown(
        (
            f'<div class="gm-title-wrap" style="display:flex;justify-content:center;margin-top:10px;">'  # ↑ espaço acima
            f'  <div class="badge gm-title" style="display:inline-flex;flex-direction:column;align-items:center;gap:4px;padding:10px 16px 10px;line-height:1.2;">'  # ↑ padding no topo
            f'    <div><b>{home_name}</b>&nbsp; X &nbsp;<b>{away_name}</b> — '
            f'    {(datetime.strptime(date_str, "%Y-%m-%d") if isinstance(date_str, str) else date_str).strftime("%d/%m/%Y")}</div>'
            f'    <div class="gm-subline" style="font-weight:500;font-size:18px;opacity:.85;margin-top:2px;">'
            f'      Sets: <b>{home_sets_w}</b> × <b>{away_sets_w}</b> | Set atual: <b>{st.session_state.set_number}</b>'
            f'    </div>'
            f'  </div>'
            f'</div>'
        ),
        unsafe_allow_html=True
    )

    with bar2:
        st.session_state.game_mode = st.toggle("🎮 **Jogo**", value=st.session_state.game_mode, key="game_mode_toggle") 
    #with bar3:
        # 2) Radio imediatamente antes do seu texto (mesma linha via CSS acima)
     #   options = ["Saque Nosso", "Saque Adv"]  # "—" = sem seleção
      #  if "quemsacou" not in st.session_state:
       #     st.session_state.quemsacou = options[0]
        #choice = st.radio("", options, key="st.session_state.quemsacou.quemsacou", horizontal=True, label_visibility="collapsed")

    st.markdown('</div>', unsafe_allow_html=True)

# rerun pós-callbacks
#if st.session_state._do_rerun_after:
#    st.session_state._do_rerun_after = False
#    st.rerun()

#st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
    
# =========================
#  Chamada inicial: Componentes Topo (Time, Jogo, Tutorial, Histórico)
# =========================
if not st.session_state.game_mode:
    top1, top2, top3, top4 = st.columns([2.5, 1, 1, 1])
    with top1:
        st.button("⚙️ Time", use_container_width=True, key="top_config_team_btn",
                on_click=lambda: st.session_state.__setitem__("show_config_team", True))
    with top2:
        st.button("🆕 Jogo", use_container_width=True, key="top_new_game_btn",
            on_click=lambda: st.session_state.__setitem__("show_cadastro", True))
    with top3:
        st.markdown(
            '<a href="/tutorial" target="_self" style="display:block;text-align:center;padding:.4rem .6rem;border:1px solid rgba(49,51,63,.2);border-radius:.5rem;font-weight:600;">📘 Tutorial</a>',
            unsafe_allow_html=True
        )
        #st.button("📘 Tutorial", use_container_width=True, key="top_tutorial_btn",
           # on_click=lambda: st.session_state.__setitem__("show_tutorial", True))
    with top4:
        # Abrir Histórico (link direto — evita issues com switch_page)
        st.markdown(
            '<a href="/historico" target="_self" style="display:block;text-align:center;padding:.4rem .6rem;border:1px solid rgba(49,51,63,.2);border-radius:.5rem;font-weight:600;">🗂️ Histórico</a>',
            unsafe_allow_html=True
        )

# =========================
# Sets criação, reabertura, fechamento set e partida
# =========================
def _get_set_rallies(frames, match_id, set_number, *, strict: bool = True):
    """Retorna os rallies do set do JOGO ATUAL (match_id), com normalização robusta de tipos."""
    rl = frames.get("rallies", None)
    if rl is None or rl.empty:
        return rl.iloc[0:0].copy() if rl is not None else pd.DataFrame()

    df = rl.copy()

    # Descobre a coluna do set entre variações comuns
    set_candidates = ("set_number", "set", "set_no", "setindex", "set_index", "setId", "set_id")
    set_col = next((c for c in set_candidates if c in df.columns), None)
    if set_col is None or "match_id" not in df.columns:
        return df.iloc[0:0].copy()

    # Normaliza números (se não der, usa comparação como string)
    df["_mid_n"] = pd.to_numeric(df["match_id"], errors="coerce")
    df["_set_n"] = pd.to_numeric(df[set_col],   errors="coerce")
    mid_n = pd.to_numeric(match_id,  errors="coerce")
    set_n = pd.to_numeric(set_number, errors="coerce")

    if strict:
        mid_mask = (df["_mid_n"] == mid_n) if not pd.isna(mid_n) else (df["match_id"].astype(str).str.strip() == str(match_id).strip())
        set_mask = (df["_set_n"] == set_n) if not pd.isna(set_n) else (df[set_col].astype(str).str.strip() == str(set_number).strip())
    else:
        mid_mask = ((df["_mid_n"] == mid_n) | (df["match_id"].astype(str).str.strip() == str(match_id).strip()))
        set_mask = ((df["_set_n"] == set_n) | (df[set_col].astype(str).str.strip() == str(set_number).strip()))

    sub = df[mid_mask & set_mask].copy()

    # Ordenação previsível
    for col in ("rally_no", "sequence", "created_at", "timestamp", "time", "ts", "rally_id"):
        if col in sub.columns:
            sub = sub.sort_values(col)
            break

    print ("@@@FN _get_set_rallies match_id=",match_id," set= ",set_number," => ",len(sub))
    # Debug opcional (mostra só o RESUMO do filtrado, não o frames inteiro)
    if st.session_state.get("DEBUG_PRINTS", False):
        st.write("💾 _get_set_rallies -> match_id={match_id} set={set_number} => {len(sub)} rows")
        dbg_print(f"@@@ _get_set_rallies -> match_id={match_id} set={set_number} => {len(sub)} rows")

    return sub

def _reopen_set():
    """
    Reabre um set:
      - Se 'set_picked' existir (cenário original), reabre esse set.
      - Se 'set_picked' NÃO existir (ex.: chamada via Desfazer), reabre o set anterior ao set atual.
    Mantém a lógica de voltar 1 rally/1 ponto do lado vencedor e limpar winner/is_closed.
    """
    import pandas as pd

    frames_local = st.session_state.frames
    stf = frames_local["sets"]
    rl  = frames_local["rallies"]
    mid = st.session_state.match_id

    # 1) Descobrir qual set reabrir (sn)
    sn = None
    try:
        # se set_picked existir no escopo glargal, usa-o
        sn = int(set_picked)  # noqa: F821  (pode não existir em alguns fluxos)
    except Exception:
        pass

    if sn is None:
        # fallback: reabre o set ANTERIOR ao atual quando chamado pelo Desfazer
        cur = int(st.session_state.get("set_number", 1))
        if cur > 1:
            sn = cur - 1
        else:
            st.warning("Nada a reabrir: não há set anterior e nenhum set foi selecionado.")
            return

    # 2) Seleciona o set
    mask_set = (stf["match_id"] == mid) & (stf["set_number"] == sn)
    if not mask_set.any():
        st.warning(f"Nada a reabrir para o set {sn}.")
        return

    # 3) Limpa vencedor/is_closed (se existirem)
    if "winner_team_id" in stf.columns:
        stf.loc[mask_set, "winner_team_id"] = pd.NA
    if "is_closed" in stf.columns:
        stf.loc[mask_set, "is_closed"] = False

    # 4) Tenta remover o último rally do set (equivale a -1 do vencedor)
    sub = rl[(rl["match_id"] == mid) & (rl["set_number"] == sn)].copy().sort_values("rally_no")

    if not sub.empty:
        last = sub.iloc[-1]
        # remove o último rally
        rl = rl[rl["rally_id"] != last["rally_id"]]
        frames_local["rallies"] = rl

        # define o placar como o do penúltimo rally (se existir), senão aplica -1 no vencedor “atual”
        if len(sub) >= 2:
            prev = sub.iloc[-2]
            hp = int(prev.get("score_home", 0))
            ap = int(prev.get("score_away", 0))
        else:
            row = stf[mask_set].iloc[0]
            hp0 = int(row.get("home_points", 0))
            ap0 = int(row.get("away_points", 0))
            if hp0 >= ap0:
                hp, ap = max(0, hp0 - 1), ap0
            else:
                hp, ap = hp0, max(0, ap0 - 1)
    else:
        # Não há rallies: aplica -1 no lado vencedor “atual”
        row = stf[mask_set].iloc[0]
        hp0 = int(row.get("home_points", 0))
        ap0 = int(row.get("away_points", 0))
        if hp0 >= ap0:
            hp, ap = max(0, hp0 - 1), ap0
        else:
            hp, ap = hp0, max(0, ap0 - 1)

    # 5) Atualiza placar
    stf.loc[mask_set, "home_points"] = hp
    stf.loc[mask_set, "away_points"] = ap
    frames_local["sets"] = stf

    # 6) Persiste e atualiza estado (foca no set reaberto quando for fallback)
    save_all(Path(st.session_state.db_path), frames_local)
    st.session_state.frames = frames_local
    # Se foi fallback (sem set_picked), atualiza o set atual para o reaberto
    try:
        _ = set_picked  # noqa: F821
    except Exception:
        st.session_state.set_number = sn

    st.session_state.data_rev += 1
    st.success(f"Set {sn} reaberto. Placar ajustado: {hp} x {ap}.")
    dbg_print(f"Set {sn} reaberto. Placar {hp}-{ap}.")

def _close_set():
    frames_local = st.session_state.frames
    mid = st.session_state.match_id
    sn = int(set_picked)

    # Diagnóstico (opcional): olha rallies, mas NÃO bloqueia pelo sub.empty
    sub = _get_set_rallies(frames_local, mid, sn)
    rcount = len(sub)

    # Placar oficial do set (fonte canônica para fechar)
    df_cur = current_set_df(frames_local, mid, sn)
    hp, ap = set_score_from_df(df_cur)

    # Logs/avisos úteis
    if st.session_state.get("DEBUG_PRINTS", False):
        st.info(f"Fechar Set {sn}: placar atual (df) = {hp} x {ap} | rallies={rcount}")
    if rcount == 0:
        st.warning(f"Atenção: nenhum rally encontrado no set {sn}, mas o placar é {hp}x{ap}. Fechando mesmo assim.")

    # ÚNICA trava: 0x0
    if hp == 0 and ap == 0:
        st.warning("Sem pontuação neste set (0x0). Registre pelo menos 1 ponto antes de fechar.")
        return

    # Fecha o set com o placar atual
    try:
        st.session_state.set_number = sn
        _apply_set_winner_and_proceed(hp, ap)
        st.session_state.data_rev += 1
        if st.session_state.get("DEBUG_PRINTS", False):
            st.success(f"Set {sn} fechado: {hp} x {ap}.")
    except Exception as e:
        st.error(f"Falha ao fechar set: {e}")
        return

    # >>> NOVO: se este era o último set configurado, finaliza a partida automaticamente
    try:
        qtd_limite = int(st.session_state.get("qtdSetsJogoAtual", 5))
    except Exception:
        qtd_limite = 5

    if sn >= qtd_limite:
        # evita duplicidade se já estiver fechado
        already_closed = False
        try:
            mt = frames_local.get("amistosos", pd.DataFrame())
            if isinstance(mt, pd.DataFrame) and "match_id" in mt.columns and "is_closed" in mt.columns:
                already_closed = bool(mt.loc[mt["match_id"] == mid, "is_closed"].fillna(False).any())
        except Exception:
            pass

        if not already_closed:
            try:
                _finalizar_partida()
            except Exception as e:
                if st.session_state.get("DEBUG_PRINTS", False):
                    st.warning(f"[auto-finalizar após set {sn}] erro: {e}")

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

# Finalizar partida direto
def _finalizar_partida():
    if st.session_state.match_id is None: 
        return

    mid = st.session_state.match_id
    try:
        # 2.3) Oferece snapshot local apenas deste jogo **antes** de finalizar manualmente
        try:
            uv_snapshot_prompt(kind="match_close", match_id=mid)
        except Exception:
            pass
        finalize_match(st.session_state.frames, mid)
    except Exception:
        pass

    # Força is_closed na tabela e salva local
    try:
        frames_local = st.session_state.frames
        mt = frames_local.get("amistosos", pd.DataFrame())
        if not mt.empty and "match_id" in mt.columns:
            mt.loc[mt["match_id"] == mid, "is_closed"] = True
            mt.loc[mt["match_id"] == mid, "closed_at"] = datetime.now().isoformat(timespec="seconds")
            frames_local["amistosos"] = mt
            _persist_all(frames_local, reason='match_close')
    except Exception:
        pass

    st.session_state.data_rev = st.session_state.get("data_rev", 0) + 1
    st.success("Partida finalizada.")


# REVISAR ->>>> CÓDIGO PERDIDO ?!?
    st.session_state.match_id = None
    st.session_state.set_number = None
    st.session_state._do_rerun_after = True

# =========================
# Componentes de informações de sets
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
    with top7:
        st.button("🔓 Reabre Set", use_container_width=True, key="reopen_btn", on_click=_reopen_set)
        st.markdown('</div>', unsafe_allow_html=True)
    with top8:
        st.button("✅ Fecha Set", use_container_width=True, key="close_set_btn", on_click=_close_set)
        st.markdown('</div>', unsafe_allow_html=True)
    with top9:
        st.button("🗑️ Remove Set Vazio", use_container_width=True, key="remove_empty_set_btn", on_click=_remove_empty_set)
        st.markdown('</div>', unsafe_allow_html=True)
    with top10:
        st.button("🏁 Finaliza Partida", use_container_width=True, on_click=_finalizar_partida)
        

# =========================
#  Chamada inicial: Modais (Config/Tutorial/Cadastro)
# =========================
if st.session_state.get("show_config_team", False):
    st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
    col_title, col_close = st.columns([4, 1])
    with col_title:
        st.subheader("⚙️ Nosso Time e Jogadoras")
    with col_close:
        st.button("❌ Fechar", key="close_config_top_btn",
                  on_click=lambda: st.session_state.__setitem__("show_config_team", False))

    st.markdown("**Nome do Nosso Time**")
    current_team_name = team_name_by_id(frames, OUR_TEAM_ID)
    new_team_name = st.text_input("Nome do time:", value=current_team_name, key="team_name_input")

    # ================================
    # NOVO: Funções auxiliares de logo
    # ================================
    def _teamcfg_ensure_equipes_logo_col():
        """Garante coluna 'team_logo_path' na aba 'equipes'."""
        try:
            eq = frames.get("equipes", pd.DataFrame())
            if eq is None or eq.empty:
                # Se não existir nada ainda, cria com colunas mínimas
                frames["equipes"] = pd.DataFrame(
                    [{"team_id": OUR_TEAM_ID, "team_name": current_team_name, "team_logo_path": pd.NA}]
                )
            else:
                if "team_logo_path" not in eq.columns:
                    eq["team_logo_path"] = pd.NA
                    frames["equipes"] = eq
        except Exception:
            pass

    def _teamcfg_current_logo_path() -> str | None:
        """Retorna o caminho (string) da logo do nosso time, se cadastrada."""
        try:
            eq = frames.get("equipes", pd.DataFrame())
            if eq is None or eq.empty:
                return None
            row = eq[eq["team_id"] == OUR_TEAM_ID]
            if row.empty:
                return None
            p = row.iloc[0].get("team_logo_path")
            if pd.notna(p) and str(p).strip():
                return str(p)
        except Exception:
            return None
        return None

    def _teamcfg_apply_watermark(img_path: Path, opacity: float = 0.10):
        """Aplica marca d'água central leve com a imagem fornecida."""
        try:
            if not img_path or not img_path.exists():
                return
            ext = img_path.suffix.lower()
            if ext in (".png",):
                mime = "image/png"
            elif ext in (".jpg", ".jpeg"):
                mime = "image/jpeg"
            elif ext in (".webp",):
                mime = "image/webp"
            else:
                mime = "image/png"
            b64 = base64.b64encode(img_path.read_bytes()).decode("utf-8")
            st.markdown(
                f"""
                <style>
                  [data-testid="stAppViewContainer"] > .main {{
                    position: relative; z-index: 1;
                  }}
                  #uv-bg-watermark {{
                    position: fixed;
                    inset: 0;
                    z-index: 0;
                    pointer-events: none;
                    background: url('data:{mime};base64,{b64}') center center no-repeat;
                    background-size: min(20vw, 20vh);
                    opacity: {opacity};
                  }}
                </style>
                <div id="uv-bg-watermark"></div>
                """,
                unsafe_allow_html=True
            )
            st.session_state["app_bg_logo"] = img_path.as_posix()
        except Exception:
            pass

    def _teamcfg_save_logo():
        """Salva a imagem enviada em ./imgs e grava caminho em 'equipes.team_logo_path'."""
        up = st.session_state.get("team_logo_uploader")
        if not up:
            st.warning("Selecione uma imagem primeiro.")
            return
        try:
            _teamcfg_ensure_equipes_logo_col()
            try:
                base_dir = Path(__file__).parent
            except Exception:
                base_dir = Path(".").resolve()
            imgs_dir = base_dir / "imgs"
            imgs_dir.mkdir(parents=True, exist_ok=True)

            ext = Path(up.name).suffix.lower()
            if ext not in (".png", ".jpg", ".jpeg", ".webp"):
                ext = ".png"
            out_path = imgs_dir / f"team{OUR_TEAM_ID}_logo{ext}"
            out_path.write_bytes(up.getbuffer())

            # Atualiza a tabela 'equipes'
            eq = frames.get("equipes", pd.DataFrame())
            if eq is None or eq.empty:
                eq = pd.DataFrame([{"team_id": OUR_TEAM_ID, "team_name": new_team_name, "team_logo_path": out_path.as_posix()}])
            else:
                if "team_logo_path" not in eq.columns:
                    eq["team_logo_path"] = pd.NA
                mask = (eq["team_id"] == OUR_TEAM_ID)
                if mask.any():
                    eq.loc[mask, "team_logo_path"] = out_path.as_posix()
                    if new_team_name.strip():
                        eq.loc[mask, "team_name"] = new_team_name.strip()
                else:
                    # Se o nosso time não existir ainda na tabela, cria a linha
                    eq = pd.concat([
                        eq,
                        pd.DataFrame([{"team_id": OUR_TEAM_ID, "team_name": new_team_name, "team_logo_path": out_path.as_posix()}])
                    ], ignore_index=True)
            frames["equipes"] = eq
            _persist_all(frames, reason="team_logo")

            # Aplica marca d'água imediatamente
            _teamcfg_apply_watermark(out_path, opacity=0.10)
            st.success("Logo salva e aplicada como marca d’água.")
        except Exception as e:
            st.error(f"Falha ao salvar logo: {e}")

    # ============================
    # download de template (xlsx + csv) — (mantido)
    # ============================
    def _download_template():
        cols = ["team_id","player_number","player_name","position"]
        df = pd.DataFrame(columns=cols)
        bio = BytesIO()
        try:
            with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="jogadoras")
            data = bio.getvalue()
            st.download_button("⬇️ Baixar modelo Excel (jogadoras.xlsx)", data=data,
                               file_name="jogadoras_template.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception:
            st.info("Não consegui gerar XLSX aqui. Baixe como CSV e abra no Excel.")
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Baixar modelo CSV (jogadoras.csv)", data=csv,
                           file_name="jogadoras_template.csv", mime="text/csv")
    _download_template()

    # ============================
    # Salvar nome do time — (mantido)
    # ============================
    def _save_team_name():
        if "equipes" in frames:
            equipes = frames["equipes"]
            mask = equipes["team_id"] == OUR_TEAM_ID
            if mask.any():
                equipes.loc[mask, "team_name"] = new_team_name
            else:
                new_team = pd.DataFrame({"team_id":[OUR_TEAM_ID], "team_name":[new_team_name]})
                # garante a coluna de logo
                if "team_logo_path" not in new_team.columns:
                    new_team["team_logo_path"] = pd.NA
                if "equipes" in frames and "team_logo_path" in frames.get("equipes", pd.DataFrame()).columns:
                    pass  # já está padronizado
                equipes = pd.concat([equipes, new_team], ignore_index=True)
            frames["equipes"] = equipes
            _persist_all(frames, reason='generic')
            st.session_state.show_config_team = False
    st.button("💾 Salvar Nome do Time", key="save_team_name_btn", on_click=_save_team_name)

    # ============================
    # NOVO: Logo do time (upload + salvar)
    # ============================
    st.markdown("---")
    st.subheader("🖼️ Logo do Time")

    # mostra logo atual, se existir
    _teamcfg_ensure_equipes_logo_col()
    _logo_path_str = _teamcfg_current_logo_path()
    if _logo_path_str:
        try:
            st.image(_logo_path_str, caption="Logo atual", width=160)
        except Exception:
            pass

    # uploader e botão salvar
    logo_up = st.file_uploader("Selecione a imagem (PNG, JPG, WEBP)",
                               type=["png","jpg","jpeg","webp"],
                               key="team_logo_uploader")
    st.button("💾 Salvar Logo do Time", key="save_team_logo_btn", on_click=_teamcfg_save_logo)

    st.markdown("---")
    st.subheader("👥 Jogadoras")

    # ============================
    # Jogadoras — (mantido)
    # ============================
    jogadoras_df = frames.get("jogadoras", pd.DataFrame())
    our_players = jogadoras_df[jogadoras_df["team_id"] == OUR_TEAM_ID].copy()
    if not our_players.empty:
        st.markdown("**Cadastradas**")
        display_df = our_players[["player_number", "player_name", "position"]].copy()
        display_df.columns = ["Número", "Nome", "Posição"]
        display_dataframe(display_df, height=140)

        st.markdown("**Excluir**")
        players_to_delete = our_players["player_number"].astype(str) + " - " + our_players["player_name"]
        player_to_delete = st.selectbox("Escolha:", players_to_delete.tolist(), key="delete_player_select")

        def _delete_player():
            if player_to_delete:
                player_num = int(player_to_delete.split(" - ")[0])
                jog_df = frames["jogadoras"]
                jog_df = jog_df[~((jog_df["team_id"] == OUR_TEAM_ID) & (jog_df["player_number"] == player_num))]
                frames["jogadoras"] = jog_df
                _persist_all(frames, reason='generic')
        st.button("🗑️ Excluir", key="delete_player_btn", on_click=_delete_player)

    st.markdown("---")
    st.subheader("➕ Adicionar")
    c1, c2, c3 = st.columns(3)
    with c1:
        new_number = st.number_input("Número:", min_value=1, max_value=99, key="new_player_number")
    with c2:
        new_name = st.text_input("Nome:", key="new_player_name")
    with c3:
        new_position = st.selectbox("Posição:", ["oposto","levantador","central","ponteiro","líbero"], key="new_player_position")

    def _add_player():
        if new_name.strip():
            new_player = pd.DataFrame({
                "team_id":[OUR_TEAM_ID],
                "player_number":[new_number],
                "player_name":[new_name],
                "position":[new_position]
            })
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
                    <iframe srcdoc='{html.escape(html_content)}' style='width:100%; height:100%; border:none; margin-top:40px;'></iframe>
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
# Cadastro rápido / NOVO JOGO  Tipo do Jogo (Amistoso/Campeonato)
# =========================

def _nvj_normalize_match_type(s: str) -> str:
    """Normaliza o tipo do jogo para armazenamento: 'amistoso' | 'campeonato'."""
    s = (s or "").strip().lower()
    return "campeonato" if s.startswith("c") else "amistoso"

def _nvj_render_match_type_fields() -> dict:
    """
    UI (no frame de Novo Jogo) para escolher:
      - Tipo: Amistoso | Campeonato
      - Se Campeonato: Nome do campeonato + Fase (Classificatório/Semi-final/Final)
      - Quantidade de Sets (1..5)  ← adicionado

    Retorna um dict com:
      {
        'match_type': 'amistoso' | 'campeonato',
        'tournament_name': str | None,
        'tournament_stage': str | None   # 'Classificatório' | 'Semi-final' | 'Final'
      }

    Obs.: Não persiste nada sozinho — chame _nvj_apply_match_type_meta(...) após criar o match_id.
    """
    st.markdown("### Tipo do jogo")

    # agora com 3 colunas: Tipo | Quantidade de Sets | espaço
    col_tipo, col_qtd, col_void = st.columns([1.0, 0.8, 0.2])

    with col_tipo:
        tipo_vis = st.radio(
            "Tipo",
            options=["Amistoso", "Campeonato"],
            horizontal=True,
            key="nvj_tipo",
        )
    tipo_norm = _nvj_normalize_match_type(tipo_vis)

    # Quantidade de sets (1..5) — salva em st.session_state["qtdSetsJogoAtual"]
    with col_qtd:
        # usa valor anterior se existir, senão default=5 (ou 3, se preferir)
        default_qtd = int(st.session_state.get("qtdSetsJogoAtual", 5))
        if default_qtd not in (1, 2, 3, 4, 5):
            default_qtd = 5
        qtd_opts = [1, 2, 3, 4, 5]
        idx_qtd = qtd_opts.index(default_qtd)
        qtd_escolhida = st.selectbox(
            "Quantidade de Sets",
            options=qtd_opts,
            index=idx_qtd,
            key="nvj_qtd_sets",
        )
        # armazena na variável solicitada
        st.session_state["qtdSetsJogoAtual"] = int(qtd_escolhida)

    camp_name = None
    stage = None
    if tipo_norm == "campeonato":
        c1, c2 = st.columns([2.0, 1.2])
        with c1:
            camp_name = st.text_input(
                "Nome do campeonato",
                key="nvj_camp_nome",
                placeholder="Ex.: Copa Regional 2025",
            ).strip() or None
        with c2:
            stage = st.selectbox(
                "Fase",
                options=["Classificatório", "Semi-final", "Final"],
                index=0,
                key="nvj_camp_fase",
            )

    meta = {
        "match_type": tipo_norm,
        "tournament_name": camp_name if tipo_norm == "campeonato" else None,
        "tournament_stage": stage if tipo_norm == "campeonato" else None,
    }
    # Deixa acessível para outras partes do fluxo de Novo Jogo
    st.session_state["new_match_meta"] = meta
    return meta

def _nvj_upgrade_amistosos_schema(frames: dict) -> None:
    """
    Garante que o DataFrame 'amistosos' possua as colunas extras:
      - match_type ('amistoso' | 'campeonato')
      - tournament_name (str ou NA)
      - tournament_stage ('Classificatório' | 'Semi-final' | 'Final' ou NA)
    Não altera outras colunas. Cria o DF vazio se não existir.
    """
    df = frames.get("amistosos", None)
    if df is None:
        frames["amistosos"] = pd.DataFrame(
            columns=[
                "match_id",
                "away_team_id",
                "date",
                "home_sets",
                "away_sets",
                "status",
                "finished_at",
                "is_closed",
                "closed_at",
                # novas colunas:
                "match_type",
                "tournament_name",
                "tournament_stage",
            ]
        )
        return

    # Se existir, apenas adiciona as novas colunas caso não existam
    need_cols = ["match_type", "tournament_name", "tournament_stage"]
    df = df.copy()
    for c in need_cols:
        if c not in df.columns:
            df[c] = pd.NA
    frames["amistosos"] = df

def _nvj_apply_match_type_meta(frames: dict, match_id, meta: dict) -> bool:
    """
    Aplica o metadata (tipo do jogo + dados de campeonato) à linha do match em 'amistosos'.
    Retorna True se aplicou, False se não encontrou o match.

    Uso típico no fluxo de Novo Jogo (logo após criar o match_id):
        meta = _nvj_render_match_type_fields()
        ok = _nvj_apply_match_type_meta(st.session_state.frames, st.session_state.match_id, meta)
        if ok:
            _persist_all(st.session_state.frames, reason="gs")
    """
    _nvj_upgrade_amistosos_schema(frames)

    df = frames["amistosos"]
    if df is None or df.empty or "match_id" not in df.columns:
        st.warning("Não foi possível localizar a aba 'amistosos' para aplicar o tipo do jogo.")
        return False

    # Filtro robusto pelo match_id (numérico ou string)
    mid_col = pd.to_numeric(df["match_id"], errors="coerce")
    mid_val = pd.to_numeric(match_id, errors="coerce")
    mask = (mid_col == mid_val)
    if not mask.any():
        # fallback por string exata
        mask = (df["match_id"].astype(str).str.strip() == str(match_id).strip())

    if not mask.any():
        st.warning(f"Partida (match_id={match_id}) não encontrada em 'amistosos'.")
        return False

    # Normaliza payload
    mtype = _nvj_normalize_match_type(meta.get("match_type", "amistoso"))
    tname = (meta.get("tournament_name") or None)
    tstage = (meta.get("tournament_stage") or None)

    # Grava
    df.loc[mask, "match_type"] = mtype
    df.loc[mask, "tournament_name"] = tname if mtype == "campeonato" else pd.NA
    df.loc[mask, "tournament_stage"] = tstage if mtype == "campeonato" else pd.NA
    frames["amistosos"] = df

    # Mensagem de confirmação (opcional)
    st.info(
        f"Tipo do jogo salvo: **{mtype}**"
        + (f" — {tname} ({tstage})" if mtype == "campeonato" and tname else "")
    )
    return True

def _nvj_get_match_meta(frames: dict, match_id) -> dict:
    """
    Lê os metadados do match em 'amistosos' (caso existam).
    Retorna dict com as chaves: match_type, tournament_name, tournament_stage (ou None).
    """
    df = frames.get("amistosos", None)
    if df is None or df.empty or "match_id" not in df.columns:
        return {"match_type": None, "tournament_name": None, "tournament_stage": None}

    mid_col = pd.to_numeric(df["match_id"], errors="coerce")
    mid_val = pd.to_numeric(match_id, errors="coerce")
    mask = (mid_col == mid_val)
    if not mask.any():
        mask = (df["match_id"].astype(str).str.strip() == str(match_id).strip())
        if not mask.any():
            return {"match_type": None, "tournament_name": None, "tournament_stage": None}

    row = df.loc[mask].iloc[0]
    return {
        "match_type": (row.get("match_type") if "match_type" in df.columns else None),
        "tournament_name": (row.get("tournament_name") if "tournament_name" in df.columns else None),
        "tournament_stage": (row.get("tournament_stage") if "tournament_stage" in df.columns else None),
    }

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

def _create_new_match(opp_name: str, dt: date, meta: dict | None = None):
    """
    Cria um novo jogo e já grava o tipo do jogo:
      meta = {
        'match_type': 'amistoso' | 'campeonato',
        'tournament_name': str | None,
        'tournament_stage': 'Classificatório' | 'Semi-final' | 'Final' | None
      }
    """
    with uv_preloader("Criando jogo..."):
        frames_local = st.session_state.frames

        # Garante colunas novas na aba 'amistosos'
        _nvj_upgrade_amistosos_schema(frames_local)

        mt = frames_local.get("amistosos", pd.DataFrame())
        if mt.empty:
            mt = pd.DataFrame(columns=[
                "match_id","away_team_id","date","home_sets","away_sets","status","finished_at","is_closed","closed_at",
                # novas:
                "match_type","tournament_name","tournament_stage"
            ])
            next_mid = 1
        else:
            next_mid = int(pd.to_numeric(mt["match_id"], errors="coerce").max() or 0) + 1

        away_id = _get_or_create_team_id_by_name(frames_local, opp_name or "Adversário")

        # Lê meta do session_state se não vier por parâmetro
        if meta is None:
            meta = st.session_state.get("new_match_meta", {}) or {}
        mtype = _nvj_normalize_match_type(meta.get("match_type", "amistoso"))
        tname = meta.get("tournament_name") if mtype == "campeonato" else None
        tstage = meta.get("tournament_stage") if mtype == "campeonato" else None

        new_row = {
            "match_id": next_mid,
            "away_team_id": away_id,
            "date": str(dt),
            "home_sets": 0,
            "away_sets": 0,
            "status": None,
            "finished_at": None,
            "is_closed": None,
            "closed_at": None,
            # novas:
            "match_type": mtype,
            "tournament_name": tname,
            "tournament_stage": tstage,
        }
        mt = pd.concat([mt, pd.DataFrame([new_row])], ignore_index=True)
        frames_local["amistosos"] = mt

        # Primeiro set
        add_set(frames_local, match_id=next_mid, set_number=1)

        # Persistência local
        save_all(Path(st.session_state.db_path), frames_local)

        # Atualiza estado
        st.session_state.frames = frames_local
        st.session_state.match_id = next_mid
        st.session_state.set_number = 1
        st.session_state.show_cadastro = False

    # Mensagem (após fechar o preloader)
    opp_label = opp_name or team_name_by_id(st.session_state.frames, away_id)
    extra = f" — {tname} ({tstage})" if mtype == "campeonato" and tname else ""
    st.success(f"Novo jogo criado: {team_name_by_id(st.session_state.frames, OUR_TEAM_ID)} x {opp_label} — tipo: {mtype}{extra}")


# =========================
# Chamada inicial: Novo Jogo
# =========================
if (st.session_state.match_id is None or st.session_state.show_cadastro) and not st.session_state.show_config_team:
    with st.container():
        st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
        st.subheader("🆕 Novo Jogo")

        cgj1, cgj2 = st.columns([2, 1])
        with cgj1:
            opp_name = st.text_input("Adversário:", key="new_game_opponent", value="")
        with cgj2:
            game_date = st.date_input("Data:", value=date.today(), key="new_game_date")

        # 👇 adiciona os campos de tipo do jogo (Amistoso/Campeonato)
        _nvj_render_match_type_fields()  # guarda em st.session_state["new_match_meta"]

        cgjb1, cgjb2 = st.columns([1,1])
        with cgjb1:
            st.button(
                "Criar Jogo",
                key="create_game_btn",
                on_click=lambda: _create_new_match(
                    st.session_state.get("new_game_opponent", "").strip(),
                    st.session_state.get("new_game_date", date.today()),
                    st.session_state.get("new_match_meta")  # meta do tipo/torneio/fase
                ),
                width='stretch'
            )
        with cgjb2:
            st.button(
                "Fechar",
                key="close_new_game_btn",
                on_click=lambda: st.session_state.__setitem__("show_cadastro", False),
                width='stretch'
            )
        st.markdown('</div>', unsafe_allow_html=True)
    st.stop()

# =========================
# _criaBtnsJogadoras
# =========================
def _criaBtnsJogadoras():
    # 1) Garante chaves ANTES de ler/usar (evita “pulo” no 1º clique)
    uv_init_state()

    # 2) CSS – injeta apenas uma vez por sessão (evita reflow a cada rerun)
    if not st.session_state.get("_uv_css_players_injected"):
        st.markdown("""
        <style>
        /* deixa o bloco do radio inline e sem margem extra */
        .element-container:has(> div[data-testid="stRadio"]) {
            display: inline-block;
            margin: 0 10px 0 0 !important;
            vertical-align: middle;
        }
        /* deixa o bloco desse markdown inline também */
        .element-container:has(> div.stMarkdown) {
            display: inline-block;
            margin: 0 !important;
            vertical-align: middle;
        }
        /* estiliza o grupo do radio para ficar compacto */
        div[data-testid="stRadio"] label { margin: 0 !important; }
        div[data-testid="stRadio"] > div[role="radiogroup"] { display: inline-flex; gap: 8px; }
        </style>
        """, unsafe_allow_html=True)
        st.session_state["_uv_css_players_injected"] = True

    # 3) Header de estado (texto + pills)
    sel_num   = st.session_state.get("uv_active_player")
    adv_state = st.session_state.get("uv_adv_state", "neutral")
    if sel_num is not None:
        sel_txt = f"#{sel_num}"
    elif adv_state in ("ok", "err"):
        sel_txt = "ADV"
    else:
        sel_txt = "-"

    is_ok  = (st.session_state.get("q_result", "Acerto") == "Acerto")
    ok_bg  = f"rgba(22,163,74,{1.0 if is_ok else 0.25})"
    ok_bd  = f"rgba(21,128,61,{1.0 if is_ok else 0.35})"
    err_bg = f"rgba(185,28,28,{1.0 if not is_ok else 0.25})"
    err_bd = f"rgba(127,29,29,{1.0 if not is_ok else 0.35})"

    st.markdown(
        f"""
        <span>Jogadora Selecionada:
        <strong style="font-size:25px; line-height:1;">{sel_txt}</strong>
        </span>
        <span style="display:inline-block; padding:2px 10px; border-radius:10px;
                    border:1px solid {ok_bd}; background:{ok_bg};
                    color:#fff; font-weight:700; line-height:1;">Acerto</span>
        <span style="display:inline-block; padding:2px 10px; border-radius:10px;
                    border:1px solid {err_bd}; background:{err_bg};
                    color:#fff; font-weight:700; line-height:1;">Erro</span>
        """,
        unsafe_allow_html=True
    )

    # 4) Callbacks (sem uv_init_state aqui!)
    def _click_player(n: int):
        # reset ADV
        st.session_state["uv_adv_state"] = "neutral"
        # reset jogadora anterior se diferente
        cur = st.session_state["uv_active_player"]
        if cur is not None and cur != n:
            st.session_state["uv_player_state"][cur] = "neutral"
        # alterna jogadora atual (1º clique = ok; depois ok<->err)
        prev = st.session_state.get("uv_player_state", {}).get(n, "neutral")
        if cur != n:
            st.session_state["uv_active_player"] = n
            st.session_state["uv_player_state"][n] = "ok"
        else:
            st.session_state["uv_player_state"][n] = "err" if prev == "ok" else "ok"
        # integrações + resultado
        st.session_state["last_selected_player"] = st.session_state["uv_active_player"]
        st.session_state["q_side"] = "Nós"
        cur_state = st.session_state.get("uv_player_state", {}).get(n, "neutral")
        if cur_state in ("ok", "err"):
            st.session_state["q_result"] = "Acerto" if cur_state == "ok" else "Erro"

    def _click_adv():
        # reset jogadora ativa
        cur = st.session_state["uv_active_player"]
        if cur is not None:
            st.session_state["uv_player_state"][cur] = "neutral"
            st.session_state["uv_active_player"] = None
        # alterna ADV (ok <-> err)
        adv = st.session_state.get("uv_adv_state", "neutral")
        st.session_state["uv_adv_state"] = "err" if adv == "ok" else "ok"
        st.session_state["q_side"] = "Adv"
        # resultado conforme ADV
        st.session_state["q_result"] = "Acerto" if st.session_state["uv_adv_state"] == "ok" else "Erro"

    # 5) Dados (com cache leve por partida → evita custo no 1º clique pós-rerun)
    frames = st.session_state.frames
    mid = st.session_state.get("match_id")
    cache = st.session_state.setdefault("_uv_roster_cache", {})
    cache_key = ("roster", mid)

    if cache_key in cache:
        nums, name_map = cache[cache_key]
    else:
        try:
            nums = resolve_our_roster_numbers(frames)
        except Exception:
            nums = list(range(1, 13))
        try:
            name_map = {r["number"]: r["name"] for r in roster_for_ui(frames)}
        except Exception:
            name_map = {}
        cache[cache_key] = (nums, name_map)

    label_mode = st.session_state.get("player_label_mode", "Número")

    # 6) Grade estável: define nº de colunas uma única vez por sessão/partida
    if "_uv_grid_cols" not in st.session_state:
        st.session_state["_uv_grid_cols"] = min(12, max(1, len(nums) + 1))
    grid_cols = st.session_state["_uv_grid_cols"]

    if nums:
        st.markdown('<div class="jogadoras-container">', unsafe_allow_html=True)

        jcols = st.columns(grid_cols)
        # Botões das jogadoras
        for i, n in enumerate(nums):
            label_txt = str(n) if label_mode == "Número" else (name_map.get(n) or str(n))
            with jcols[i % grid_cols]:
                st.button(
                    label_txt,
                    key=f"pill_main_{n}",
                    on_click=_click_player,
                    args=(n,),
                    use_container_width=True
                )
        # Botão ADV
        with jcols[(len(nums)) % grid_cols]:
            st.button("ADV", key="pill_main_adv", on_click=_click_adv, use_container_width=True)

        # 7) Cores nos próprios botões (após renderizar)
        def _esc(label: str) -> str:
            return str(label).replace("\\", "\\\\").replace('"', '\\"')

        rules = []
        states = st.session_state["uv_player_state"]
        for n in nums:
            lab = str(n) if label_mode == "Número" else (name_map.get(n) or str(n))
            lab_esc = _esc(lab)
            stt = states.get(n, "neutral")
            if stt == "ok":
                rules.append(
                    f'.stButton button[aria-label="{lab_esc}"] {{ '
                    f'background:#16a34a !important; color:#fff !important; border:1px solid #15803d !important; }}'
                )
            elif stt == "err":
                rules.append(
                    f'.stButton button[aria-label="{lab_esc}"] {{ '
                    f'background:#b91c1c !important; color:#fff !important; border:1px solid #7f1d1d !important; }}'
                )

        adv_state = st.session_state.get("uv_adv_state", "neutral")
        if adv_state == "ok":
            rules.append('.stButton button[aria-label="ADV"] { background:#16a34a !important; color:#fff !important; border:1px solid #15803d !important; }')
        elif adv_state == "err":
            rules.append('.stButton button[aria-label="ADV"] { background:#b91c1c !important; color:#fff !important; border:1px solid #7f1d1d !important; }')

        if rules:
            st.markdown("<style>\n" + "\n".join(rules) + "\n</style>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.caption("Sem jogadoras")

def _criaBtnsAtalhos():
    # ----------------- Atalhos -----------------
    st.markdown('<div class="gm-quick-row">', unsafe_allow_html=True)
    #st.markdown("**Atalhos**")
    atalho_specs = [
        ("d",    "Diag"),
        ("l",    "Par"),
        ("m",    "Mei"),
        ("larg", "Larg"),
        ("seg",  "Seg"),
        ("pi",   "Pipe"),
        ("re",   "Recp"),
        ("b",    "Bloq"),
        ("sa",   "Saq"),
        ("rede", "Red"),
    ]

    # --- Linha de Atalhos + [Front/Back] + [↩️] na MESMA linha ---
    max_cols = 12  # grade "fictícia" de 12
    n_btns = len(atalho_specs)

    # Deixa 2 colunas para o rádio e o botão minúsculo
    first_row_btns = max_cols - 2 if n_btns >= (max_cols - 2) else n_btns

    # Pesos: muitos botões (1 cada), rádio um pouco maior (1.4), desfazer minúsculo (0.001)
    row_weights = [1] * first_row_btns + [1.4, 0.001]
    row1 = st.columns(row_weights)

    # Botões da 1ª linha
    for i in range(first_row_btns):
        code, label = atalho_specs[i]
        with row1[i]:
            st.button(
                label,
                key=f"main_quick_{code}",
                on_click=lambda code=code: gm_quick_click(code),
                use_container_width=True
            )

    # Rádio na penúltima coluna
    with row1[-2]:
        st.session_state.q_position = st.radio(
            "", ["Front", "Back"], horizontal=True,
            index=["Front", "Back"].index(st.session_state.q_position),
            key="gm_q_position", label_visibility="collapsed"
        )

    # Botão DESFAZER bem pequeno na última coluna
    with row1[-1]:
        st.button("↩️", key="btn_undo_main", on_click=undo_last_rally_current_set, help="Desfazer")

    # Linhas seguintes (se ainda houver botões de atalho)
    remaining = atalho_specs[first_row_btns:]
    while remaining:
        chunk = remaining[:max_cols]  # até 12 por linha
        cols = st.columns(len(chunk))
        for i, (code, label) in enumerate(chunk):
            with cols[i]:
                st.button(
                    label,
                    key=f"main_quick_{code}",
                    on_click=lambda code=code: gm_quick_click(code),
                    use_container_width=True
                )
        remaining = remaining[max_cols:]

    _paint_adv_rede_buttons()

def _criaPlacar():
    # --- LINHA DO PLACAR + QUADRA + FILTROS (MODO JOGO) — ULTRA COMPACT (sem :has) ---
    try:
        _df_for_score = df_hm if 'df_hm' in locals() else current_set_df(
            st.session_state.frames, st.session_state.match_id, st.session_state.set_number
        )
        _home_pts, _away_pts = set_score_from_df(_df_for_score)
        _set_raw = st.session_state.get("set_number"); _setn = 1
        if _set_raw is not None:
            _s = str(_set_raw).strip()
            if _s.isdigit(): _setn = int(_s)
        st.markdown(
            f"<div class='gm-preline'><strong>Set {_setn} — Placar: {_home_pts} x {_away_pts}</strong></div>", unsafe_allow_html=True
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

    # ===== Centralização real (reduz espaços) – funciona no mobile =====
    st.markdown("""
    <style>
      .uv-scorebar{
        display:flex; justify-content:center; align-items:flex-end;
        gap:10px; margin:4px 0 0 0;            /* menos espaço vertical no desktop */
      }
      .uv-scorebar .score-box{
        display:flex; flex-direction:column; align-items:center;
      }
      .uv-scorebar .score-team{
        font-weight:700; text-align:center; font-size:32px;  /* maior no desktop */
        line-height:1.1;
      }
      .uv-scorebar .score-points{
        font-size:56px; line-height:1; margin-top:2px;       /* maior no desktop */
      }
      .uv-scorebar .score-x{
        font-size:28px; line-height:1; opacity:.9;
      }

      /* Mobile */
      @media (max-width: 600px){
        .uv-scorebar{ gap:8px; margin:0; }                   /* tira espaço em cima no mobile */
        .uv-scorebar .score-team{ font-size:20px; }          /* ajuste fino no mobile */
        .uv-scorebar .score-points{ font-size:40px; }        /* corrigido 40px (antes 40x) */
        .uv-scorebar .score-x{ font-size:22px; }
        .gm-preline{ margin-bottom:4px !important; }         /* reduz espaço logo acima do placar */
      }
    </style>
    """, unsafe_allow_html=True)

    frames = st.session_state.frames
    df_set = current_set_df(frames, st.session_state.match_id, st.session_state.set_number)
    home_pts, away_pts = set_score_from_df(df_set)

    stf = frames["sets"]; sm = stf[stf["match_id"] == st.session_state.match_id]
    home_sets_w = int((sm["winner_team_id"] == 1).sum())
    away_sets_w = int((sm["winner_team_id"] == 2).sum())

    # Nomes (com contagem de sets vencidos ao lado se > 0)
    try:
        _home_label_base = html.escape(home_name or "Nós")
    except Exception:
        _home_label_base = "Nós"
    try:
        _away_label_base = html.escape(away_name)
    except Exception:
        _away_label_base = "ADV"

    _home_label = _home_label_base + (f" ({home_sets_w})" if home_sets_w > 0 else "")
    _away_label = _away_label_base + (f" ({away_sets_w})" if away_sets_w > 0 else "")

    # --- Render do placar, agora sem colunas (zero “espaço fantasma”) ---
    st.markdown(
        f"""
        <div class="uv-scorebar">
          <div class="score-box">
            <div class="score-team">{_home_label}</div>
            <div class="score-points">{home_pts}</div>
          </div>
          <div class="score-x">×</div>
          <div class="score-box">
            <div class="score-team">{_away_label}</div>
            <div class="score-points">{away_pts}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    # (Set summary opcional estava comentado na sua versão; mantive assim)
    # st.markdown(
    #     f"<div class='set-summary'>Sets: <b>{home_sets_w}</b> × <b>{away_sets_w}</b>  |  Set atual: <b>{st.session_state.set_number}</b></div>",
    #     unsafe_allow_html=True
    # )

    render_court_html(
        pts_succ, pts_errs, pts_adv, pts_adv_err,
        enable_click=True, key="gm", show_numbers=st.session_state.show_heat_numbers
    )

def uv_apply_game_mode_branding(our_team_id: int | None = None) -> None:
    """
    Aplica a marca visual no Modo Jogo:
      - Marca-d’água central leve ( ::before )
      - Logo pequena fixa no canto superior direito ( ::after ) como fallback

    Onde usar:
      Chame imediatamente após abrir o wrapper do Modo Jogo:
        st.markdown('<div id="uv-game-mode">', unsafe_allow_html=True)
        uv_apply_game_mode_branding(OUR_TEAM_ID)
    """

    try:
        # 1) tenta session_state (mesma lógica já usada no seu código)
        logo_path = st.session_state.get("app_bg_logo", "")

        # 2) fallback: equipes.team_logo_path (mesma lógica já usada)
        if not logo_path:
            eq = st.session_state.frames.get("equipes", pd.DataFrame())
            tid = our_team_id if our_team_id is not None else st.session_state.get("OUR_TEAM_ID")
            if not tid:
                tid = glargals().get("OUR_TEAM_ID", None)
            if isinstance(eq, pd.DataFrame) and not eq.empty and "team_logo_path" in eq.columns and tid is not None:
                row = eq[eq["team_id"] == tid]
                if not row.empty:
                    logo_path = str(row.iloc[0].get("team_logo_path") or "")

        if not logo_path:
            return  # sem logo, não injeta nada

        p = Path(logo_path)
        if not p.exists():
            return

        mime = mimetypes.guess_type(p.name)[0] or "image/png"
        b64  = base64.b64encode(p.read_bytes()).decode("utf-8")

        # CSS:
        # - ::before = marca-d’água central leve, com mix-blend para aparecer mesmo com fundos claros
        # - ::after  = selo pequeno no canto superior direito (fallback visual)
        st.markdown(
            f"""
            <style>
              #uv-game-mode {{
                position: relative !important;
                isolation: isolate !important; /* contexto de empilhamento separado */
              }}

              /* Marca-d’água central leve */
              #uv-game-mode::before {{
                content: "";
                position: absolute; inset: 0;
                pointer-events: none;
                background: url('data:{mime};base64,{b64}') center 45% no-repeat;
                background-size: min(42vw, 360px);
                opacity: 0.12;             /* ajuste fino de intensidade */
                z-index: 0;                 /* atrás do conteúdo */
                mix-blend-mode: multiply;   /* melhora visibilidade em fundos claros */
                filter: saturate(0.85);
              }}

              /* Logo pequena fixa (fallback garantido) */
              #uv-game-mode::after {{
                content: "";
                position: absolute;
                top: -80px; right: 8px;
                width: 88px; height: 88px;      /* ajuste o tamanho conforme preferir */
                background: url('data:{mime};base64,{b64}') center center / contain no-repeat;
                opacity: 0.95;
                z-index: 3;                      /* acima dos componentes */
                pointer-events: none;
              }}

              /* No mobile, reduz um pouco o selo para não encostar nos botões */
              @media (max-width: 640px) {{
                #uv-game-mode::after {{
                  top: 6px; right: 6px; width: 56px; height: 56px; opacity: 0.92;
                }}
              }}
            </style>
            """,
            unsafe_allow_html=True
        )

    except Exception:
        # Silencioso para não quebrar o Modo Jogo em caso de exceção
        pass

# =========================
# Chamada inicial: ODO JOGO
# =========================
if st.session_state.game_mode:
    with st.container():
        # ABRE o wrapper do Modo Jogo
        st.markdown('<div id="uv-game-mode">', unsafe_allow_html=True)

        # === (NOVO) Marca-d'água específica do Modo Jogo ===
        # ABRE o wrapper do Modo Jogo
        st.markdown('<div id="uv-game-mode">', unsafe_allow_html=True)

        # ✔️ NOVO: aplica marca visual (marca-d’água + logo no canto)
        uv_apply_game_mode_branding(OUR_TEAM_ID)

# === (FIM NOVO) Marca-d'água ===

        # (SEU CÓDIGO EXISTENTE)
        # st.markdown('<div class="gm-players-row">', unsafe_allow_html=True)
        uv_init_state()
        _criaBtnsJogadoras()
        _criaBtnsAtalhos()
        _criaPlacar()
    # FECHA o wrapper do Modo Jogo (DEPOIS de renderizar tudo)
    st.markdown('</div>', unsafe_allow_html=True)

    st.stop()

# =========================
# Garante inicialização de chaves de estado usadas pelos botões
# =========================
def _ensure_uv_state() -> None:
    """Garante que as chaves de estado usadas pelos botões existam."""
    ss = st.session_state
    if "uv_player_state" not in ss or not isinstance(ss["uv_player_state"], dict):
        ss["uv_player_state"] = {}
    if "uv_adv_state" not in ss or ss["uv_adv_state"] not in ("neutral", "active", "ok", "err"):
        ss["uv_adv_state"] = "neutral"
    if "uv_active_player" not in ss:
        ss["uv_active_player"] = None
    # defaults úteis para o fluxo (não obrigatórios, mas seguros)
    if "q_side" not in ss:
        ss["q_side"] = "Nós"
    if "q_result" not in ss:
        ss["q_result"] = "Acerto"

# — Chamada inicial: garante inicialização de chaves de estado usadas pelos botões
try:
    _ensure_uv_state()
except Exception:
    pass

# =========================
# Uma das chamadas de debug
# =========================
if DEBUG_PRINTS:
    if show_debug_ui() and st.session_state.get("dbg_prints"):
            st.markdown("---")
            st.markdown("**🧰 Debug (logs recentes)**")
            
    if show_debug_ui():
        with st.expander("🔎 Debug Heatmap (Painel Principal)"):
            st.write(
                f"Acertos (azul): **{len(pts_succ)}**  |  Erros (vermelho): **{len(pts_errs)}**  |  "
                f"ADV acertos (magenta): **{len(pts_adv)}**  |  ADV erros (roxo): **{len(pts_adv_err)}**"
            )
 
# =========================
# Chamada inicial: Painel principal
# =========================
with st.container():
    frames = st.session_state.frames
    df_set = current_set_df(frames, st.session_state.match_id, st.session_state.set_number)

    # wrapper p/ CSS responsivo
    st.markdown('<div id="hm-wrap">', unsafe_allow_html=True)

    # 2 colunas: mapa (bar5) e controles (bar4)
    # DICA: no desktop a esquerda fica o mapa; no mobile elas empilham (via CSS abaixo)
    bar5, bar4 = st.columns([1.0, 0.35])

    with bar4:
        st.session_state.player_label_mode = st.radio(
            "Mostrar botões por:", options=["Número", "Nome"], horizontal=True,
            index=["Número", "Nome"].index(st.session_state.player_label_mode),
            key="player_label_mode_main"
        )
        st.session_state.show_heat_numbers = st.checkbox(
                "Mostrar número/ADV nas bolinhas",
                value=st.session_state.show_heat_numbers,
                key="hm_show_numbers_main"
            )
    with bar5:
        st.markdown("**🗺️ Mapa de Calor: Informações mostradas**")

        # marcador para a faixa 1 (usaremos CSS p/ torná-la responsiva)
        st.markdown('<div id="hm-row1-marker"></div>', unsafe_allow_html=True)

        # === ROW 1: 3 colunas no desktop (vira 1 no mobile) ===
        c1, c2, c3 = st.columns([2.2, 1.2, 1.2])

        # ---- col1: Jogadoras (multiselect) ----
        with c1:
            nums_all = resolve_our_roster_numbers(st.session_state.frames)
            sel_players_list = st.multiselect(
                "Jogadoras (nº)",
                options=nums_all,
                default=[],
                key="hm_players_filter_main_multi",
                help="Selecione uma ou mais jogadoras. Deixe vazio para considerar todas."
            )
        sel_players = None if len(sel_players_list) == 0 else sel_players_list

        # ---- col2: Nossos acertos/erros ----
        with c2:
            show_success = st.checkbox("Nossos acertos", value=True, key="hm_show_succ_main")
            show_errors  = st.checkbox("Nossos erros",   value=True, key="hm_show_err_main")
        # ---- col3: ADV acertos/erros + Mostrar número/ADV ----
        with c3:
            show_adv_pts = st.checkbox("ADV acertos",    value=True, key="hm_show_adv_ok_main")
            show_adv_err = st.checkbox("ADV erros",      value=True, key="hm_show_adv_err_main")
            
        # === CSS: força a ROW1 virar grid responsivo de 3→1 colunas e limita larguras ===
        st.markdown(
            """
            <style>
            /* Nunca deixe o app rolar horizontalmente */
            [data-testid="stAppViewContainer"] { overflow-x: hidden; }

            /* Localiza o bloco de colunas imediatamente após o marcador */
            #hm-row1-marker + div[data-testid="stHorizontalBlock"] {
                display: grid !important;
                grid-template-columns: 2.2fr 1.2fr 1.2fr;  /* desktop: 3 colunas */
                gap: 10px !important;
                width: 100%;
            }
            /* zera larguras fixas herdadas das colunas do Streamlit */
            #hm-row1-marker + div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
                width: auto !important;
                flex: 1 1 auto !important;
                min-width: 0 !important;
            }

            /* Multiselect ocupa toda a coluna mas com limite p/ não “estourar” */
            #hm-row1-marker + div [data-baseweb="select"] {
                width: 100% !important;
                max-width: 460px;            /* desktop/tablet */
            }

            /* Tablets: 2 colunas (jogadoras | grupo Nossos+ADV) */
            @media (max-width: 1050px) {
                #hm-row1-marker + div[data-testid="stHorizontalBlock"] {
                grid-template-columns: 1.6fr 1.4fr;   /* 2 colunas */
                }
                /* Junta c2 e c3 numa coluna empilhando internamente */
                #hm-row1-marker + div[data-testid="stHorizontalBlock"] > div[data-testid="column"]:nth-child(3) {
                grid-column: 2 / 3;
                }
            }

            /* Celular: 1 coluna (tudo empilhado, SEM rolagem lateral) */
            @media (max-width: 700px) {
                #hm-row1-marker + div[data-testid="stHorizontalBlock"] {
                grid-template-columns: 1fr;   /* empilha */
                }
                #hm-row1-marker + div [data-baseweb="select"] {
                max-width: 100%;              /* ocupa 100% no mobile */
                }
            }

            /* Garante quebra de linha em labels longas (não cria scroll) */
            #hm-row1-marker + div label {
                white-space: normal !important;
                word-break: break-word !important;
            }
            </style>
            """,
            unsafe_allow_html=True
        )


    # ================= Jogadoras (Número/Nome) — cores no próprio botão; sem textos =================
    #st.caption("**Jogadoras:**")
    _ensure_uv_state()
    # Indicador de selecionada (jogadora ou ADV)
    # ================= RETIRANDO DA VISUALIZACAO INICIAL =================
    #_criaBtnsJogadoras()
    #_criaBtnsAtalhos()

# -------- DIREITA --------
#with right:

def salva_google(frames, reason, statuses, _perf):
    """
    Salva em Google Sheets (e Webhook como fallback), registrando métricas no _perf.
    - frames: dict de DataFrames
    - reason: razão do salvamento (rally, set_open, set_close, match_close, manual)
    - statuses: list[str] mutável para acumular mensagens
    - _perf: dicionário de métricas criado por _perf_begin
    """
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

# =========================
# Gravação final opcional do usuário -> Snapshot helpers (por partida) — AUTO-CONTIDOS ===
# =========================
# === 
def _mask_eq_num_or_str(series, value):
    import pandas as pd
    s_num = pd.to_numeric(series, errors="coerce")
    v_num = pd.to_numeric(value, errors="coerce")
    return (s_num == v_num) if not pd.isna(v_num) else series.astype(str).str.strip() == str(value).strip()

def _filter_frames_for_match(frames, match_id):
    import pandas as pd
    out = {}
    mt  = frames.get("amistosos", pd.DataFrame())
    stf = frames.get("sets", pd.DataFrame())
    rl  = frames.get("rallies", pd.DataFrame())
    eq  = frames.get("equipes", pd.DataFrame())
    pl  = frames.get("jogadoras", pd.DataFrame())

    out["amistosos"] = mt[_mask_eq_num_or_str(mt["match_id"], match_id)] if isinstance(mt, pd.DataFrame) and "match_id" in mt.columns else (mt if isinstance(mt, pd.DataFrame) else pd.DataFrame())
    out["sets"]      = stf[_mask_eq_num_or_str(stf["match_id"], match_id)] if isinstance(stf, pd.DataFrame) and "match_id" in stf.columns else (stf if isinstance(stf, pd.DataFrame) else pd.DataFrame())
    out["rallies"]   = rl[_mask_eq_num_or_str(rl["match_id"], match_id)] if isinstance(rl, pd.DataFrame) and "match_id" in rl.columns else (rl if isinstance(rl, pd.DataFrame) else pd.DataFrame())

    try:
        import pandas as pd
        team_ids = set()
        team_ids.add(int(OUR_TEAM_ID))
        if isinstance(out["amistosos"], pd.DataFrame) and not out["amistosos"].empty and "away_team_id" in out["amistosos"].columns:
            team_ids.update(pd.to_numeric(out["amistosos"]["away_team_id"], errors="coerce").dropna().astype(int).tolist())
        if isinstance(eq, pd.DataFrame) and "team_id" in eq.columns and team_ids:
            out["equipes"] = eq[pd.to_numeric(eq["team_id"], errors="coerce").astype("Int64").isin(list(team_ids)).fillna(False)].copy()
        else:
            out["equipes"] = eq if isinstance(eq, pd.DataFrame) else pd.DataFrame()
    except Exception:
        out["equipes"] = eq if isinstance(eq, pd.DataFrame) else pd.DataFrame()

    out["jogadoras"] = pl[_mask_eq_num_or_str(pl["team_id"], OUR_TEAM_ID)] if isinstance(pl, pd.DataFrame) and "team_id" in pl.columns else (pl if isinstance(pl, pd.DataFrame) else pd.DataFrame())
    return out

def uv_snapshot_prompt(kind="set_close", match_id=None):
    try:
        frames = st.session_state.frames
    except Exception:
        return
    mid = match_id if match_id is not None else st.session_state.get("match_id")
    if mid is None:
        return
    try:
        filtered = _filter_frames_for_match(frames, mid)
    except Exception:
        filtered = frames
    payload = {"_meta":{"exported_at":_dt.now().isoformat(timespec="seconds"),"match_id":int(mid),"kind":str(kind)}}
    for k, df in filtered.items():
        if isinstance(df, pd.DataFrame):
            try:
                payload[k] = df.to_dict(orient="records")
            except Exception:
                payload[k] = []
    data = _json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    with st.container():
        st.info("Deseja salvar um arquivo temporário com os dados **deste jogo**?")
        c1, c2 = st.columns([1,1])
        with c1:
            st.download_button(
                "⬇️ Baixar dados do jogo (JSON)",
                data=data,
                file_name=f"univolei_{kind}_match{mid}_{_dt.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
                key=f"dl_snap_{kind}_{mid}",
                use_container_width=True
            )
        with c2:
            st.button("Não, seguir normalmente", key=f"skip_snap_{kind}_{mid}", use_container_width=True)

# =========================
# Logo de fundo na criação de time
# =========================
# === Marca-d'água / background de seção ===
def _uv_encode_file_to_data_url(path: str | os.PathLike) -> str | None:
    """Converte um arquivo de imagem local em data URL (inline)."""
    try:
        p = Path(path)
        if not p.exists():
            return None
        mime, _ = mimetypes.guess_type(p.name)
        mime = mime or "image/png"
        data = base64.b64encode(p.read_bytes()).decode("ascii")
        return f"data:{mime};base64,{data}"
    except Exception:
        return None

def uv_inject_watermark_css(
    data_url: str,
    selectors: list[str],
    *,
    opacity: float = 0.12,
    size: str = "clamp(160px, 40vw, 520px)",   # controla o tamanho (responsivo)
    position: str = "center center",           # ex.: "center 40%", "right 20px bottom 30%"
    zindex: int = 0
):
    """
    Injeta CSS que desenha a imagem como marca-d'água via ::before nos 'selectors' informados.
    Dica: use ids que você mesmo renderiza com st.markdown('<div id="...">').
    """
    sel = ", ".join(selectors)
    st.markdown(
        f"""
        <style>
        {sel} {{
          position: relative !important;
          z-index: {zindex};
        }}
        {sel}::before {{
          content: "";
          position: absolute;
          inset: 0;
          pointer-events: none;
          background-image: url("{data_url}");
          background-repeat: no-repeat;
          background-position: {position};
          background-size: {size};
          opacity: {max(0.0, min(opacity, 1.0))};
        }}
        /* Modo escuro: leve ajuste opcional */
        @media (prefers-color-scheme: dark) {{
          {sel}::before {{ opacity: {max(0.0, min(opacity*0.9, 1.0))}; }}
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

def uv_guess_team_logo_data_url() -> str | None:
    """
    Busca a logo em:
      1) st.session_state['team_logo_path'] (se você salvou no 'Configurar Time')
      2) ./imgs/logo_team_1.* (png|jpg|jpeg|webp)
    Retorna data URL para usar no CSS.
    """
    # 1) session_state direto
    p = st.session_state.get("team_logo_path")
    if p:
        du = _uv_encode_file_to_data_url(p)
        if du:
            return du

    # 2) nomes comuns na pasta imgs
    base = Path(__file__).parent / "imgs"
    for name in ("logo_team_1.png", "logo_team_1.jpg", "logo_team_1.jpeg", "logo_team_1.webp", "univolei_logo.png"):
        du = _uv_encode_file_to_data_url(base / name)
        if du:
            return du
    return None


###Chamada inicial: FUNDO DA CRIACAO DE NOVO TIME
try:
    lp = st.session_state.get("app_bg_logo") or _teamcfg_current_logo_path()
    if lp:
        _teamcfg_apply_watermark(Path(lp), opacity=0.10)
except Exception:
    pass


#///////////////////////////////////////////////
# =========================
# Chamada inicial: Verificar salvamentos e bases de dados
# =========================
p = Path(st.session_state.get("db_path", ""))
gsheet_on = bool(st.session_state.get("persist_google_enabled") or st.session_state.get("gsheets_enable") or st.session_state.get("salvar_google"))

st.info(
    "📦 **INFORMAÇÕES TEMPORÁRIAS: Fonte atual de dados**\n\n"
    f"- **db_path**: `{p}` (existe: {p.exists() if p else False})\n"
    f"- **Tamanho**: {p.stat().st_size if p and p.exists() else 'N/A'} bytes\n"
    f"- **Google Sheets ativo**: {gsheet_on}\n"
    f"- **Backup dir**: `{st.session_state.get('backups_dir', 'backups')}`\n",
    icon="ℹ️"
)


# tenta ler uma célula “assinatura” da aba 'amistosos' para mostrar a origem
try:
    frames = st.session_state.frames
    amistosos = frames.get("amistosos")
    origem_hint = "frames em memória"
    if amistosos is not None and len(amistosos) > 0:
        st.caption(f"Aba 'amistosos' carregada: {len(amistosos)} linhas ({origem_hint}).")
except Exception as e:
    st.warning(f"Não consegui inspecionar frames: {e}")

#///////////////////////////////////////////////
# =========================
# Chamada inicial: Boot para Render
# =========================
if __name__ == "__main__":
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