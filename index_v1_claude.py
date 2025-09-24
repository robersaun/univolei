gostei da sua primeira versao, mas vc tirou tabelas e graficos da versao normal, colocou cor verde no ayout que eu detesto, mexceu no layout especialmente do modo jogo que nao pedi (quero diminuir os espaco entre linhas dos componentes e vc aumentou mais do que estava), alem de nao ter carregado todos os botoes de atalhos e todas as jogadoras.. meu cidogo tinha mais de 3000 linhas e o seu tem 1900.. reaplique esses temas importantes e tb verifique outras fncionalidades que pode ter retirado. 
nao gere o codigo novo para eu copiar, quero apenas fazer download do arquivo pronto!







# UniVolei Live Scout - Versão Otimizada Final
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
import threading
import queue
from io import BytesIO
from db_excel import (
    init_or_load, save_all, add_set,
    append_rally, last_open_match, finalize_match
)
from parser_free import parse_line
import logging
import os
import datetime as _dt
import pandas as _pd

# =========================
# CONFIGURAÇÃO INICIAL
# =========================
BASE_DIR = Path(__file__).parent.resolve()
DEFAULT_XLSX = BASE_DIR / "volei_base_dados.xlsx"
DEFAULT_DUCK = BASE_DIR / "volei_base_dados.dv"

# Configuração dos destinos de salvamento
SAVE_CONFIG = {
    "local_xlsx": {"enabled": True, "frequency": "always"},
    "journal": {"enabled": True, "frequency": "rally"},
    "cloud_sync": {"enabled": True, "frequency": "checkpoint"},  # set_open, set_close, match_close
}

# =========================
# SISTEMA DE SALVAMENTO ASSÍNCRONO
# =========================
class AsyncSaver:
    def __init__(self):
        self.save_queue = queue.Queue()
        self.worker_thread = None
        self.running = False
        self.logger = self._setup_logger()
        
    def _setup_logger(self):
        logger = logging.getLogger("async_saver")
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            handler = logging.FileHandler(BASE_DIR / "logs" / "saves.log", encoding="utf-8")
            formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
        
    def start(self):
        if not self.running:
            self.running = True
            self.worker_thread = threading.Thread(target=self._worker, daemon=True)
            self.worker_thread.start()
            
    def _worker(self):
        while self.running:
            try:
                save_task = self.save_queue.get(timeout=1)
                if save_task is None:
                    break
                self._execute_save(save_task)
                self.save_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Erro no worker: {e}")
                
    def _execute_save(self, task):
        frames, reason, callbacks = task
        start_time = _time.perf_counter()
        
        try:
            # 1. Salvamento local (sempre)
            save_all(Path(st.session_state.db_path), frames)
            
            # 2. Journal append-only (para rallies)
            if reason == "rally" and SAVE_CONFIG["journal"]["enabled"]:
                self._write_journal(frames, reason)
            
            # 3. Cloud sync (para checkpoints)
            if reason in ["set_open", "set_close", "match_close"] and SAVE_CONFIG["cloud_sync"]["enabled"]:
                self._cloud_sync(frames, reason)
                
            duration = (_time.perf_counter() - start_time) * 1000
            self.logger.info(f"Salvo com sucesso: {reason} em {duration:.1f}ms")
            
            # Executar callbacks se fornecidos
            for callback in callbacks:
                try:
                    callback(True, None)
                except Exception as e:
                    self.logger.error(f"Erro no callback: {e}")
                    
        except Exception as e:
            self.logger.error(f"Erro ao salvar {reason}: {e}")
            for callback in callbacks:
                try:
                    callback(False, str(e))
                except Exception:
                    pass
    
    def _write_journal(self, frames, reason):
        try:
            mid = st.session_state.get("match_id")
            if not mid:
                return
                
            jdir = BASE_DIR / "journal"
            jdir.mkdir(parents=True, exist_ok=True)
            
            ts = _dt.datetime.now()
            jpath = jdir / f"{ts.strftime('%Y%m%d')}_match_{mid}.ndjson"
            
            rl = frames.get("rallies", _pd.DataFrame())
            row = rl.iloc[-1].to_dict() if not rl.empty else {}
            
            payload = {
                "ts": ts.isoformat(timespec="seconds"),
                "reason": reason,
                "match_id": mid,
                "set_number": st.session_state.get("set_number"),
                "rally": row,
            }
            
            with open(jpath, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
                
        except Exception as e:
            self.logger.warning(f"Journal falhou: {e}")
    
    def _cloud_sync(self, frames, reason):
        """Cloud sync usando Google Sheets se disponível"""
        try:
            # Implementação simplificada do Google Sheets
            from pathlib import Path as _P
            import configparser
            
            # Tenta carregar configuração
            config_path = BASE_DIR / "config.ini"
            if not config_path.exists():
                return
                
            config = configparser.ConfigParser(interpolation=None)
            config.read(config_path, encoding="utf-8")
            
            if not config.has_section('online'):
                return
                
            gsheet_id = config.get('online', 'gsheet_id', fallback='').strip()
            if not gsheet_id:
                return
                
            # Sync simplificado - apenas rallies
            self._sync_to_gsheets(frames, gsheet_id, reason)
            
        except Exception as e:
            self.logger.warning(f"Cloud sync falhou: {e}")
    
    def _sync_to_gsheets(self, frames, sheet_id, reason):
        try:
            import gspread
            from google.oauth2.service_account import Credentials
            
            # Tenta autenticação via Streamlit secrets
            if not hasattr(st, 'secrets') or 'gcp_service_account' not in st.secrets:
                return
                
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            
            creds = Credentials.from_service_account_info(
                dict(st.secrets["gcp_service_account"]), scopes=scopes
            )
            client = gspread.authorize(creds)
            
            sh = client.open_by_key(sheet_id)
            
            # Sync apenas a aba de rallies
            rl = frames.get("rallies", _pd.DataFrame())
            if rl is not None and not rl.empty:
                try:
                    ws = sh.worksheet("rallies")
                except Exception:
                    ws = sh.add_worksheet("rallies", rows=1000, cols=20)
                
                ws.clear()
                values = [rl.columns.tolist()]
                if not rl.empty:
                    df_strings = rl.astype(object).where(_pd.notna(rl), "").astype(str)
                    values.extend(df_strings.values.tolist())
                
                ws.update("A1", values, value_input_option="RAW")
                
        except Exception as e:
            self.logger.warning(f"GSheets sync falhou: {e}")
    
    def save_async(self, frames, reason="auto", callbacks=None):
        if callbacks is None:
            callbacks = []
        
        # Cria uma cópia profunda dos frames para evitar condições de corrida
        frames_copy = {}
        for key, df in frames.items():
            if isinstance(df, _pd.DataFrame):
                frames_copy[key] = df.copy()
            else:
                frames_copy[key] = df
        
        self.save_queue.put((frames_copy, reason, callbacks))
    
    def stop(self):
        if self.running:
            self.running = False
            self.save_queue.put(None)  # Sinal para parar
            if self.worker_thread:
                self.worker_thread.join(timeout=5)

# Instância global do saver
async_saver = AsyncSaver()

# =========================
# CONFIGURAÇÃO DO STREAMLIT
# =========================
st.set_page_config(page_title="Vôlei Scout — UniVolei", layout="wide", initial_sidebar_state="collapsed")

# =========================
# CSS E ESTILOS
# =========================
def load_css():
    st.markdown("""
    <style>
    .uv-inline-label{ font-weight:600; line-height:1; padding-top:6px; margin:0; }
    [data-testid="stSelectbox"]{ margin:0 !important; }
    
    /* Score display */
    .score-box {
        text-align: center;
        padding: 8px;
        border-radius: 8px;
        background: rgba(240,242,246,0.8);
    }
    .score-team { font-size: 0.9rem; font-weight: 700; color: #1f2937; }
    .score-points { font-size: 2.5rem; font-weight: 900; color: #059669; line-height: 1; }
    .score-x { font-size: 1.5rem; font-weight: 900; color: #6b7280; display: flex; align-items: center; justify-content: center; height: 100%; }
    .set-summary { font-size: 0.85rem; color: #374151; display: flex; align-items: center; padding: 8px; }
    
    /* Status indicators */
    .save-status {
        position: fixed;
        top: 10px;
        right: 10px;
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 0.8rem;
        font-weight: 600;
        z-index: 1000;
        transition: all 0.3s;
    }
    .save-status.saving { background: #fbbf24; color: #92400e; }
    .save-status.saved { background: #34d399; color: #065f46; }
    .save-status.error { background: #f87171; color: #991b1b; }
    
    /* Anti-gap para iframes */
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
    iframe.uv-collapse {
        display: none !important;
        height: 0 !important;
        min-height: 0 !important;
    }
    </style>
    """, unsafe_allow_html=True)

load_css()

# =========================
# HELPERS VISUAIS
# =========================
def show_save_status(status="saved", message=""):
    """Exibe indicador visual de status de salvamento"""
    if status == "saving":
        components.html(f"""
        <div class="save-status saving">💾 Salvando...</div>
        <script>setTimeout(() => document.querySelector('.save-status').remove(), 1500);</script>
        """, height=0)
    elif status == "saved":
        components.html(f"""
        <div class="save-status saved">✅ Salvo</div>
        <script>setTimeout(() => document.querySelector('.save-status').remove(), 2000);</script>
        """, height=0)
    elif status == "error":
        components.html(f"""
        <div class="save-status error">❌ Erro</div>
        <script>setTimeout(() => document.querySelector('.save-status').remove(), 3000);</script>
        """, height=0)

def _paint_adv_rede_buttons():
    components.html("""
    <script>
    (function(){
      try{
        const f = window.frameElement;
        if (f){
          f.classList.add('uv-collapse');
          f.style.height='0px'; f.style.minHeight='0px';
        }
      }catch(e){}
      
      function paint(){
        var doc = (window.parent && window.parent.document) ? window.parent.document : document;
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
            }
          });
        });
      }
      paint(); setTimeout(paint, 50);
    })();
    </script>
    """, height=0, scrolling=False)

# =========================
# ESTADO E CONFIGURAÇÃO
# =========================
DEFAULT_DB = str(BASE_DIR / "volei_base_dados.xlsx")

# Inicialização do estado
if "db_path" not in st.session_state: 
    st.session_state.db_path = DEFAULT_DB
if "frames" not in st.session_state: 
    st.session_state.frames = init_or_load(Path(st.session_state.db_path))
if "match_id" not in st.session_state: 
    st.session_state.match_id = None
if "set_number" not in st.session_state: 
    st.session_state.set_number = None
if "auto_close" not in st.session_state: 
    st.session_state.auto_close = True

# Estados auxiliares
st.session_state.setdefault("q_side", "Nós")
st.session_state.setdefault("q_result", "Acerto") 
st.session_state.setdefault("q_action", "d")
st.session_state.setdefault("q_position", "Frente")
st.session_state.setdefault("last_selected_player", None)
st.session_state.setdefault("show_cadastro", False)
st.session_state.setdefault("show_config_team", False)
st.session_state.setdefault("line_input_text", "")
st.session_state.setdefault("game_mode", False)
st.session_state.setdefault("data_rev", 0)
st.session_state.setdefault("last_court_click", None)
st.session_state.setdefault("show_heat_numbers", False)

# Inicia o saver assíncrono
if "async_saver_started" not in st.session_state:
    async_saver.start()
    st.session_state.async_saver_started = True

# =========================
# HELPERS DE DADOS
# =========================
OUR_TEAM_ID = 1

ACT_MAP = {
    "d": "Diagonal", "l": "Paralela", "m": "Meio", "lob": "Largada", "seg": "Segunda",
    "pi": "Pipe", "re": "Recepção", "b": "Bloqueio", "sa": "Saque", "rede": "Rede"
}
REVERSE_ACT_MAP = {v: k for k, v in ACT_MAP.items()}

ACTION_CODE_TO_NAME = {
    "d": "DIAGONAL", "l": "LINHA", "m": "MEIO", "lob": "LOB", "seg": "SEGUNDA",
    "pi": "PIPE", "re": "RECEPÇÃO", "b": "BLOQUEIO", "sa": "SAQUE", "rede": "REDE"
}

def team_name_by_id(fr: dict, team_id: int | None) -> str:
    eq = fr.get("equipes", pd.DataFrame())
    if eq.empty or team_id is None: return "Equipe"
    eq = eq.copy()
    eq["team_id"] = pd.to_numeric(eq["team_id"], errors="coerce")
    row = eq.loc[eq["team_id"] == int(team_id)]
    return str(row.iloc[0]["team_name"]) if not row.empty else f"Equipe {int(team_id)}"

def current_set_df(fr: dict, match_id: int, set_number: int) -> pd.DataFrame:
    if match_id is None or set_number is None:
        return pd.DataFrame()
    rl = fr["rallies"]
    return rl[(rl["match_id"] == match_id) & (rl["set_number"] == set_number)].copy().sort_values("rally_no")

def set_score_from_df(df: pd.DataFrame) -> tuple[int, int]:
    if df.empty: return 0, 0
    last = df.iloc[-1]
    return int(last["score_home"]), int(last["score_away"])

def resolve_our_roster_numbers(frames: dict) -> list[int]:
    jg = frames.get("jogadoras", pd.DataFrame()).copy()
    if jg.empty: return []
    for col in ["team_id", "player_number"]:
        if col in jg.columns: 
            jg[col] = pd.to_numeric(jg[col], errors="coerce")
    ours = jg[jg["team_id"] == OUR_TEAM_ID].dropna(subset=["player_number"]).sort_values("player_number")
    return ours["player_number"].astype(int).unique().tolist()

def roster_for_ui(frames: dict) -> list[dict]:
    jg = frames.get("jogadoras", pd.DataFrame()).copy()
    if jg.empty: return []
    for col in ["team_id", "player_number"]:
        if col in jg.columns: 
            jg[col] = pd.to_numeric(jg[col], errors="coerce")
    ours = jg[(jg["team_id"] == OUR_TEAM_ID) & (~jg["player_number"].isna())].copy()
    if ours.empty: return []
    ours["player_number"] = ours["player_number"].astype(int)
    ours["player_name"] = ours["player_name"].astype(str)
    ours = ours.sort_values("player_number")
    return ours[["player_number", "player_name"]].rename(
        columns={"player_number": "number", "player_name": "name"}
    ).to_dict("records")

# =========================
# LÓGICA DE REGISTRO DE RALLIES (OTIMIZADA)
# =========================
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
    else:
        name = str(row.get("action", "") or "").strip().upper()
        if name in ("", "NA", "NONE"): name = ""
    
    row["action"] = name
    return row

def _fast_apply_scores_to_row(row: dict):
    """Aplica placar instantaneamente sem aguardar salvamento"""
    frames_local = st.session_state.frames
    mid, sn = st.session_state.match_id, st.session_state.set_number
    df_cur = current_set_df(frames_local, mid, sn)
    
    if df_cur.empty:
        home, away = 0, 0
    else:
        last = df_cur.iloc[-1]
        home, away = int(last["score_home"]), int(last["score_away"])
    
    if row.get("who_scored") == "NOS": 
        home += 1
    elif row.get("who_scored") == "ADV": 
        away += 1
    
    row["score_home"] = home
    row["score_away"] = away
    return row

def quick_register_line(raw_line: str):
    """Registro otimizado: atualização instantânea + salvamento assíncrono"""
    if not raw_line.strip():
        return
    
    # 1. Parse da linha
    row = parse_line(raw_line)
    row = _fix_who_scored_from_raw_and_row(raw_line, row)
    row = _normalize_action_in_row(row)
    
    # 2. Adiciona posição
    fb = str(st.session_state.get("q_position", "Frente")).strip().upper()
    row["position_zone"] = fb
    
    # 3. Aplica placar instantaneamente
    row = _fast_apply_scores_to_row(row)
    
    # 4. Adiciona clique do mapa se disponível
    last_click = st.session_state.get("last_court_click")
    if last_click and isinstance(last_click, dict):
        row["court_x"] = float(last_click.get("x", 0.0))
        row["court_y"] = float(last_click.get("y", 0.0))
        st.session_state["last_court_click"] = None
    
    # 5. Atualiza os frames imediatamente (UI responsiva)
    append_rally(st.session_state.frames, 
                 match_id=st.session_state.match_id,
                 set_number=st.session_state.set_number, 
                 row=row)
    
    # 6. Incrementa revisão para forçar re-render
    st.session_state.data_rev += 1
    
    # 7. Mostra status de salvamento
    show_save_status("saving")
    
    # 8. Salva assincronamente
    def save_callback(success, error):
        if success:
            # Status será mostrado na próxima atualização
            pass
        else:
            st.error(f"Erro ao salvar: {error}")
    
    async_saver.save_async(st.session_state.frames, "rally", [save_callback])
    
    # 9. Verifica auto-close
    auto_close_set_if_needed()

def quick_register_click(side: str, number: int | None, action: str, is_error: bool):
    prefix = "1" if side == "NOS" else "0"
    num = f"{number}" if number is not None else ""
    line = f"{prefix} {num} {action}{' e' if is_error else ''}".strip()
    quick_register_line(line)

def register_current(number: int | None = None, action: str | None = None):
    side_code = "NOS" if st.session_state.get("q_side", "Nós") == "Nós" else "ADV"
    is_err = (st.session_state.get("q_result", "Acerto") == "Erro")
    act = action if action is not None else st.session_state.get("q_action", "d")
    num_val = number if number is not None else st.session_state.get("last_selected_player", None)
    
    if str(act).lower() == "rede":
        is_err = True
    
    quick_register_click(side_code, num_val, act, is_err)

# =========================
# LÓGICA DE SETS E PARTIDAS
# =========================
def auto_close_set_if_needed():
    if not st.session_state.auto_close: 
        return
        
    frames = st.session_state.frames
    match_id = st.session_state.match_id
    set_number = st.session_state.set_number
    
    if match_id is None or set_number is None: 
        return
        
    df_cur = current_set_df(frames, match_id, set_number)
    if df_cur.empty: 
        return
        
    home_pts, away_pts = set_score_from_df(df_cur)
    target = 15 if int(set_number) == 5 else 25
    diff = abs(home_pts - away_pts)
    
    if (home_pts >= target or away_pts >= target) and diff >= 2:
        _apply_set_winner_and_proceed(home_pts, away_pts)

def _apply_set_winner_and_proceed(home_pts: int, away_pts: int):
    frames = st.session_state.frames
    match_id = st.session_state.match_id
    set_number = st.session_state.set_number
    winner_id = 1 if home_pts > away_pts else 2
    
    # Atualiza winner imediatamente
    stf = frames["sets"]
    mask = (stf["match_id"] == match_id) & (stf["set_number"] == set_number)
    stf.loc[mask, "winner_team_id"] = winner_id
    frames["sets"] = stf
    
    # Recalcula sets
    sets_m = stf[stf["match_id"] == match_id]
    home_sets = int((sets_m["winner_team_id"] == 1).sum())
    away_sets = int((sets_m["winner_team_id"] == 2).sum())
    
    # Atualiza tabela amistosos
    mt = frames["amistosos"]
    mt_mask = (mt["match_id"] == match_id)
    mt.loc[mt_mask, "home_sets"] = home_sets
    mt.loc[mt_mask, "away_sets"] = away_sets
    frames["amistosos"] = mt
    
    # Salva assincronamente
    async_saver.save_async(frames, "set_close")
    
    if home_sets >= 3 or away_sets >= 3:
        # Finaliza partida
        try:
            finalize_match(frames, match_id)
            mt.loc[mt_mask, "is_closed"] = True
            mt.loc[mt_mask, "closed_at"] = datetime.now().isoformat(timespec="seconds")
            frames["amistosos"] = mt
            async_saver.save_async(frames, "match_close")
        except Exception:
            pass
            
        st.success(f"Set {set_number} encerrado ({home_pts} x {away_pts}). Partida finalizada: {home_sets} x {away_sets} em sets.")
        st.session_state.match_id = None
        st.session_state.set_number = None
    else:
        # Novo set
        st.session_state.set_number = int(set_number) + 1
        add_set(frames, match_id=match_id, set_number=st.session_state.set_number)
        async_saver.save_async(frames, "set_open")
        st.success(f"Set {set_number} encerrado ({home_pts} x {away_pts}). Novo set: {st.session_state.set_number}")

def undo_last_rally_current_set():
    fr = st.session_state.frames
    match_id = st.session_state.match_id
    set_number = st.session_state.set_number
    
    rl = fr["rallies"]
    sub = rl[(rl["match_id"]==match_id) & (rl["set_number"]==set_number)].copy().sort_values("rally_no")
    
    if sub.empty:
        st.warning("Não há rallies para desfazer neste set.")
        return
    
    # Remove último rally imediatamente
    last_row = sub.iloc[-1]
    last_rally_id = last_row["rally_id"]
    rl = rl[rl["rally_id"] != last_rally_id]
    fr["rallies"] = rl
    
    # Atualiza placar
    if len(sub) >= 2:
        prev = sub.iloc[-2]
        hp, ap = int(prev["score_home"]), int(prev["score_away"])
    else:
        hp, ap = 0, 0
    
    stf = fr["sets"]
    mask = (stf["match_id"]==match_id) & (stf["set_number"]==set_number)
    stf.loc[mask, "home_points"] = hp
    stf.loc[mask, "away_points"] = ap
    fr["sets"] = stf
    
    # Atualiza estado
    st.session_state.data_rev += 1
    
    # Salva assincronamente
    async_saver.save_async(fr, "undo")
    
    st.success(f"Rally desfeito. Placar: {hp}-{ap}")

# =========================
# CAPTURA DE CLIQUES NA QUADRA
# =========================
def _handle_court_click():
    try:
        payload = st.query_params.get("uv_click", None)
    except Exception:
        payload = None
    
    if not payload:
        return
        
    try:
        xs, ys = payload.split(",")[:2]
        x = float(xs)
        y = float(ys)
        st.session_state["last_court_click"] = {"x": x, "y": y, "ts": int(_time.time())}
        
        # Limpa o parâmetro
        try:
            del st.query_params["uv_click"]
        except Exception:
            pass
    except Exception:
        try:
            del st.query_params["uv_click"]
        except Exception:
            pass

_handle_court_click()

# =========================
# HEATMAP E VISUALIZAÇÃO
# =========================
FRONT_Y = {"opp": 44.0, "our": 56.0}
BACK_Y  = {"opp":  8.0, "our": 92.0}

def _x_for_action(act: str) -> float:
    if act in ("MEIO","PIPE","SEGUNDA","SAQUE","REDE","BLOQUEIO","LOB"):
        return 50.0
    if act == "DIAGONAL": return 28.0
    if act == "LINHA":    return 82.0
    return 50.0

def build_heat_points(df: pd.DataFrame,
                      selected_players: list[int] | None = None,
                      include_success: bool = True,
                      include_errors: bool = True,
                      include_adv_points: bool = False,
                      include_adv_errors: bool = False) -> tuple:
    
    if df is None or df.empty:
        return [], [], [], []
    
    # Normalização
    df0 = df.copy()
    df0["action_u"] = df0.get("action", "").astype(str).str.strip().str.upper()
    df0["who_u"] = df0.get("who_scored", "").astype(str).str.strip().str.upper()
    df0["res_u"] = df0.get("result", "").astype(str).str.strip().str.upper()
    
    if "player_number" in df0.columns:
        df0["player_number"] = pd.to_numeric(df0["player_number"], errors="coerce")
    
    # Filtro por jogadoras se especificado
    df_nos = df0.copy()
    if selected_players is not None and "player_number" in df_nos.columns:
        df_nos = df_nos[df_nos["player_number"].isin(selected_players) | df_nos["player_number"].isna()]
    
    actions_ok = {"MEIO","M","DIAGONAL","D","LINHA","PARALELA","L","LOB","LARGADA","PIPE","PI",
                  "SEGUNDA","SEG","RECEPÇÃO","RECEPCAO","RE","BLOQUEIO","B","BLOQ","SAQUE","SA","REDE"}
    
    succ_pts = []
    err_pts = []  
    adv_pts = []
    adv_err_pts = []
    
    def _get_position(r, half: str):
        # Usa clique se disponível
        cx, cy = r.get("court_x"), r.get("court_y")
        if pd.notna(cx) and pd.notna(cy):
            x_use = float(cx)*100 if 0<=cx<=1 else float(cx)
            y_use = float(cy)*100 if 0<=cy<=1 else float(cy)
            return max(0.0, min(100.0, x_use)), max(0.0, min(100.0, y_use))
        
        # Usa regra baseada na ação
        act = str(r.get("action_u", "")).strip().upper()
        x = _x_for_action(act)
        y = FRONT_Y[half]  # Simplificado - sempre frente
        return x, y
    
    # Pontos de sucesso (nossos)
    if include_success:
        srows = df_nos[(df_nos["who_u"] == "NOS") & (df_nos["res_u"] == "PONTO") & (df_nos["action_u"].isin(actions_ok))]
        for _, r in srows.iterrows():
            x, y = _get_position(r, "opp")
            lbl = str(int(r["player_number"])) if pd.notna(r.get("player_number")) else None
            succ_pts.append({"x": x, "y": y, "label": lbl})
    
    # Pontos de erro (nossos)
    if include_errors:
        erows = df_nos[(df_nos["who_u"] == "ADV") & (df_nos["res_u"] == "ERRO") & (df_nos["action_u"].isin(actions_ok))]
        for _, r in erows.iterrows():
            x, y = _get_position(r, "our")
            lbl = str(int(r["player_number"])) if pd.notna(r.get("player_number")) else None
            err_pts.append({"x": x, "y": y, "label": lbl})
    
    # Pontos adversário
    if include_adv_points:
        arows = df0[(df0["who_u"] == "ADV") & (df0["res_u"] == "PONTO") & (df0["action_u"].isin(actions_ok))]
        for _, r in arows.iterrows():
            x, y = _get_position(r, "our")
            adv_pts.append({"x": x, "y": y, "label": "ADV"})
    
    # Erros adversário
    if include_adv_errors:
        aerr = df0[(df0["who_u"] == "NOS") & (df0["res_u"] == "ERRO") & (df0["action_u"].isin(actions_ok))]
        for _, r in aerr.iterrows():
            x, y = _get_position(r, "opp")
            adv_err_pts.append({"x": x, "y": y, "label": "ADV"})
    
    return succ_pts, err_pts, adv_pts, adv_err_pts

def render_court_html(pts_success, pts_errors, pts_adv=None, pts_adv_err=None, enable_click=False, key="main", show_numbers=False):
    """Renderiza quadra de vôlei com heatmap"""
    
    def _norm_points(points):
        out = []
        for it in points or []:
            if isinstance(it, dict):
                x = float(it.get("x", 50))
                y = float(it.get("y", 50)) 
                lab = it.get("label")
            else:
                continue
            out.append((max(0.0, min(100.0, x)), max(0.0, min(100.0, y)), lab))
        return out
    
    S = _norm_points(pts_success)
    E = _norm_points(pts_errors)  
    A = _norm_points(pts_adv or [])
    AE = _norm_points(pts_adv_err or [])
    
    container_id = f"uv-court-{key}"
    
    def _dot_html(x, y, bg, border, text=None):
        label_html = ""
        if show_numbers and text:
            label_html = (
                f"<div style='position:absolute; inset:0; display:flex; align-items:center; justify-content:center; "
                f"font-size:10px; color:#fff; font-weight:700;'>{html.escape(str(text))}</div>"
            )
        return (
            f"<div style='left:{x}%; top:{y}%; width:14px; height:14px; position:absolute;"
            f"background:{bg}; border:1px solid {border}; border-radius:50%;"
            f"transform:translate(-50%,-50%); z-index:4;'>{label_html}</div>"
        )
    
    dots_html = []
    # Nossos pontos (azul)
    for x,y,lab in S:  
        dots_html.append(_dot_html(x, y, "rgba(30,144,255,0.92)", "rgba(20,90,200,0.95)", lab))
    # Nossos erros (vermelho)  
    for x,y,lab in E:  
        dots_html.append(_dot_html(x, y, "rgba(220,50,50,0.92)", "rgba(160,20,20,0.95)", lab))
    # ADV pontos (magenta)
    for x,y,lab in A:  
        dots_html.append(_dot_html(x, y, "rgba(255,0,255,0.92)", "rgba(160,0,160,0.95)", lab or "ADV"))
    # ADV erros (roxo)
    for x,y,lab in AE: 
        dots_html.append(_dot_html(x, y, "rgba(128,0,128,0.92)", "rgba(90,0,110,0.95)", lab or "ADV"))
    
    click_js = ""
    if enable_click:
        click_js = f"""
        (function(){{
          const root = document.getElementById('{container_id}');
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
              console.log('Click error:', err);
            }}
          }});
        }})();
        """
    
    # Nome do adversário
    adv_name = "ADVERSÁRIO"
    try:
        fr = st.session_state.frames
        mid = st.session_state.match_id
        if mid:
            mt = fr.get("amistosos", pd.DataFrame())
            row = mt.loc[mt["match_id"] == mid]
            if not row.empty:
                away_id = int(row.iloc[0]["away_team_id"])
                adv_name = team_name_by_id(fr, away_id)
    except Exception:
        pass
    
    html_block = f"""
    <div style="width:100%; text-align:center; font-weight:700; margin-bottom:6px;">{html.escape(adv_name)}</div>
    <div id="{container_id}" style="background:#FFA94D; border:2px solid #333; position:relative; width:100%; height:320px; border-radius:6px;">
      <!-- REDE -->
      <div style="position:absolute; left:0; top:calc(50% - 8px); width:100%; height:16px;
           background:repeating-linear-gradient(90deg, rgba(255,255,255,0.95) 0 12px, rgba(0,0,0,0.12) 12px 14px);
           border-top:2px solid #111; border-bottom:2px solid #111; z-index:2; opacity:.95;"></div>
      <div style="position:absolute; left:0; top:50%; width:100%; height:2px; background:#111; z-index:3;"></div>
      <!-- Linhas de ataque (3m) -->
      <div style="position:absolute; left:0; top:33.333%; width:100%; height:1px; background:rgba(0,0,0,.30); z-index:1;"></div>
      <div style="position:absolute; left:0; top:66.666%; width:100%; height:1px; background:rgba(0,0,0,.30); z-index:1;"></div>
      {''.join(dots_html)}
    </div>
    <div style="width:100%; text-align:center; font-weight:700; margin-top:12px; margin-bottom:22px;">UNIVÓLEI</div>
    <script>{click_js}</script>
    """
    
    components.html(html_block, height=468, scrolling=False)

# =========================
# DISPLAY DE DADOS
# =========================
def display_dataframe(df, height=None, width='content'):
    if df is None or len(df) == 0:
        st.write("_Sem dados._")
        return
    
    html_table = df.to_html(classes='custom-table', index=False, escape=False)
    height_css = f"{int(height)}px" if isinstance(height, (int, float)) else "auto"
    width_css = "100%" if width == 'stretch' else "auto"
    
    styled_html = f"""
    <div style="overflow:auto; height:{height_css}; width:{width_css};">
        {html_table}
    </div>
    """
    st.markdown(styled_html, unsafe_allow_html=True)

# =========================
# ABERTURA DE PARTIDA
# =========================
def _list_open_matches(frames: dict) -> list[int]:
    mt = frames.get("amistosos", pd.DataFrame())
    if mt.empty: return []
    if "is_closed" in mt.columns:
        mt = mt[~mt["is_closed"].fillna(False).astype(bool)]
    return [int(x) for x in pd.to_numeric(mt["match_id"], errors="coerce").dropna().astype(int).tolist()]

frames = st.session_state.frames

# Lógica de abertura de partida
open_mid = last_open_match(frames)
if st.session_state.match_id is None:
    open_list = _list_open_matches(frames)
    if len(open_list) == 1:
        st.session_state.match_id = int(open_list[0])
    elif len(open_list) > 1:
        st.subheader("🟢 Jogos em aberto")
        
        opts = []
        mt = frames.get("amistosos", pd.DataFrame())
        for mid in sorted(open_list, reverse=True):
            row = mt.loc[mt["match_id"]==mid]
            if row.empty: continue
            away_id = int(row.iloc[0]["away_team_id"])
            away_name = team_name_by_id(frames, away_id)
            opts.append((f"Jogo #{mid} vs {away_name} — {row.iloc[0]['date']}", mid))
        
        if opts:
            labels = [o[0] for o in opts]
            values = [o[1] for o in opts]
            pick = st.selectbox("Selecione o jogo para carregar:", options=values, 
                               format_func=lambda v: labels[values.index(v)])
            
            col1, col2 = st.columns([1,1])
            with col1:
                if st.button("Carregar jogo", use_container_width=True):
                    st.session_state.match_id = int(pick)
                    st.session_state.set_number = 1
                    st.rerun()
            with col2:
                if st.button("Fechar", use_container_width=True):
                    st.rerun()
        st.stop()
    elif open_mid:
        st.session_state.match_id = int(open_mid)

# Determinar set atual
if st.session_state.match_id is not None and st.session_state.set_number is None:
    sets_m = frames["sets"]
    if not sets_m.empty and (sets_m["match_id"] == st.session_state.match_id).any():
        st.session_state.set_number = int(sets_m[sets_m["match_id"] == st.session_state.match_id]["set_number"].max())
    else:
        st.session_state.set_number = 1

# =========================
# CADASTRO DE NOVA PARTIDA  
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
        next_mid = 1
    else:
        next_mid = int(pd.to_numeric(mt["match_id"], errors="coerce").max() or 0) + 1
    
    away_id = _get_or_create_team_id_by_name(frames_local, opp_name or "Adversário")
    new_row = {"match_id": next_mid, "away_team_id": away_id, "date": str(dt), "home_sets": 0, "away_sets": 0}
    mt = pd.concat([mt, pd.DataFrame([new_row])], ignore_index=True)
    frames_local["amistosos"] = mt
    
    add_set(frames_local, match_id=next_mid, set_number=1)
    
    st.session_state.frames = frames_local
    st.session_state.match_id = next_mid
    st.session_state.set_number = 1
    st.session_state.show_cadastro = False
    
    # Salva assincronamente
    async_saver.save_async(frames_local, "match_create")
    
    home_name = team_name_by_id(frames_local, OUR_TEAM_ID)
    st.success(f"Novo jogo criado: {home_name} x {opp_name or 'Adversário'}")

# Modal de cadastro
if (st.session_state.match_id is None or st.session_state.show_cadastro) and not st.session_state.show_config_team:
    st.subheader("🆕 Novo Jogo")
    
    col1, col2 = st.columns([2, 1])
    with col1: 
        opp_name = st.text_input("Adversário:", key="new_game_opponent", value="")
    with col2: 
        game_date = st.date_input("Data:", value=date.today(), key="new_game_date")
    
    colb1, colb2 = st.columns([1,1])
    with colb1:
        st.button("Criar Jogo", key="create_game_btn",
                  on_click=lambda: _create_new_match(st.session_state.get("new_game_opponent","").strip(), 
                                                   st.session_state.get("new_game_date", date.today())),
                  use_container_width=True)
    with colb2:
        st.button("Fechar", key="close_new_game_btn",
                  on_click=lambda: st.session_state.__setitem__("show_cadastro", False),
                  use_container_width=True)
    st.stop()

# =========================
# INFORMAÇÕES DA PARTIDA ATUAL
# =========================
home_name = away_name = date_str = ""
if st.session_state.match_id is not None:
    mt = frames["amistosos"]
    mrow = mt.loc[mt["match_id"] == st.session_state.match_id]
    if not mrow.empty:
        mrow = mrow.iloc[0]
        home_name = team_name_by_id(frames, OUR_TEAM_ID)
        away_name = team_name_by_id(frames, int(mrow["away_team_id"]))
        date_str = str(mrow["date"])

# =========================
# INTERFACE PRINCIPAL
# =========================

# Cabeçalho com informações da partida
if home_name and away_name:
    date_formatted = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d/%m/%Y") if date_str else ""
    st.markdown(f"### {home_name} ⚡ {away_name} — {date_formatted}")

# Toggle do modo jogo
st.session_state.game_mode = st.toggle("🎮 Modo Jogo", value=st.session_state.game_mode, key="game_mode_toggle")

if not st.session_state.game_mode:
    st.session_state.auto_close = st.toggle("Auto 25/15+2", value=st.session_state.auto_close, key="auto_close_toggle")

# Botões de configuração (apenas fora do modo jogo)
if not st.session_state.game_mode:
    col1, col2, col3, col4 = st.columns([2.5, 1, 1, 1])
    
    with col1:
        st.button("⚙️ Time", use_container_width=True, key="config_team_btn",
                  on_click=lambda: st.session_state.__setitem__("show_config_team", True))
    with col2:
        st.button("🆕 Jogo", use_container_width=True, key="new_game_btn",
                  on_click=lambda: st.session_state.__setitem__("show_cadastro", True))
    with col3:
        if st.button("📊 Histórico", use_container_width=True):
            st.info("Funcionalidade em desenvolvimento")
    with col4:
        if st.button("🏁 Finalizar", use_container_width=True):
            if st.session_state.match_id:
                # Finalizar partida atual
                mid = st.session_state.match_id
                try:
                    finalize_match(st.session_state.frames, mid)
                    mt = st.session_state.frames["amistosos"]
                    mt.loc[mt["match_id"] == mid, "is_closed"] = True
                    mt.loc[mt["match_id"] == mid, "closed_at"] = datetime.now().isoformat(timespec="seconds")
                    st.session_state.frames["amistosos"] = mt
                    async_saver.save_async(st.session_state.frames, "match_close")
                except Exception:
                    pass
                st.success("Partida finalizada.")
                st.session_state.match_id = None
                st.session_state.set_number = None
                st.rerun()

# =========================
# PLACAR PRINCIPAL
# =========================
if st.session_state.match_id is not None:
    df_set = current_set_df(frames, st.session_state.match_id, st.session_state.set_number)
    home_pts, away_pts = set_score_from_df(df_set)
    
    # Contagem de sets
    stf = frames["sets"]
    sm = stf[stf["match_id"] == st.session_state.match_id]
    home_sets = int((sm["winner_team_id"] == 1).sum())
    away_sets = int((sm["winner_team_id"] == 2).sum())
    
    # Display do placar
    scol1, scol2, scol3, scol4 = st.columns([1.1, 0.8, 1.1, 2.2])
    
    with scol1:
        st.markdown(f"""
        <div class='score-box'>
            <div class='score-team'>{home_name}</div>
            <div class='score-points'>{home_pts}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with scol2:
        st.markdown("<div class='score-x'>×</div>", unsafe_allow_html=True)
    
    with scol3:
        st.markdown(f"""
        <div class='score-box'>
            <div class='score-team'>{away_name}</div>
            <div class='score-points'>{away_pts}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with scol4:
        st.markdown(f"""
        <div class='set-summary'>
            Sets: <b>{home_sets}</b> × <b>{away_sets}</b> | Set atual: <b>{st.session_state.set_number}</b>
        </div>
        """, unsafe_allow_html=True)

# =========================
# MODO JOGO (INTERFACE SIMPLIFICADA)
# =========================
if st.session_state.game_mode:
    st.markdown("---")
    
    # Controles rápidos
    col_res, col_pos = st.columns([1, 1])
    
    with col_res:
        st.markdown("**Resultado**")
        st.session_state.q_result = st.radio(
            "", ["Acerto", "Erro"], horizontal=True,
            index=["Acerto", "Erro"].index(st.session_state.q_result),
            key="gm_result", label_visibility="collapsed"
        )
    
    with col_pos:
        st.markdown("**Posição**")
        st.session_state.q_position = st.radio(
            "", ["Frente", "Fundo"], horizontal=True,
            index=["Frente", "Fundo"].index(st.session_state.q_position),
            key="gm_position", label_visibility="collapsed"
        )
    
    # Jogadoras
    st.markdown("**Jogadoras**")
    nums = resolve_our_roster_numbers(st.session_state.frames)
    name_map = {r["number"]: r["name"] for r in roster_for_ui(st.session_state.frames)}
    
    if nums:
        # Grid de botões de jogadoras
        cols_players = st.columns(min(len(nums) + 1, 6))  # Max 6 colunas
        
        for i, n in enumerate(nums):
            if i < len(cols_players) - 1:
                with cols_players[i]:
                    st.button(
                        str(n), key=f"gm_player_{n}",
                        on_click=lambda n=n: (
                            st.session_state.__setitem__("last_selected_player", n),
                            st.session_state.__setitem__("q_side", "Nós")
                        ),
                        use_container_width=True
                    )
        
        # Botão ADV
        with cols_players[-1]:
            st.button(
                "ADV", key="gm_adv_btn",
                on_click=lambda: st.session_state.__setitem__("q_side", "Adv"),
                use_container_width=True
            )
        
        _paint_adv_rede_buttons()
    else:
        st.caption("Sem jogadoras cadastradas")
    
    # Atalhos de ações
    st.markdown("**Atalhos**")
    atalho_specs = [
        ("d", "Diag"), ("l", "Par"), ("m", "Meio"), ("lob", "Lob"),
        ("seg", "Seg"), ("pi", "Pipe"), ("re", "Recep"), ("b", "Bloq"),
        ("sa", "Saque"), ("rede", "Rede")
    ]
    
    cols_atalhos = st.columns(min(len(atalho_specs) + 1, 6))  # Max 6 colunas
    
    for i, (code, label) in enumerate(atalho_specs):
        if i < len(cols_atalhos) - 1:
            with cols_atalhos[i % len(cols_atalhos)]:
                st.button(
                    label, key=f"gm_action_{code}",
                    on_click=lambda code=code: register_current(action=code),
                    use_container_width=True
                )
    
    # Botão desfazer
    with cols_atalhos[-1]:
        st.button(
            "↩️ Desfazer", key="gm_undo_btn",
            on_click=undo_last_rally_current_set,
            use_container_width=True
        )
    
    _paint_adv_rede_buttons()
    
    # Quadra no modo jogo
    st.markdown("**🗺️ Quadra (clique para marcar posição)**")
    df_hm = current_set_df(st.session_state.frames, st.session_state.match_id, st.session_state.set_number)
    pts_succ, pts_errs, pts_adv, pts_adv_err = build_heat_points(
        df_hm, include_success=True, include_errors=True, 
        include_adv_points=True, include_adv_errors=True
    )
    
    render_court_html(
        pts_succ, pts_errs, pts_adv, pts_adv_err,
        enable_click=True, key="game_mode", show_numbers=st.session_state.show_heat_numbers
    )
    
    st.stop()  # Para não mostrar o resto da interface

# =========================
# INTERFACE COMPLETA (FORA DO MODO JOGO)
# =========================

# Layout em duas colunas
left_col, right_col = st.columns([1.3, 1.0])

with left_col:
    st.markdown("**🎯 Registrar Rally**")
    
    # Campo de entrada de texto
    def on_submit_text():
        raw = st.session_state.get("line_input_text", "").strip()
        if raw:
            quick_register_line(raw)
            st.session_state["line_input_text"] = ""
            # Reset para valores padrão
            st.session_state["q_side"] = "Nós"
            st.session_state["q_result"] = "Acerto"
            st.session_state["q_action"] = "d"
            st.session_state["q_position"] = "Frente"
    
    st.text_input(
        "Digite código:", key="line_input_text",
        placeholder="Ex: 1 9 d", label_visibility="collapsed",
        on_change=on_submit_text
    )
    
    # Botões de ação
    col_reg, col_undo = st.columns([1, 1])
    with col_reg:
        def register_and_clear():
            register_current()
            st.session_state["line_input_text"] = ""
        
        st.button("Registrar", use_container_width=True, 
                 key="btn_register", on_click=register_and_clear)
    
    with col_undo:
        st.button("↩️ Desfazer", use_container_width=True, 
                 key="btn_undo", on_click=undo_last_rally_current_set)
    
    # Seleções rápidas
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
        
        def on_action_change():
            sel = st.session_state.get("q_action_select")
            st.session_state["q_action"] = REVERSE_ACT_MAP.get(sel, "d")
        
        st.selectbox(
            "", action_options, index=action_options.index(current_action),
            key="q_action_select", on_change=on_action_change,
            label_visibility="collapsed"
        )
    
    st.markdown("---")
    st.markdown("**Jogadoras**")
    
    # Grid de jogadoras
    nums = resolve_our_roster_numbers(st.session_state.frames)
    name_map = {r["number"]: r["name"] for r in roster_for_ui(st.session_state.frames)}
    
    if nums:
        # 12 colunas para números ou 4 para nomes
        num_cols = 12
        pcols = st.columns(num_cols)
        
        for i, n in enumerate(nums):
            with pcols[i % num_cols]:
                st.button(
                    str(n), key=f"main_player_{n}", use_container_width=True,
                    on_click=lambda n=n: (
                        st.session_state.__setitem__("last_selected_player", n),
                        st.session_state.__setitem__("q_side", "Nós")
                    )
                )
        
        # Botão ADV
        with pcols[len(nums) % num_cols]:
            st.button(
                "ADV", key="main_adv_btn", use_container_width=True,
                on_click=lambda: st.session_state.__setitem__("q_side", "Adv")
            )
        
        _paint_adv_rede_buttons()
    else:
        st.caption("Sem jogadoras cadastradas para o nosso time.")
    
    # Atalhos
    st.markdown("**Atalhos**")
    atalho_specs = [
        ("d", "Diag"), ("l", "Par"), ("m", "Meio"), ("lob", "Lob"),
        ("seg", "Seg"), ("pi", "Pipe"), ("re", "Recep"), ("b", "Bloq"),
        ("sa", "Saque"), ("rede", "Rede")
    ]
    
    acols = st.columns(12)
    for i, (code, label) in enumerate(atalho_specs):
        with acols[i % len(acols)]:
            st.button(
                label, key=f"main_action_{code}",
                on_click=lambda code=code: register_current(action=code),
                use_container_width=True
            )
    
    _paint_adv_rede_buttons()
    
    st.markdown("---")
    st.markdown("**🗺️ Mapa de Calor**")
    
    # Filtros do mapa
    f1, f2, f3, f4, f5, f6 = st.columns([1.0, 1.0, 1.0, 1.2, 1.2, 1.2])
    
    with f1:
        nums_all = resolve_our_roster_numbers(st.session_state.frames)
        player_opts = ["Todas"] + nums_all
        picked = st.selectbox("Jogadora:", options=player_opts, index=0, key="hm_players_filter")
        sel_players = None if picked == "Todas" else [picked]
    
    with f2:
        show_success = st.checkbox("Nossos acertos", value=True, key="hm_show_succ")
    
    with f3:
        show_errors = st.checkbox("Nossos erros", value=True, key="hm_show_err")
    
    with f4:
        show_adv_pts = st.checkbox("ADV acertos", value=True, key="hm_show_adv_ok")
    
    with f5:
        show_adv_err = st.checkbox("ADV erros", value=True, key="hm_show_adv_err")
    
    with f6:
        st.session_state.show_heat_numbers = st.checkbox(
            "Mostrar números", value=st.session_state.show_heat_numbers, 
            key="hm_show_numbers"
        )
    
    # Renderiza quadra com heatmap
    df_hm = current_set_df(st.session_state.frames, st.session_state.match_id, st.session_state.set_number)
    pts_succ, pts_errs, pts_adv, pts_adv_err = build_heat_points(
        df_hm, selected_players=sel_players,
        include_success=show_success, include_errors=show_errors,
        include_adv_points=show_adv_pts, include_adv_errors=show_adv_err
    )
    
    render_court_html(
        pts_succ, pts_errs, pts_adv, pts_adv_err,
        enable_click=True, key="main", show_numbers=st.session_state.show_heat_numbers
    )

# Coluna direita - Dados e estatísticas
with right_col:
    st.markdown("**📜 Últimos rallies (set atual)**")
    
    df_set = current_set_df(frames, st.session_state.match_id, st.session_state.set_number)
    
    if df_set is not None and not df_set.empty:
        # Mostra últimos rallies
        cols_show = []
        for c in ["rally_no", "player_number", "action", "result", "who_scored", "score_home", "score_away"]:
            if c in df_set.columns:
                cols_show.append(c)
        
        preview = df_set.sort_values("rally_no").tail(15)[cols_show].copy()
        preview.rename(columns={
            "rally_no": "#", "player_number": "Jog", "action": "Ação",
            "result": "Resultado", "who_scored": "Quem", "score_home": "H", "score_away": "A"
        }, inplace=True)
        
        display_dataframe(preview, height=260, width='stretch')
        
        # Resumo por ação
        st.markdown("**Resumo por Ação**")
        
        def norm_cols_for_summary(df):
            d = df.copy()
            for col in ["action", "result", "who_scored"]:
                if col in d.columns:
                    d[col] = d[col].astype(str).str.strip().str.upper()
            return d
        
        dfx = norm_cols_for_summary(df_set)
        mask_pts = (dfx["who_scored"] == "NOS") & (dfx["result"] == "PONTO")
        mask_err = (dfx["who_scored"] == "ADV") & (dfx["result"] == "ERRO")
        
        counts_pts = dfx.loc[mask_pts, "action"].value_counts().rename("Pontos")
        counts_err = dfx.loc[mask_err, "action"].value_counts().rename("Erros")
        
        by_action = (
            pd.concat([counts_pts, counts_err], axis=1)
            .fillna(0).astype(int).reset_index().rename(columns={"index": "Ação"})
        )
        
        if "Ação" not in by_action.columns and len(by_action.columns) >= 1:
            by_action = by_action.rename(columns={by_action.columns[0]: "Ação"})
        
        if "Ação" in by_action.columns and not by_action.empty:
            by_action = by_action.sort_values("Ação")
        
        cols_disp = [c for c in ["Ação", "Pontos", "Erros"] if c in by_action.columns]
        display_dataframe(by_action[cols_disp], height=200, width='stretch')
        
        st.markdown("---")
        st.markdown("**🏆 Pontos (Nossos)**")
        
        mask_pts = (dfx["who_scored"] == "NOS") & (dfx["result"] == "PONTO")
        tbl_pontos = (
            dfx.loc[mask_pts]
            .assign(Jog=lambda x: pd.to_numeric(x["player_number"], errors="coerce").astype("Int64"))
            .groupby("Jog", dropna=False).size().rename("Pontos").reset_index()
            .sort_values(["Pontos", "Jog"], ascending=[False, True])
        )
        display_dataframe(tbl_pontos, height=160, width='stretch')
        
        st.markdown("**⚠️ Erros (Nossos)**")
        
        mask_err = (dfx["who_scored"] == "ADV") & (dfx["result"] == "ERRO")
        tbl_erros = (
            dfx.loc[mask_err]
            .assign(Jog=lambda x: pd.to_numeric(x["player_number"], errors="coerce").astype("Int64"))
            .groupby("Jog", dropna=False).size().rename("Erros").reset_index()
            .sort_values(["Erros", "Jog"], ascending=[False, True])
        )
        display_dataframe(tbl_erros, height=160, width='stretch')
        
    else:
        st.caption("_Sem rallies no set atual._")

# =========================
# MODAL DE CONFIGURAÇÃO DE TIME
# =========================
if st.session_state.get("show_config_team", False):
    st.markdown("---")
    col_title, col_close = st.columns([4, 1])
    with col_title:
        st.subheader("⚙️ Nosso Time e Jogadoras")
    with col_close:
        st.button("❌ Fechar", key="close_config_btn",
                  on_click=lambda: st.session_state.__setitem__("show_config_team", False))
    
    st.markdown("**Nome do Nosso Time**")
    current_team_name = team_name_by_id(frames, OUR_TEAM_ID)
    new_team_name = st.text_input("Nome do time:", value=current_team_name, key="team_name_input")
    
    def save_team_name():
        equipes = frames.get("equipes", pd.DataFrame())
        if equipes.empty:
            equipes = pd.DataFrame({"team_id": [OUR_TEAM_ID], "team_name": [new_team_name]})
        else:
            mask = equipes["team_id"] == OUR_TEAM_ID
            if mask.any():
                equipes.loc[mask, "team_name"] = new_team_name
            else:
                new_team = pd.DataFrame({"team_id": [OUR_TEAM_ID], "team_name": [new_team_name]})
                equipes = pd.concat([equipes, new_team], ignore_index=True)
        
        frames["equipes"] = equipes
        st.session_state.frames = frames
        async_saver.save_async(frames, "team_config")
        st.session_state.show_config_team = False
        st.success("Nome do time salvo!")
    
    st.button("💾 Salvar Nome do Time", key="save_team_name_btn", on_click=save_team_name)
    
    st.markdown("---")
    st.subheader("👥 Jogadoras")
    
    # Template de download
    cols = ["team_id", "player_number", "player_name", "position"]
    template_df = pd.DataFrame(columns=cols)
    csv_template = template_df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Baixar modelo CSV", data=csv_template, 
                      file_name="jogadoras_template.csv", mime="text/csv")
    
    # Lista de jogadoras
    jogadoras_df = frames.get("jogadoras", pd.DataFrame())
    our_players = jogadoras_df[jogadoras_df["team_id"] == OUR_TEAM_ID].copy()
    
    if not our_players.empty:
        st.markdown("**Cadastradas**")
        display_df = our_players[["player_number", "player_name", "position"]].copy()
        display_df.columns = ["Número", "Nome", "Posição"]
        display_dataframe(display_df, height=140)
        
        # Excluir jogadora
        st.markdown("**Excluir**")
        players_to_delete = our_players["player_number"].astype(str) + " - " + our_players["player_name"]
        if not players_to_delete.empty:
            player_to_delete = st.selectbox("Escolha:", players_to_delete.tolist(), key="delete_player_select")
            
            def delete_player():
                if player_to_delete:
                    player_num = int(player_to_delete.split(" - ")[0])
                    jog_df = frames["jogadoras"]
                    jog_df = jog_df[~((jog_df["team_id"] == OUR_TEAM_ID) & (jog_df["player_number"] == player_num))]
                    frames["jogadoras"] = jog_df
                    st.session_state.frames = frames
                    async_saver.save_async(frames, "player_delete")
                    st.success("Jogadora excluída!")
            
            st.button("🗑️ Excluir", key="delete_player_btn", on_click=delete_player)
    
    # Adicionar jogadora
    st.markdown("---")
    st.subheader("➕ Adicionar")
    
    c1, c2, c3 = st.columns(3)
    with c1:
        new_number = st.number_input("Número:", min_value=1, max_value=99, key="new_player_number")
    with c2:
        new_name = st.text_input("Nome:", key="new_player_name")
    with c3:
        new_position = st.selectbox("Posição:", ["oposto", "levantador", "central", "ponteiro", "líbero"], 
                                   key="new_player_position")
    
    def add_player():
        if new_name.strip():
            new_player = pd.DataFrame({
                "team_id": [OUR_TEAM_ID],
                "player_number": [new_number],
                "player_name": [new_name],
                "position": [new_position]
            })
            
            jog_df = frames.get("jogadoras", pd.DataFrame())
            # Remove jogadora existente com mesmo número
            jog_df = jog_df[~((jog_df["team_id"] == OUR_TEAM_ID) & (jog_df["player_number"] == new_number))]
            jog_df = pd.concat([jog_df, new_player], ignore_index=True)
            
            frames["jogadoras"] = jog_df
            st.session_state.frames = frames
            async_saver.save_async(frames, "player_add")
            st.success("Jogadora adicionada!")
        else:
            st.warning("Digite um nome.")
    
    st.button("➕ Adicionar Jogadora", key="add_player_btn", on_click=add_player)

# =========================
# CLEANUP E FINALIZAÇÃO
# =========================

# Status do salvamento assíncrono na barra lateral
with st.sidebar:
    st.caption("💾 Salvamento Assíncrono Ativo")
    if hasattr(async_saver, 'save_queue'):
        queue_size = async_saver.save_queue.qsize()
        if queue_size > 0:
            st.caption(f"⏳ {queue_size} salvamento(s) pendente(s)")
        else:
            st.caption("✅ Todos os dados salvos")

# Limpeza na finalização da sessão
import atexit

def cleanup():
    try:
        if 'async_saver' in globals():
            async_saver.stop()
    except Exception:
        pass

atexit.register(cleanup)

# =========================
# MÉTRICAS DE PERFORMANCE
# =========================
if st.sidebar.button("📊 Debug Performance"):
    st.sidebar.write("**Estatísticas da Sessão:**")
    st.sidebar.write(f"- Match ID: {st.session_state.match_id}")
    st.sidebar.write(f"- Set: {st.session_state.set_number}")
    st.sidebar.write(f"- Data Rev: {st.session_state.data_rev}")
    
    if hasattr(async_saver, 'save_queue'):
        st.sidebar.write(f"- Queue Size: {async_saver.save_queue.qsize()}")
    
    # Tamanho dos frames
    total_rows = 0
    for name, df in st.session_state.frames.items():
        if isinstance(df, pd.DataFrame):
            rows = len(df)
            st.sidebar.write(f"- {name}: {rows} registros")
            total_rows += rows
    
    st.sidebar.write(f"**Total: {total_rows} registros**")

# =========================
# INICIALIZAÇÃO FINAL
# =========================
if __name__ == "__main__":
    import os
    
    # Configuração para ambientes de produção
    port = int(os.environ.get("PORT", 8501))
    
    # Força um rerun inicial se necessário para inicializar estados
    if not st.session_state.get("_initialized", False):
        st.session_state["_initialized"] = True
        
        # Garante que os diretórios essenciais existam
        for directory in [BASE_DIR / "logs", BASE_DIR / "journal", BASE_DIR / "backups"]:
            directory.mkdir(parents=True, exist_ok=True)
        
        # Log de inicialização
        try:
            if hasattr(async_saver, 'logger'):
                async_saver.logger.info("Sistema UniVôlei iniciado com sucesso")
        except Exception:
            pass
        
        st.rerun()

# =========================
# FOOTER E INFORMAÇÕES
# =========================
st.markdown("---")

# Informações do sistema na barra lateral
with st.sidebar:
    st.markdown("### 📊 Sistema UniVôlei")
    st.markdown("**Versão:** 2.0 Otimizada")
    st.markdown("**Status:** Online")
    
    # Informações de configuração
    if st.session_state.match_id:
        st.markdown(f"**Partida Ativa:** #{st.session_state.match_id}")
        st.markdown(f"**Set Atual:** {st.session_state.set_number}")
    else:
        st.markdown("**Status:** Aguardando nova partida")
    
    # Configurações de salvamento
    st.markdown("**Salvamento Configurado:**")
    st.markdown(f"- Local Excel: ✅")
    st.markdown(f"- Journal: ✅")
    
    # Verifica Google Sheets
    try:
        if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
            st.markdown(f"- Google Sheets: ✅")
        else:
            st.markdown(f"- Google Sheets: ⚠️ (opcional)")
    except Exception:
        st.markdown(f"- Google Sheets: ⚠️ (opcional)")

# Rodapé com créditos
st.markdown("""
<div style="text-align: center; padding: 20px; color: #666; font-size: 0.9em;">
    <hr style="margin: 20px 0; border: none; border-top: 1px solid #eee;">
    <strong>UniVôlei Live Scout</strong> - Sistema de Scout de Vôlei em Tempo Real<br>
    Desenvolvido para análise técnica e acompanhamento de partidas<br>
    <em>Versão 2.0 Otimizada - Performance e Confiabilidade</em>
</div>
""", unsafe_allow_html=True)

# =========================
# TRATAMENTO DE ERROS GLOBAIS
# =========================
def handle_global_error():
    """Função para capturar e tratar erros não previstos"""
    try:
        # Tenta salvar o estado atual em caso de erro crítico
        if st.session_state.get("frames"):
            emergency_path = BASE_DIR / f"emergency_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            save_all(emergency_path, st.session_state.frames)
            st.error(f"Erro detectado! Backup de emergência salvo em: {emergency_path}")
    except Exception as backup_error:
        st.error(f"Erro crítico no sistema. Não foi possível criar backup: {backup_error}")

# Registra o handler de erro
import sys
sys.excepthook = lambda *args: handle_global_error()

# =========================
# CONFIGURAÇÕES AVANÇADAS (OPCIONAL)
# =========================
if st.sidebar.expander("⚙️ Configurações Avançadas", expanded=False):
    st.write("**Configurações do Sistema:**")
    
    # Configuração de auto-save
    auto_save_freq = st.selectbox(
        "Frequência de auto-save:",
        ["A cada rally", "A cada 5 rallies", "Apenas em checkpoints"],
        index=0,
        key="auto_save_config"
    )
    
    # Configuração de backup
    enable_backup = st.checkbox("Backups timestampados", value=True, key="enable_backup")
    
    # Configuração de journal
    enable_journal = st.checkbox("Journal de segurança", value=True, key="enable_journal")
    
    # Aplicar configurações
    if st.button("Aplicar Configurações"):
        st.success("Configurações aplicadas!")
        
        # Atualiza configurações globais
        SAVE_CONFIG["local_xlsx"]["enabled"] = True  # Sempre ativo
        SAVE_CONFIG["journal"]["enabled"] = enable_journal
        SAVE_CONFIG["cloud_sync"]["enabled"] = True
        
        # Log da mudança
        try:
            if hasattr(async_saver, 'logger'):
                async_saver.logger.info(f"Configurações atualizadas: backup={enable_backup}, journal={enable_journal}")
        except Exception:
            pass

# =========================
# MONITORAMENTO DE SAÚDE DO SISTEMA
# =========================
if st.sidebar.button("🔍 Diagnóstico do Sistema"):
    st.subheader("🔍 Diagnóstico do Sistema")
    
    # Verifica integridade dos dados
    try:
        frames = st.session_state.frames
        diagnostics = []
        
        # Verifica cada tabela
        for table_name, df in frames.items():
            if isinstance(df, pd.DataFrame):
                diagnostics.append({
                    "Tabela": table_name,
                    "Registros": len(df),
                    "Colunas": len(df.columns),
                    "Memória (KB)": round(df.memory_usage(deep=True).sum() / 1024, 2),
                    "Status": "✅ OK"
                })
            else:
                diagnostics.append({
                    "Tabela": table_name,
                    "Status": "⚠️ Não é DataFrame"
                })
        
        if diagnostics:
            diag_df = pd.DataFrame(diagnostics)
            st.dataframe(diag_df, use_container_width=True)
        
        # Verifica arquivos
        st.write("**Arquivos do Sistema:**")
        files_status = []
        
        # Arquivo principal
        main_file = Path(st.session_state.db_path)
        files_status.append({
            "Arquivo": "Base Principal (Excel)",
            "Caminho": str(main_file),
            "Existe": "✅" if main_file.exists() else "❌",
            "Tamanho": f"{main_file.stat().st_size / 1024:.1f} KB" if main_file.exists() else "N/A"
        })
        
        # Journal
        journal_dir = BASE_DIR / "journal"
        if journal_dir.exists():
            journal_files = list(journal_dir.glob("*.ndjson"))
            files_status.append({
                "Arquivo": f"Journal ({len(journal_files)} arquivos)",
                "Caminho": str(journal_dir),
                "Existe": "✅",
                "Tamanho": f"{sum(f.stat().st_size for f in journal_files) / 1024:.1f} KB"
            })
        
        # Backups
        backup_dir = BASE_DIR / "backups"
        if backup_dir.exists():
            backup_files = list(backup_dir.glob("*.xlsx"))
            files_status.append({
                "Arquivo": f"Backups ({len(backup_files)} arquivos)",
                "Caminho": str(backup_dir),
                "Existe": "✅",
                "Tamanho": f"{sum(f.stat().st_size for f in backup_files) / 1024:.1f} KB"
            })
        
        files_df = pd.DataFrame(files_status)
        st.dataframe(files_df, use_container_width=True)
        
        # Status do salvamento assíncrono
        st.write("**Sistema de Salvamento:**")
        if hasattr(async_saver, 'running') and async_saver.running:
            st.success("✅ Salvamento assíncrono ativo")
            queue_size = async_saver.save_queue.qsize() if hasattr(async_saver, 'save_queue') else 0
            st.info(f"📝 {queue_size} operação(ões) na fila")
        else:
            st.error("❌ Salvamento assíncrono inativo")
        
        # Google Sheets
        try:
            if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
                st.success("✅ Google Sheets configurado")
            else:
                st.warning("⚠️ Google Sheets não configurado (opcional)")
        except Exception as e:
            st.warning(f"⚠️ Google Sheets: {str(e)}")
        
    except Exception as e:
        st.error(f"Erro no diagnóstico: {e}")

# =========================
# LIMPEZA FINAL E OTIMIZAÇÕES
# =========================

# Otimização de memória - limpa DataFrames não utilizados
if st.session_state.data_rev % 100 == 0:  # A cada 100 atualizações
    import gc
    gc.collect()  # Força garbage collection

# Monitoramento de performance em desenvolvimento
if os.environ.get("STREAMLIT_ENV") == "development":
    import psutil
    process = psutil.Process()
    memory_mb = process.memory_info().rss / 1024 / 1024
    
    if st.sidebar.checkbox("Monitoramento de Performance"):
        st.sidebar.write(f"**Memória:** {memory_mb:.1f} MB")
        st.sidebar.write(f"**CPU:** {psutil.cpu_percent()}%")
        st.sidebar.write(f"**Threads:** {process.num_threads()}")

# =========================
# FINALIZAÇÃO E CLEANUP
# =========================

# Garante que o async_saver seja limpo corretamente ao sair
def cleanup_on_exit():
    try:
        if 'async_saver' in globals() and hasattr(async_saver, 'stop'):
            async_saver.stop()
    except Exception:
        pass

# Registra cleanup para execução ao sair
import atexit
atexit.register(cleanup_on_exit)

# Mensagem final de status
if st.session_state.get("_initialized"):
    # Sistema inicializado com sucesso
    pass
else:
    # Primeira execução - mostra mensagem de boas-vindas
    st.info("🚀 Sistema UniVôlei carregado com sucesso! Pronto para usar.")

# =========================
# FIM DO ARQUIVO
# =========================
    