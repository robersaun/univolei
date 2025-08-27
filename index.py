# UniVolei Live Scout (Heatmap, Modo Jogo, debug detalhado e prints reais)
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
import time
from datetime import date
import json
from math import ceil

from db_excel import (
    init_or_load, save_all, add_match, add_set,
    append_rally, last_open_match, finalize_match
)
from parser_free import parse_line

DEBUG_PRINTS = True
def debug_print(*args, **kwargs):
    if DEBUG_PRINTS:
        print("[UV-DEBUG]", *args, **kwargs, flush=True)

# =========================
# Config + Estilos
# =========================
st.set_page_config(page_title="", layout="wide", initial_sidebar_state="collapsed")

# anti-scroll-jump: preserva posição
components.html("""
<script>
const KEY='uv_scroll_y';
window.addEventListener('load', ()=>{const y=sessionStorage.getItem(KEY); if(y!==null){window.scrollTo(0,parseInt(y));}});
window.addEventListener('beforeunload', ()=>{sessionStorage.setItem(KEY, window.scrollY.toString());});
</script>
""", height=0)

# =========================
# CSS externo
# =========================
BASE_DIR = Path(__file__).parent.resolve()
def load_css(filename: str = "univolei.css"):
    css_path = BASE_DIR / filename
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)
    else:
        st.warning(f"Arquivo CSS não encontrado: {filename}")
load_css("univolei.css")

# Título com SVG
st.markdown(
    '''
    <div class="header-title">
      <svg width="18" height="18" viewBox="0 0 24 24" aria-hidden="true" style="margin-right:6px; vertical-align:-2px; flex:0 0 auto;">
        <circle cx="12" cy="12" r="10" fill="none" stroke="currentColor" stroke-width="2"/>
        <path d="M2 12a10 10 0 0 0 20 0" fill="none" stroke="currentColor" stroke-width="2"/>
        <path d="M12 2a10 10 0 0 0 0 20" fill="none" stroke="currentColor" stroke-width="2"/>
        <path d="M4.5 4.5a10 10 0 0 1 0 15" fill="none" stroke="currentColor" stroke-width="2"/>
        <path d="M19.5 4.5a10 10 0 0 0 0 15" fill="none" stroke="currentColor" stroke-width="2"/>
      </svg>
      <span>Vôlei Scout – UniVolei</span>
    </div>
    ''',
    unsafe_allow_html=True
)

# =========================
# Figuras ultra-compactas
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

def small_fig(w=2.6, h=1.15):
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

# =========================
# DataFrame HTML
# =========================
def display_dataframe(df, height=None, use_container_width=False, extra_class: str = ""):
    if df is None or len(df) == 0:
        st.write("_Sem dados._"); return
    classes = ('custom-table ' + extra_class).strip()
    html_table = df.to_html(classes=classes, index=False, escape=False)
    styled_html = f"""
    <div style='overflow:auto; height:{height if height else "auto"}px; width: {"100%" if use_container_width else "auto"};'>
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
if "match_id" not in st.session_state: st.session_state.match_id = None
if "set_number" not in st.session_state: st.session_state.set_number = None
if "auto_close" not in st.session_state: st.session_state.auto_close = True
if "graph_filter" not in st.session_state: st.session_state.graph_filter = "Ambos"
st.session_state.setdefault("data_rev", 0)

# chaves auxiliares
st.session_state.setdefault("q_side", "Nós")
st.session_state.setdefault("q_result", "Acerto")
st.session_state.setdefault("q_action", "d")
st.session_state.setdefault("q_position", "Frente")  # NOVO
st.session_state.setdefault("last_selected_player", None)
st.session_state.setdefault("show_cadastro", False)
st.session_state.setdefault("show_tutorial", False)
st.session_state.setdefault("show_config_team", False)
st.session_state.setdefault("line_input_text", "")
st.session_state.setdefault("perf_logs", [])

# Heatmap / clique
st.session_state.setdefault("last_court_click", None)   # {"x":float,"y":float,"ts":int}
st.session_state.setdefault("heatmap_debug", True)

# Estado do Modo Jogo
st.session_state.setdefault("game_mode", False)

# Rótulo botões (Número | Nome)
st.session_state.setdefault("btn_label_mode", "Número")
st.session_state.setdefault("player_label_mode", "Número")

# Mostrar números nas bolinhas
st.session_state.setdefault("show_heat_numbers", False)

# =========== Debug/prints ===========
st.session_state.setdefault("dbg_prints", [])
def dbg_print(msg: str):
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    st.session_state["dbg_prints"] = (st.session_state["dbg_prints"] + [line])[-200:]
    print(line)

# Captura de clique via query param
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
        st.session_state["last_court_click"] = {"x": x, "y": y, "ts": int(time.time())}
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

# Perf logs
PERF_DEBUG = False
def _add_perf_log(msg: str):
    logs = st.session_state.get("perf_logs", [])
    logs.append(f"{time.strftime('%H:%M:%S')} {msg}")
    st.session_state["perf_logs"] = logs[-30:]

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

# >>> AÇÃO "rede" incluída <<<
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
    if home_sets >= 3 or away_sets >= 3:
        finalize_match(frames, match_id); save_all(Path(st.session_state.db_path), frames)
        st.success(f"Set {set_number} encerrado ({home_pts} x {away_pts}). Partida finalizada: {home_sets} x {away_sets} em sets.")
        st.session_state.match_id = None; st.session_state.set_number = None
    else:
        st.session_state.set_number = int(set_number) + 1
        add_set(frames, match_id=match_id, set_number=st.session_state.set_number)
        save_all(Path(st.session_state.db_path), frames)
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
    """Rápido: remove só o último evento e restaura o placar para o penúltimo."""
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
    dbg_print(f"Desfeito rally_id={last_rally_id}. Placar {hp}-{ap} (sem recomputar tudo).")
    st.success("Último rally desfeito.")

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

def quick_register_line(raw_line: str):
    if not raw_line.strip():
        dbg_print("Linha vazia ignorada."); return
    t0 = time.perf_counter()
    row = parse_line(raw_line)
    row_before = row.copy()
    row = _fix_who_scored_from_raw_and_row(raw_line, row)
    row = _normalize_action_in_row(row)
    # Posição Frente/Fundo
    row["position_zone"] = str(st.session_state.get("q_position","Frente")).strip().upper()
    row = _fast_apply_scores_to_row(row)

    # clique pendente
    last_click = st.session_state.get("last_court_click")
    used_xy = None
    if last_click and isinstance(last_click, dict):
        row["court_x"] = float(last_click.get("x", 0.0))
        row["court_y"] = float(last_click.get("y", 0.0))
        used_xy = ("clique", row["court_x"], row["court_y"])
        st.session_state["last_court_click"] = None

    t1 = time.perf_counter()
    append_rally(st.session_state.frames, match_id=st.session_state.match_id, set_number=st.session_state.set_number, row=row)
    save_all(Path(st.session_state.db_path), st.session_state.frames)
    st.session_state.data_rev += 1
    auto_close_set_if_needed()
    if PERF_DEBUG:
        t2 = time.perf_counter()
        _add_perf_log(f"parse+fix+score: {(t1-t0)*1000:.1f} ms | append+save+auto: {(t2-t1)*1000:.1f} ms")

    dbg_print(
        f"REGISTRO: raw='{raw_line}' -> action='{row.get('action')}', result='{row.get('result')}', "
        f"who_scored='{row.get('who_scored')}', player={row.get('player_number')}, "
        f"pos={row.get('position_zone')}, placar={row.get('score_home')}-{row.get('score_away')}, "
        f"xy={'%s %.3f %.3f' % used_xy if used_xy else '—'} | row_before={row_before}"
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

    # ação 'rede' => sempre erro nosso (bolinha vermelha colada à rede)
    if str(act).lower() == "rede":
        side_code = "ADV"
        is_err = True

    dbg_print(f"register_current: side={side_code}, num={num_val}, action={act}, is_err={is_err}, pos={st.session_state.get('q_position')}")
    quick_register_click(side_code, num_val, act, is_err)

# ========= HEATMAP =========
def _y_for_half(half: str, fb: str | None) -> float:
    """
    Metade superior ('opp') ≈ adversário; metade inferior ('our') ≈ nós.
    Frente = antes dos 3m (perto da rede); Fundo = perto da linha de fundo.
    """
    if fb == "FRENTE":
        return 41.0 if half == "opp" else 59.0   # antes dos 3m
    if fb == "FUNDO":
        return 14.0 if half == "opp" else 86.0   # perto da linha de fundo
    return 28.0 if half == "opp" else 72.0       # neutro

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
    """
    Correções:
    - “Segunda”, “Pipe” e “Saque” passam a plotar normalmente (acerto/erro).
    - Frente/Fundo aplicado em TODAS as ações (inclusive Diagonal e Paralela).
    - “Rede” e “Bloqueio” colados à rede (espalhando no eixo X).
    """
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

    # inclui 'position_zone' e outras colunas como fonte de Frente/Fundo
    FB_COLS = ["position_zone","pos_fb","posicao_fb","posicao","pos",
               "frente_fundo","frente_fundo_sel","zona_fb","zona"]
    def _row_fb(r) -> str | None:
        for c in FB_COLS:
            if c in r and pd.notna(r[c]):
                v = str(r[c]).strip().upper()  # <<< correção: .upper() (sem .str)
                if v in ("FRENTE","F","FR","FRONTAL","ATAQUE"):
                    return "FRENTE"
                if v in ("FUNDO","B","U","BACK","TRAS","TRÁS","DEFESA"):
                    return "FUNDO"
        return None

    df0 = df.copy()
    df0["action_u"] = df0.get("action", "").astype(str).str.strip().str.upper()
    df0["who_u"]    = df0.get("who_scored", "").astype(str).str.strip().str.upper()
    df0["res_u"]    = df0.get("result", "").astype(str).str.strip().str.upper()
    if "player_number" in df0.columns:
        df0["player_number"] = pd.to_numeric(df0["player_number"], errors="coerce")

    # filtro por jogadoras (mantendo NaN)
    df_nos = df0.copy()
    if selected_players is not None and "player_number" in df_nos.columns:
        df_nos["player_number"] = df_nos["player_number"].astype("Int64")
        if len(selected_players) == 0:
            empty_dbg = pd.DataFrame(columns=["rally_no","player_number","action_u","res_u","who_u","used_x","used_y","origem","cor"])
            return ([], [], [], [], empty_dbg) if return_debug else ([], [], [], [])
        sel = pd.Series(selected_players, dtype="Int64")
        df_nos = df_nos[df_nos["player_number"].isin(sel) | df_nos["player_number"].isna()]

    # aceitamos estes nomes/códigos
    actions_ok = {
        "MEIO","M",
        "DIAGONAL","D",
        "LINHA","PARALELA","L",
        "LOB","LARGADA",
        "PIPE","PI",
        "SEGUNDA","SEG",
        "RECEPÇÃO","RECEPCAO","RE",
        "BLOQUEIO","B","BLOQ",
        "SAQUE","SA",
        "REDE"
    }

    succ_pts: list[dict] = []
    err_pts:  list[dict] = []
    adv_pts:  list[dict] = []
    adv_err_pts: list[dict] = []
    dbg_rows = []
    cluster_counters = {}

    group_bias = {
        "azul": (-0.8, -0.8),
        "vermelho": (0.8, -0.8),
        "laranja": (-0.8, 0.8),
        "laranja_escuro": (0.8, 0.8),
    }

    def _offset_for_index(idx: int, step_pct: float = 2.2) -> tuple[float, float]:
        if idx <= 0: return (0.0, 0.0)
        order = [
            (1,0), (-1,0), (0,1), (0,-1),
            (1,1), (-1,1), (1,-1), (-1,-1),
            (2,0), (-2,0), (0,2), (0,-2),
            (2,1), (2,-1), (-2,1), (-2,-1),
            (1,2), (-1,2), (1,-2), (-1,-2),
            (3,0), (-3,0), (0,3), (0,-3),
        ]
        base = order[(idx-1) % len(order)]
        mult = 1 + ((idx-1) // len(order))
        return (base[0]*step_pct*mult, base[1]*step_pct*mult)

    def _add_point(lst: list, x: float, y: float, color_tag: str, cluster_key: str, label: str | None, dbg_row: list):
        idx = cluster_counters.get(cluster_key, 0)
        dx, dy = _offset_for_index(idx)
        cluster_counters[cluster_key] = idx + 1
        gx, gy = group_bias.get(color_tag, (0.0, 0.0))
        xx = min(100.0, max(0.0, x + dx + gx))
        yy = min(100.0, max(0.0, y + dy + gy))
        lst.append({"x": xx, "y": yy, "label": label, "cluster": cluster_key})
        if return_debug:
            dbg_row = dbg_row.copy()
            dbg_row[5] = xx; dbg_row[6] = yy
            dbg_row[8] = color_tag
            dbg_rows.append(dbg_row)

    def _who_performed_is_nos(r) -> bool:
        """Deduza quem executou: NOS se (NOS,PONTO) ou (ADV,ERRO); senão ADV."""
        w = r.get("who_u","")
        res = r.get("res_u","")
        return (w == "NOS" and res == "PONTO") or (w == "ADV" and res == "ERRO")

    def _infer_point(r, color_tag: str, bucket: list, label: str | None):
        act = _norm_action(r.get("action_u",""))
        fb = _row_fb(r)

        # clique do usuário tem prioridade
        cx, cy = r.get("court_x"), r.get("court_y")
        if pd.notna(cx) and pd.notna(cy):
            x_use = float(cx)*100 if 0<=cx<=1 else float(cx)
            y_use = float(cy)*100 if 0<=cy<=1 else float(cy)
            _add_point(bucket, x_use, y_use, color_tag, f"{color_tag}:{act}", label,
                       [r.get("rally_no"), r.get("player_number"), act, r.get("res_u"), r.get("who_u"),
                        x_use, y_use, "clique", color_tag])
            return

        # metade para posicionar o Y: de quem EXECUTOU
        perf_is_nos = _who_performed_is_nos(r)
        eff_half = "our" if perf_is_nos else "opp"

        # recepção sempre na nossa metade
        if act == "RECEPÇÃO":
            eff_half = "our"

        # BLOQUEIO/REDE -> colado na rede + espalha no X
        if act in ("BLOQUEIO","REDE"):
            y0 = _y_net_touch(eff_half) if fb in (None, "FRENTE") else _y_for_half(eff_half, "FUNDO")
            idx = cluster_counters.get(f"{color_tag}:{act}", 0)
            x0 = 10.0 + (idx % 16) * (80.0/15.0)  # distribui ao longo da rede
        else:
            x0 = _x_for_action(act)
            y0 = _y_for_half(eff_half, fb)

        _add_point(bucket, x0, y0, color_tag, f"{color_tag}:{act}", label,
                   [r.get("rally_no"), r.get("player_number"), act, r.get("res_u"), r.get("who_u"),
                    x0, y0, "inferência", color_tag])

    # --------- listas ----------
    if include_success:
        srows = df_nos[(df_nos["who_u"] == "NOS") & (df_nos["res_u"] == "PONTO") & (df_nos["action_u"].isin(actions_ok))]
        for _, r in srows.iterrows():
            lbl = str(int(r["player_number"])) if pd.notna(r.get("player_number")) else None
            _infer_point(r, color_tag="azul", bucket=succ_pts, label=lbl)

    if include_errors:
        erows = df_nos[(df_nos["who_u"] == "ADV") & (df_nos["res_u"] == "ERRO") & (df_nos["action_u"].isin(actions_ok))]
        for _, r in erows.iterrows():
            lbl = str(int(r["player_number"])) if pd.notna(r.get("player_number")) else None
            _infer_point(r, color_tag="vermelho", bucket=err_pts, label=lbl)

    if include_adv_points:
        arows = df0[(df0["who_u"] == "ADV") & (df0["res_u"] == "PONTO") & (df0["action_u"].isin(actions_ok))]
        for _, r in arows.iterrows():
            _infer_point(r, color_tag="laranja", bucket=adv_pts, label=None)

    if include_adv_errors:
        aerr = df0[(df0["who_u"] == "NOS") & (df0["res_u"] == "ERRO") & (df0["action_u"].isin(actions_ok))]
        for _, r in aerr.iterrows():
            _infer_point(r, color_tag="laranja_escuro", bucket=adv_err_pts, label=None)

    if return_debug:
        dbg = pd.DataFrame(dbg_rows, columns=["rally_no","player_number","action_u","res_u","who_u","used_x","used_y","origem","cor"])
        return succ_pts, err_pts, adv_pts, adv_err_pts, dbg
    else:
        return succ_pts, err_pts, adv_pts, adv_err_pts

# =========================
# QUADRA HTML
# =========================
def render_court_html(pts_success, pts_errors, pts_adv=None, pts_adv_err=None, enable_click=False, key="set", show_numbers=False):
    """
    Desenha quadra com labels externos “ADV” (topo) e “NÓS” (baixo).
    """
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

    def _dot_html(x, y, bg, border, text=None):
        label_html = ""
        if show_numbers and text:
            label_html = (
                "<div style='position:absolute; inset:0; display:flex; align-items:center; justify-content:center; "
                "font-size:9px; color:#fff; font-weight:700;'>"
                f"{html.escape(str(text))}</div>"
            )
        return (
            f"<div style='left:{x}%; top:{y}%; width:12px; height:12px; position:absolute;"
            f"background:{bg}; border:1px solid {border}; border-radius:50%;"
            f"transform:translate(-50%,-50%); z-index:4;'>{label_html}</div>"
        )

    dots_html = []
    for x,y,lab in S:
        dots_html.append(_dot_html(x, y, "rgba(30,144,255,0.92)", "rgba(20,90,200,0.95)", lab))
    for x,y,lab in E:
        dots_html.append(_dot_html(x, y, "rgba(220,50,50,0.92)", "rgba(160,20,20,0.95)", lab))
    for x,y,lab in A:
        dots_html.append(_dot_html(x, y, "rgba(255,140,0,0.92)", "rgba(180,90,0,0.95)", lab or "ADV"))
    for x,y,lab in AE:
        dots_html.append(_dot_html(x, y, "rgba(210,100,0,0.92)", "rgba(150,70,0,0.95)", lab or "ADV"))

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

    html_block = f"""
    <div style="width:100%; text-align:center; font-weight:700; margin-bottom:6px;">ADV</div>
    <div id="{container_id}" style="background:#FFA94D; border:2px solid #333; position:relative; width:100%; height:380px; border-radius:6px;">
      <!-- REDE -->
      <div style="
           position:absolute; left:0; top:calc(50% - 8px); width:100%; height:16px;
           background:repeating-linear-gradient(90deg, rgba(255,255,255,0.95) 0 12px, rgba(0,0,0,0.12) 12px 14px);
           border-top:2px solid #111; border-bottom:2px solid #111; z-index:2; opacity:.95;"></div>
      <div style="position:absolute; left:0; top:50%; width:100%; height:2px; background:#111; z-index:3;"></div>
      <!-- Linhas de ataque (3m) -->
      <div style="position:absolute; left:0; top:33.333%; width:100%; height:1px; background:rgba(0,0,0,.30); z-index:1;"></div>
      <div style="position:absolute; left:0; top:66.666%; width:100%; height:1px; background:rgba(0,0,0,.30); z-index:1;"></div>

      {''.join(dots_html)}
    </div>
    <div style="width:100%; text-align:center; font-weight:700; margin-top:12px; margin-bottom:22px;">NÓS</div>
    <script>{click_js}</script>
    """
    components.html(html_block, height=468, scrolling=False)

# =========================
# Abertura de partida
# =========================
open_mid = last_open_match(frames)
if open_mid and st.session_state.match_id is None:
    st.session_state.match_id = int(open_mid)
    sets_m = frames["sets"]
    if not sets_m.empty and (sets_m["match_id"] == open_mid).any():
        st.session_state.set_number = int(sets_m[sets_m["match_id"] == open_mid]["set_number"].max())
    else:
        st.session_state.set_number = 1

home_name = away_name = date_str = ""
if st.session_state.match_id is not None:
    mt = frames["amistosos"]
    mrow = mt.loc[mt["match_id"] == st.session_state.match_id].iloc[0]
    home_name = team_name_by_id(frames, OUR_TEAM_ID)
    away_name = team_name_by_id(frames, mrow["away_team_id"])
    date_str = str(mrow["date"])

# topo
top1, top2, top3, top4, top5 = st.columns([2.5, 1, 1, 1, 1])
with top1:
    if home_name and away_name:
        st.markdown(f'<div class="badge"><b>{home_name}</b> x <b>{away_name}</b> — {date_str}</div>', unsafe_allow_html=True)
with top2:
    st.button("⚙️ Time", use_container_width=True, key="config_team_btn", on_click=lambda: st.session_state.__setitem__("show_config_team", True))
with top3:
    st.button("🆕 Jogo", use_container_width=True, key="new_game_btn", on_click=lambda: st.session_state.__setitem__("show_cadastro", True))
with top4:
    st.button("📘 Tutorial", use_container_width=True, key="tutorial_btn", on_click=lambda: st.session_state.__setitem__("show_tutorial", True))
with top5:
    hist_candidates = [
        "pages/02_historico.py",
        "pages/historico.py",
        "02_historico.py",
        "historico.py",
    ]
    found_hist = None
    for p in hist_candidates:
        if (BASE_DIR / p).exists():
            found_hist = p
            break
    def _go_hist(p=found_hist):
        try:
            st.switch_page(p)
        except Exception:
            st.warning("Não consegui abrir a página. Atualize seu Streamlit.")
    st.button("🗂️ Histórico", use_container_width=True, on_click=_go_hist)

# =========================
# Modais (Config/Tutorial)
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
    def _save_team_name():
        if "equipes" in frames:
            equipes = frames["equipes"]; mask = equipes["team_id"] == OUR_TEAM_ID
            if mask.any(): equipes.loc[mask, "team_name"] = new_team_name
            else:
                new_team = pd.DataFrame({"team_id":[OUR_TEAM_ID], "team_name":[new_team_name]})
                equipes = pd.concat([equipes, new_team], ignore_index=True)
            frames["equipes"] = equipes; save_all(Path(st.session_state.db_path), frames)
            st.session_state.show_config_team = False
            dbg_print(f"Nome do time atualizado para '{new_team_name}'.")
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
                frames["jogadoras"] = jog_df; save_all(Path(st.session_state.db_path), frames)
                dbg_print(f"Jogadora #{player_num} removida.")
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
            save_all(Path(st.session_state.db_path), frames)
            dbg_print(f"Jogadora adicionada: #{new_number} {new_name} ({new_position}).")
        else:
            st.warning("Digite um nome.")
    st.button("➕ Adicionar Jogadora", key="add_player_btn", on_click=_add_player)
    st.markdown('</div>', unsafe_allow_html=True)

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
                    <iframe srcdoc='{html.escape(html_content)}'
                             style='width:100%; height:100%; border:none; margin-top:40px;'></iframe>
                </div>
                <script>
                  (function(){{
                    const btn = document.getElementById('uv-close');
                    btn.addEventListener('click', function(){{
                      try {{
                        const url = new URL(window.parent.location.href);
                        url.searchParams.set('uv_tutorial_close','1');
                        window.parent.history.replaceState({{}}, '', url);
                        document.getElementById('uv-modal').style.display='none';
                      }} catch(e) {{}}
                    }});
                  }})();
                </script>
                """,
                height=900, scrolling=True
            )
            params = dict(st.query_params)
            if params.get("uv_tutorial_close") == "1":
                st.session_state.show_tutorial = False
                newp = {k: v for k, v in dict(st.query_params).items() if k != "uv_tutorial_close"}
                st.query_params.from_dict(newp)
        else:
            st.error("Arquivo de tutorial não encontrado.")
    except Exception as e:
        st.error(f"Não consegui abrir o tutorial: {e}")
    st.button("❌ Fechar Tutorial", key="close_tutorial_btn", on_click=lambda: st.session_state.__setitem__("show_tutorial", False))

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
    dbg_print(f"Novo jogo criado (match_id={next_mid}) contra '{opp_name}' na data {dt}.")
if (st.session_state.match_id is None or st.session_state.show_cadastro) and not st.session_state.show_config_team:
    with st.container():
        st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
        st.subheader("🆕 Novo Jogo")

        cgj1, cgj2, cgj3 = st.columns([2, 1, 1])
        with cgj1:
            opp_name = st.text_input("Adversário:", key="new_game_opponent", value="")
        with cgj2:
            game_date = st.date_input("Data:", value=date.today(), key="new_game_date")
        with cgj3:
            st.markdown('<div class="btn-xxs" style="margin-top:20px;">', unsafe_allow_html=True)
            st.button("Criar Jogo", key="create_game_btn",
                      on_click=lambda: _create_new_match(st.session_state.get("new_game_opponent","").strip(), st.session_state.get("new_game_date", date.today())))
            st.markdown('</div>', unsafe_allow_html=True)

        st.markdown("---")
        st.subheader("🎯 Registrar Rally (Pré-jogo)")

        def on_submit_text_pre():
            raw = st.session_state.get("line_input_text", "").strip()
            if not raw: return
            quick_register_line(raw)
            st.session_state["line_input_text"] = ""
            st.session_state["q_side"] = "Nós"; st.session_state["q_result"] = "Acerto"; st.session_state["q_action"] = "d"; st.session_state["q_position"] = "Frente"

        _ = st.text_input("Digite código:", key="line_input_text", placeholder="Ex: 1 9 d",
                          label_visibility="collapsed", on_change=on_submit_text_pre)

        def _cb_register_pre():
            register_current(); st.session_state["line_input_text"] = ""

        c_reg_pre, c_undo_pre = st.columns([1, 1])
        with c_reg_pre:
            st.markdown('<div class="btn-xxs">', unsafe_allow_html=True)
            st.button("➕ Registrar", use_container_width=True, key="register_btn_pre", on_click=_cb_register_pre)
            st.markdown('</div>', unsafe_allow_html=True)
        with c_undo_pre:
            st.markdown('<div class="btn-xxs">', unsafe_allow_html=True)
            st.button("↩️ Desfazer Rally", use_container_width=True, key="undo_btn_pre", on_click=undo_last_rally_current_set)
            st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)
    st.stop()

# =========================
# Barra do sistema
# =========================
with st.container():
    st.markdown('<div class="sectionCard">', unsafe_allow_html=True)

    bar1, bar3, bar4, bar5 = st.columns([1.4, 3.2, 1.2, 1.4])
    with bar1:
        st.session_state.auto_close = st.toggle("Auto 25/15+2", value=st.session_state.auto_close, key="auto_close_toggle")
        st.session_state.game_mode = st.toggle("🎮 Modo Jogo", value=st.session_state.game_mode, key="game_mode_toggle")
    with bar3:
        sets_match_all = frames["sets"].loc[frames["sets"]["match_id"] == st.session_state.match_id].sort_values("set_number")
        sel_vals = sets_match_all["set_number"].tolist() if not sets_match_all.empty else [1]
        c31, c32, c33 = st.columns([1, 1, 1])
        with c31:
            set_to_reopen = st.selectbox("Set:", sel_vals, label_visibility="collapsed", key="set_select")
        def _close_set():
            frames_local = st.session_state.frames
            df_cur = current_set_df(frames_local, st.session_state.match_id, int(set_to_reopen))
            if df_cur.empty: st.warning("Sem rallies neste set.")
            else:
                hp, ap = set_score_from_df(df_cur)
                if hp == ap: st.warning("Empate — defina o set antes.")
                else:
                    _apply_set_winner_and_proceed(hp, ap)
                    st.session_state.data_rev += 1
        with c32:
            st.markdown('<div class="btn-xxs">', unsafe_allow_html=True)
            st.button("🔓 Reabrir Set", use_container_width=True, key="reopen_btn",
                      on_click=lambda: dict() if 'reopen_set' in globals() and callable(globals()['reopen_set']) and globals()['reopen_set'](st.session_state.match_id, int(set_to_reopen)) else None)
            st.markdown('</div>', unsafe_allow_html=True)
        with c33:
            st.markdown('<div class="btn-xxs">', unsafe_allow_html=True)
            st.button("✅ Fechar Set", use_container_width=True, key="close_set_btn", on_click=_close_set)
            st.markdown('</div>', unsafe_allow_html=True)
    with bar4:
        def _remove_empty_set():
            frames_local = st.session_state.frames
            stf = frames_local["sets"]; rl = frames_local["rallies"]; mid = st.session_state.match_id
            sets_m = stf[stf["match_id"]==mid]
            if sets_m.empty: st.warning("Sem sets cadastrados.")
            else:
                max_set = int(sets_m["set_number"].max())
                sub = rl[(rl["match_id"]==mid) & (rl["set_number"]==max_set)]
                if not sub.empty: st.warning(f"O Set {max_set} tem rallies e não será removido.")
                else:
                    stf = stf[~((stf["match_id"]==mid) & (stf["set_number"]==max_set))]
                    frames_local["sets"] = stf; save_all(Path(st.session_state.db_path), frames_local); st.success(f"Set {max_set} removido.")
                    st.session_state.frames = frames_local
                    st.session_state.data_rev += 1
        st.markdown('<div class="btn-xxs">', unsafe_allow_html=True)
        st.button("🗑️ Remover Set Vazio", use_container_width=True, key="remove_empty_set_btn", on_click=_remove_empty_set)
        st.markdown('</div>', unsafe_allow_html=True)
    with bar5:
        st.session_state.graph_filter = st.radio("Filtro Gráficos:", options=["Nós","Adversário","Ambos"],
            horizontal=True, index=["Nós","Adversário","Ambos"].index(st.session_state.graph_filter), key="graph_filter_radio")
    st.markdown('</div>', unsafe_allow_html=True)

# =========================
# PLACAR
# =========================
with st.container():
    st.markdown('<div class="sectionCard">', unsafe_allow_html=True)

    frames = st.session_state.frames

    df_set = current_set_df(frames, st.session_state.match_id, st.session_state.set_number)
    home_pts, away_pts = set_score_from_df(df_set)
    stf = frames["sets"]; sm = stf[stf["match_id"] == st.session_state.match_id]
    home_sets_w = int((sm["winner_team_id"] == 1).sum()); away_sets_w = int((sm["winner_team_id"] == 2).sum())

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

# =========================
# MODO JOGO
# =========================
if st.session_state.game_mode:
    with st.container():
        st.markdown('<div class="sectionCard game-mode-container">', unsafe_allow_html=True)
        st.subheader("🎮 Modo Jogo")

        st.caption("Jogadoras (toque rápido define lado = Nós):")
        label_mode_col, _ = st.columns([1.0, 3.0])
        with label_mode_col:
            st.session_state.player_label_mode = st.radio(
                "Mostrar botões por:", options=["Número","Nome"], horizontal=True,
                index=["Número","Nome"].index(st.session_state.player_label_mode),
                key="player_label_mode_gm"
            )
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
                        on_click=lambda n=n: (st.session_state.__setitem__("last_selected_player", n),
                                              st.session_state.__setitem__("q_side", "Nós")),
                        use_container_width=True
                    )
            with jcols[(len(nums)) % num_cols]:
                st.button("ADV", key="gm_adv_btn",
                          on_click=lambda: st.session_state.__setitem__("q_side", "Adv"),
                          use_container_width=True)
        else:
            st.caption("Sem jogadoras")

        st.caption("Resultado / Posição:")
        rc1, rc2 = st.columns([1.2, 1.0])
        with rc1:
            st.session_state.q_result = st.radio("Resultado", options=["Acerto","Erro"], horizontal=True, index=0,
                                                 key="gm_q_result", label_visibility="collapsed")
        with rc2:
            st.session_state.q_position = st.radio("Posição", options=["Frente","Fundo"], horizontal=True,
                                                   index=["Frente","Fundo"].index(st.session_state.q_position),
                                                   key="gm_q_position", label_visibility="collapsed")

        st.caption("Atalhos de Ação:")
        acols = st.columns(12)
        for i, code in enumerate(["d","l","m","lob","seg","pi","re","b","sa","rede"]):
            with acols[i % len(acols)]:
                label = ACT_MAP.get(code, code)[:3]
                st.button(label, key=f"gm_quick_{code}",
                          on_click=lambda code=code: register_current(action=code), use_container_width=True)

        st.button("↩️ Desfazer Rally", use_container_width=True, key="gm_undo_btn", on_click=undo_last_rally_current_set)

        st.markdown("---")
        st.markdown("**🗺️ Mapa de Calor (clique para marcar o local do ataque)**")

        st.session_state.show_heat_numbers = st.checkbox(
            "Mostrar número/ADV nas bolinhas (nossos + adversário)",
            value=st.session_state.show_heat_numbers, key="gm_show_numbers_chk"
        )

        df_hm = current_set_df(st.session_state.frames, st.session_state.match_id, st.session_state.set_number)
        pts_succ, pts_errs, pts_adv, pts_adv_err, dbg_gm = build_heat_points(
            df_hm,
            selected_players=None,
            include_success=True,
            include_errors=True,
            include_adv_points=True,
            include_adv_errors=True,
            return_debug=True
        )

        dbg_print(f"[Modo Jogo] Heatmap: succ={len(pts_succ)} err={len(pts_errs)} adv={len(pts_adv)} advErr={len(pts_adv_err)} (set={st.session_state.set_number})")

        render_court_html(pts_succ, pts_errs, pts_adv, pts_adv_err, enable_click=True, key="gm", show_numbers=st.session_state.show_heat_numbers)

        with st.expander("🔎 Debug Heatmap (Modo Jogo)"):
            st.write(f"Acertos (azul): **{len(pts_succ)}**  |  Erros (vermelho): **{len(pts_errs)}**  |  ADV (laranja): **{len(pts_adv)}**  |  ADV erros: **{len(pts_adv_err)}**")
            if not dbg_gm.empty:
                view = dbg_gm[["rally_no","player_number","action_u","res_u","who_u","used_x","used_y","origem","cor"]].tail(20)
                display_dataframe(view, height=180, use_container_width=True)
            else:
                st.write("_Sem registros elegíveis._")

        lc = st.session_state.get("last_court_click")
        if lc:
            st.caption(f"🧪 Último clique capturado: x={lc['x']:.2f}  y={lc['y']:.2f}  (será anexado ao próximo registro)")
        else:
            st.caption("🧪 Nenhum clique pendente. Clique na quadra para capturar x/y.")

        st.markdown('</div>', unsafe_allow_html=True)

    st.stop()

# =========================
# Painel principal
# =========================
with st.container():
    st.markdown('<div class="sectionCard">', unsafe_allow_html=True)

    frames = st.session_state.frames
    df_set = current_set_df(frames, st.session_state.match_id, st.session_state.set_number)

    left, right = st.columns([1.25, 1.0])

    # -------- ESQUERDA --------
    with left:
        st.markdown("**🎯 Registrar Rally**")

        def on_submit_text_main():
            raw = st.session_state.get("line_input_text", "").strip()
            if not raw: return
            quick_register_line(raw)
            st.session_state["line_input_text"] = ""
            st.session_state["q_side"] = "Nós"; st.session_state["q_result"] = "Acerto"; st.session_state["q_action"] = "d"; st.session_state["q_position"] = "Frente"

        st.text_input("Digite código:", key="line_input_text",
                      placeholder="Ex: 1 9 d", label_visibility="collapsed", on_change=on_submit_text_main)

        def _cb_register_main():
            register_current(); st.session_state["line_input_text"] = ""

        c_reg, c_undo = st.columns([1, 1])
        with c_reg:
            st.markdown('<div class="btn-xxs">', unsafe_allow_html=True)
            st.button("➕ Registrar", use_container_width=True, key="register_btn_main", on_click=_cb_register_main)
            st.markdown('</div>', unsafe_allow_html=True)
        with c_undo:
            st.markdown('<div class="btn-xxs">', unsafe_allow_html=True)
            st.button("↩️ Desfazer Rally", use_container_width=True, key="undo_btn_main", on_click=undo_last_rally_current_set)
            st.markdown('</div>', unsafe_allow_html=True)

        # Jogadoras (Número/Nome)
        st.caption("Jogadoras (selecione):")
        label_mode_col, _ = st.columns([1.0, 3.0])
        with label_mode_col:
            st.session_state.player_label_mode = st.radio(
                "Mostrar botões por:", options=["Número","Nome"], horizontal=True,
                index=["Número","Nome"].index(st.session_state.player_label_mode),
                key="player_label_mode_main"
            )

        nums = resolve_our_roster_numbers(frames)
        name_map = {r["number"]: r["name"] for r in roster_for_ui(frames)}

        if nums:
            num_cols = 12 if st.session_state.player_label_mode == "Número" else 4
            jcols = st.columns(num_cols)
            for i, n in enumerate(nums):
                label_txt = str(n) if st.session_state.player_label_mode == "Número" else (name_map.get(n) or str(n))
                with jcols[i % num_cols]:
                    st.button(
                        f"{label_txt}",
                        key=f"pill_main_{n}",
                        on_click=lambda n=n: (st.session_state.__setitem__("last_selected_player", n),
                                              st.session_state.__setitem__("q_side", "Nós")),
                        use_container_width=True
                    )
            with jcols[(len(nums)) % num_cols]:
                st.button("ADV", key="pill_main_adv",
                          on_click=lambda: st.session_state.__setitem__("q_side", "Adv"),
                          use_container_width=True)
            sel = st.session_state.get("last_selected_player")
            if sel is not None:
                st.caption(f"Selecionada: **#{sel}**")
            else:
                st.caption("Sem jogadoras")

            # Resultado + Posição
            row2 = st.columns([1.0, 1.0])
            with row2[0]:
                st.caption("Resultado:")
                st.session_state.q_result = st.radio(
                    "Resultado", options=["Acerto","Erro"], horizontal=True, index=0,
                    key="q_result_radio_main", label_visibility="collapsed"
                )
            with row2[1]:
                st.caption("Posição:")
                st.session_state.q_position = st.radio(
                    "Posição", options=["Frente","Fundo"], horizontal=True,
                    index=["Frente","Fundo"].index(st.session_state.q_position),
                    key="q_position_radio_main", label_visibility="collapsed"
                )

            # Ação (select que também registra)
            def on_action_change():
                selected_label = st.session_state.get("q_action_select_main", None)
                if not selected_label:
                    return
                code = REVERSE_ACT_MAP.get(selected_label, "d")
                st.session_state["q_action"] = code
                if st.session_state.get("last_selected_player") is None:
                    st.warning("Selecione uma jogadora antes de escolher a ação.")
                    return
                register_current(action=code)

            st.caption("Ação:")
            action_options = list(ACT_MAP.values())
            current_action = ACT_MAP.get(st.session_state.q_action, "Diagonal")
            st.selectbox(
                "Ação", action_options, index=action_options.index(current_action),
                label_visibility="collapsed", key="q_action_select_main",
                on_change=on_action_change
            )

            # Atalhos (inclui 'rede')
            st.caption("Atalhos:")
            st.markdown('<div class="atalhos-container small-btn">', unsafe_allow_html=True)
            acols = st.columns(12)
            codes = ["d","l","m","lob","seg","pi","re","b","sa","rede"]
            for i, code in enumerate(codes):
                with acols[i % len(acols)]:
                    label = ACT_MAP.get(code, code)[:3]
                    st.button(
                        label, key=f"quick_main_{code}",
                        on_click=lambda code=code: register_current(action=code),
                        use_container_width=True
                    )
            st.markdown('</div>', unsafe_allow_html=True)

            # Tabelas rápidas (pontuadoras / erros / histórico)
            tt1, tt2, tt3 = st.columns([1.0, 1.0, 1.2])
            df_cur = df_set
            with tt1:
                st.markdown("**Pontuadoras**")
                if not df_cur.empty:
                    atq = df_cur[
                        (df_cur["who_scored"]=="NOS") &
                        (df_cur["action"].isin(ATTACK_ACTIONS))
                    ].copy()
                    if not atq.empty:
                        tbl = atq.groupby(["player_number"]).size().reset_index(name="pontos").sort_values("pontos", ascending=False)
                        display_dataframe(tbl, height=360, use_container_width=True)
                    else:
                        st.write("_Sem dados_")
                else:
                    st.write("_Sem dados._")
            with tt2:
                st.markdown("**Erros (Nossos)**")
                if not df_cur.empty:
                    er = df_cur[(df_cur["result"]=="ERRO") & (df_cur["who_scored"]=="ADV")].copy()
                    if not er.empty:
                        er["player_number"] = er["player_number"].fillna("—").astype(str)
                        tbl = er.groupby(["player_number"]).size().reset_index(name="erros").sort_values("erros", ascending=False)
                        display_dataframe(tbl, height=360, use_container_width=True)
                    else:
                        st.write("_Sem erros_")
                else:
                    st.write("_Sem dados._")
            with tt3:
                st.markdown("**Histórico**")
                if not df_cur.empty:
                    seq = ["Nós" if str(w).upper() == "NOS" else "Adv" for w in df_cur["who_scored"]]
                    histo = pd.DataFrame({"Rally": range(1, len(seq)+1), "Quem pontuou": seq})
                    display_dataframe(histo, height=360, use_container_width=True)
                else:
                    st.info("Sem rallies")

        # -------- DIREITA: GRÁFICOS / KPIs --------
        with right:
            def filter_df_for_graphs(df: pd.DataFrame, who: str) -> pd.DataFrame:
                if df.empty: return df
                if who == "Nós": return df[df["who_scored"] == "NOS"]
                if who == "Adversário": return df[df["who_scored"] == "ADV"]
                return df

            df_viz = filter_df_for_graphs(df_set, st.session_state.graph_filter)

            # Placar (evolução no set)
            st.markdown("**Placar**")
            if not df_set.empty:
                fig3, ax3 = small_fig()
                from matplotlib.ticker import MaxNLocator
                ax3.xaxis.set_major_locator(MaxNLocator(integer=True))
                ax3.yaxis.set_major_locator(MaxNLocator(integer=True))
                ax3.plot(df_set["rally_no"], df_set["score_home"], marker="o", markersize=2.4, linewidth=0.9, label=home_name or "Nós")
                ax3.plot(df_set["rally_no"], df_set["score_away"], marker="o", markersize=2.4, linewidth=0.9, label=away_name or "Adv")
                if not df_set.empty:
                    last_rally = int(df_set["rally_no"].iloc[-1])
                    ax3.scatter([last_rally], [df_set["score_home"].iloc[-1]], s=28, zorder=5)
                    ax3.scatter([last_rally], [df_set["score_away"].iloc[-1]], s=28, zorder=5)
                    ax3.annotate(str(df_set["score_home"].iloc[-1]), (last_rally, df_set["score_home"].iloc[-1]),
                                textcoords="offset points", xytext=(4, 4), fontsize=7, ha='center')
                    ax3.annotate(str(df_set["score_away"].iloc[-1]), (last_rally, df_set["score_away"].iloc[-1]),
                                textcoords="offset points", xytext=(4, -10), fontsize=7, ha='center')
                fig3 = trim_ax(ax3, xlabel="Rally", ylabel="Pts", legend=True, max_xticks=10, max_yticks=6)
                ax3.legend(loc="upper left", frameon=False, handlelength=1.0, borderaxespad=0.1)
                st.pyplot(fig3)
            else:
                st.write("_Sem dados._")

            # Erros por jogadora
            st.markdown("**Erros (por jogadora)**")
            err = df_set[(df_set["result"]=="ERRO") & (df_set["who_scored"]=="ADV")].copy()
            if not err.empty:
                err["player_number"] = err["player_number"].fillna("—").astype(str)
                tbl = err.groupby(["player_number"]).size().reset_index(name="erros").sort_values("erros", ascending=False)
                fig2, ax2 = small_fig()
                ax2.bar(tbl["player_number"], tbl["erros"])
                fig2 = trim_ax(ax2, xlabel="Jog.", ylabel="Erros", legend=False, max_xticks=10, max_yticks=5)
                st.pyplot(fig2)
            else:
                st.write("_Sem erros._")

            # Eficiência por jogadora (ataque)
            st.markdown("**Eficiência por Jogadora (ataque)**")
            def build_attack_rows_for_side(df_base: pd.DataFrame, side_sel: str) -> pd.DataFrame:
                if df_base.empty:
                    return df_base
                mask_action = df_base["action"].isin(ATTACK_ACTIONS)
                if side_sel == "Nós":
                    pts  = df_base[mask_action & (df_base["who_scored"]=="NOS")]
                    errs = df_base[mask_action & (df_base["who_scored"]=="ADV") & (df_base["result"]=="ERRO")]
                elif side_sel == "Adversário":
                    pts  = df_base[mask_action & (df_base["who_scored"]=="ADV")]
                    errs = df_base[mask_action & (df_base["who_scored"]=="NOS") & (df_base["result"]=="ERRO")]
                else:
                    return df_base[mask_action]
                return pd.concat([pts, errs], ignore_index=True) if not pts.empty or not errs.empty else pd.DataFrame(columns=df_base.columns)

            att = build_attack_rows_for_side(df_set, st.session_state.graph_filter)
            if not att.empty:
                att = att.copy()
                if st.session_state.graph_filter == "Nós":
                    att["is_ponto"] = ((att["who_scored"]=="NOS") & (att["result"]=="PONTO")).astype(int)
                    att["is_erro"]  = ((att["who_scored"]=="ADV") & (att["result"]=="ERRO")).astype(int)
                elif st.session_state.graph_filter == "Adversário":
                    att["is_ponto"] = ((att["who_scored"]=="ADV") & (att["result"]=="PONTO")).astype(int)
                    att["is_erro"]  = ((att["who_scored"]=="NOS") & (att["result"]=="ERRO")).astype(int)
                else:
                    att["is_ponto"] = (att["result"]=="PONTO").astype(int)
                    att["is_erro"]  = (att["result"]=="ERRO").astype(int)

                eff = att.groupby(["player_number"]).agg(
                    tentativas=("result","count"),
                    pontos=("is_ponto","sum"),
                    erros=("is_erro","sum")
                ).reset_index()
                eff["eficiencia"] = (eff["pontos"] - eff["erros"]) / eff["tentativas"].replace(0, 1)
                eff["player_number"] = eff["player_number"].fillna("—").astype(str)
                fig1, ax1 = small_fig()
                ax1.bar(eff["player_number"], eff["eficiencia"])
                fig1 = trim_ax(ax1, xlabel="Jog.", ylabel="Ef.", legend=False, max_xticks=10, max_yticks=5)
                st.pyplot(fig1)
            else:
                st.write("_Sem dados._")

        st.markdown('</div>', unsafe_allow_html=True)

# =========================
# Mapa de Calor (Quadra) — Set atual
# =========================
with st.container():
    st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
    st.subheader("🗺️ Mapa de Calor (Quadra) — Set atual")

    colf1, colf2, colf3, colf4, colf5 = st.columns([1.2, .8, .9, .9, 1.2])
    with colf1:
        nums = resolve_our_roster_numbers(st.session_state.frames)
        selected_players = st.multiselect("Jogadoras:", options=nums, default=nums, placeholder="Todas")
    with colf2:
        show_success = st.checkbox("Acertos (azul)", value=True)
    with colf3:
        show_errors = st.checkbox("Erros (vermelho)", value=True)
    with colf4:
        show_adv_pts = st.checkbox("ADV pontos (laranja)", value=True)
    with colf5:
        show_adv_errs = st.checkbox("ADV erros (laranja escuro)", value=True)

    st.session_state.show_heat_numbers = st.checkbox(
        "Mostrar número/ADV nas bolinhas (nossos + adversário)",
        value=st.session_state.show_heat_numbers, key="set_show_numbers_chk"
    )

    df_hm = current_set_df(st.session_state.frames, st.session_state.match_id, st.session_state.set_number)
    pts_succ, pts_errs, pts_adv, pts_adv_err, dbg = build_heat_points(
        df_hm,
        selected_players=selected_players,
        include_success=show_success,
        include_errors=show_errors,
        include_adv_points=show_adv_pts,
        include_adv_errors=show_adv_errs,
        return_debug=True
    )
    dbg_print(f"[Heatmap Set] succ={len(pts_succ)} err={len(pts_errs)} advPts={len(pts_adv)} advErr={len(pts_adv_err)} | filtros: players={selected_players}")

    render_court_html(
        pts_succ,
        pts_errs,
        pts_adv if show_adv_pts else [],
        pts_adv_err if show_adv_errs else [],
        enable_click=True,
        key="set",
        show_numbers=st.session_state.show_heat_numbers
    )

    with st.expander("🔎 Debug Heatmap (Set atual)"):
        st.write(
            f"Acertos (azul): **{len(pts_succ)}** | Erros (vermelho): **{len(pts_errs)}** | "
            f"ADV pontos (laranja): **{len(pts_adv)}** | ADV erros (laranja escuro): **{len(pts_adv_err)}**"
        )
        if not dbg.empty:
            view = dbg[["rally_no","player_number","action_u","res_u","who_u","used_x","used_y","origem","cor"]].tail(30)
            display_dataframe(view, height=180, use_container_width=True)
        else:
            st.write("_Sem dados elegíveis._")

    st.markdown('</div>', unsafe_allow_html=True)

# =========================
# Tabelas complementares / Debug
# =========================
with st.container():
    st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
    st.subheader("🧾 Tabelas rápidas do Set")

    df_cur = current_set_df(st.session_state.frames, st.session_state.match_id, st.session_state.set_number)

    cta, ctb = st.columns(2)

    with cta:
        st.markdown("**Rallies (últimos 50)**")
        if not df_cur.empty:
            cols_show = ["rally_no","player_number","action","result","who_scored","score_home","score_away"]
            cols_show = [c for c in cols_show if c in df_cur.columns]
            view = df_cur[cols_show].tail(50).copy()
            view = view.rename(columns={
                "rally_no":"Rally",
                "player_number":"Jog",
                "action":"Ação",
                "result":"Res",
                "who_scored":"Quem",
                "score_home":"Nós",
                "score_away":"Adv"
            })
            display_dataframe(view, height=260, use_container_width=True)
        else:
            st.write("_Sem dados._")

    with ctb:
        st.markdown("**Ataques (nossos) – últimos 30**")
        if not df_cur.empty:
            atq = df_cur[(df_cur["who_scored"]=="NOS") & (df_cur["action"].isin(ATTACK_ACTIONS))].copy()
            if not atq.empty:
                cols_show2 = ["rally_no","player_number","result"]
                cols_show2 = [c for c in cols_show2 if c in atq.columns]
                v2 = atq[cols_show2].tail(30).rename(columns={
                    "rally_no":"Rally","player_number":"Jog","result":"Res"
                })
                display_dataframe(v2, height=260, use_container_width=True)
            else:
                st.write("_Sem dados._")
        else:
            st.write("_Sem dados._")

    st.markdown('</div>', unsafe_allow_html=True)

# =========================
# Rodapé
# =========================
st.markdown(
    "<div style='text-align:center; opacity:.6; font-size:.78rem; margin:8px 0;'>"
    "UniVolei • Live Scout</div>",
    unsafe_allow_html=True
)

# =========================
# Configuração para deploy (Render)
# =========================
if __name__ == "__main__":
    import os
    import streamlit as st

    port = int(os.environ.get("PORT", 10000))
    if not st.session_state.get("_boot_rerun_done", False):
        st.session_state["_boot_rerun_done"] = True
        st.rerun()
