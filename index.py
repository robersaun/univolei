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
import time  # diagnóstico leve
from datetime import date

from db_excel import (
    init_or_load, save_all, add_match, add_set,
    append_rally, last_open_match, finalize_match
)
from parser_free import parse_line

# =========================
# Config + Estilos
# =========================
st.set_page_config(page_title="", layout="wide")

# anti-scroll-jump: preserva posição
components.html("""
<script>
const KEY='uv_scroll_y';
window.addEventListener('load', ()=>{const y=sessionStorage.getItem(KEY); if(y!==null){window.scrollTo(0,parseInt(y));}});
window.addEventListener('beforeunload', ()=>{sessionStorage.setItem(KEY, window.scrollY.toString());});
</script>
""", height=0)

# =========================
# Função para carregar CSS externo (univolei.css)
# =========================
BASE_DIR = Path(__file__).parent.resolve()
def load_css(filename: str = "univolei.css"):
    css_path = BASE_DIR / filename
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)
    else:
        st.warning(f"Arquivo CSS não encontrado: {filename}")

load_css("univolei.css")

# Título com SVG da bola (robusto mesmo sem emoji)
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
# Figuras ultra-compactas (helper)
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
# DataFrame HTML (sem PyArrow)
# =========================
def display_dataframe(df, height=None, use_container_width=False, extra_class: str = ""):
    if df.empty:
        st.write("_Sem dados._"); return
    classes = ('custom-table ' + extra_class).strip()
    html_table = df.to_html(classes=classes, index=False, escape=False)
    styled_html = f"""
    <div style='overflow:auto; height:{height}px; width: {"100%" if use_container_width else "auto"};'>
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
st.session_state.setdefault("last_selected_player", None)
st.session_state.setdefault("show_cadastro", False)
st.session_state.setdefault("show_tutorial", False)
st.session_state.setdefault("show_config_team", False)
st.session_state.setdefault("line_input_text_pre", "")
st.session_state.setdefault("line_input_text_main", "")
st.session_state.setdefault("perf_logs", [])

# ===== Diagnóstico opcional de performance =====
PERF_DEBUG = False  # ative/desative aqui

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
ATTACK_ACTIONS = ["DIAGONAL","LINHA","PIPE","SEGUNDA","LOB","MEIO"]
ACT_MAP = {"d": "Diagonal","l": "Paralela","m": "Meio","lob": "Largada","seg": "Segunda","pi": "Pipe","re": "Recepção","b": "Bloqueio","sa": "Saque"}
REVERSE_ACT_MAP = {v: k for k, v in ACT_MAP.items()}

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
    fr = st.session_state.frames
    match_id = st.session_state.match_id
    set_number = st.session_state.set_number
    rl = fr["rallies"]; sub = rl[(rl["match_id"]==match_id) & (rl["set_number"]==set_number)]
    if sub.empty:
        st.warning("Não há rallies para desfazer neste set."); return
    last_rally_id = sub.iloc[-1]["rally_id"]
    rl = rl[rl["rally_id"] != last_rally_id]; fr["rallies"] = rl
    recompute_set_score_fields(fr, match_id, set_number)
    save_all(Path(st.session_state.db_path), fr)
    st.session_state.data_rev += 1
    st.success("Último rally desfeito e placar recalculado.")

# ===== GARANTIA de quem pontuou (who_scored) conforme lado + erro/ponto =====
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

def _fast_apply_scores_to_row(row: dict):
    """Atualiza score_home/score_away apenas para o novo rally (O(1))."""
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
    if not raw_line.strip(): return
    t0 = time.perf_counter()
    row = parse_line(raw_line)
    row = _fix_who_scored_from_raw_and_row(raw_line, row)
    row = _fast_apply_scores_to_row(row)
    t1 = time.perf_counter()
    append_rally(st.session_state.frames, match_id=st.session_state.match_id, set_number=st.session_state.set_number, row=row)
    save_all(Path(st.session_state.db_path), st.session_state.frames)
    st.session_state.data_rev += 1
    auto_close_set_if_needed()
    if PERF_DEBUG:
        t2 = time.perf_counter()
        _add_perf_log(f"parse+fix+score: {(t1-t0)*1000:.1f} ms | append+save+auto: {(t2-t1)*1000:.1f} ms")

def quick_register_click(side: str, number: int | None, action: str, is_error: bool):
    prefix = "1" if side == "NOS" else "0"
    if is_error:
        line = f"{prefix} {number if number is not None else ''} e".strip()
    else:
        line = f"{prefix} {number if number is not None else ''} {action}".strip()
    quick_register_line(line)

def resolve_our_roster_numbers(frames: dict) -> list[int]:
    jg = frames.get("jogadoras", pd.DataFrame()).copy()
    if jg.empty: return []
    for col in ["team_id","player_number"]:
        if col in jg.columns: jg[col] = pd.to_numeric(jg[col], errors="coerce")
    ours = jg[jg["team_id"] == OUR_TEAM_ID].dropna(subset=["player_number"]).sort_values("player_number")
    return ours["player_number"].astype(int).unique().tolist()

# central de registro (usada por: botão Registrar, atalhos e mudança de ação)
def register_current(number: int | None = None, action: str | None = None):
    side_code = "NOS" if st.session_state.get("q_side", "Nós") == "Nós" else "ADV"
    is_err = (st.session_state.get("q_result", "Acerto") == "Erro")
    act = action if action is not None else st.session_state.get("q_action", "d")
    num_val = number if number is not None else st.session_state.get("last_selected_player", None)
    if num_val is None:
        raw = st.session_state.get("line_input_text_main", "")
        m = re.findall(r"\b(\d{1,2})\b", raw)
        num_val = int(m[-1]) if m else None
    quick_register_click(side_code, num_val, act, is_err)

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
    # Tenta resolver o caminho real do histórico
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

    if found_hist:
        try:
            st.page_link(found_hist, label="🗂️ Histórico")
        except Exception:
            def _go_hist(p=found_hist):
                try:
                    st.switch_page(p)
                except Exception:
                    st.warning("Não consegui abrir a página. Atualize seu Streamlit.")
            st.button("🗂️ Histórico", use_container_width=True, on_click=_go_hist)
    else:
        st.button("🗂️ Histórico", use_container_width=True,
                  on_click=lambda: st.warning("Página de histórico não encontrada."))

# =========================
# Modal do Tutorial (corrigido)
# =========================
if st.session_state.get("show_tutorial", False):
    try:
        html_path = BASE_DIR / "tutorial_scout.html"
        if html_path.exists():
            html_content = html_path.read_text(encoding="utf-8")
            components.html(
                f"""
                <div id='uv-tutorial' style='position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); 
                width: 90vw; height: 85vh; background-color: white; z-index: 9999; 
                border: 2px solid #ccc; border-radius: 10px; overflow: hidden; box-shadow: 0 10px 30px rgba(0,0,0,.25);'>
                    <div style='position: absolute; top: 10px; right: 10px; z-index: 10000;'>
                        <button id='uv-close'
                            style='background: #ff4b4b; color: white; border: none; border-radius: 50%; 
                                   width: 30px; height: 30px; cursor: pointer; font-weight: bold;'>X</button>
                    </div>
                    <iframe srcdoc='{html.escape(html_content)}' 
                            style='width: 100%; height: 100%; border: none; margin-top: 40px;'></iframe>
                </div>

                <script>
                (function(){{
                  var btn = document.getElementById('uv-close');
                  var box = document.getElementById('uv-tutorial');

                  function closeTutorial() {{
                    if (box) box.style.display = 'none';
                    var fr = window.frameElement;
                    if (fr) {{
                      fr.style.height = '0px';
                      fr.style.width = '0px';
                      fr.style.display = 'none';
                      fr.style.border = '0';
                    }}
                    try {{
                      var pdoc = window.parent.document;
                      var btns = pdoc.querySelectorAll('button');
                      for (var i=0; i<btns.length; i++) {{
                        var t = (btns[i].innerText || '').trim();
                        if (t.indexOf('Fechar Tutorial') !== -1) {{
                          btns[i].click();
                          break;
                        }}
                      }}
                    }} catch (e) {{}}
                  }}

                  if (btn) btn.addEventListener('click', closeTutorial);
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
# Modais resumidos (Config/Tutorial) — mantidos (Config do Time)
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
        else:
            st.warning("Digite um nome.")
    st.button("➕ Adicionar Jogadora", key="add_player_btn", on_click=_add_player)
    st.markdown('</div>', unsafe_allow_html=True)

# =========================
# Cadastro rápido / NOVO JOGO (sem jogo OU quando clicar "🆕 Jogo")
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
            raw = st.session_state.get("line_input_text_pre", "").strip()
            if not raw: return
            quick_register_line(raw)
            st.session_state["line_input_text_pre"] = ""
            st.session_state["q_side"] = "Nós"; st.session_state["q_result"] = "Acerto"; st.session_state["q_action"] = "d"

        _ = st.text_input("Digite código:", key="line_input_text_pre", placeholder="Ex: 1 9 d",
                          label_visibility="collapsed", on_change=on_submit_text_pre)

        def _cb_register_pre():
            register_current(); st.session_state["line_input_text_pre"] = ""

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

# Se ainda não há partida, encerra após área de cadastro
if st.session_state.match_id is None or st.session_state.show_config_team:
    st.stop()

# =========================
# Barra do sistema
# =========================
with st.container():
    st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
    if home_name and away_name: st.markdown(f"**Jogo:** {home_name} x {away_name} — {date_str}")
    bar1, bar3, bar4, bar5 = st.columns([1, 3.2, 1.2, 1.4])
    with bar1:
        st.session_state.auto_close = st.toggle("Auto 25/15+2", value=st.session_state.auto_close, key="auto_close_toggle")
    with bar3:
        sets_match_all = frames["sets"][frames["sets"]["match_id"]==st.session_state.match_id].sort_values("set_number")
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
                      on_click=lambda: dict() if reopen_set(st.session_state.match_id, int(set_to_reopen)) else None)
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

# ============ Painel de DEBUG (aparece só se PERF_DEBUG=True) ============
if PERF_DEBUG:
    with st.container():
        st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
        st.markdown("**Debug de performance**")
        logs = st.session_state.get("perf_logs", [])
        if logs:
            st.text("\n".join(logs[-12:]))
        else:
            st.caption("Sem logs ainda.")
        st.markdown('</div>', unsafe_allow_html=True)

# =========================
# Painel principal: Esquerda (inputs) | Direita (gráficos)
# =========================
with st.container():
    st.markdown('<div class="sectionCard">', unsafe_allow_html=True)

    frames = st.session_state.frames

    # placar topo
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

    left, right = st.columns([1.25, 1.0])

    # -------- ESQUERDA: INPUTS --------
    with left:
        st.markdown("**🎯 Registrar Rally**")

        def on_submit_text_main():
            raw = st.session_state.get("line_input_text_main", "").strip()
            if not raw: return
            quick_register_line(raw)
            st.session_state["line_input_text_main"] = ""
            st.session_state["q_side"] = "Nós"; st.session_state["q_result"] = "Acerto"; st.session_state["q_action"] = "d"

        st.text_input("Digite código:", key="line_input_text_main",
                      placeholder="Ex: 1 9 d", label_visibility="collapsed", on_change=on_submit_text_main)

        def _cb_register_main():
            register_current(); st.session_state["line_input_text_main"] = ""

        c_reg, c_undo = st.columns([1, 1])
        with c_reg:
            st.markdown('<div class="btn-xxs">', unsafe_allow_html=True)
            st.button("➕ Registrar", use_container_width=True, key="register_btn_main", on_click=_cb_register_main)
            st.markdown('</div>', unsafe_allow_html=True)
        with c_undo:
            st.markdown('<div class="btn-xxs">', unsafe_allow_html=True)
            st.button("↩️ Desfazer Rally", use_container_width=True, key="undo_btn_main", on_click=undo_last_rally_current_set)
            st.markdown('</div>', unsafe_allow_html=True)

        st.caption("Jogadoras (selecione):")
        nums = resolve_our_roster_numbers(frames)
        if nums:
            st.markdown('<div class="jogadoras-container">', unsafe_allow_html=True)
            jcols = st.columns(min(12, max(1, len(nums))))
            for i, n in enumerate(nums):
                with jcols[i % len(jcols)]:
                    st.button(f"{n}", key=f"pill_main_{n}", on_click=lambda n=n: st.session_state.__setitem__("last_selected_player", n), use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
            sel = st.session_state.get("last_selected_player")
            if sel is not None:
                st.caption(f"Selecionada: **#{sel}**")
        else:
            st.caption("Sem jogadoras")

        row2 = st.columns([1.0, 1.0, 0.6])
        with row2[0]:
            st.caption("Equipe:")
            st.session_state.q_side = st.radio("Equipe", options=["Nós","Adv"], horizontal=True, index=0,
                                               key="q_side_radio_main", label_visibility="collapsed")
        with row2[1]:
            st.caption("Resultado:")
            st.session_state.q_result = st.radio("Resultado", options=["Acerto","Erro"], horizontal=True, index=0,
                                                 key="q_result_radio_main", label_visibility="collapsed")

        def on_action_change():
            selected_label = st.session_state.get("q_action_select_main", None)
            if not selected_label: return
            code = REVERSE_ACT_MAP.get(selected_label, "d")
            st.session_state["q_action"] = code
            if st.session_state.get("last_selected_player") is None:
                st.warning("Selecione uma jogadora antes de escolher a ação."); return
            register_current(action=code)

        with row2[2]:
            st.caption("Ação:")
            action_options = list(ACT_MAP.values())
            current_action = ACT_MAP.get(st.session_state.q_action, "Diagonal")
            st.selectbox("Ação", action_options, index=action_options.index(current_action),
                         label_visibility="collapsed", key="q_action_select_main", on_change=on_action_change)

        st.caption("Atalhos:")
        st.markdown('<div class="atalhos-container small-btn">', unsafe_allow_html=True)
        acols = st.columns(12)
        codes = ["d","l","m","lob","seg","pi","re","b","sa"]
        for i, code in enumerate(codes):
            with acols[i % len(acols)]:
                label = ACT_MAP.get(code, code)[:3]
                st.button(label, key=f"quick_main_{code}",
                          on_click=lambda code=code: register_current(action=code), use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # Tabelas compactas
        tt1, tt2, tt3 = st.columns([1.0, 1.0, 1.2])
        df_cur = df_set
        with tt1:
            st.markdown("**Pontuadoras**")
            if not df_cur.empty:
                atq = df_cur[(df_cur["who_scored"]=="NOS") & (df_cur["action"].isin(ATTACK_ACTIONS))].copy()
                if not atq.empty:
                    tbl = atq.groupby(["player_number"]).size().reset_index(name="pontos").sort_values("pontos", ascending=False)
                    display_dataframe(tbl, height=360, use_container_width=True)
                else: st.write("_Sem dados_")
            else: st.write("_Sem dados_")
        with tt2:
            st.markdown("**Erros (Nossos)**")
            if not df_cur.empty:
                er = df_cur[(df_cur["result"]=="ERRO") & (df_cur["who_scored"]=="ADV")].copy()
                if not er.empty:
                    er["player_number"] = er["player_number"].fillna("—").astype(str)
                    tbl = er.groupby(["player_number"]).size().reset_index(name="erros").sort_values("erros", ascending=False)
                    display_dataframe(tbl, height=360, use_container_width=True)
                else: st.write("_Sem erros_")
            else: st.write("_Sem dados_")
        with tt3:
            st.markdown("**Histórico**")
            if not df_cur.empty:
                seq = ["N" if w == "NOS" else "A" for w in df_cur["who_scored"]]
                histo = pd.DataFrame({"Rally": range(1, len(seq)+1), "Ponto": seq})
                display_dataframe(histo, height=360, use_container_width=True)
            else:
                st.info("Sem rallies")

    # -------- DIREITA: GRÁFICOS EMPILHADOS --------
    with right:
        def filter_df_for_graphs(df: pd.DataFrame, who: str) -> pd.DataFrame:
            if df.empty: return df
            if who == "Nós": return df[df["who_scored"] == "NOS"]
            if who == "Adversário": return df[df["who_scored"] == "ADV"]
            return df
        df_viz = filter_df_for_graphs(df_set, st.session_state.graph_filter)

        # 1) Placar
        st.markdown("**Placar**")
        if not df_set.empty:
            fig3, ax3 = small_fig()
            from matplotlib.ticker import MaxNLocator
            ax3.xaxis.set_major_locator(MaxNLocator(integer=True))
            ax3.yaxis.set_major_locator(MaxNLocator(integer=True))
            ax3.plot(df_set["rally_no"], df_set["score_home"], marker="o", markersize=2.4, linewidth=0.9, label=home_name or "Nós")
            ax3.plot(df_set["rally_no"], df_set["score_away"], marker="o", markersize=2.4, linewidth=0.9, label=away_name or "Adv")
            last_rally = int(df_set["rally_no"].iloc[-1])
            ax3.scatter([last_rally], [home_pts], s=28, zorder=5)
            ax3.scatter([last_rally], [away_pts], s=28, zorder=5)
            ax3.annotate(str(home_pts), (last_rally, home_pts), textcoords="offset points", xytext=(4, 4), fontsize=7, ha='center')
            ax3.annotate(str(away_pts), (last_rally, away_pts), textcoords="offset points", xytext=(4, -10), fontsize=7, ha='center')
            fig3 = trim_ax(ax3, xlabel="Rally", ylabel="Pts", legend=True, max_xticks=10, max_yticks=6)
            ax3.legend(loc="upper left", frameon=False, handlelength=1.0, borderaxespad=0.1)
            st.pyplot(fig3)
        else:
            st.write("_Sem dados._")

        # 2) Erros (por jogadora)
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

        # 3) Eficiência por Jogadora (ataque)
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
# KPIs — Set atual e Partida, para NÓS e ADV (com colunas de ataque)
# =========================
def compute_kpis_scope(frames, match_id, set_number, scope="set") -> pd.DataFrame:
    rl = frames["rallies"]
    if scope == "set":
        df = current_set_df(frames, match_id, set_number)
    else:
        df = rl[rl["match_id"] == match_id].copy().sort_values(["set_number","rally_no"])
    if df.empty:
        return pd.DataFrame(columns=["Lado","Aces","Bloqueios ponto","Erros","Paralela","Diagonal","Meio","Clutch saldo"])

    aces_nos = len(df[(df["who_scored"]=="NOS") & (df["action"]=="SAQUE")])
    aces_adv = len(df[(df["who_scored"]=="ADV") & (df["action"]=="SAQUE")])

    blq_nos = len(df[(df["who_scored"]=="NOS") & (df["action"]=="BLOQUEIO")])
    blq_adv = len(df[(df["who_scored"]=="ADV") & (df["action"]=="BLOQUEIO")])

    err_nos = len(df[(df["result"]=="ERRO") & (df["who_scored"]=="ADV")])
    err_adv = len(df[(df["result"]=="ERRO") & (df["who_scored"]=="NOS")])

    par_nos = len(df[(df["who_scored"]=="NOS") & (df["action"]=="LINHA")])
    par_adv = len(df[(df["who_scored"]=="ADV") & (df["action"]=="LINHA")])

    diag_nos = len(df[(df["who_scored"]=="NOS") & (df["action"]=="DIAGONAL")])
    diag_adv = len(df[(df["who_scored"]=="ADV") & (df["action"]=="DIAGONAL")])

    meio_nos = len(df[(df["who_scored"]=="NOS") & (df["action"]=="MEIO")])
    meio_adv = len(df[(df["who_scored"]=="ADV") & (df["action"]=="MEIO")])

    diff = (df["score_home"] - df["score_away"]).abs()
    in_clutch = (df["score_home"].between(20,25)) | (df["score_away"].between(20,25)) | (diff <= 2)
    clutch = df[in_clutch]
    clutch_nos = int((clutch["who_scored"]=="NOS").sum() - (clutch["who_scored"]=="ADV").sum()) if not clutch.empty else 0
    clutch_adv = -clutch_nos

    out = pd.DataFrame([
        {"Lado":"Nós","Aces":aces_nos,"Bloqueios ponto":blq_nos,"Erros":err_nos,
         "Paralela":par_nos,"Diagonal":diag_nos,"Meio":meio_nos,"Clutch saldo":clutch_nos},
        {"Lado":"Adversário","Aces":aces_adv,"Bloqueios ponto":blq_adv,"Erros":err_adv,
         "Paralela":par_adv,"Diagonal":diag_adv,"Meio":meio_adv,"Clutch saldo":clutch_adv},
    ])
    return out

with st.container():
    st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
    st.subheader("📈 KPIs")

    frames = st.session_state.frames
    k_set = compute_kpis_scope(frames, st.session_state.match_id, st.session_state.set_number, scope="set")
    k_match = compute_kpis_scope(frames, st.session_state.match_id, st.session_state.set_number, scope="match")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**Set atual (Set {st.session_state.set_number})**")
        display_dataframe(k_set, height=110, use_container_width=True)
    with c2:
        st.markdown("**Partida (até agora)**")
        display_dataframe(k_match, height=110, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# =========================
# KPI por Jogadora — Erros gerais (vermelho) e Ataques ponto por tipo (verde)
# =========================
with st.container():
    st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
    st.subheader("📌 KPI por Jogadora (Set atual)")

    df_cur = current_set_df(st.session_state.frames, st.session_state.match_id, st.session_state.set_number)

    cA, cB = st.columns(2)

    # ---- Erros gerais (nossos) ----
    with cA:
        st.markdown("**Erros gerais (nossos)**")
        if not df_cur.empty:
            er = df_cur[(df_cur["result"]=="ERRO") & (df_cur["who_scored"]=="ADV")].copy()
            if not er.empty:
                er["player_number"] = er["player_number"].fillna("—")
                # Total por jogadora
                tot = er.groupby("player_number").size().reset_index(name="Erros")
                # Quebra por tipo (action)
                piv_err = er.groupby(["player_number","action"]).size().unstack(fill_value=0)

                # Ações relevantes (inclui ataques e fundamentos onde erro ocorre)
                desired_cols = [
                    "DIAGONAL","LINHA","MEIO","PIPE","SEGUNDA","LOB",
                    "SAQUE","BLOQUEIO","RECEPÇÃO","RECEPCAO"
                ]
                for c in desired_cols:
                    if c not in piv_err.columns:
                        piv_err[c] = 0

                # combinar RECEPÇÃO/RECEPCAO numa coluna única "Recepção"
                piv_err["RECEPÇÃO"] = piv_err.get("RECEPÇÃO", 0) + piv_err.get("RECEPCAO", 0)
                order_cols = ["DIAGONAL","LINHA","MEIO","PIPE","SEGUNDA","LOB","SAQUE","BLOQUEIO","RECEPÇÃO"]
                piv_err = piv_err[order_cols].rename(columns={
                    "DIAGONAL":"Diagonal",
                    "LINHA":"Paralela",
                    "MEIO":"Meio",
                    "PIPE":"Pipe",
                    "SEGUNDA":"Segunda",
                    "LOB":"Largada",
                    "SAQUE":"Saque",
                    "BLOQUEIO":"Bloqueio",
                    "RECEPÇÃO":"Recepção",
                }).reset_index()

                # Mesclar total + tipos
                tbl_err = pd.merge(tot, piv_err, on="player_number", how="left")
                tbl_err = tbl_err.rename(columns={"player_number":"Jog."}).sort_values("Erros", ascending=False)

                display_dataframe(tbl_err, height=200, use_container_width=True, extra_class="header-red")
            else:
                st.write("_Sem erros._")
        else:
            st.write("_Sem dados._")

    # ---- Ataques ponto por tipo (nossos) ----
    with cB:
        st.markdown("**Ataques ponto por tipo (nossos)**")
        if not df_cur.empty:
            atp = df_cur[(df_cur["result"]=="PONTO") & (df_cur["who_scored"]=="NOS")].copy()
            if not atp.empty:
                atp["player_number"] = atp["player_number"].fillna("—")
                piv = atp.groupby(["player_number","action"]).size().unstack(fill_value=0)
                col_map = {
                    "DIAGONAL":"Diagonal",
                    "LINHA":"Paralela",
                    "MEIO":"Meio",
                    "PIPE":"Pipe",
                    "SEGUNDA":"Segunda",
                    "LOB":"Largada",
                    "SAQUE":"Saque",
                }
                for k in col_map.keys():
                    if k not in piv.columns: piv[k] = 0
                piv = piv[list(col_map.keys())].rename(columns=col_map).reset_index()
                piv = piv.rename(columns={"player_number":"Jog."})
                display_dataframe(piv, height=200, use_container_width=True, extra_class="header-green")
            else:
                st.write("_Sem pontos de ataque._")
        else:
            st.write("_Sem dados._")

    st.markdown('</div>', unsafe_allow_html=True)

# =========================
# NOVA SEÇÃO — Análises Visuais (técnico)
# =========================
def _attack_rows_us(df):
    if df.empty: return df
    mask = df["action"].isin(ATTACK_ACTIONS)
    pts  = df[mask & (df["who_scored"]=="NOS")].copy()
    errs = df[mask & (df["who_scored"]=="ADV") & (df["result"]=="ERRO")].copy()
    return pd.concat([pts, errs], ignore_index=True) if (not pts.empty or not errs.empty) else pd.DataFrame(columns=df.columns)

def _player_efficiency_attacks(df):
    if df.empty: 
        return pd.DataFrame(columns=["player_number","tentativas","pontos","erros","eficiencia"])
    df = df.copy()
    df["is_ponto"] = ((df["who_scored"]=="NOS") & (df["result"]=="PONTO")).astype(int)
    df["is_erro"]  = ((df["who_scored"]=="ADV") & (df["result"]=="ERRO")).astype(int)
    g = df.groupby("player_number").agg(
        tentativas=("result","count"),
        pontos=("is_ponto","sum"),
        erros=("is_erro","sum")
    ).reset_index()
    g["eficiencia"] = (g["pontos"] - g["erros"]) / g["tentativas"].replace(0, 1)
    return g

def _team_eff_by_set(frames, match_id):
    sets_df = frames["sets"]
    if sets_df.empty: return pd.DataFrame(columns=["set_number","ef_ataque","erro_rate"])
    set_list = sets_df[sets_df["match_id"]==match_id]["set_number"].sort_values().unique().tolist()
    rows = []
    rl = frames["rallies"]
    for s in set_list:
        d = current_set_df(frames, match_id, int(s))
        if d.empty:
            rows.append({"set_number":s, "ef_ataque":0.0, "erro_rate":0.0}); continue
        att = _attack_rows_us(d)
        eff = _player_efficiency_attacks(att)
        ef_team = float((eff["pontos"].sum() - eff["erros"].sum()) / eff["tentativas"].sum()) if not eff.empty else 0.0
        erros_gerais = len(d[(d["result"]=="ERRO") & (d["who_scored"]=="ADV")])
        total_rallies = len(d)
        erro_rate = float(erros_gerais / total_rallies) if total_rallies else 0.0
        rows.append({"set_number":s, "ef_ataque":ef_team, "erro_rate":erro_rate})
    return pd.DataFrame(rows)

with st.container():
    st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
    st.subheader("🎛️ Análises Visuais de Desempenho")

    frames = st.session_state.frames
    match_id = st.session_state.match_id
    set_atual = st.session_state.set_number

    # Seletor do set em foco (default: atual)
    all_sets = frames["sets"][frames["sets"]["match_id"]==match_id]["set_number"].sort_values().unique().tolist()
    col_sel, _ = st.columns([1.2, 2])
    with col_sel:
        set_foco = st.selectbox("Set em foco", options=all_sets if all_sets else [set_atual],
                                index=(all_sets.index(set_atual) if set_atual in all_sets else 0))

    df_foco = current_set_df(frames, match_id, int(set_foco))

    # ---- Heatmap de erros por tipo x jogadora (set foco) ----
    st.markdown("**Heatmap de Erros por Tipo × Jogadora (set em foco)**")
    if not df_foco.empty:
        er = df_foco[(df_foco["result"]=="ERRO") & (df_foco["who_scored"]=="ADV")].copy()
        if not er.empty:
            er["player_number"] = er["player_number"].fillna("—")
            piv = er.groupby(["player_number","action"]).size().unstack(fill_value=0)
            tipos = ["DIAGONAL","LINHA","MEIO","PIPE","SEGUNDA","LOB","SAQUE","BLOQUEIO","RECEPÇÃO","RECEPCAO"]
            for t in tipos:
                if t not in piv.columns: piv[t] = 0
            # Junta recepção
            piv["RECEPÇÃO"] = piv.get("RECEPÇÃO", 0) + piv.get("RECEPCAO", 0)
            ordem = ["DIAGONAL","LINHA","MEIO","PIPE","SEGUNDA","LOB","SAQUE","BLOQUEIO","RECEPÇÃO"]
            piv = piv[ordem]
            mat = piv.values
            labels_x = [ {"DIAGONAL":"Diag","LINHA":"Par","MEIO":"Meio","PIPE":"Pipe","SEGUNDA":"Seg","LOB":"Larg","SAQUE":"Saq","BLOQUEIO":"Bloq","RECEPÇÃO":"Rec"}[c] for c in ordem ]
            labels_y = [str(x) for x in piv.index]

            fig_hm, ax_hm = plt.subplots(figsize=(4.8, 2.6), dpi=110)
            im = ax_hm.imshow(mat, aspect="auto")
            ax_hm.set_xticks(np.arange(len(labels_x)))
            ax_hm.set_yticks(np.arange(len(labels_y)))
            ax_hm.set_xticklabels(labels_x, fontsize=7, rotation=0)
            ax_hm.set_yticklabels(labels_y, fontsize=7)
            for i in range(mat.shape[0]):
                for j in range(mat.shape[1]):
                    val = int(mat[i, j])
                    if val:
                        ax_hm.text(j, i, str(val), ha="center", va="center", fontsize=7)
            ax_hm.set_xlabel("Tipo de erro", fontsize=7)
            ax_hm.set_ylabel("Jog.", fontsize=7)
            fig_hm.tight_layout(pad=0.2)
            st.pyplot(fig_hm)
        else:
            st.caption("_Sem erros no set em foco._")
    else:
        st.caption("_Sem dados no set em foco._")

    # ---- Delta de eficiência por jogadora (set foco vs média anteriores) ----
    st.markdown("**Delta de Eficiência de Ataque por Jogadora** (set em foco × média dos sets anteriores)")
    if not df_foco.empty and len(all_sets) > 1:
        anteriores = [s for s in all_sets if s < set_foco]
        df_prev = pd.concat([current_set_df(frames, match_id, int(s)) for s in anteriores], ignore_index=True) if anteriores else pd.DataFrame()
        att_cur = _attack_rows_us(df_foco)
        eff_cur = _player_efficiency_attacks(att_cur)
        att_prev = _attack_rows_us(df_prev) if not df_prev.empty else pd.DataFrame(columns=df_foco.columns)
        eff_prev = _player_efficiency_attacks(att_prev)
        eff_prev = eff_prev.rename(columns={"eficiencia":"ef_prev"})[["player_number","ef_prev"]]
        comp = pd.merge(eff_cur[["player_number","eficiencia"]], eff_prev, on="player_number", how="left")
        comp["ef_prev"] = comp["ef_prev"].fillna(0.0)
        comp["delta"] = comp["eficiencia"] - comp["ef_prev"]
        comp["player_number"] = comp["player_number"].fillna("—").astype(str)
        comp = comp.sort_values("delta", ascending=True)

        figd, axd = small_fig(w=3.4, h=1.6)
        axd.bar(comp["player_number"], comp["delta"])
        axd.axhline(0, linewidth=0.8)
        for i, v in enumerate(comp["delta"].values):
            axd.annotate(f"{v:+.2f}", (i, v), textcoords="offset points", xytext=(0, -7 if v<0 else 3), ha="center", fontsize=6)
        figd = trim_ax(axd, xlabel="Jog.", ylabel="Δ Ef.", legend=False, max_xticks=12, max_yticks=5)
        st.pyplot(figd)
        st.caption("Acima de 0: evoluiu no set em foco vs. sua média anterior. Abaixo de 0: piorou.")
    else:
        st.caption("_Sem sets anteriores para comparação._")

    # ---- Tendência por set da equipe (eficiência de ataque e taxa de erros) ----
    st.markdown("**Tendência da Equipe por Set** (eficiência de ataque e taxa de erros)")
    trend = _team_eff_by_set(frames, match_id)
    if not trend.empty:
        figt, axt = plt.subplots(figsize=(4.2, 2.2), dpi=110)
        axt.plot(trend["set_number"], trend["ef_ataque"], marker="o", linewidth=0.9, label="Ef. ataque (time)")
        axt.plot(trend["set_number"], trend["erro_rate"], marker="o", linewidth=0.9, label="Taxa de erros (time)")
        axt.set_xlabel("Set", fontsize=7); axt.set_ylabel("Valor", fontsize=7)
        axt.grid(True, alpha=0.15)
        axt.legend(loc="best", fontsize=7, frameon=False, handlelength=1.0)
        figt.tight_layout(pad=0.2)
        st.pyplot(figt)
        st.caption("Objetivo: subir a eficiência de ataque e reduzir a taxa de erros ao longo dos sets.")
    else:
        st.caption("_Sem dados para tendência._")

    st.markdown('</div>', unsafe_allow_html=True)

# =========================
# Histórico de Sets (mantido)
# =========================
with st.container():
    st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
    st.subheader("📊 Histórico de Sets")
    stf_all = st.session_state.frames["sets"]
    sm_consulta = stf_all[stf_all["match_id"] == st.session_state.match_id].copy()
    if not sm_consulta.empty:
        home_name_cons = home_name; away_name_cons = away_name
        sets_list = sm_consulta.sort_values("set_number")[["set_number","home_points","away_points","winner_team_id"]].copy()
        sets_list["Vencedor"] = sets_list["winner_team_id"].apply(lambda x: home_name_cons if x == 1 else away_name_cons if x == 2 else "")
        view_tbl = sets_list.rename(columns={
            "set_number":"Set", "home_points":f"Pts {home_name_cons}", "away_points":f"Pts {away_name_cons}"
        })[["Set", f"Pts {home_name_cons}", f"Pts {away_name_cons}", "Vencedor"]]
        display_dataframe(view_tbl, height=120, use_container_width=True)
    else:
        st.info("Sem sets registrados")
    st.markdown('</div>', unsafe_allow_html=True)

# =========================
# Configuração para deploy no Render
# =========================
if __name__ == "__main__":
    import os
    import streamlit as st
    
    # Configurações específicas para deploy
    port = int(os.environ.get("PORT", 10000))
    
    # Executa o app Streamlit com as configurações do Render
    # Evita loop infinito de reruns
    if not st.session_state.get("_boot_rerun_done", False):
        st.session_state["_boot_rerun_done"] = True
        st.rerun()  # Ou qualquer lógica de inicialização do seu app
