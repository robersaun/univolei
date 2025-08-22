# index.py — UniVolei Live Scout (compacto, estável e com KPIs por lado)
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

st.markdown('<div class="header-title">🏐 Vôlei Scout – UniVolei</div>', unsafe_allow_html=True)

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
    for side in ("top", "right"): 
        ax.spines[side].set_visible(False)
    ax.margins(x=0.02)
    ax.tick_params(length=2.5, width=0.6, pad=1.5)
    return fig, ax

def trim_ax(ax, xlabel="", ylabel="", legend=False, max_xticks=6, max_yticks=5):
    from matplotlib.ticker import MaxNLocator
    if xlabel: ax.set_xlabel(xlabel, fontsize=7, labelpad=1.5)
    if ylabel: ax.set_ylabel(ylabel, fontsize=7, labelpad=1.5)
    ax.xaxis.set_major_locator(MaxNLocator(nbins=max_xticks, integer=True))
    ax.yaxis.set_major_locator(MaxNLocator(nbins=max_yticks, integer=True))
    if not legend and ax.get_legend(): 
        ax.get_legend().remove()
    ax.get_figure().tight_layout(pad=0.15)
    return ax.get_figure()

# =========================
# DataFrame HTML (sem PyArrow)
# =========================
def display_dataframe(df, height=None, use_container_width=False):
    if df.empty:
        st.write("_Sem dados._"); return
    html_table = df.to_html(classes='custom-table', index=False, escape=False)
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

# chaves auxiliares
st.session_state.setdefault("q_side", "Nós")
st.session_state.setdefault("q_result", "Acerto")
st.session_state.setdefault("q_action", "d")
st.session_state.setdefault("last_selected_player", None)
st.session_state.setdefault("show_cadastro", False)
st.session_state.setdefault("show_tutorial", False)
st.session_state.setdefault("show_config_team", False)
st.session_state.setdefault("line_input_text", "")  # chave única p/ input

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
    st.success("Último rally desfeito e placar recalculado.")

def quick_register_line(raw_line: str):
    if not raw_line.strip(): return
    row = parse_line(raw_line)
    append_rally(st.session_state.frames, match_id=st.session_state.match_id, set_number=st.session_state.set_number, row=row)
    save_all(Path(st.session_state.db_path), st.session_state.frames)
    auto_close_set_if_needed()

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
        raw = st.session_state.get("line_input_text", "")
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
top1, top2, top3, top4 = st.columns([2.5, 1, 1, 1])
with top1:
    if home_name and away_name:
        st.markdown(f'<div class="badge"><b>{home_name}</b> x <b>{away_name}</b> — {date_str}</div>', unsafe_allow_html=True)
with top2:
    st.button("⚙️ Time", use_container_width=True, key="config_team_btn", on_click=lambda: st.session_state.__setitem__("show_config_team", True))
with top3:
    st.button("🆕 Jogo", use_container_width=True, key="new_game_btn", on_click=lambda: st.session_state.__setitem__("show_cadastro", True))
with top4:
    st.button("📘 Tutorial", use_container_width=True, key="tutorial_btn", on_click=lambda: st.session_state.__setitem__("show_tutorial", True))

# =========================
# Modais resumidos (Config/Tutorial) — mantidos
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

if st.session_state.get("show_tutorial", False):
    try:
        html_path = BASE_DIR / "tutorial_scout.html"
        if html_path.exists():
            html_content = html_path.read_text(encoding="utf-8")
            components.html(
                f"""
                <div style='position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); 
                width: 90vw; height: 85vh; background-color: white; z-index: 1000; 
                border: 2px solid #ccc; border-radius: 10px; overflow: hidden;'>
                    <div style='position: absolute; top: 10px; right: 10px; z-index: 1001;'>
                        <button onclick='window.parent.document.querySelector("iframe").contentWindow.document.body.innerHTML=""; window.parent.document.querySelector(".stApp").dispatchEvent(new CustomEvent("CLOSE_TUTORIAL"))' 
                        style='background: #ff4b4b; color: white; border: none; border-radius: 50%; width: 30px; height: 30px; cursor: pointer; font-weight: bold;'>X</button>
                    </div>
                    <iframe srcdoc='{html.escape(html_content)}' style='width: 100%; height: 100%; border: none; margin-top: 40px;'></iframe>
                </div>
                """,
                height=900, scrolling=True
            )
            components.html("""
            <script>
            document.addEventListener('CLOSE_TUTORIAL', function() {
                const event = new CustomEvent('TUTORIAL_CLOSED');
                window.parent.document.dispatchEvent(event);
            });
            window.parent.document.addEventListener('TUTORIAL_CLOSED', function() {
                window.location.reload();
            });
            </script>
            """, height=0)
        else:
            st.error("Arquivo de tutorial não encontrado.")
    except Exception as e:
        st.error(f"Não consegui abrir o tutorial: {e}")
    st.button("❌ Fechar Tutorial", key="close_tutorial_btn", on_click=lambda: st.session_state.__setitem__("show_tutorial", False))

# =========================
# Cadastro rápido (sem jogo) — mantido
# =========================
if (st.session_state.match_id is None or st.session_state.show_cadastro) and not st.session_state.show_config_team:
    with st.container():
        st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
        st.subheader("🎯 Registrar Rally (Pré-jogo)")

        def on_submit_text_pre():
            raw = st.session_state.get("line_input_text", "").strip()
            if not raw: return
            quick_register_line(raw)
            st.session_state["line_input_text"] = ""
            st.session_state["q_side"] = "Nós"; st.session_state["q_result"] = "Acerto"; st.session_state["q_action"] = "d"

        _ = st.text_input("Digite código:", key="line_input_text", placeholder="Ex: 1 9 d",
                          label_visibility="collapsed", on_change=on_submit_text_pre)

        def _cb_register_pre():
            register_current(); st.session_state["line_input_text"] = ""
        st.button("➕ Registrar", use_container_width=True, key="register_btn_pre", on_click=_cb_register_pre)

        st.button("↩️ Desfazer Rally", use_container_width=True, key="undo_btn_pre", on_click=undo_last_rally_current_set)
        st.markdown('</div>', unsafe_allow_html=True)

# Se ainda não há partida, encerra
if st.session_state.match_id is None or st.session_state.show_config_team:
    st.stop()

# =========================
# Barra do sistema
# =========================
with st.container():
    st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
    if home_name and away_name: st.markdown(f"**Jogo:** {home_name} x {away_name} — {date_str}")
    bar1, bar3, bar4, bar5 = st.columns([1, 2.4, 1.05, 1.4])
    with bar1:
        st.session_state.auto_close = st.toggle("Auto 25/15+2", value=st.session_state.auto_close, key="auto_close_toggle")
    with bar3:
        sets_match_all = frames["sets"][frames["sets"]["match_id"]==st.session_state.match_id].sort_values("set_number")
        sel_vals = sets_match_all["set_number"].tolist() if not sets_match_all.empty else [1]
        c31, c32, c33 = st.columns([1, 1, 1])
        with c31:
            set_to_reopen = st.selectbox("Set", sel_vals, label_visibility="collapsed", key="set_select")
        st.button("🔓 Reabrir Set", use_container_width=True, key="reopen_btn", on_click=lambda: dict() if reopen_set(st.session_state.match_id, int(set_to_reopen)) else None)
        def _close_set():
            df_cur = current_set_df(frames, st.session_state.match_id, int(set_to_reopen))
            if df_cur.empty: st.warning("Sem rallies neste set.")
            else:
                hp, ap = set_score_from_df(df_cur)
                if hp == ap: st.warning("Empate — defina o set antes.")
                else: _apply_set_winner_and_proceed(hp, ap)
        st.button("✅ Fechar Set", use_container_width=True, key="close_set_btn", on_click=_close_set)
    with bar4:
        def _remove_empty_set():
            stf = frames["sets"]; rl = frames["rallies"]; mid = st.session_state.match_id
            sets_m = stf[stf["match_id"]==mid]
            if sets_m.empty: st.warning("Sem sets cadastrados.")
            else:
                max_set = int(sets_m["set_number"].max())
                sub = rl[(rl["match_id"]==mid) & (rl["set_number"]==max_set)]
                if not sub.empty: st.warning(f"O Set {max_set} tem rallies e não será removido.")
                else:
                    stf = stf[~((stf["match_id"]==mid) & (stf["set_number"]==max_set))]
                    frames["sets"] = stf; save_all(Path(st.session_state.db_path), frames); st.success(f"Set {max_set} removido.")
        st.button("🗑️ Remover Set Vazio", use_container_width=True, key="remove_empty_set_btn", on_click=_remove_empty_set)
    with bar5:
        st.session_state.graph_filter = st.radio("Filtro Gráficos", options=["Nós","Adversário","Ambos"],
            horizontal=True, index=["Nós","Adversário","Ambos"].index(st.session_state.graph_filter), key="graph_filter_radio")
    st.markdown('</div>', unsafe_allow_html=True)

# =========================
# Painel principal: Esquerda (inputs) | Direita (gráficos)
# =========================
with st.container():
    st.markdown('<div class="sectionCard">', unsafe_allow_html=True)

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
        st.markdown(f"<div style='text-align:left; font-size: 13px; margin-top:6px;'>Sets: <b>{home_sets_w}</b> × <b>{away_sets_w}</b> &nbsp;|&nbsp; Set atual: <b>{st.session_state.set_number}</b></div>", unsafe_allow_html=True)

    left, right = st.columns([1.25, 1.0])

    # -------- ESQUERDA: INPUTS --------
    with left:
        st.markdown("**🎯 Registrar Rally**")

        # input texto
        def on_submit_text_main():
            raw = st.session_state.get("line_input_text", "").strip()
            if not raw: return
            quick_register_line(raw)
            st.session_state["line_input_text"] = ""
            st.session_state["q_side"] = "Nós"; st.session_state["q_result"] = "Acerto"; st.session_state["q_action"] = "d"

        st.text_input("Digite código:", key="line_input_text",
                      placeholder="Ex: 1 9 d", label_visibility="collapsed", on_change=on_submit_text_main)

        # botões abaixo do input (callbacks sem rerun manual)
        def _cb_register_main():
            register_current(); st.session_state["line_input_text"] = ""
        st.button("➕ Registrar", use_container_width=True, key="register_btn_main", on_click=_cb_register_main)

        st.button("↩️ Desfazer Rally", use_container_width=True, key="undo_btn_main", on_click=undo_last_rally_current_set)

        # jogadoras – clicar APENAS seleciona
        st.caption("Jogadoras (selecione):")
        nums = resolve_our_roster_numbers(frames)
        if nums:
            st.markdown('<div class="jogadoras-container">', unsafe_allow_html=True)
            jcols = st.columns(min(12, max(1, len(nums))))
            for i, n in enumerate(nums):
                with jcols[i % len(jcols)]:
                    # Ajuste p/ botão do número: sem largura cheia (encaixa só o número)
                    st.button(f"{n}", key=f"pill_main_{n}", on_click=lambda n=n: st.session_state.__setitem__("last_selected_player", n), use_container_width=False)
            st.markdown('</div>', unsafe_allow_html=True)
            sel = st.session_state.get("last_selected_player")
            if sel is not None:
                st.caption(f"Selecionada: **#{sel}**")
        else:
            st.caption("Sem jogadoras")

        # Equipe + Resultado + Ação (Ação 1/3) — mudar ação REGISTRA (se houver jogadora)
        row2 = st.columns([1.0, 1.0, 0.6])
        with row2[0]:
            st.caption("Equipe:")
            st.session_state.q_side = st.radio("", options=["Nós","Adv"], horizontal=True, index=0,
                                               key="q_side_radio_main", label_visibility="collapsed")
        with row2[1]:
            st.caption("Resultado:")
            st.session_state.q_result = st.radio("", options=["Acerto","Erro"], horizontal=True, index=0,
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
            st.selectbox("", action_options, index=action_options.index(current_action),
                         label_visibility="collapsed", key="q_action_select_main", on_change=on_action_change)

        # Atalhos mini — clicou => REGISTRA (usa jogadora selecionada)
        st.caption("Atalhos:")
        st.markdown('<div class="atalhos-container small-btn">', unsafe_allow_html=True)
        acols = st.columns(12)
        shortcuts = [("d","D"),("l","P"),("m","M"),("lob","Lg"),("seg","Sg"),("pi","Pi"),("re","R"),("b","B"),("sa","Sa")]
        for i, (code, label) in enumerate(shortcuts):
            with acols[i % len(acols)]:
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
                    display_dataframe(tbl, height=120, use_container_width=True)
                else: st.write("_Sem dados_")
            else: st.write("_Sem dados_")
        with tt2:
            st.markdown("**Erros (Nossos)**")
            if not df_cur.empty:
                er = df_cur[(df_cur["result"]=="ERRO") & (df_cur["who_scored"]=="ADV")].copy()
                if not er.empty:
                    er["player_number"] = er["player_number"].fillna("—").astype(str)
                    tbl = er.groupby(["player_number"]).size().reset_index(name="erros").sort_values("erros", ascending=False)
                    display_dataframe(tbl, height=120, use_container_width=True)
                else: st.write("_Sem erros_")
            else: st.write("_Sem dados_")
        with tt3:
            st.markdown("**Histórico**")
            if not df_cur.empty:
                seq = ["N" if w == "NOS" else "A" for w in df_cur["who_scored"]]
                histo = pd.DataFrame({"Rally": range(1, len(seq)+1), "Ponto": seq})
                display_dataframe(histo, height=120, use_container_width=True)
            else:
                st.info("Sem rallies")

    # -------- DIREITA: GRÁFICOS EMPILHADOS (ULTRA-COMPACTOS) --------
    with right:
        def filter_df_for_graphs(df: pd.DataFrame, who: str) -> pd.DataFrame:
            if df.empty: return df
            if who == "Nós": return df[df["who_scored"] == "NOS"]
            if who == "Adversário": return df[df["who_scored"] == "ADV"]
            return df
        df_viz = filter_df_for_graphs(df_set, st.session_state.graph_filter)

        # Placar — destacando último ponto/valor (AGORA VEM PRIMEIRO)
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
            st.markdown("<div style='height: 80px;'></div>", unsafe_allow_html=True)

        # ======= NOVO: Eficiência por Jogadora (ataque) considerando lado correto =======
        st.markdown("**Eficiência por Jogadora (ataque)**")

        def build_attack_rows_for_side(df_base: pd.DataFrame, side_sel: str) -> pd.DataFrame:
            if df_base.empty: 
                return df_base
            mask_action = df_base["action"].isin(ATTACK_ACTIONS)
            if side_sel == "Nós":
                pts = df_base[mask_action & (df_base["who_scored"]=="NOS")]
                errs = df_base[mask_action & (df_base["who_scored"]=="ADV") & (df_base["result"]=="ERRO")]
                return pd.concat([pts, errs], ignore_index=True) if not pts.empty or not errs.empty else pd.DataFrame(columns=df_base.columns)
            elif side_sel == "Adversário":
                pts = df_base[mask_action & (df_base["who_scored"]=="ADV")]
                errs = df_base[mask_action & (df_base["who_scored"]=="NOS") & (df_base["result"]=="ERRO")]
                return pd.concat([pts, errs], ignore_index=True) if not pts.empty or not errs.empty else pd.DataFrame(columns=df_base.columns)
            else:  # Ambos
                return df_base[mask_action]

        att = build_attack_rows_for_side(df_set, st.session_state.graph_filter)
        if not att.empty:
            att = att.copy()
            # PONTO = quem pontuou foi o lado da seleção; ERRO = o outro lado pontuou por erro
            if st.session_state.graph_filter == "Nós":
                att["is_ponto"] = ((att["who_scored"]=="NOS") & (att["result"]=="PONTO")).astype(int)
                att["is_erro"] = ((att["who_scored"]=="ADV") & (att["result"]=="ERRO")).astype(int)
            elif st.session_state.graph_filter == "Adversário":
                att["is_ponto"] = ((att["who_scored"]=="ADV") & (att["result"]=="PONTO")).astype(int)
                att["is_erro"] = ((att["who_scored"]=="NOS") & (att["result"]=="ERRO")).astype(int)
            else:  # Ambos
                # conta global: ponto quando result == PONTO; erro quando result == ERRO
                att["is_ponto"] = (att["result"]=="PONTO").astype(int)
                att["is_erro"] = (att["result"]=="ERRO").astype(int)

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
            st.markdown("<div style='height: 80px;'></div>", unsafe_allow_html=True)

        # ======= NOVO: Erros (por jogadora) — respeitando filtro de lado =======
        st.markdown("**Erros (por jogadora)**")

        def errors_df_side(df_base: pd.DataFrame, side_sel: str) -> pd.DataFrame:
            if df_base.empty: return df_base
            if side_sel == "Nós":
                # nossos erros = ADV pontuou por ERRO nosso
                return df_base[(df_base["result"]=="ERRO") & (df_base["who_scored"]=="ADV")].copy()
            elif side_sel == "Adversário":
                # erros do adv = NOS pontuou por ERRO deles
                return df_base[(df_base["result"]=="ERRO") & (df_base["who_scored"]=="NOS")].copy()
            else:
                return df_base[df_base["result"]=="ERRO"].copy()

        err = errors_df_side(df_set, st.session_state.graph_filter)
        if not err.empty:
            err["player_number"] = err["player_number"].fillna("—").astype(str)
            tbl = err.groupby(["player_number"]).size().reset_index(name="erros").sort_values("erros", ascending=False)
            fig2, ax2 = small_fig()
            ax2.bar(tbl["player_number"], tbl["erros"])
            fig2 = trim_ax(ax2, xlabel="Jog.", ylabel="Erros", legend=False, max_xticks=10, max_yticks=5)
            st.pyplot(fig2)
        else:
            # fallback por ação (mesmo critério)
            err2 = errors_df_side(df_set, st.session_state.graph_filter)
            if not err2.empty:
                tbla = err2.groupby(["action"]).size().reset_index(name="erros").sort_values("erros", ascending=False)
                fig2a, ax2a = small_fig()
                ax2a.bar(tbla["action"].astype(str), tbla["erros"])
                fig2a = trim_ax(ax2a, xlabel="Ação", ylabel="Erros", legend=False, max_xticks=10, max_yticks=5)
                st.pyplot(fig2a)
            else:
                st.write("_Sem erros._")

    st.markdown('</div>', unsafe_allow_html=True)

# =========================
# KPIs — Set atual e Partida, para NÓS e ADV
# =========================
def compute_kpis_scope(frames, match_id, set_number, scope="set") -> pd.DataFrame:
    rl = frames["rallies"]
    if scope == "set":
        df = current_set_df(frames, match_id, set_number)
    else:
        df = rl[rl["match_id"] == match_id].copy().sort_values(["set_number","rally_no"])
    if df.empty:
        return pd.DataFrame(columns=["Lado","Aces","Bloqueios ponto","Erros","Clutch saldo"])

    # Aces = pontos com ação SAQUE
    aces_nos = len(df[(df["who_scored"]=="NOS") & (df["action"]=="SAQUE")])
    aces_adv = len(df[(df["who_scored"]=="ADV") & (df["action"]=="SAQUE")])

    # Bloqueios ponto
    blq_nos = len(df[(df["who_scored"]=="NOS") & (df["action"]=="BLOQUEIO")])
    blq_adv = len(df[(df["who_scored"]=="ADV") & (df["action"]=="BLOQUEIO")])

    # Erros por lado:
    err_nos = len(df[(df["result"]=="ERRO") & (df["who_scored"]=="ADV")])
    err_adv = len(df[(df["result"]=="ERRO") & (df["who_scored"]=="NOS")])

    # Clutch saldo por lado (faixa 20–25 ou diferença ≤2)
    diff = (df["score_home"] - df["score_away"]).abs()
    in_clutch = (df["score_home"].between(20,25)) | (df["score_away"].between(20,25)) | (diff <= 2)
    clutch = df[in_clutch]
    clutch_nos = int((clutch["who_scored"]=="NOS").sum() - (clutch["who_scored"]=="ADV").sum()) if not clutch.empty else 0
    clutch_adv = -clutch_nos

    out = pd.DataFrame([
        {"Lado":"Nós","Aces":aces_nos,"Bloqueios ponto":blq_nos,"Erros":err_nos,"Clutch saldo":clutch_nos},
        {"Lado":"Adversário","Aces":aces_adv,"Bloqueios ponto":blq_adv,"Erros":err_adv,"Clutch saldo":clutch_adv},
    ])
    return out

with st.container():
    st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
    st.subheader("📈 KPIs")

    k_set = compute_kpis_scope(frames, st.session_state.match_id, st.session_state.set_number, scope="set")
    k_match = compute_kpis_scope(frames, st.session_state.match_id, st.session_state.set_number, scope="match")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Set atual**")
        display_dataframe(k_set, height=110, use_container_width=True)
    with c2:
        st.markdown("**Partida (até agora)**")
        display_dataframe(k_match, height=110, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# =========================
# Histórico de Sets (mantido)
# =========================
with st.container():
    st.markdown('<div class="sectionCard">', unsafe_allow_html=True)
    st.subheader("📊 Histórico de Sets")
    stf_all = frames["sets"]
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
