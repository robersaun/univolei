# pages/02_historico.py — Histórico de jogos e detalhes (sem PyArrow)
from __future__ import annotations

from pathlib import Path
import pandas as pd
import streamlit as st

from db_excel import init_or_load  # save_all não é necessário aqui

# =========================
# Utilitários locais (iguais/compatíveis com index.py)
# =========================
OUR_TEAM_ID = 1

def _base_dir() -> Path:
    # /pages/02_historico.py -> raiz do app
    return Path(__file__).resolve().parents[1]

def _default_db_path() -> Path:
    return _base_dir() / "volei_base_dados.xlsx"

def load_frames():
    db_path = st.session_state.get("db_path", str(_default_db_path()))
    return init_or_load(Path(db_path))

def team_name_by_id(fr: dict, team_id: int | None) -> str:
    eq = fr.get("equipes", pd.DataFrame())
    if eq.empty or team_id is None: return "Equipe"
    eq = eq.copy(); eq["team_id"] = pd.to_numeric(eq["team_id"], errors="coerce")
    row = eq.loc[eq["team_id"] == int(team_id)]
    return str(row.iloc[0]["team_name"]) if not row.empty else f"Equipe {int(team_id)}"

# ---- DataFrame HTML (sem PyArrow)
def display_dataframe(df: pd.DataFrame, height: int | None = None, use_container_width: bool = False):
    if df is None or df.empty:
        st.write("_Sem dados._"); return
    html_table = df.to_html(classes="custom-table", index=False, escape=False)
    h = f"{height}px" if height else "auto"
    w = "100%" if use_container_width else "auto"
    st.markdown(f"<div style='overflow:auto; height:{h}; width:{w};'>{html_table}</div>", unsafe_allow_html=True)

def current_match_df(fr: dict, match_id: int) -> pd.DataFrame:
    rl = fr.get("rallies", pd.DataFrame())
    return rl[rl["match_id"] == match_id].copy().sort_values(["set_number","rally_no"])

def current_set_df(fr: dict, match_id: int, set_number: int) -> pd.DataFrame:
    rl = fr.get("rallies", pd.DataFrame())
    return rl[(rl["match_id"]==match_id) & (rl["set_number"]==set_number)].copy().sort_values("rally_no")

# =========================
# Página
# =========================
st.set_page_config(page_title="Histórico — UniVolei", layout="wide")
st.title("🗂️ Histórico de Jogos")

# ---- Botão FECHAR (volta ao index)
c_top_l, c_top_r = st.columns([6,1])
with c_top_r:
    def _back_index():
        try:
            st.switch_page("index.py")
        except Exception:
            try:
                st.switch_page("../index.py")
            except Exception:
                st.write("Abra o Index no menu lateral.")
    # Tenta link nativo primeiro
    try:
        st.page_link("index.py", label="❌ Fechar")
    except Exception:
        st.button("❌ Fechar", on_click=_back_index, use_container_width=True)

frames = load_frames()
mt = frames.get("amistosos", pd.DataFrame())
sets = frames.get("sets", pd.DataFrame())

if mt.empty:
    st.info("Nenhum jogo cadastrado ainda.")
    st.stop()

# Enriquecer nomes
home_name = team_name_by_id(frames, OUR_TEAM_ID)

# Tabela de jogos
mt = mt.copy()
mt["match_id"] = pd.to_numeric(mt["match_id"], errors="coerce").astype("Int64")
mt["date"] = mt["date"].astype(str)

def away_name_for(row):
    return team_name_by_id(frames, row["away_team_id"])

mt["Adversário"] = mt.apply(away_name_for, axis=1)
mt["Sets (Nós-Adv)"] = mt.apply(lambda r: f'{int(r.get("home_sets",0))} - {int(r.get("away_sets",0))}', axis=1)

# Lista de jogos
st.markdown("### Jogos")
games_list = mt.sort_values(["date","match_id"]).reset_index(drop=True)
display_dataframe(
    games_list[["match_id","date","Adversário","Sets (Nós-Adv)"]].rename(columns={"match_id":"ID","date":"Data"}),
    height=220, use_container_width=True
)

# Escolha de jogo
ids = games_list["match_id"].dropna().astype(int).tolist()
col_sel, _ = st.columns([3,1])
with col_sel:
    sel_id = st.selectbox("Selecione o jogo (ID):", ids, index=len(ids)-1 if ids else 0)

if sel_id is None:
    st.stop()

# Cabeçalho do jogo
row = games_list[games_list["match_id"] == sel_id].iloc[0]
away_name = row["Adversário"]
date_str = str(row["date"])
st.markdown(f"**Jogo:** {home_name} x {away_name} — {date_str}")

# Resumo de sets do jogo escolhido
st.markdown("### Sets")
if not sets.empty:
    sm = sets[sets["match_id"] == sel_id].copy().sort_values("set_number")
    if not sm.empty:
        view_tbl = sm.rename(columns={
            "set_number":"Set",
            "home_points":f"Pts {home_name}",
            "away_points":f"Pts {away_name}",
        })[["Set", f"Pts {home_name}", f"Pts {away_name}", "winner_team_id"]]
        view_tbl["Vencedor"] = view_tbl["winner_team_id"].map({1: home_name, 2: away_name}).fillna("")
        display_dataframe(view_tbl.drop(columns=["winner_team_id"]), height=180, use_container_width=True)
    else:
        st.write("_Sem sets para este jogo._")
else:
    st.write("_Sem sets._")

# Filtro de set e listagem de rallies
st.markdown("### Rallies")
all_df = current_match_df(frames, sel_id)
if all_df.empty:
    st.info("Sem rallies para este jogo.")
else:
    sets_disp = sorted(all_df["set_number"].dropna().unique().tolist())
    fcol1, _ = st.columns([1, 5])
    with fcol1:
        set_sel = st.selectbox("Set:", ["Todos"] + [int(s) for s in sets_disp])
    if set_sel == "Todos":
        df_show = all_df
    else:
        df_show = current_set_df(frames, sel_id, int(set_sel))

    cols_keep = ["set_number","rally_no","who_scored","result","action","player_number","score_home","score_away"]
    cols_existing = [c for c in cols_keep if c in df_show.columns]
    df_view = df_show[cols_existing].rename(columns={
        "set_number":"Set","rally_no":"Rally","who_scored":"Quem pontuou",
        "result":"Resultado","action":"Ação","player_number":"Jog.", "score_home":"Home","score_away":"Away"
    })
    display_dataframe(df_view, height=300, use_container_width=True)

# KPIs por jogadora (partida)
st.markdown("### KPI por Jogadora (Partida)")

ATTACK_ACTIONS = ["DIAGONAL","LINHA","PIPE","SEGUNDA","LOB","MEIO"]

left, right = st.columns(2)

with left:
    st.markdown("**Erros gerais (nossos)**")
    er = all_df[(all_df["result"]=="ERRO") & (all_df["who_scored"]=="ADV")].copy()
    if er.empty:
        st.write("_Sem erros._")
    else:
        er["player_number"] = er["player_number"].fillna("—")
        tot = er.groupby("player_number").size().reset_index(name="Erros")
        piv_err = er.groupby(["player_number","action"]).size().unstack(fill_value=0)
        desired_cols = [
            "DIAGONAL","LINHA","MEIO","PIPE","SEGUNDA","LOB",
            "SAQUE","BLOQUEIO","RECEPÇÃO","RECEPCAO"
        ]
        for c in desired_cols:
            if c not in piv_err.columns: piv_err[c] = 0
        piv_err["RECEPÇÃO"] = piv_err.get("RECEPÇÃO", 0) + piv_err.get("RECEPCAO", 0)
        order_cols = ["DIAGONAL","LINHA","MEIO","PIPE","SEGUNDA","LOB","SAQUE","BLOQUEIO","RECEPÇÃO"]
        piv_err = piv_err[order_cols].rename(columns={
            "DIAGONAL":"Diagonal","LINHA":"Paralela","MEIO":"Meio","PIPE":"Pipe",
            "SEGUNDA":"Segunda","LOB":"Largada","SAQUE":"Saque","BLOQUEIO":"Bloqueio","RECEPÇÃO":"Recepção",
        }).reset_index()
        tbl_err = pd.merge(tot, piv_err, on="player_number", how="left")
        tbl_err = tbl_err.rename(columns={"player_number":"Jog."}).sort_values("Erros", ascending=False)
        display_dataframe(tbl_err, height=260, use_container_width=True)

with right:
    st.markdown("**Ataques ponto por tipo (nossos)**")
    atp = all_df[(all_df["result"]=="PONTO") & (all_df["who_scored"]=="NOS")].copy()
    if atp.empty:
        st.write("_Sem pontos de ataque._")
    else:
        atp["player_number"] = atp["player_number"].fillna("—")
        piv = atp.groupby(["player_number","action"]).size().unstack(fill_value=0)
        col_map = {
            "DIAGONAL":"Diagonal","LINHA":"Paralela","MEIO":"Meio","PIPE":"Pipe",
            "SEGUNDA":"Segunda","LOB":"Largada","SAQUE":"Saque",
        }
        for k in col_map.keys():
            if k not in piv.columns: piv[k] = 0
        piv = piv[list(col_map.keys())].rename(columns=col_map).reset_index()
        piv = piv.rename(columns={"player_number":"Jog."})
        display_dataframe(piv, height=260, use_container_width=True)

# Voltar (mantido) — e continua disponível mesmo com o botão "Fechar" no topo
st.markdown("---")
try:
    st.page_link("index.py", label="⬅️ Voltar ao jogo")
except Exception:
    def _go_back():
        try:
            st.switch_page("index.py")
        except Exception:
            st.write("Abra o Index no menu lateral.")
    st.button("⬅️ Voltar ao jogo", on_click=_go_back)
