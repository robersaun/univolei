
# pages/02_historico.py — Histórico de jogos (UI melhorada)
from __future__ import annotations

from pathlib import Path
import math
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

# ---------- CSS tema (somente nesta página) ----------
st.markdown(
    """
    <style>
      .hist-card{background:linear-gradient(180deg,#ffffff 0%,#f8fafc 100%);
                 border:1px solid #e2e8f0; border-radius:14px; padding:14px 14px; box-shadow:0 1px 3px rgba(15,23,42,.06);}
      .hist-title{font-weight:800; letter-spacing:.3px;}
      .muted{color:#64748b; font-weight:600;}
      .hg-pill{display:inline-block; padding:3px 8px; border-radius:999px; font-size:.75rem; font-weight:700;}
      .hg-pill.ok{background:#10b98122; color:#047857; border:1px solid #10b98155;}
      .hg-pill.open{background:#f59e0b22; color:#92400e; border:1px solid #f59e0b55;}
      .hg-table{width:100%; border-collapse:separate; border-spacing:0; font-size:.92rem;}
      .hg-table thead th{position:sticky; top:0; background:#0f172a; color:#fff; text-align:left; padding:10px 12px; font-weight:800;}
      .hg-table tbody td{padding:8px 12px; border-bottom:1px solid #e2e8f0;}
      .hg-table tbody tr:nth-child(even){background:#f8fafc;}
      .hg-table tbody tr:hover{background:#e2e8f0; cursor:pointer;}
      .scroll-wrap{overflow:auto; border:1px solid #e2e8f0; border-radius:12px; background:#fff;}
      .controls-row{display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin:6px 0 10px 0;}
      .btn-ghost{background:#fff; border:1px solid #cbd5e1; border-radius:10px; padding:6px 10px; cursor:pointer; font-weight:700;}
      .btn-ghost:hover{background:#f8fafc;}
    </style>
    """, unsafe_allow_html=True
)

st.title("🗂️ Histórico de Jogos")

# ---- Botões topo (voltar)
top_l, top_r = st.columns([6,1])
with top_r:
    def _back_index():
        try:
            st.switch_page("index.py")
        except Exception:
            try:
                st.switch_page("../index.py")
            except Exception:
                st.write("Abra o Index no menu lateral.")
    try:
        st.page_link("index.py", label="❌ Fechar")
    except Exception:
        st.button("❌ Fechar", on_click=_back_index, use_container_width=True)

# =========================
# Dados
# =========================
frames = load_frames()
mt = frames.get("amistosos", pd.DataFrame())
sets = frames.get("sets", pd.DataFrame())

if mt.empty:
    st.info("Nenhum jogo cadastrado ainda.")
    st.stop()

home_name = team_name_by_id(frames, OUR_TEAM_ID)

# Normalizações
mt = mt.copy()
mt["match_id"] = pd.to_numeric(mt["match_id"], errors="coerce").astype("Int64")
mt["date"] = mt["date"].astype(str)

def away_name_for(row):
    return team_name_by_id(frames, row["away_team_id"])

mt["Adversário"] = mt.apply(away_name_for, axis=1)
mt["Sets (Nós-Adv)"] = mt.apply(lambda r: f'{int(r.get("home_sets",0))} - {int(r.get("away_sets",0))}', axis=1)

# =========================
# Lista de jogos — alta/expandível, clicável e com paginação
# =========================
st.markdown("### Jogos")

games_list = mt.sort_values(["date","match_id"], ascending=[False, True]).reset_index(drop=True)

# Estado de UI
st.session_state.setdefault("hist_full_open", False)
st.session_state.setdefault("hist_page_size", 50)
st.session_state.setdefault("hist_page", 1)

c_controls = st.container()
with c_controls:
    left, right = st.columns([4, 3])
    with left:
        toggled = st.toggle("🔎 Mostrar lista completa", value=st.session_state["hist_full_open"], help="Quando ativado, exibe paginação e a lista completa de partidas.")
        st.session_state["hist_full_open"] = bool(toggled)
    with right:
        if st.session_state["hist_full_open"]:
            ps = st.selectbox("Tamanho da página:", [25, 50, 100, 200], index=[25,50,100,200].index(st.session_state["hist_page_size"]), key="hist_page_size")
            total = len(games_list)
            pages = max(1, math.ceil(total / ps))
            st.session_state["hist_page"] = st.number_input("Página:", min_value=1, max_value=pages, value=min(st.session_state["hist_page"], pages), step=1)
        else:
            ps = 10  # fechado mostra poucos
            st.session_state["hist_page"] = 1

# Paginação / recorte
if st.session_state["hist_full_open"]:
    start = (st.session_state["hist_page"] - 1) * ps
    end = start + ps
    show_df = games_list.iloc[start:end].copy()
else:
    show_df = games_list.head(ps).copy()

# Status (aberto/fechado)
def _row_status(r):
    is_closed = bool(r.get("is_closed", False))
    if is_closed:
        return "<span class='hg-pill ok'>Fechado</span>"
    return "<span class='hg-pill open'>Aberto</span>"

show_df["Status"] = show_df.apply(_row_status, axis=1)

# Tabela HTML clicável
sel_qp = None
try:
    sel_qp = st.query_params.get("sel_id", None)
except Exception:
    sel_qp = None

table_key = "hg_tbl"

html_rows = []
for _, r in show_df.iterrows():
    mid = int(r["match_id"])
    date = str(r["date"])
    adv = str(r["Adversário"])
    sets_txt = str(r["Sets (Nós-Adv)"])
    status = r["Status"]
    html_rows.append(f"<tr data-id='{mid}'><td>{mid}</td><td>{date}</td><td><b>{adv}</b></td><td>{sets_txt}</td><td>{status}</td></tr>")

height = 540 if st.session_state["hist_full_open"] else 280
st.markdown(f"""
<div class='hist-card'>
  <div class='controls-row'>
    <span class='muted'>Clique em um jogo para abrir os detalhes.</span>
    {'<button class="btn-ghost" onclick="window.scrollTo(0,0)">Topo ↑</button>' if st.session_state["hist_full_open"] else ''}
  </div>
  <div class='scroll-wrap' style='height:{height}px'>
    <table class='hg-table' id='{table_key}'>
      <thead><tr><th>ID</th><th>Data</th><th>Adversário</th><th>Sets</th><th>Status</th></tr></thead>
      <tbody>
        {''.join(html_rows)}
      </tbody>
    </table>
  </div>
</div>
<script>
(function(){{
  const T=document.getElementById('{table_key}');
  if(!T) return;
  const go=(id)=>{{
    try{{
      const P = new URLSearchParams(window.parent.location.search||'');
      P.set('sel_id', String(id));
      const url = window.parent.location.pathname + '?' + P.toString() + window.parent.location.hash;
      window.parent.history.replaceState({{}}, '', url);
      window.parent.location.reload();
    }}catch(e){{
      const P = new URLSearchParams(window.location.search||'');
      P.set('sel_id', String(id));
      const url = window.location.pathname + '?' + P.toString() + window.location.hash;
      window.history.replaceState({{}}, '', url);
      window.location.reload();
    }}
  }};
  T.querySelectorAll('tbody tr').forEach(tr=>{{
    tr.addEventListener('click', ()=>go(tr.getAttribute('data-id')));
  }});
}})();
</script>
""", unsafe_allow_html=True)

# =========================
# Seleção de jogo (query param ou último)
# =========================
ids = games_list["match_id"].dropna().astype(int).tolist()
id_to_title = {
    int(r["match_id"]): f"{home_name} x {r['Adversário']} — {r['date']}"
    for _, r in games_list.iterrows()
}

default_id = ids[0] if ids else None
if sel_qp and str(sel_qp).isdigit():
    default_id = int(sel_qp)

col_sel, _ = st.columns([3,1])
with col_sel:
    if default_id in ids:
        default_idx = ids.index(default_id)
    else:
        default_idx = len(ids)-1 if ids else 0
    sel_id = st.selectbox("Selecione o jogo:", ids, index=default_idx, format_func=lambda x: id_to_title.get(int(x), str(x)))

if sel_id is None:
    st.stop()

# =========================
# Cabeçalho do jogo
# =========================
row = games_list[games_list["match_id"] == sel_id].iloc[0]
away_name = row["Adversário"]
date_str = str(row["date"])
st.markdown(f"<div class='hist-card'><span class='hist-title'>Jogo:</span> <b>{home_name}</b> x <b>{away_name}</b> — <span class='muted'>{date_str}</span></div>", unsafe_allow_html=True)

# =========================
# Resumo de sets do jogo escolhido
# =========================
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
        display_dataframe(view_tbl.drop(columns=["winner_team_id"]), height=200, use_container_width=True)
    else:
        st.write("_Sem sets para este jogo._")
else:
    st.write("_Sem sets._")

# =========================
# Filtro de set e listagem de rallies
# =========================
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
    display_dataframe(df_view, height=320, use_container_width=True)

# =========================
# KPIs por jogadora (partida)
# =========================
st.markdown("### KPI por Jogadora (Partida)")

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
        tbl_err = pd.merge(tot, piv_err, on='player_number', how="left")
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
        col_map = {"DIAGONAL":"Diagonal","LINHA":"Paralela","MEIO":"Meio","PIPE":"Pipe","SEGUNDA":"Segunda","LOB":"Largada","SAQUE":"Saque"}
        for k in col_map.keys():
            if k not in piv.columns: piv[k] = 0
        piv = piv[list(col_map.keys())].rename(columns=col_map).reset_index()
        piv = piv.rename(columns={"player_number":"Jog."})
        display_dataframe(piv, height=260, use_container_width=True)

# Voltar (mantido)
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
