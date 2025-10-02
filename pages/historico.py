# pages/historico.py — Histórico de jogos (tabela 3 linhas, 2 gráficos pequenos com largura fixa)
from __future__ import annotations

# ===== Imports no topo =====
from pathlib import Path
import io
import math
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from db_excel import init_or_load

# Matplotlib (opcional)
try:
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator
    MPL_AVAILABLE = True
except Exception:
    MPL_AVAILABLE = False

# ===== Config fácil p/ tamanho dos gráficos (edite aqui, se quiser) =====
# => Ambos são exibidos com st.image(width=...), então o valor abaixo manda de verdade.
CHART_FUND_PX = 900   # largura (px) do gráfico de barras “Comparativo por fundamento”
CHART_EVOL_PX = 900   # largura (px) do gráfico de linha “Placar (evolução)”

# ~aspect ratios usados ao salvar a figura (só para ficar proporcional bonitinho)
CHART_FUND_INCH = (9.0, 3.9)
CHART_EVOL_INCH = (9.0, 3.6)


# =========================
# Utilitários compatíveis
# =========================
OUR_TEAM_ID = 1

def _base_dir() -> Path:
    return Path(__file__).resolve().parents[1]

def _default_db_path() -> Path:
    return _base_dir() / "volei_base_dados.xlsx"

def load_frames():
    db_path = st.session_state.get("db_path", str(_default_db_path()))
    return init_or_load(Path(db_path))

def team_name_by_id(fr: dict, team_id: int | None) -> str:
    eq = fr.get("equipes", pd.DataFrame())
    if eq.empty or team_id is None:
        return "Equipe"
    eq = eq.copy(); eq["team_id"] = pd.to_numeric(eq["team_id"], errors="coerce")
    row = eq.loc[eq["team_id"] == int(team_id)]
    return str(row.iloc[0]["team_name"]) if not row.empty else f"Equipe {int(team_id)}"

def display_dataframe(df: pd.DataFrame, height: int | None = None, use_container_width: bool = False, header_bg: str = "#0f172a"):
    if df is None or df.empty:
        st.write("_Sem dados._"); return
    thead_style = f"background:{header_bg};color:#fff;"
    html_table = df.to_html(classes="custom-table", index=False, escape=False)
    html_table = html_table.replace("<thead>", f"<thead style='{thead_style}'>")
    h = f"{height}px" if height else "auto"
    w = "100%" if use_container_width else "auto"
    st.markdown(
        f"<div style='overflow:auto; height:{h}; width:{w}; margin:0'>{html_table}</div>",
        unsafe_allow_html=True
    )

def current_match_df(fr: dict, match_id: int) -> pd.DataFrame:
    rl = fr.get("rallies", pd.DataFrame())
    return rl[rl["match_id"] == match_id].copy().sort_values(["set_number","rally_no"])

def current_set_df(fr: dict, match_id: int, set_number: int) -> pd.DataFrame:
    rl = fr.get("rallies", pd.DataFrame())
    return rl[(rl["match_id"]==match_id) & (rl["set_number"]==set_number)].copy().sort_values("rally_no")

def fig_to_png_bytes(fig, dpi=140) -> bytes:
    """Converte uma figura matplotlib para PNG bytes; ideal para st.image(width=...)."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


# =========================
# AgGrid (bonito) + Fallback para tabela antiga
# =========================
def render_games_grid(df_show: pd.DataFrame, total: int):
    """
    Mostra a grade com AgGrid (busca global, filtros por coluna, ordenação e clique).
    Altura: no modo compacto, exatamente 3 linhas (com rolagem). Em "Lista completa", altura maior.
    Fallback automático para a tabela HTML antiga se streamlit-aggrid não estiver instalada.
    Retorna o match_id selecionado (ou None).
    """
    # Alturas calculadas p/ 3 linhas
    ROW_H = 26
    HEADER_H = 28
    COMPACT_H = HEADER_H + ROW_H * 3 + 10   # => ~3 linhas visíveis (o que você pediu)
    FULL_H = 360

    # IMPORT opcional (aqui dentro): evita erro de import quando o pacote não está instalado
    try:
        from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
        AGGRID_OK = True
    except Exception:
        AGGRID_OK = False

    if not AGGRID_OK:
        # ---- FALLBACK: tabela HTML (melhorada) ----
        rows_html = []
        for _, r in df_show.iterrows():
            mid = int(r["match_id"]) if pd.notna(r["match_id"]) else 0
            date = str(r["date"])
            adv = str(r["Adversário"])
            sets_txt = str(r["Sets (Nós-Adv)"])
            res_html = r.get("ResHTML","—")
            status = r["Status"]
            rows_html.append(
                f"<tr data-id='{mid}'>"
                f"<td>{mid}</td><td>{date}</td><td><b>{adv}</b></td><td>{sets_txt}</td>"
                f"<td>{res_html}</td><td>{status}</td></tr>"
            )
        rows_html = "".join(rows_html)
        height = FULL_H if st.session_state.get("hist_full_open") else COMPACT_H
        table_id = "hg_tbl"

        html_template = r"""
        <style>
        .hg-table{width:100%; border-collapse:separate; border-spacing:0; font-size:.92rem;}
        .hg-table thead th{position:sticky; top:0; background:#0f172a; color:#fff; text-align:left;
                           padding:8px 10px; font-weight:800; user-select:none; cursor:pointer;}
        .hg-table tbody td{padding:6px 10px; border-bottom:1px solid #e2e8f0;}
        .hg-table tbody tr:nth-child(even){background:#f8fafc;}
        .hg-table tbody tr:hover{background:#e2e8f0; cursor:pointer;}
        .th-sort-asc::after{content:" \25B2";} .th-sort-desc::after{content:" \25BC";}
        .res-badge{display:inline-block; padding:2px 8px; border-radius:999px; font-weight:800; font-size:.82rem;}
        .res-win{background:#dcfce7; color:#14532d; border:1px solid #86efac;}
        .res-draw{background:#fef9c3; color:#78350f; border:1px solid #fde68a;}
        .res-loss{background:#fee2e2; color:#7f1d1d; border:1px solid #fecaca;}
        </style>
        <div class='hist-card' style='margin-top:6px'>
          <div class='controls-row'>
            <span class='muted'>Clique em um jogo para abrir os detalhes · Clique no cabeçalho para ordenar</span>
            <button class="btn-ghost" onclick="window.scrollTo(0,0)">Topo ↑</button>
            <span class='tiny-note'>Total: __TOTAL__</span>
          </div>
          <div class='scroll-wrap' style='height:__HEIGHT__px'>
            <table class='hg-table' id='__TABLE_ID__'>
              <thead>
                <tr>
                  <th data-col='0'>ID</th>
                  <th data-col='1'>Data</th>
                  <th data-col='2'>Adversário</th>
                  <th data-col='3'>Sets</th>
                  <th data-col='4'>Resultado</th>
                  <th data-col='5'>Status</th>
                </tr>
              </thead>
              <tbody>__ROWS__</tbody>
            </table>
          </div>
        </div>
        <script>
        (function(){
          const T=document.getElementById('__TABLE_ID__'); if(!T) return;
          const go=(id)=>{
            try{
              const P=new URLSearchParams(window.parent.location.search||'');
              P.set('sel_id', String(id));
              const url=window.parent.location.pathname+'?'+P.toString()+window.parent.location.hash;
              window.parent.history.replaceState({},'',url);
              window.parent.location.reload();
            }catch(e){
              const P=new URLSearchParams(window.location.search||'');
              P.set('sel_id', String(id));
              const url=window.location.pathname+'?'+P.toString()+window.location.hash;
              window.history.replaceState({},'',url);
              window.location.reload();
            }
          };
          T.querySelectorAll('tbody tr').forEach(tr=>tr.addEventListener('click', ()=>go(tr.getAttribute('data-id'))));
          const parseVal=(txt)=>{ if(!txt) return {type:'str', val:''};
            const t=txt.trim();
            const num=parseFloat(t.replace(/[^0-9\.\-]/g,'')); if(!isNaN(num)) return {type:'num', val:num};
            const d=Date.parse(t.replace(/\//g,'-')); if(!Number.isNaN(d)) return {type:'date', val:d};
            return {type:'str', val:t.toLowerCase()};
          };
          const ths=[...T.querySelectorAll('thead th')];
          ths.forEach((th,idx)=>{
            th.addEventListener('click', ()=>{
              const tbody=T.querySelector('tbody');
              const rows=[...tbody.querySelectorAll('tr')];
              const cur=th.getAttribute('data-sort')||'none';
              const dir=cur==='asc'?'desc':'asc';
              ths.forEach(x=>{x.removeAttribute('data-sort'); x.classList.remove('th-sort-asc','th-sort-desc');});
              th.setAttribute('data-sort', dir);
              th.classList.add(dir==='asc'?'th-sort-asc':'th-sort-desc');
              rows.sort((a,b)=>{
                const A=a.children[idx].innerText, B=b.children[idx].innerText;
                const pa=parseVal(A), pb=parseVal(B);
                if(pa.type===pb.type){
                  if(pa.val<pb.val) return dir==='asc'?-1:1;
                  if(pa.val>pb.val) return dir==='asc'?1:-1;
                  return 0;
                }
                const rank={'num':0,'date':1,'str':2};
                return dir==='asc'?(rank[pa.type]-rank[pb.type]):(rank[pb.type]-rank[pa.type]);
              });
              rows.forEach(r=>tbody.appendChild(r));
            });
          });
        })();
        </script>
        """
        html_final = (
            html_template
              .replace("__TOTAL__", str(total))
              .replace("__HEIGHT__", str(height))
              .replace("__TABLE_ID__", table_id)
              .replace("__ROWS__", rows_html)
        )
        components.html(html_final, height=height+100, scrolling=True)
        return None

    # ============ AgGrid ============
    df_grid = df_show.copy()
    df_grid["Status"] = df_grid["Status"].astype(str).str.replace(r"<.*?>", "", regex=True)
    df_grid = df_grid.rename(columns={
        "match_id": "ID",
        "date": "Data",
        "Adversário": "Adversário",
        "Sets (Nós-Adv)": "Sets"
    })[["ID","Data","Adversário","Sets","Resultado","Status"]]

    gob = GridOptionsBuilder.from_dataframe(df_grid)
    gob.configure_default_column(filter=True, sortable=True, resizable=True)
    res_style = JsCode("""
        function(p){
          const v = (p.value || "").toLowerCase();
          if (v.indexOf("vitória")>=0) return {'backgroundColor':'#dcfce7','color':'#14532d','fontWeight':'700'};
          if (v.indexOf("derrota")>=0) return {'backgroundColor':'#fee2e2','color':'#7f1d1d','fontWeight':'700'};
          if (v.indexOf("empate")>=0)  return {'backgroundColor':'#fef9c3','color':'#78350f','fontWeight':'700'};
          return {};
        }
    """)
    gob.configure_column("Resultado", width=140, cellStyle=res_style)
    gob.configure_grid_options(
        rowSelection="single",
        suppressRowClickSelection=False,
        domLayout="normal",
        pagination=False,             # rolagem
        rowHeight=ROW_H, headerHeight=HEADER_H,
        enableCellTextSelection=True,
        animateRows=True,
        suppressMenuHide=False,
        quickFilterText = st.session_state.get("hist_q", "")
    )
    gob.configure_column("ID", width=90)
    gob.configure_column("Data", width=140)
    gob.configure_column("Adversário", flex=2, minWidth=220)
    gob.configure_column("Sets", width=120)
    gob.configure_column("Status", width=110)
    grid_options = gob.build()

    st.markdown("""
        <style>
        .ag-root-wrapper, .ag-theme-alpine { border-radius: 10px; }
        .ag-header { font-weight: 700; }
        .ag-paging-panel { padding: 2px 8px !important; }
        .ag-theme-alpine .ag-row-hover { background-color: #f1f5f9 !important; }
        .ag-theme-alpine .ag-row-selected { background-color: #e2e8f0 !important; }
        </style>
    """, unsafe_allow_html=True)

    height_grid = FULL_H if st.session_state.get("hist_full_open") else COMPACT_H
    grid = AgGrid(
        df_grid,
        gridOptions=grid_options,
        theme="alpine",
        update_mode=GridUpdateMode.SELECTION_CHANGED | GridUpdateMode.MODEL_CHANGED,
        allow_unsafe_jscode=True,
        height=height_grid,
        fit_columns_on_grid_load=True
    )

    rows = grid.get("selected_rows", [])
    if rows:
        return int(rows[0]["ID"])
    return None


# =========================
# Página
# =========================
st.set_page_config(page_title="Histórico — UniVolei", layout="wide")

# ---------- CSS (remove header/toolbar/fundo branco; layout enxuto) ----------
st.markdown(
    """
    <style>
      header[data-testid="stHeader"], [data-testid="stToolbar"], [data-testid="stDecoration"] { display:none !important; }
      [data-testid="stAppViewContainer"] { padding-top:0 !important; background:transparent !important; }

      .block-container { padding-top: .2rem !important; padding-bottom: .6rem !important; }
      .topbar-row { margin-top: 6px; }
      .stMarkdown, .stText, .stSelectbox, .stNumberInput, .stButton { margin:0 !important; padding:0 !important; }
      .stSelectbox > div, .stTextInput > div, .stMultiSelect > div, .stNumberInput > div { margin:0 !important; }
      .stSelectbox label, .stTextInput label, .stMultiSelect label, .stNumberInput label { margin-bottom:2px !important; }

      .hist-card{background:#fff; border:1px solid #e2e8f0; border-radius:10px;
        padding:8px 10px; margin:6px 0; box-shadow:0 1px 2px rgba(15,23,42,.04);}
      .hist-title{font-weight:800; letter-spacing:.2px; margin:0}
      .muted{color:#64748b; font-weight:600; margin:0}
      .hg-pill{display:inline-block; padding:2px 6px; border-radius:999px; font-size:.75rem; font-weight:700;}
      .hg-pill.ok{background:#dcfce7; color:#14532d; border:1px solid #86efac;}
      .hg-pill.open{background:#fef9c3; color:#78350f; border:1px solid #fde68a;}

      .res-badge{display:inline-block; padding:2px 8px; border-radius:999px; font-weight:800; font-size:.82rem;}
      .res-win{background:#dcfce7; color:#14532d; border:1px solid #86efac;}
      .res-draw{background:#fef9c3; color:#78350f; border:1px solid #fde68a;}
      .res-loss{background:#fee2e2; color:#7f1d1d; border:1px solid #fecaca;}

      .scroll-wrap{overflow:auto; border:1px solid #e2e8f0; border-radius:10px; background:#fff;}
      .controls-row{display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin:2px 0 4px 0;}
      .btn-ghost{background:#fff; border:1px solid #cbd5e1; border-radius:8px; padding:4px 8px; cursor:pointer; font-weight:700;}
      .btn-ghost:hover{background:#f8fafc;}

      .tiny-note{font-size:.8rem; color:#64748b; margin-left:6px;}
      .subtle{font-size:.9rem; color:#475569; margin:2px 0 4px 0;}

      .card-title{font-weight:800; color:#fff; padding:6px 10px; border-radius:8px 8px 0 0;}
      .card-wrap{border:1px solid #e2e8f0; border-radius:10px; overflow:hidden; box-shadow:0 1px 2px rgba(15,23,42,.04); margin:6px 0;}
      .title-blue{background:linear-gradient(90deg,#0ea5e9,#1e3a8a);}  /* usado nos dois cards principais */
      .title-amber{background:linear-gradient(90deg,#f59e0b,#92400e);}
      .title-slate{background:linear-gradient(90deg,#475569,#0f172a);}
    </style>
    """,
    unsafe_allow_html=True
)

# =========================
# Dados
# =========================
frames = load_frames()
mt = frames.get("amistosos", pd.DataFrame())
sets = frames.get("sets", pd.DataFrame())

if mt.empty:
    st.info("Nenhum jogo cadastrado ainda."); st.stop()

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
# TOPO: título + filtros principais NA MESMA LINHA + toggle + fechar
# =========================
st.session_state.setdefault("hist_full_open", False)
st.session_state.setdefault("hist_page_size", 50)
st.session_state.setdefault("hist_page", 1)
st.session_state.setdefault("hist_q", "")
st.session_state.setdefault("hist_filter_cols", [])
st.session_state.setdefault("hist_filter_status", "Todos")

st.markdown("<div class='topbar-row'>", unsafe_allow_html=True)
c_title, c_busca, c_cols, c_toggle, c_close = st.columns([3, 3, 3, 2, 1])
with c_title:
    st.markdown("### 🗂️ Histórico e análise de jogos")
with c_busca:
    st.session_state["hist_q"] = st.text_input("Busca geral", st.session_state["hist_q"], placeholder="ID, data, adversário, sets…")
with c_cols:
    cols_opts = ["ID", "Data", "Adversário", "Sets", "Status"]
    st.session_state["hist_filter_cols"] = st.multiselect("Filtrar por colunas", cols_opts, default=st.session_state["hist_filter_cols"])
with c_toggle:
    st.toggle("🔎 Lista completa", key="hist_full_open")
with c_close:
    def _back_index():
        try: st.switch_page("index.py")
        except Exception:
            try: st.switch_page("../index.py")
            except Exception: st.write("Abrir o Index pelo menu lateral.")
    try:
        st.page_link("index.py", label="❌ Fechar")
    except Exception:
        st.button("❌ Fechar", on_click=_back_index, use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)

# Sub-filtros (linha compacta)
sf1, sf2, sf3 = st.columns([4, 4, 4])
with sf1:
    id_filter = st.text_input("ID contém", key="hist_f_id") if "ID" in st.session_state["hist_filter_cols"] else ""
with sf2:
    date_filter = st.text_input("Data contém", key="hist_f_date") if "Data" in st.session_state["hist_filter_cols"] else ""
    adv_filter = st.text_input("Adversário contém", key="hist_f_adv") if "Adversário" in st.session_state["hist_filter_cols"] else ""
with sf3:
    sets_filter = st.text_input("Sets contém", key="hist_f_sets") if "Sets" in st.session_state["hist_filter_cols"] else ""
    if "Status" in st.session_state["hist_filter_cols"]:
        st.session_state["hist_filter_status"] = st.selectbox("Status", ["Todos", "Aberto", "Fechado"],
                                                              index=["Todos","Aberto","Fechado"].index(st.session_state["hist_filter_status"]))

# =========================
# Base, filtros, paginação
# =========================
games_list = mt.sort_values(["date","match_id"], ascending=[False, True]).reset_index(drop=True)
gl = games_list.copy()
gl["__StatusTxt__"] = gl.apply(lambda r: "Fechado" if bool(r.get("is_closed", False)) else "Aberto", axis=1)

def _res_text(r):
    try:
        hs, as_ = int(r.get("home_sets", 0)), int(r.get("away_sets", 0))
        if hs > as_: return "✅ Vitória"
        if hs < as_: return "❌ Derrota"
        return "🟨 Empate"
    except Exception:
        return "—"
def _res_html(r):
    txt = _res_text(r)
    if "Vitória" in txt: return f"<span class='res-badge res-win'>{txt}</span>"
    if "Derrota" in txt: return f"<span class='res-badge res-loss'>{txt}</span>"
    if "Empate" in txt:  return f"<span class='res-badge res-draw'>{txt}</span>"
    return txt

gl["Resultado"] = gl.apply(_res_text, axis=1)
gl["ResHTML"]  = gl.apply(_res_html, axis=1)

q = (st.session_state.get("hist_q") or "").strip().lower()
if q:
    gl = gl[
        gl["Adversário"].astype(str).str.lower().str.contains(q, na=False) |
        gl["date"].astype(str).str.lower().str.contains(q, na=False) |
        gl["Sets (Nós-Adv)"].astype(str).str.lower().str.contains(q, na=False) |
        gl["match_id"].astype(str).str.lower().str.contains(q, na=False)
    ]
if "ID" in st.session_state["hist_filter_cols"]:
    v = (st.session_state.get("hist_f_id") or "").strip().lower()
    if v: gl = gl[gl["match_id"].astype(str).str.lower().str.contains(v, na=False)]
if "Data" in st.session_state["hist_filter_cols"]:
    v = (st.session_state.get("hist_f_date") or "").strip().lower()
    if v: gl = gl[gl["date"].astype(str).str.lower().str.contains(v, na=False)]
if "Adversário" in st.session_state["hist_filter_cols"]:
    v = (st.session_state.get("hist_f_adv") or "").strip().lower()
    if v: gl = gl[gl["Adversário"].astype(str).str.lower().str.contains(v, na=False)]
if "Sets" in st.session_state["hist_filter_cols"]:
    v = (st.session_state.get("hist_f_sets") or "").strip().lower()
    if v: gl = gl[gl["Sets (Nós-Adv)"].astype(str).str.lower().str.contains(v, na=False)]
if "Status" in st.session_state["hist_filter_cols"]:
    stv = st.session_state["hist_filter_status"]
    if stv in ("Aberto", "Fechado"):
        gl = gl[gl["__StatusTxt__"] == stv]

# Paginação (quando lista completa)
total = len(gl)
if st.session_state["hist_full_open"]:
    pages = max(1, math.ceil(total / st.session_state["hist_page_size"]))
    st.session_state["hist_page"] = st.number_input("Página:", min_value=1, max_value=pages,
                                                    value=min(st.session_state["hist_page"], pages), step=1)
    start = (st.session_state["hist_page"] - 1) * st.session_state["hist_page_size"]
    end = start + st.session_state["hist_page_size"]
    show_df = gl.iloc[start:end].copy()
else:
    show_df = gl.head(12).copy()

def _row_status(r):
    is_closed = bool(r.get("is_closed", False))
    return "<span class='hg-pill ok'>Fechado</span>" if is_closed else "<span class='hg-pill open'>Aberto</span>"

show_df["Status"] = show_df.apply(_row_status, axis=1)

# =========================
# Tabela de jogos
# =========================
sel_from_grid = render_games_grid(show_df.assign(match_id=show_df["match_id"]), total=total)
if sel_from_grid is not None:
    try: st.query_params.update({"sel_id": str(sel_from_grid)})
    except Exception: pass
    st.session_state["sel_id_override"] = sel_from_grid
    st.rerun()

# =========================
# Seleção de jogo
# =========================
ids = gl["match_id"].dropna().astype(int).tolist()
default_id = ids[0] if ids else None

sel_qp = None
try:
    qp = st.query_params.get("sel_id", None)
    if qp and str(qp).isdigit(): sel_qp = int(qp)
except Exception:
    sel_qp = None

sel_id = st.session_state.get("sel_id_override", None)
if sel_id is None:
    sel_id = sel_qp if sel_qp is not None else default_id
if sel_id is None:
    st.info("Selecione um jogo na tabela acima para ver os detalhes."); st.stop()

# =========================
# Cabeçalho do jogo
# =========================
row = mt[mt["match_id"] == sel_id].iloc[0]
away_name = row["Adversário"]; date_str = str(row["date"])
st.markdown(
    f"<div class='hist-card' style='padding:6px 8px'><span class='hist-title'>Jogo:</span> "
    f"<b>{home_name}</b> x <b>{away_name}</b> — <span class='muted'>{date_str}</span></div>",
    unsafe_allow_html=True
)

# =========================
# Sets
# =========================
st.markdown("<div class='subtle'>Sets</div>", unsafe_allow_html=True)
sets_df = frames.get("sets", pd.DataFrame())
if not sets_df.empty:
    sm = sets_df[sets_df["match_id"] == sel_id].copy().sort_values("set_number")
    if not sm.empty:
        view_tbl = sm.rename(columns={
            "set_number":"Set",
            "home_points":f"Pts {home_name}",
            "away_points":f"Pts {away_name}",
        })[["Set", f"Pts {home_name}", f"Pts {away_name}", "winner_team_id"]]
        view_tbl["Vencedor"] = view_tbl["winner_team_id"].map({1: home_name, 2: away_name}).fillna("")
        display_dataframe(view_tbl.drop(columns=["winner_team_id"]), height=130, use_container_width=True, header_bg="#1e293b")
    else:
        st.write("_Sem sets para este jogo._")
else:
    st.write("_Sem sets._")

# =========================
# 📊 Estatísticas do Jogo (Todos por padrão)
# =========================
all_df = current_match_df(frames, sel_id)
sets_disp = sorted(all_df["set_number"].dropna().unique().tolist()) if not all_df.empty and "set_number" in all_df.columns else []

scope_col, _ = st.columns([1.6, 6])
with scope_col:
    set_scope = st.selectbox("📊 Estatísticas — escopo", (["Todos"] + [int(s) for s in sets_disp]), index=0, key="hist_stats_scope")

df_scope = all_df.copy() if set_scope == "Todos" else current_set_df(frames, sel_id, int(set_scope))

if df_scope is not None and not df_scope.empty:
    dfx = df_scope.copy()
    for col in ["action","result","who_scored"]:
        if col in dfx.columns:
            dfx[col] = dfx[col].astype(str).str.strip().str.upper()

    attack_actions = {"DIAGONAL","LINHA","MEIO","PIPE","SEGUNDA","LOB","ATAQUE"}
    def _cnt(mask): return int(mask.sum()) if hasattr(mask, "sum") else 0

    atk_home = _cnt((dfx.get("who_scored","")=="NOS") & (dfx.get("result","")=="PONTO") & (dfx.get("action","").isin(attack_actions)))
    blk_home = _cnt((dfx.get("who_scored","")=="NOS") & (dfx.get("result","")=="PONTO") & (dfx.get("action","")=="BLOQUEIO"))
    ace_home = _cnt((dfx.get("who_scored","")=="NOS") & (dfx.get("result","")=="PONTO") & (dfx.get("action","")=="SAQUE"))
    erradv_home = _cnt((dfx.get("who_scored","")=="NOS") & (dfx.get("result","")=="ERRO"))

    atk_away = _cnt((dfx.get("who_scored","")=="ADV") & (dfx.get("result","")=="PONTO") & (dfx.get("action","").isin(attack_actions)))
    blk_away = _cnt((dfx.get("who_scored","")=="ADV") & (dfx.get("result","")=="PONTO") & (dfx.get("action","")=="BLOQUEIO"))
    ace_away = _cnt((dfx.get("who_scored","")=="ADV") & (dfx.get("result","")=="PONTO") & (dfx.get("action","")=="SAQUE"))
    erradv_away = _cnt((dfx.get("who_scored","")=="ADV") & (dfx.get("result","")=="ERRO"))

    try:
        last_row = dfx.sort_values(["set_number","rally_no"]).iloc[-1]
        tot_home = int(last_row.get("score_home", 0))
        tot_away = int(last_row.get("score_away", 0))
    except Exception:
        tot_home = atk_home + blk_home + ace_home + erradv_home
        tot_away = atk_away + blk_away + ace_away + erradv_away

    df_stats_final = pd.DataFrame({
        "Fundamento": ["Pontos de Ataque", "Pontos de Bloqueio", "Aces (saques)", "Erros do adversário", "Total de pontos"],
        home_name:    [atk_home,        blk_home,            ace_home,          erradv_home,              tot_home],
        away_name:    [atk_away,        blk_away,            ace_away,          erradv_away,              tot_away],
    })

    s_left, s_right = st.columns(2)
    with s_left:
        st.markdown("<div class='card-wrap'><div class='card-title title-blue'>📊 Estatísticas do Jogo</div></div>", unsafe_allow_html=True)
        display_dataframe(df_stats_final, height=170, use_container_width=True, header_bg="#0ea5e9")

    by_fund = pd.DataFrame({
        "Fundamento": ["Ataque","Bloqueio","Ace","Erro Adversário"],
        home_name:    [atk_home, blk_home, ace_home, erradv_home],
        away_name:    [atk_away, blk_away, ace_away, erradv_away],
    })
    with s_right:
        st.markdown("<div class='card-wrap'><div class='card-title title-blue'>🧩 Comparativo por Fundamento</div></div>", unsafe_allow_html=True)
        display_dataframe(by_fund, height=170, use_container_width=True, header_bg="#0ea5e9")

    # ======== Gráficos (apenas 2; exportados para PNG e exibidos com largura fixa) ========
    if MPL_AVAILABLE:
        # 1) Barras comparativas (fundamentos)
        figB, axB = plt.subplots(figsize=CHART_FUND_INCH)
        cats = ["Ataque","Bloqueio","Ace","Erro Adv."]
        H = [atk_home, blk_home, ace_home, erradv_home]
        A = [atk_away, blk_away, ace_away, erradv_away]
        x = range(len(cats)); width = 0.34
        axB.bar([i - width/2 for i in x], H, width, label=home_name)
        axB.bar([i + width/2 for i in x], A, width, label=away_name)
        axB.set_xticks(list(x)); axB.set_xticklabels(cats, fontsize=7)
        axB.set_ylabel("Pontos", fontsize=7); axB.set_title("Comparativo por fundamento", fontsize=8)
        axB.legend(loc="best", fontsize=6)
        axB.grid(True, linestyle='--', alpha=0.35)
        st.image(fig_to_png_bytes(figB), width=CHART_FUND_PX)   # <<< tamanho controlado

    st.markdown("<div class='subtle'>Rallies</div>", unsafe_allow_html=True)
else:
    st.caption("_Sem rallies no escopo selecionado._")

# =========================
# Rallies (compacto + seletor de set)
# =========================
if all_df.empty:
    st.info("Sem rallies para este jogo.")
else:
    sets_disp_r = sorted(all_df["set_number"].dropna().unique().tolist())
    fcol1, _ = st.columns([1, 6])
    with fcol1:
        set_sel = st.selectbox("Set:", ["Todos"] + [int(s) for s in sets_disp_r], key="hist_rallies_set")
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
    display_dataframe(df_view, height=200, use_container_width=True, header_bg="#475569")

# =========================
# 📈 Placar (evolução) — segundo e ÚNICO outro gráfico
# =========================
st.markdown("**📈 Placar (evolução)**")
if MPL_AVAILABLE:
    df_evo = df_scope if ('df_scope' in locals() and df_scope is not None and not df_scope.empty) else None
    if df_evo is not None:
        fig3, ax3 = plt.subplots(figsize=CHART_EVOL_INCH)
        ax3.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax3.yaxis.set_major_locator(MaxNLocator(integer=True))
        if all(k in df_evo.columns for k in ["rally_no","score_home","score_away"]):
            ax3.plot(df_evo["rally_no"], df_evo["score_home"], marker="o", markersize=1.6, linewidth=0.7, label=home_name)
            ax3.plot(df_evo["rally_no"], df_evo["score_away"], marker="o", markersize=1.6, linewidth=0.7, label=away_name)
        ax3.set_xlabel("Rally", fontsize=7); ax3.set_ylabel("Pontos", fontsize=7)
        ax3.legend(loc="best", fontsize=6)
        ax3.grid(True, linestyle='--', alpha=0.6)
        st.image(fig_to_png_bytes(fig3), width=CHART_EVOL_PX)   # <<< tamanho controlado
    else:
        st.caption("_Sem dados para o gráfico de placar._")
else:
    st.caption("_Matplotlib não disponível para gerar gráficos_")

# =========================
# KPIs por jogadora (partida)
# =========================
left, right = st.columns(2)

with left:
    st.markdown("<div class='card-wrap'><div class='card-title title-amber'>🏅 Pontos (nossos)</div></div>", unsafe_allow_html=True)
    atp = all_df[(all_df["result"]=="PONTO") & (all_df["who_scored"]=="NOS")].copy()
    if atp.empty:
        st.write("_Sem pontos de ataque._")
    else:
        atp["player_number"] = atp["player_number"].fillna("—")
        piv = atp.groupby(["player_number","action"]).size().unstack(fill_value=0)
        col_map = {"DIAGONAL":"Diagonal","LINHA":"Paralela","MEIO":"Meio","PIPE":"Pipe","SEGUNDA":"Segunda","LOB":"Largada","SAQUE":"Saque"}
        for k in col_map.keys():
            if k not in piv.columns: piv[k] = 0
        piv = piv[list(col_map.keys())].rename(columns=col_map).reset_index().rename(columns={"player_number":"Jog."})
        display_dataframe(piv, height=160, use_container_width=True, header_bg="#f59e0b")

with right:
    st.markdown("<div class='card-wrap'><div class='card-title title-slate'>⚠️ Erros (nossos)</div></div>", unsafe_allow_html=True)
    er = all_df[(all_df["result"]=="ERRO") & (all_df["who_scored"]=="ADV")].copy()
    if er.empty:
        st.write("_Sem erros._")
    else:
        er["player_number"] = er["player_number"].fillna("—")
        tot = er.groupby("player_number").size().reset_index(name="Erros")
        piv_err = er.groupby(["player_number","action"]).size().unstack(fill_value=0)
        desired_cols = ["DIAGONAL","LINHA","MEIO","PIPE","SEGUNDA","LOB","SAQUE","BLOQUEIO","RECEPÇÃO","RECEPCAO"]
        for c in desired_cols:
            if c not in piv_err.columns: piv_err[c] = 0
        piv_err["RECEPÇÃO"] = piv_err.get("RECEPÇÃO", 0) + piv_err.get("RECEPCAO", 0)
        order_cols = ["DIAGONAL","LINHA","MEIO","PIPE","SEGUNDA","LOB","SAQUE","BLOQUEIO","RECEPÇÃO"]
        piv_err = piv_err[order_cols].rename(columns={
            "DIAGONAL":"Diagonal","LINHA":"Paralela","MEIO":"Meio","PIPE":"Pipe",
            "SEGUNDA":"Segunda","LOB":"Largada","SAQUE":"Saque","BLOQUEIO":"Bloqueio","RECEPÇÃO":"Recepção",
        }).reset_index()
        tbl_err = pd.merge(tot, piv_err, on='player_number', how="left").rename(columns={"player_number":"Jog."}).sort_values("Erros", ascending=False)
        display_dataframe(tbl_err, height=160, use_container_width=True, header_bg="#475569")

# =========================
# Últimos rallies do set selecionado (curto)
# =========================
st.markdown("<div class='subtle'>Últimos rallies (set atual)</div>", unsafe_allow_html=True)
df_set = None
try:
    if isinstance(st.session_state.get("hist_rallies_set"), str) and st.session_state["hist_rallies_set"] == "Todos":
        df_set = None
    else:
        _setnum = st.session_state.get("hist_rallies_set")
        if _setnum is not None and _setnum != "Todos":
            df_set = current_set_df(frames, sel_id, int(_setnum))
except Exception:
    df_set = None

if df_set is not None and not df_set.empty:
    cols_show = [c for c in ["rally_no","player_number","action","result","who_scored","score_home","score_away"] if c in df_set.columns]
    preview = df_set.sort_values("rally_no").tail(12)[cols_show].copy()
    preview.rename(columns={
        "rally_no":"#","player_number":"Jog","action":"Ação",
        "result":"Resultado","who_scored":"Quem","score_home":"H","score_away":"A"
    }, inplace=True)
    display_dataframe(preview, height=160, use_container_width=True, header_bg="#1e293b")
else:
    st.caption("_Selecione um set para ver os últimos rallies._")
