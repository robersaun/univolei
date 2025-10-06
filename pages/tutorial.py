
import os
import base64
from pathlib import Path
from typing import Optional
import streamlit as st

# ===== Config =====
st.set_page_config(
    page_title="Tutorial — UniVolei Live Scout",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ===== Helpers =====
BASE_DIR = Path(__file__).resolve().parent
IMG_DIR = (BASE_DIR / ".." / "imgs").resolve()

def img_to_data_uri(path: Path) -> Optional[str]:
    """Converte imagem local para data URI (base64) para embutir em HTML."""
    try:
        ext = path.suffix.lower()
        mime = "image/png" if ext == ".png" else "image/jpeg"
        b64 = base64.b64encode(path.read_bytes()).decode("ascii")
        return f"data:{mime};base64,{b64}"
    except Exception:
        return None

# ===== Title =====
st.markdown(
    "<h1 style='text-align:center; color:#1e3a8a; margin-top:.25rem'>📘 Tutorial UniVolei Live Scout</h1>"
    "<span id='uv-close'></span>",
    unsafe_allow_html=True,
)

# ===== Styles (centralização + tamanhos + paleta azul escura) =====
st.markdown(
    """
<style>
  /* --- Tab menu bonito com borda arredondada e azul mais escuro --- */
  .stTabs [role="tablist"]{
    border: 2px solid #1e3a8a;
    border-radius: 12px;
    padding: 8px;              /* +20% mais "respiro" */
    gap: 10px;
    width: fit-content;
    margin: 0 auto 10px auto;  /* centraliza o menu */
    background: #ffffff;
  }
  .stTabs [role="tab"]{
    border: 1px solid #93c5fd;
    border-radius: 10px;
    padding: 8px 16px;         /* títulos das abas um pouco mais largos (~+20%) */
    background: #ffffff;
    color: #0f172a;
  }
  .stTabs [aria-selected="true"]{
    background: #93c5fd;     /* azul mais escuro (blue-400) */
    border-color: #2563eb;   /* blue-600 */
    color: #0f172a;
  }

  /* Área centralizada que evita largura total e reduz espaços */
  .uv-tabbox{
    border: 3px solid #1e3a8a;
    border-radius: 14px;
    padding: 16px;
    margin: 10px auto 20px auto;
    background: #ffffff;
    max-width: 1440px;              /* antes: 1200px  (+20%) */
  }
  .uv-wrap{ max-width: 1320px; margin: 0 auto; } /* antes: 1100px (+20%) */

  /* Linha de conteúdo padrão (texto à esquerda / imagem à direita) */
  .uv-tabrow{
    display: flex; gap: 24px; align-items: flex-start; /* gap um pouco maior */
  }
  .uv-l{ flex: 3 1 0; min-width: 360px; }
  .uv-r{ flex: 2 1 0; min-width: 260px; display:flex; justify-content:flex-end; }

  /* Tamanhos das imagens (1 e 2 +20%, 3 +40%) */
  .uv-r img.uv-img-std{ max-width: 504px; height: auto; border-radius: 10px; display: block; }
  .uv-r img.uv-img-3{   max-width: 588px; height: auto; border-radius: 10px; display: block; }

  /* Remover a linha cinza (qualquer <hr/>) */
  .uv-tabbox hr{ display: none !important; }
  .uv-tabbox [data-testid="stMarkdownContainer"] p{ margin: .25rem 0; }

  /* Rodízio: 2 cards por linha com alturas iguais e centralizado */
  .uv-grid{
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 18px;                /* um pouco mais largo */
    align-items: stretch;
    max-width: 1320px;        /* +20% */
    margin: 2px auto 0 auto;
  }
  .uv-col{ display: flex; min-width: 420px; }
  .uv-card{
    border: 1px solid #3b82f6;
    border-radius: 12px;
    background: #f8fafc;
    padding: 14px;
    display: flex;
    gap: 18px;
    align-items: flex-start;
    width: 100%;
    height: 100%;
  }
  .uv-title{
    font-size: 1.12rem; font-weight: 700; margin: 2px 0 8px 0; color: #0f172a;
  }
  .uv-rodizio-text{ font-size: 1.05rem; line-height: 1.45; margin: 0; color: #0f172a; }
  .uv-img{ width: 40%; min-width: 220px; max-width: 520px; }
  .uv-img img{
    width: 100%; height: auto; border-radius: 10px; display: block;
    max-height: 420px; object-fit: contain;
  }
  .uv-txt{ flex: 1 1 auto; display: flex; flex-direction: column; }
  .uv-txt ul{ margin: 0; padding-left: 1.1rem; }

  /* Barra do botão fechar no topo */
  .topbar-row{
    display: flex; justify-content: flex-end;
    max-width: 1440px;  /* +20% */
    margin: 0 auto;
  }

  /* --- Tabelas/DataFrames globais centralizadas --- */
  .stTable, .stDataFrame{
    max-width: 1320px;
    margin-left: auto;
    margin-right: auto;
  }
  .stDataFrame > div{ width: 100% !important; }

  /* === Lightbox (zoom ao clicar) === */
  .uv-zoom-thumb { cursor: zoom-in; display:block; }
  #uv-close { display:block; height:0; width:0; overflow:hidden; }
  .uv-lightbox{
    position: fixed; inset: 0; display: none;
    background: rgba(0,0,0,.88);
    z-index: 9999;
    align-items: center; justify-content: center;
    padding: 24px;
  }
  .uv-lightbox:target{ display: flex; }
  .uv-lightbox__bg{
    position: fixed; inset: 0; display:block;
  }
  .uv-lightbox .uv-content{
    position: relative;
    z-index: 1;
    max-width: 96vw; max-height: 96vh;
    display: flex; align-items: center; justify-content: center;
  }
  .uv-lightbox img{
    max-width: 92vw; max-height: 92vh; border-radius: 14px;
    box-shadow: 0 6px 28px rgba(0,0,0,.5);
  }
  .uv-lightbox a.uv-close{
    position: absolute; top: 10px; right: 14px;
    font-size: 34px; color: #fff; text-decoration: none; line-height: 1;
    z-index: 2;
  }
</style>
    """,
    unsafe_allow_html=True,
)

# ===== Close button row =====
st.markdown("<div class='topbar-row'>", unsafe_allow_html=True)
sp, closec = st.columns([12, 1])
with closec:
    def _back_index():
        try:
            st.switch_page("index.py")
        except Exception:
            try:
                st.switch_page("../index.py")
            except Exception:
                st.write("Abrir o Index pelo menu lateral.")
    try:
        st.page_link("index.py", label="❌ Fechar")
    except Exception:
        st.button("❌ Fechar", on_click=_back_index, use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)

# ===== Tabs =====
tab1, tab2, tab3, tab4 = st.tabs(["Início", "Modo Jogo", "Histórico", "Rodízio 5x1"])
# ---------------- Tab 1 ----------------
with tab1:
    img = img_to_data_uri(IMG_DIR / "print_1.jpg")
    img_html_1 = (f'<img class="uv-img-std" src="{img}" alt="Tela inicial do aplicativo"/>' 
                  if img else '<em>Imagem não encontrada (print_1.jpg)</em>')
    html = f"""
<div class='uv-tabbox'><div class='uv-wrap'>
  <div class='uv-tabrow'>
    <div class='uv-l'>
      <h3>🔹 Acesso inicial</h3>
      <ul>
        <li>Entre no endereço: <b>https://univolei-scout.streamlit.app/</b></li>
        <li>Caso exista <b>jogo em aberto</b>, o título e a data aparecerão automaticamente no cabeçalho.</li>
        <li>Na <b>primeira linha</b> você encontra os botões principais:
          <ul>
            <li><b>Time</b> → cadastrar/editar o time e jogadoras.</li>
            <li><b>Jogo</b> → iniciar ou continuar uma partida em andamento.</li>
            <li><b>Tutorial</b> → abre esta página de instruções.</li>
            <li><b>Histórico</b> → acessar estatísticas e análises de jogos anteriores.</li>
          </ul>
        </li>
      </ul>
      O aplicativo salva dados em <b>Excel, DuckDB e Google Sheets</b> (quando habilitado), garantindo backup e histórico.
    </div>
    <div class='uv-r'>
      {img_html_1}
    </div>
  </div>
</div></div>
"""
    st.markdown(html, unsafe_allow_html=True)
# ---------------- Tab 2 ----------------
with tab2:
    img = img_to_data_uri(IMG_DIR / "print_2.jpg")
    img_html_2 = (f'<img class="uv-img-std" src="{img}" alt="Modo Jogo — principal área de marcação de pontos"/>' 
                  if img else '<em>Imagem não encontrada (print_2.jpg)</em>')
    html = f"""
<div class='uv-tabbox'><div class='uv-wrap'>
  <div class='uv-tabrow'>
    <div class='uv-l'>
      <h3>🔹 Modo Jogo (principal)</h3>
      <p>O <b>Modo Jogo</b> é o coração do sistema: é aqui que você registra todas as jogadas da partida.</p>
      <ul>
        <li><b>Botões de Jogadoras</b> → clique para marcar quem participou do rally.
          <ul><li>1º clique = <b>Acerto</b> ✅</li><li>2º clique = <b>Erro</b> ❌</li></ul>
        </li>
        <li><b>Botão ADV</b> → registra pontos do adversário.</li>
        <li><b>Quadra Interativa (Heatmap)</b> → clique na quadra para marcar a região de cada ação.</li>
        <li><b>Placar em tempo real</b> → exibido sempre acima da quadra.</li>
        <li><b>Gestão de Sets</b> → abrir, fechar e finalizar sets; remover set vazio quando necessário.</li>
      </ul>
      🔑 <b>Importante:</b> O <b>Modo Jogo</b> é o principal local de marcação de pontos. 
      Cada ação registrada aqui alimenta as estatísticas do <b>Histórico</b> e direciona os treinos.
    </div>
    <div class='uv-r'>
      {img_html_2}
    </div>
  </div>
</div></div>
"""
    st.markdown(html, unsafe_allow_html=True)
# ---------------- Tab 3 ----------------
with tab3:
    img = img_to_data_uri(IMG_DIR / "print_3.jpg")
    img_html_3 = (f'<img class="uv-img-3" src="{img}" alt="Histórico — visão analítica"/>' 
                  if img else '<em>Imagem não encontrada (print_3.jpg)</em>')
    html = f"""
<div class='uv-tabbox'><div class='uv-wrap'>
  <div class='uv-tabrow'>
    <div class='uv-l'>
      <h3>🔹 Histórico de Jogos</h3>
      <p>O <b>Histórico</b> é o <b>dashboard central de análise</b>.</p>
      <ul>
        <li>📋 <b>Lista de jogos</b>: ID, data, adversário, sets e status (aberto/fechado).</li>
        <li>✅ <b>Resultado</b>: Vitória, Derrota ou Empate.</li>
        <li>🔍 <b>Filtros e buscas</b> por ID, data, adversário e status.</li>
        <li>📊 <b>Estatísticas detalhadas</b>: evolução do placar, comparativo por fundamento, erros por categoria, mapas de calor.</li>
      </ul>
      💡 <b>Reforço:</b> O <b>Histórico</b> é o local-chave para análise de desempenho e definição de treinos específicos.
    </div>
    <div class='uv-r'>
      {img_html_3}
    </div>
  </div>
</div></div>
"""
    st.markdown(html, unsafe_allow_html=True)
# ---------------- Tab 4 — Rodízio 5x1 (com zoom e fechar) ----------------
with tab4:
    st.markdown("<div class='uv-tabbox'><div class='uv-wrap'>", unsafe_allow_html=True)
    st.markdown("### 🔹 Rodízio 5x1 — Movimentações básicas por rotação")

    rotacoes = [
        ("p1.jpg", "Rotação com a (P1) Levantadora no fundo direito", [
            "(P1) Levantadora: sai do fundo direito e se desloca rapidamente para a zona de levantamento (próxima à P2/P3, na rede).",
            "(P2) Central: avança para meio de rede (posição 3) para atacar bola rápida.",
            "(P3) Oposta: assume a rede direita como opção de ataque.",
            "(P4) Ponteira 1: permanece como atacante da entrada esquerda.",
            "(P5) Ponteira 2: cobre fundo esquerdo, ajuda na recepção.",
            "(P6) Líbero: cobre fundo central, principal responsável pela defesa/recepção.",
        ]),
        ("p2.jpg", "Rotação com a (P2) Levantadora na rede direita", [
            "(P2) Levantadora: já posicionada na rede direita para levantar.",
            "(P3) Central: preparado no meio de rede para ataque rápido.",
            "(P4) Oposta: desloca-se para o fundo esquerdo, cobrindo defesa.",
            "(P5) Ponteira 1: cobre fundo esquerdo, pode vir para recepção.",
            "(P6) Ponteira 2: cobre fundo central, apoio na recepção.",
            "(P1) Líbero: cobre fundo direito.",
        ]),
        ("p3.jpg", "Rotação com a (P3) Levantadora no meio de rede", [
            "(P3) Levantadora: desloca-se lateralmente para o lado direito da rede (posição 2) para levantar.",
            "(P4) Central: entra pelo fundo esquerdo, participando da recepção.",
            "(P5) Oposta: cobre fundo esquerdo, pode atacar fundo.",
            "(P6) Ponteira 1: defesa no fundo central.",
            "(P1) Ponteira 2: cobre fundo direito.",
            "(P2) Líbero: cobre bolas curtas, apoio defensivo próximo à rede.",
        ]),
        ("p4.jpg", "Rotação com a (P4) Levantadora na rede esquerda", [
            "(P4) Levantadora: desloca-se da rede esquerda para a rede direita (zona 2) para armar.",
            "(P5) Central: atua no fundo esquerdo, participando da recepção.",
            "(P6) Oposta: cobre fundo central, com opção de ataque pipe.",
            "(P1) Ponteira 1: fundo direito, defesa/recepção.",
            "(P2) Ponteira 2: sobe para rede direita, atua como atacante auxiliar.",
            "(P3) Líbero: entra para cobrir o fundo central, se aplicável.",
        ]),
        ("p5.jpg", "Rotação com a (P5) Levantadora no fundo esquerdo", [
            "(P5) Levantadora: desloca-se do fundo esquerdo para rede direita (zona 2).",
            "(P6) Central: fundo central, apoio na recepção.",
            "(P1) Oposta: fundo direito, possível ataque de fundo.",
            "(P2) Ponteira 1: sobe para rede direita, apoio de ataque.",
            "(P3) Ponteira 2: central na rede, ataque rápido.",
            "(P4) Líbero: cobre fundo esquerdo, reforço da recepção.",
        ]),
        ("p6.jpg", "Rotação com a (P6) Levantadora no fundo central", [
            "(P6) Levantadora: desloca-se do fundo central para rede direita (zona 2).",
            "(P1) Central: fundo direito, possível recepção.",
            "(P2) Oposta: sobe para rede direita, ataque principal.",
            "(P3) Ponteira 1: central de rede, ataque rápido.",
            "(P4) Ponteira 2: atacante de entrada esquerda.",
            "(P5) Líbero: fundo esquerdo, reforço da defesa.",
        ]),
    ]

    parts = ["<div class='uv-grid'>"]
    overlays = []
    for i, (fname, titulo, bullets) in enumerate(rotacoes, start=1):
        zoom_id = f"uvzoom-{i}"
        img = img_to_data_uri(IMG_DIR / fname)
        lis = "".join([f"<li>{b}</li>" for b in bullets])
        if img:
            img_thumb = f'<a href="#{zoom_id}" class="uv-zoom-thumb"><img src="{img}" alt="{fname}"/></a>'
            overlay = f"""<div id="{zoom_id}" class="uv-lightbox">
  <a href="#uv-close" class="uv-lightbox__bg" aria-label="Fechar"></a>
  <div class="uv-content">
    <a href="#uv-close" class="uv-close" aria-label="Fechar">×</a>
    <img src="{img}" alt="Zoom {fname} — {titulo}"/>
  </div>
</div>"""
            overlays.append(overlay)
        else:
            img_thumb = f'<em>Imagem não encontrada ({fname})</em>'

        card = (
            "<div class='uv-col'>"
            "<div class='uv-card'>"
            f"<div class='uv-img'>{img_thumb}</div>"
            "<div class='uv-txt'>"
            f"<div class='uv-title'>{titulo}</div>"
            f"<ul class='uv-rodizio-text'>{lis}</ul>"
            "</div>"
            "</div>"
            "</div>"
        )
        parts.append(card)
    parts.append("</div>")  # fecha grid
    # Renderiza a grade + lightboxes
    st.markdown("".join(parts) + "".join(overlays), unsafe_allow_html=True)
    st.markdown("</div></div>", unsafe_allow_html=True)  # fecha uv-wrap + uv-tabbox
