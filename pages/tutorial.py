import os
from pathlib import Path
import streamlit as st

# --- Tentativa de usar PIL para obter dimensões das imagens (opcional) ---
try:
    from PIL import Image
    PIL_OK = True
except Exception:
    PIL_OK = False

st.set_page_config(
    page_title="Tutorial — UniVolei Live Scout",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ===== Helpers =====
BASE_DIR = Path(__file__).resolve().parent
IMG_DIR = BASE_DIR / "../imgs"

def img_width_scaled(path: Path, scale: float = 0.6) -> int | None:
    """
    Retorna a largura em px escalada pelo 'scale' (ex.: 0.6 = 60% do tamanho original).
    Se PIL não estiver disponível ou falhar, retorna None (Streamlit cuida do fallback).
    """
    if not PIL_OK:
        return None
    try:
        with Image.open(path) as im:
            w, _ = im.size
        w_scaled = max(1, int(w * scale))
        return w_scaled
    except Exception:
        return None

# ===== Título =====
st.markdown(
    "<h1 style='text-align:center; color:#1e3a8a; margin-top: .25rem'>📘 Tutorial UniVolei Live Scout</h1>",
    unsafe_allow_html=True
)

# ===== Estilos extras =====
st.markdown(
    """
    <style>
      .uv-card {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        box-shadow: 0 1px 4px rgba(15,23,42,.06);
        padding: 14px;
      }
      .uv-note {
        background:#eef2ff; border:1px solid #c7d2fe; padding:10px 12px; border-radius:10px;
      }
      .uv-muted { color:#64748b; }
      .uv-rodizio-text {
        font-size: 3em;              /* 3x maior */
        line-height: 1.15;
        font-weight: 700;
        color: #0f172a;
        margin-top: .25rem;
        margin-bottom: 1rem;
      }
      .uv-list li { margin: .25rem 0; }
      .uv-section-title {
        margin: .3rem 0 .8rem 0;
        font-weight: 800;
        color: #0f172a;
      }
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown("<div class='topbar-row'>", unsafe_allow_html=True)
# Spacer gigante + coluna do botão
spacer, c_close = st.columns([12, 1])  # aumente o 12 se quiser mais “empurro” para a direita
with c_close:
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

# -------------------------------
# Tab 1 - Início (texto à esquerda, imagem à direita)
# -------------------------------
with tab1:
    c_text, c_img = st.columns([3, 2], gap="large")

    with c_text:
        st.markdown("### 🔹 Acesso inicial", help="Informações básicas de navegação")
        st.markdown(
            """
            - Entre no endereço: **https://univolei-scout.streamlit.app/**  
            - Caso exista **jogo em aberto**, o título e a data aparecerão automaticamente no cabeçalho.  
            - Na **primeira linha** você encontra os botões principais:
              - **Time** → cadastrar/editar o time e jogadoras.
              - **Jogo** → iniciar ou continuar uma partida em andamento.
              - **Tutorial** → abre esta página de instruções.
              - **Histórico** → acessar estatísticas e análises de jogos anteriores.
            
            ---
            O aplicativo salva dados em **Excel, DuckDB e Google Sheets** (quando habilitado), garantindo backup e histórico.
            """,
            unsafe_allow_html=True
        )

    with c_img:
        img_path = IMG_DIR / "print_1.jpg"
        # Sem esticar: manter tamanho natural
        if img_path.exists():
            st.image(str(img_path), caption="Tela inicial do aplicativo", use_container_width=False)
        else:
            st.warning(f"Imagem não encontrada: {img_path}")

# -------------------------------
# Tab 2 - Modo Jogo (texto à esquerda, imagem à direita)
# -------------------------------
with tab2:
    c_text, c_img = st.columns([3, 2], gap="large")

    with c_text:
        st.markdown("### 🔹 Modo Jogo (principal)", help="Área de marcação de pontos")
        st.markdown(
            """
            O **Modo Jogo** é o coração do sistema: é aqui que você registra todas as jogadas da partida.  

            - **Botões de Jogadoras** → clique para marcar quem participou do rally.  
              - 1º clique = **Acerto** ✅  
              - 2º clique = **Erro** ❌  
            - **Botão ADV** → registra pontos do adversário.  
            - **Quadra Interativa (Heatmap)** → clique na quadra para marcar a região de cada ação.  
            - **Placar em tempo real** → exibido sempre acima da quadra.  
            - **Gestão de Sets** → abrir, fechar e finalizar sets; remover set vazio quando necessário.

            ---
            🔑 **Importante:**  
            O **Modo Jogo é o principal local de marcação de pontos**.  
            Cada ação registrada aqui alimenta as estatísticas do **Histórico** e direciona os treinos.
            """,
            unsafe_allow_html=True
        )

    with c_img:
        img_path = IMG_DIR / "print_2.jpg"
        # Sem esticar: manter tamanho natural
        if img_path.exists():
            st.image(str(img_path), caption="Modo Jogo — principal área de marcação de pontos", use_container_width=False)
        else:
            st.warning(f"Imagem não encontrada: {img_path}")

# -------------------------------
# Tab 3 - Histórico (sem imagem, apenas descrição)
# -------------------------------
with tab3:
    st.markdown("### 🔹 Histórico de Jogos", help="Dashboard analítico")
    st.markdown(
        """
        O **Histórico** é o **dashboard central de análise**.  

        **Principais recursos:**
        - 📋 **Lista de jogos**: ID, data, adversário, sets e status (aberto/fechado).  
        - ✅ Resultado destacado: Vitória, Derrota ou Empate.  
        - 🔍 **Filtros e buscas** por ID, data, adversário e status.  
        - 📊 **Estatísticas detalhadas** de cada partida:
          - Evolução do placar ao longo dos sets.
          - Comparativo por fundamento (ataque, passe, saque etc.).
          - Erros cometidos organizados em tabela.
          - Mapas de calor das jogadas (zonas de ataque/defesa).

        ---
        💡 **Reforço:**  
        O **Histórico é o local-chave para análise de desempenho e definição de treinos específicos**.  
        """,
        unsafe_allow_html=True
    )

# -------------------------------
# Tab 4 - Rodízio 5x1 (imagens 40% menores + textos 3x maiores)
# -------------------------------
with tab4:
    st.markdown("### 🔹 Rodízio 5x1 — Referência visual e explicativa")

    # Lista de (arquivo, texto)
    rod_items = [
        ("p1.jpg", "**P1 (Levantadora):** posição 1, arma jogadas rápidas, cobre defesa direita."),
        ("p2.jpg", "**P2 (Oposta):** atacante pela direita, responsável também por bolas de fundo."),
        ("p3.jpg", "**P3 (Central):** ataque rápido pelo meio, foco em bloqueios centrais."),
        ("p4.jpg", "**P4 (Ponteira):** atacante pela esquerda, importante no passe e coberturas."),
        ("p5.jpg", "**P5 (Defensora/Ponteira):** fundo esquerdo, prioridade em recepção."),
        ("p6.jpg", "**P6 (Líbero ou Ponteira de fundo):** fundo central, defesa principal e recepção de saque."),
    ]

    for fname, txt in rod_items:
        img_path = IMG_DIR / fname

        # Layout: imagem à esquerda (menor), texto gigante à direita
        c_img, c_txt = st.columns([2, 3], gap="large")

        with c_img:
            if img_path.exists():
                # Tenta calcular largura ~60% do original (40% menor)
                width_scaled = img_width_scaled(img_path, scale=0.6)
                if width_scaled is not None:
                    st.image(str(img_path), caption=f"Rodízio — {fname[:-4].upper()}", width=width_scaled)
                else:
                    # Fallback: se não conseguir medir, usa container_width com clamp via column
                    st.image(str(img_path), caption=f"Rodízio — {fname[:-4].upper()}", use_container_width=False)
            else:
                st.warning(f"Imagem não encontrada: {img_path}")

        with c_txt:
            # Texto 3x maior (CSS)
            st.markdown(f"<div class='uv-rodizio-text'>{txt}</div>", unsafe_allow_html=True)
