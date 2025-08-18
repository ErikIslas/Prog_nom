# app_streamlit.py — Front Streamlit (muestra Fuente y Título)
import os
from pathlib import Path
import streamlit as st
import requests

st.set_page_config(page_title="Buscador CNBV", page_icon="🔎", layout="wide")

st.markdown("""
<style>
:root { --cnbv:#0f9ca3; }
.stButton>button { background: var(--cnbv); color:#fff; border:none; border-radius:9999px; padding:.6rem 1.2rem; font-weight:600; }
[data-baseweb="input"] input, .stNumberInput input { border-radius:9999px !important; }
.result-card { border:1px solid #e6e6e6; border-radius:16px; padding:14px 16px; margin-bottom:10px; background:#fff; }
.result-meta { font-size:.85rem; color:#c9d1d9; }
.progress { height:10px; background:#eee; border-radius:9999px; overflow:hidden; }
.progress>div { height:100%; background:var(--cnbv); }
.block-container h1:first-child { margin-top:.25rem; }
</style>
""", unsafe_allow_html=True)

# ---- URL de API con fallback robusto ----
DEFAULT_API = "http://127.0.0.1:8000/search"
def get_api_url():
    try:
        return st.secrets["API_URL"]
    except Exception:
        pass
    return os.getenv("API_URL", DEFAULT_API)
API_URL = get_api_url()

# ---- Encabezado ----
col_logo, col_title = st.columns([1,5], vertical_alignment="center")
with col_logo:
    logo_path = Path(__file__).with_name("logo_cnbv.png")
    if logo_path.exists():
        st.image(str(logo_path), width=120)
    else:
        st.markdown("### CNBV")
with col_title:
    st.markdown("<h2 style='margin:0'>Buscar en CNBV</h2>", unsafe_allow_html=True)
    st.caption("Ingrese su consulta sobre leyes, decretos o regulaciones para obtener un resumen claro y conciso generado por IA, con fuentes verificadas.")

st.divider()

# ---- Controles de búsqueda ----
left, mid, right = st.columns([5, 3, 2])
with left:
    query = st.text_input("", placeholder="Ej. 'Ley General de ...', 'artículo 1 banca múltiple'", label_visibility="collapsed")
with mid:
    tablas = st.multiselect(
        "Tablas",
        options=["documentos", "modificaciones", "anexos", "articulos"],
        default=["documentos", "modificaciones", "anexos", "articulos"],
        help="Puedes limitar a una o varias tablas"
    )
with right:
    limit = st.number_input("Resultados", min_value=1, max_value=100, value=10, step=1)

buscar = st.button("🔎 Buscar")

def llamar_api(query: str, limit: int, tablas: list[str]):
    if not query:
        return []
    params = {
        "query": query,
        "limit": limit,
        "tables": ",".join(tablas) if tablas else None
    }
    try:
        resp = requests.get(API_URL, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("results", data) if isinstance(data, dict) else data
        st.error(f"HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        st.error(f"No fue posible consultar la API ({e}). Verifica que FastAPI esté en: {API_URL}")
    return []

# ---- Ejecutar búsqueda ----
if buscar and query:
    with st.spinner("Buscando..."):
        resultados = llamar_api(query, int(limit), tablas)

    if resultados:
        st.success(f"{len(resultados)} resultado(s)")
        for i, res in enumerate(resultados, start=1):
            texto = res.get("texto") or res.get("content") or res.get("fragment") or "Sin texto"
            sim = res.get("similaridad") or res.get("score") or 0
            ruta = res.get("ruta_archivo") or res.get("url") or res.get("source")
            fuente = res.get("fuente")
            titulo = res.get("titulo")
            fecha = res.get("fecha_publicacion")
            id_doc = res.get("id_documento")

            st.markdown('<div class="result-card">', unsafe_allow_html=True)
            st.markdown(f"**{i}. {texto}**")

            # Meta: fuente, título, fecha, id_documento
            metas = []
            if fuente: metas.append(f"Fuente: {fuente}")
            if titulo: metas.append(f"Título: {titulo}")
            if fecha:  metas.append(f"Fecha: {fecha}")
            if id_doc: metas.append(f"id_documento: {id_doc}")
            if metas:
                st.markdown(f'<div class="result-meta">{" · ".join(metas)}</div>', unsafe_allow_html=True)

            # Barra de similitud
            try:
                val = float(sim)
                if val > 1.0:  # normaliza si vino 0..100
                    val = val / 100.0
                pct = int(val * 100)
                st.markdown(f'<div class="result-meta">Similaridad: {pct}%</div>', unsafe_allow_html=True)
                st.markdown(f'<div class="progress"><div style="width:{pct}%"></div></div>', unsafe_allow_html=True)
            except Exception:
                pass

            # Fuente/ruta si existe
            if ruta:
                is_url = str(ruta).startswith(("http://","https://"))
                if is_url:
                    st.markdown(f'<div class="result-meta">Ruta/Fuente: <a href="{ruta}" target="_blank">{ruta}</a></div>', unsafe_allow_html=True)
                else:
                    st.markdown(f'<div class="result-meta">Ruta/Fuente: {ruta}</div>', unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.info("No se encontraron resultados. Intenta con otras palabras clave.")
elif buscar and not query:
    st.warning("Escribe una consulta antes de buscar.")

st.caption(f"API: {API_URL}")
