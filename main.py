# main.py — API de búsqueda semántica (MySQL + múltiples tablas)
import os
from dataclasses import dataclass
from typing import List, Optional, Dict

import numpy as np
import mysql.connector as mysql
from fastapi import FastAPI, Query
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

# ========== Configuración ==========
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASSWORD", "1234")
DB_NAME = os.getenv("DB_NAME", "buscador_normativo")

# Listado de tablas a consultar (por defecto TODAS las soportadas)
TABLES = [t.strip() for t in os.getenv(
    "TABLES",
    "documentos,modificaciones,anexos,articulos"
).split(",") if t.strip()]

MODEL_NAME = os.getenv("MODEL_NAME", "intfloat/multilingual-e5-base")
USE_E5_PREFIX = True  # Para e5, prefijos "query: " / "passage: "


# ========== Esquema por tabla ==========
@dataclass
class TableConf:
    table: str
    id_col: str
    text_col: str
    embed_cols: List[str]                 # se tomará el primero NO nulo
    ruta_col: Optional[str] = None
    id_doc_col: Optional[str] = None
    title_col: Optional[str] = None
    date_col: Optional[str] = None

# Ajusta estas configs a tu esquema real
TABLE_CONFIGS: Dict[str, TableConf] = {
    # ---- DOCUMENTOS ----
    "documentos": TableConf(
        table="documentos",
        id_col="id_documento",
        text_col="nombre_regulacion",                 # Texto a mostrar
        embed_cols=["embedding_completo"],            # Embedding del documento
        ruta_col="ruta_archivo",
        title_col="nombre_regulacion",
        date_col="fecha_publicacion",
    ),
    # ---- MODIFICACIONES ----
    "modificaciones": TableConf(
        table="modificaciones",
        id_col="id_modificacion",
        id_doc_col="id_documento",
        text_col="texto_modificacion",
        embed_cols=["embedding_texto_modificacion"],  # Embedding por fragmento
        ruta_col="ruta_archivo",
        title_col="nombre_regulacion",
        date_col="fecha_publicacion",
    ),
    # ---- ANEXOS ----
    # Estructura dada:
    # id_anexo, id_documento, nombre_anexo, texto_anexo, ruta_archivo,
    # embedding_completo, embedding_texto
    "anexos": TableConf(
        table="anexos",
        id_col="id_anexo",
        id_doc_col="id_documento",
        text_col="texto_anexo",
        embed_cols=["embedding_texto", "embedding_completo"],  # intenta texto, luego completo
        ruta_col="ruta_archivo",
        title_col="nombre_anexo",
        date_col=None,
    ),
    # ---- ARTICULOS ----
    # id_articulo, id_documento, numero_articulo, texto_articulo, embedding_articulo
    "articulos": TableConf(
        table="articulos",
        id_col="id_articulo",
        id_doc_col="id_documento",
        text_col="texto_articulo",
        embed_cols=["embedding_articulo"],
        ruta_col=None,
        title_col="numero_articulo",  # se mostrará como "Título"
        date_col=None,
    ),
}

# ========== App/Modelo ==========
app = FastAPI(title="Buscador semántico CNBV (multi-tabla)")
model = SentenceTransformer(MODEL_NAME)

class Resultado(BaseModel):
    id: int
    fuente: str                       # nombre de la tabla
    id_documento: Optional[int] = None
    titulo: Optional[str] = None
    texto: str
    similaridad: float
    ruta_archivo: Optional[str] = None
    fecha_publicacion: Optional[str] = None  # YYYY-MM-DD

class Respuesta(BaseModel):
    results: List[Resultado]


# ========== Utilidades ==========
def get_conn():
    return mysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME
    )

def blob_to_vec(blob: bytes) -> np.ndarray:
    return np.frombuffer(blob, dtype=np.float32)

def encode_query(q: str) -> np.ndarray:
    if USE_E5_PREFIX:
        q = "query: " + q
    v = model.encode(q, normalize_embeddings=True)  # normaliza q
    return v.astype(np.float32)

def fetch_rows(conf: TableConf):
    # Construye SELECT con aliases genéricos
    base_cols = {
        conf.id_col: "_id",
        conf.text_col: "_text",
    }
    if conf.ruta_col:    base_cols[conf.ruta_col]  = "_ruta"
    if conf.id_doc_col:  base_cols[conf.id_doc_col]= "_id_doc"
    if conf.title_col:   base_cols[conf.title_col] = "_title"
    if conf.date_col:    base_cols[conf.date_col]  = "_date"

    # Embeddings: alias _emb0, _emb1, ...
    emb_aliases = []
    for i, c in enumerate(conf.embed_cols):
        emb_aliases.append((c, f"_emb{i}"))

    select_parts = [f"{col} AS {alias}" for col, alias in base_cols.items()]
    select_parts += [f"{col} AS {alias}" for col, alias in emb_aliases]

    sql = f"""
        SELECT {', '.join(select_parts)}
        FROM {conf.table}
        WHERE {conf.text_col} IS NOT NULL
          AND ({' OR '.join([f'{c} IS NOT NULL' for c in conf.embed_cols])})
    """

    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(sql)
    for row in cur:
        yield row
    cur.close()
    conn.close()


# ========== Endpoint ==========
@app.get("/search", response_model=Respuesta)
def search(
    query: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=100),
    tables: Optional[str] = Query(None, description="Lista separada por comas para filtrar tablas")
):
    q_vec = encode_query(query)

    # Determina qué tablas usar
    tables_to_use = [t.strip() for t in (tables.split(",") if tables else TABLES)]
    # Filtra por las que tengan config
    tables_to_use = [t for t in tables_to_use if t in TABLE_CONFIGS]

    resultados: List[Resultado] = []

    for tname in tables_to_use:
        conf = TABLE_CONFIGS[tname]
        try:
            for row in fetch_rows(conf):
                # Toma el primer embedding disponible
                emb_blob = None
                for i in range(len(conf.embed_cols)):
                    emb_blob = row.get(f"_emb{i}")
                    if emb_blob:
                        break
                if not emb_blob:
                    continue

                v = blob_to_vec(emb_blob)
                denom = np.linalg.norm(v) + 1e-12  # por si no están normalizados en BD
                sim = float(np.dot(q_vec, v) / denom)

                resultados.append(
                    Resultado(
                        id=row["_id"],
                        fuente=tname,
                        id_documento=row.get("_id_doc"),
                        titulo=row.get("_title"),
                        texto=row["_text"],
                        similaridad=sim,
                        ruta_archivo=row.get("_ruta"),
                        fecha_publicacion=str(row.get("_date")) if row.get("_date") else None,
                    )
                )
        except Exception as e:
            # No detenga toda la búsqueda si una tabla falla
            print(f"[WARN] Falló tabla {tname}: {e}")

    # Orden global y top-k
    resultados.sort(key=lambda r: r.similaridad, reverse=True)
    return {"results": resultados[:limit]}

@app.get("/")
def root():
    return {"status": "ok", "tables": TABLES, "model": MODEL_NAME}
