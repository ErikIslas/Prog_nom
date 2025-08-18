import os
import sys
import math
import time
import argparse
import numpy as np
from typing import List, Tuple

import mysql.connector as mysql
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

# =======================
# CONFIGURACIÓN GENERAL
# =======================
CONFIG = {
    "MODEL_NAME": "intfloat/multilingual-e5-base",  # Alternativa: "BAAI/bge-m3"
    "DB_BATCH": 200,        # cuántas filas pedimos por lote desde MySQL
    "ENC_BATCH": 32,        # tamaño de batch para model.encode
    "MAX_CHARS": 20000,     # recorte duro por texto antes de embed
    "ONLY_NULLS": True,     # solo filas cuyo embedding destino sea NULL
    "NORMALIZE": True,      # normalizar embeddings (recomendado)
    "USE_PYMUPDF": True,    # para extraer texto de PDFs cuando aplique
    "PDF_MAX_PAGES": 200,   # límite de páginas a leer por PDF
    "CHUNK_CHARS": 3000,    # tamaño de chunk si el texto es largo
    "CHUNK_OVERLAP": 300,   # solape entre chunks
}

# Carga condicional de PyMuPDF
if CONFIG["USE_PYMUPDF"]:
    try:
        import fitz  # PyMuPDF
    except Exception as e:
        print("[WARN] PyMuPDF no disponible. Desactiva USE_PYMUPDF o instala pymupdf.", e)
        CONFIG["USE_PYMUPDF"] = False


# =======================
# CONEXIÓN A BASE DE DATOS
# =======================
def connect_db():
    load_dotenv()

    # Nombres estándar; con fallbacks a tus nombres previos
    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", "3306"))
    user = os.getenv("DB_USER") or os.getenv("MYSQL_USER") or os.getenv("DB_root")
    password = os.getenv("DB_PASSWORD") or os.getenv("MYSQL_PASSWORD") or os.getenv("DB_1234")
    database = (
        os.getenv("DB_NAME")
        or os.getenv("MYSQL_DATABASE")
        or os.getenv("DB_buscador_normativo")
        or os.getenv("DB_buscador_normativo.sql")
    )

    cfg = dict(host=host, port=port, user=user, password=password, database=database)

    missing = [k for k, v in cfg.items() if v in (None, "")]
    if missing:
        print("[ERROR] Faltan variables .env:", missing)
        print("Ejemplo de .env:")
        print("DB_HOST=localhost")
        print("DB_PORT=3306")
        print("DB_USER=tu_usuario_mysql")
        print("DB_PASSWORD=tu_contraseña_mysql")
        print("DB_NAME=buscador_normativo")
        sys.exit(1)

    return mysql.connect(**cfg)


# =======================
# UTILIDADES
# =======================
def as_bytes_float32(vec: np.ndarray) -> bytes:
    if vec.dtype != np.float32:
        vec = vec.astype(np.float32)
    return vec.tobytes()

def mean_pool(vectors: List[np.ndarray]) -> np.ndarray:
    if not vectors:
        return np.zeros((384,), dtype=np.float32)  # tamaño por defecto; el modelo lo definirá realmente
    M = np.vstack(vectors).astype(np.float32)
    return M.mean(axis=0)

def chunk_text(s: str, max_chars: int, chunk: int, overlap: int) -> List[str]:
    s = (s or "").strip()
    if not s:
        return []
    if max_chars and len(s) > max_chars:
        s = s[:max_chars]
    if len(s) <= chunk:
        return [s]
    out = []
    start = 0
    while start < len(s):
        end = min(len(s), start + chunk)
        out.append(s[start:end])
        if end == len(s):
            break
        start = max(end - overlap, start + 1)
    return out

def preprocess_for_e5(txt: str) -> str:
    # E5 / bge suelen mejorar con prefijo "passage: "
    return "passage: " + (txt or "").strip()

def encode_texts(model: SentenceTransformer, texts: List[str], normalize=True, enc_batch=32) -> np.ndarray:
    if not texts:
        return np.zeros((0, 384), dtype=np.float32)
    emb = model.encode(
        texts,
        batch_size=enc_batch,
        convert_to_numpy=True,
        normalize_embeddings=normalize
    )
    if emb.dtype != np.float32:
        emb = emb.astype(np.float32)
    return emb

def extract_pdf_text(path: str, max_pages: int = 200) -> str:
    if not CONFIG["USE_PYMUPDF"] or not path:
        return ""
    try:
        doc = fitz.open(path)
    except Exception:
        return ""
    texts = []
    pages = min(len(doc), max_pages)
    for i in range(pages):
        try:
            texts.append(doc[i].get_text("text"))
        except Exception:
            continue
    doc.close()
    return "\n".join(texts).strip()

def count_rows(conn, table: str, where: str) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM `{table}` {where}")
    n = int(cur.fetchone()[0])
    cur.close()
    return n

def fetch_ids(conn, table: str, id_field: str, where: str, limit: int, offset: int) -> List[int]:
    cur = conn.cursor()
    q = f"SELECT `{id_field}` FROM `{table}` {where} ORDER BY `{id_field}` ASC LIMIT %s OFFSET %s"
    cur.execute(q, (limit, offset))
    rows = [r[0] for r in cur.fetchall()]
    cur.close()
    return rows

def update_blob(conn, table: str, id_field: str, target_field: str, pairs: List[Tuple[int, np.ndarray]]):
    if not pairs:
        return
    cur = conn.cursor()
    q = f"UPDATE `{table}` SET `{target_field}`=%s WHERE `{id_field}`=%s"
    cur.executemany(q, [(as_bytes_float32(v), _id) for _id, v in pairs])
    conn.commit()
    cur.close()

def build_where(only_nulls: bool, target_field: str, extra: str = "") -> str:
    parts = []
    if only_nulls:
        parts.append(f"`{target_field}` IS NULL")
    if extra.strip():
        parts.append(f"({extra.strip()})")
    return ("WHERE " + " AND ".join(parts)) if parts else ""


# =======================
# PIPELINES POR TABLA
# =======================
def process_documentos(model: SentenceTransformer, conn, args):
    """
    Embeddings por campo en `documentos`:
      nombre_regulacion -> embedding_nombre
      ambito_aplicacion -> embedding_ambito
      tipo_de_ordenamiento -> embedding_tipo
      emisor -> embedding_emisor
      embedding_completo (opcional): desde PDF (ruta_archivo) o concatenando campos si no hay PDF
    """
    table = "documentos"
    idf = "id_documento"

    tasks = [
        ("nombre_regulacion", "embedding_nombre", "nombre_regulacion"),
        ("ambito_aplicacion", "embedding_ambito", "ambito_aplicacion"),
        ("tipo_de_ordenamiento", "embedding_tipo", "tipo_de_ordenamiento"),
        ("emisor", "embedding_emisor", "emisor"),
    ]

    for src, dst, _ in tasks:
        where = build_where(CONFIG["ONLY_NULLS"], dst)
        total = count_rows(conn, table, where)
        if total == 0:
            print(f"[documentos] {dst}: nada por hacer.")
            continue

        print(f"[documentos] {dst}: {total} filas a procesar.")
        processed = 0
        batches = math.ceil(total / CONFIG["DB_BATCH"])
        for b in range(batches):
            ids = fetch_ids(conn, table, idf, where, CONFIG["DB_BATCH"], b * CONFIG["DB_BATCH"])
            if not ids:
                break

            placeholders = ",".join(["%s"] * len(ids))
            cur = conn.cursor()
            cur.execute(
                f"SELECT `{idf}`, `{src}` FROM `{table}` WHERE `{idf}` IN ({placeholders})",
                ids
            )
            rows = cur.fetchall()
            cur.close()

            # Construir lista plana de textos (con chunking)
            flat_texts, map_rows = [], []
            for _id, txt in rows:
                s = "" if txt is None else str(txt).strip()
                chunks = chunk_text(s, CONFIG["MAX_CHARS"], CONFIG["CHUNK_CHARS"], CONFIG["CHUNK_OVERLAP"])
                if not chunks:
                    continue
                for ch in chunks:
                    flat_texts.append(preprocess_for_e5(ch))
                    map_rows.append((_id, True))

            if not flat_texts:
                print(f"[documentos] {dst}: lote {b+1}/{batches} vacío.")
                continue

            t0 = time.time()
            emb = encode_texts(model, flat_texts, normalize=CONFIG["NORMALIZE"], enc_batch=CONFIG["ENC_BATCH"])
            took = time.time() - t0

            by_id = {}
            for (row_id, _), vec in zip(map_rows, emb):
                by_id.setdefault(row_id, []).append(vec)
            pairs = [(row_id, mean_pool(vecs)) for row_id, vecs in by_id.items()]

            update_blob(conn, table, idf, dst, pairs)
            processed += len(pairs)
            print(f"[OK documentos] {dst} lote {b+1}/{batches}: {len(pairs)} filas en {took:.2f}s | {processed}/{total}")

    # Opcional: embedding_completo desde PDF o concatenación
    dst = "embedding_completo"
    where = build_where(CONFIG["ONLY_NULLS"], dst)
    total = count_rows(conn, table, where)
    if total > 0:
        print(f"[documentos] {dst}: {total} filas a procesar (PDF o concat).")
        batches = math.ceil(total / CONFIG["DB_BATCH"])
        for b in range(batches):
            ids = fetch_ids(conn, table, idf, where, CONFIG["DB_BATCH"], b * CONFIG["DB_BATCH"])
            if not ids:
                break
            placeholders = ",".join(["%s"] * len(ids))
            cur = conn.cursor()
            cur.execute(
                f"""SELECT `{idf}`, `ruta_archivo`, `nombre_regulacion`, `ambito_aplicacion`,
                           `tipo_de_ordenamiento`, `emisor`
                    FROM `{table}` WHERE `{idf}` IN ({placeholders})""",
                ids
            )
            rows = cur.fetchall()
            cur.close()

            flat_texts, map_rows = [], []
            for _id, ruta, nombre, ambito, tipo, emisor in rows:
                text = ""
                if CONFIG["USE_PYMUPDF"] and ruta and str(ruta).strip():
                    text = extract_pdf_text(str(ruta).strip(), CONFIG["PDF_MAX_PAGES"])
                if not text:
                    parts = [nombre, ambito, tipo, emisor]
                    text = " | ".join([p for p in parts if p]) or ""
                chunks = chunk_text(text, CONFIG["MAX_CHARS"], CONFIG["CHUNK_CHARS"], CONFIG["CHUNK_OVERLAP"])
                if not chunks:
                    continue
                for ch in chunks:
                    flat_texts.append(preprocess_for_e5(ch))
                    map_rows.append((_id, True))

            if not flat_texts:
                print(f"[documentos] {dst}: lote {b+1}/{batches} sin textos.")
                continue

            t0 = time.time()
            emb = encode_texts(model, flat_texts, normalize=CONFIG["NORMALIZE"], enc_batch=CONFIG["ENC_BATCH"])
            took = time.time() - t0

            by_id = {}
            for (row_id, _), vec in zip(map_rows, emb):
                by_id.setdefault(row_id, []).append(vec)
            pairs = [(rid, mean_pool(vs)) for rid, vs in by_id.items()]
            update_blob(conn, table, idf, dst, pairs)
            print(f"[OK documentos] {dst} lote {b+1}/{batches}: {len(pairs)} filas en {took:.2f}s.")


def process_articulos(model: SentenceTransformer, conn, args):
    """
    articulos.texto_articulo -> articulos.embedding_articulo
    """
    table, idf, src, dst = "articulos", "id_articulo", "texto_articulo", "embedding_articulo"
    where = build_where(CONFIG["ONLY_NULLS"], dst)
    total = count_rows(conn, table, where)
    if total == 0:
        print("[articulos] nada por hacer.")
        return

    print(f"[articulos] {total} filas a procesar.")
    processed = 0
    batches = math.ceil(total / CONFIG["DB_BATCH"])
    for b in range(batches):
        ids = fetch_ids(conn, table, idf, where, CONFIG["DB_BATCH"], b * CONFIG["DB_BATCH"])
        if not ids:
            break
        placeholders = ",".join(["%s"] * len(ids))
        cur = conn.cursor()
        cur.execute(f"SELECT `{idf}`, `{src}` FROM `{table}` WHERE `{idf}` IN ({placeholders})", ids)
        rows = cur.fetchall()
        cur.close()

        flat_texts, map_rows = [], []
        for _id, txt in rows:
            s = "" if txt is None else str(txt)
            chunks = chunk_text(s, CONFIG["MAX_CHARS"], CONFIG["CHUNK_CHARS"], CONFIG["CHUNK_OVERLAP"])
            if not chunks:
                continue
            for ch in chunks:
                flat_texts.append(preprocess_for_e5(ch))
                map_rows.append((_id, True))

        if not flat_texts:
            print(f"[articulos] lote {b+1}/{batches} vacío.")
            continue

        t0 = time.time()
        emb = encode_texts(model, flat_texts, normalize=CONFIG["NORMALIZE"], enc_batch=CONFIG["ENC_BATCH"])
        took = time.time() - t0

        by_id = {}
        for (rid, _), vec in zip(map_rows, emb):
            by_id.setdefault(rid, []).append(vec)
        pairs = [(rid, mean_pool(vs)) for rid, vs in by_id.items()]
        update_blob(conn, table, idf, dst, pairs)
        processed += len(pairs)
        print(f"[OK articulos] lote {b+1}/{batches}: {len(pairs)} filas en {took:.2f}s | {processed}/{total}")


def process_modificaciones(model: SentenceTransformer, conn, args):
    """
    modificaciones.texto_modificacion -> modificaciones.embedding_completo
    """
    table, idf, src, dst = "modificaciones", "id_modificacion", "texto_modificacion", "embedding_completo"
    where = build_where(CONFIG["ONLY_NULLS"], dst)
    total = count_rows(conn, table, where)
    if total == 0:
        print("[modificaciones] nada por hacer.")
        return

    print(f"[modificaciones] {total} filas a procesar.")
    processed = 0
    batches = math.ceil(total / CONFIG["DB_BATCH"])
    for b in range(batches):
        ids = fetch_ids(conn, table, idf, where, CONFIG["DB_BATCH"], b * CONFIG["DB_BATCH"])
        if not ids:
            break
        placeholders = ",".join(["%s"] * len(ids))
        cur = conn.cursor()
        cur.execute(f"SELECT `{idf}`, `{src}` FROM `{table}` WHERE `{idf}` IN ({placeholders})", ids)
        rows = cur.fetchall()
        cur.close()

        flat_texts, map_rows = [], []
        for _id, txt in rows:
            s = "" if txt is None else str(txt)
            chunks = chunk_text(s, CONFIG["MAX_CHARS"], CONFIG["CHUNK_CHARS"], CONFIG["CHUNK_OVERLAP"])
            if not chunks:
                continue
            for ch in chunks:
                flat_texts.append(preprocess_for_e5(ch))
                map_rows.append((_id, True))

        if not flat_texts:
            print(f"[modificaciones] lote {b+1}/{batches} vacío.")
            continue

        t0 = time.time()
        emb = encode_texts(model, flat_texts, normalize=CONFIG["NORMALIZE"], enc_batch=CONFIG["ENC_BATCH"])
        took = time.time() - t0

        by_id = {}
        for (rid, _), vec in zip(map_rows, emb):
            by_id.setdefault(rid, []).append(vec)
        pairs = [(rid, mean_pool(vs)) for rid, vs in by_id.items()]
        update_blob(conn, table, idf, dst, pairs)
        processed += len(pairs)
        print(f"[OK modificaciones] lote {b+1}/{batches}: {len(pairs)} filas en {took:.2f}s | {processed}/{total}")


def process_anexos(model: SentenceTransformer, conn, args):
    """
    anexos.texto_anexo -> embedding_texto
    anexos.embedding_completo -> desde PDF si hay ruta_archivo; si no, concat(nombre_anexo + texto_anexo)
    """
    table, idf = "anexos", "id_anexo"

    # 1) embedding_texto
    dst = "embedding_texto"
    where = build_where(CONFIG["ONLY_NULLS"], dst)
    total = count_rows(conn, table, where)
    if total > 0:
        print(f"[anexos] {dst}: {total} filas a procesar.")
        batches = math.ceil(total / CONFIG["DB_BATCH"])
        for b in range(batches):
            ids = fetch_ids(conn, table, idf, where, CONFIG["DB_BATCH"], b * CONFIG["DB_BATCH"])
            if not ids:
                break
            placeholders = ",".join(["%s"] * len(ids))
            cur = conn.cursor()
            cur.execute(f"SELECT `{idf}`, `texto_anexo` FROM `{table}` WHERE `{idf}` IN ({placeholders})", ids)
            rows = cur.fetchall()
            cur.close()

            flat_texts, map_rows = [], []
            for _id, txt in rows:
                s = "" if txt is None else str(txt)
                chunks = chunk_text(s, CONFIG["MAX_CHARS"], CONFIG["CHUNK_CHARS"], CONFIG["CHUNK_OVERLAP"])
                if not chunks:
                    continue
                for ch in chunks:
                    flat_texts.append(preprocess_for_e5(ch))
                    map_rows.append((_id, True))

            if flat_texts:
                t0 = time.time()
                emb = encode_texts(model, flat_texts, normalize=CONFIG["NORMALIZE"], enc_batch=CONFIG["ENC_BATCH"])
                took = time.time() - t0

                by_id = {}
                for (rid, _), vec in zip(map_rows, emb):
                    by_id.setdefault(rid, []).append(vec)
                pairs = [(rid, mean_pool(vs)) for rid, vs in by_id.items()]
                update_blob(conn, table, idf, dst, pairs)
                print(f"[OK anexos] {dst} lote {b+1}/{batches}: {len(pairs)} filas en {took:.2f}s.")

    # 2) embedding_completo (PDF o concat)
    dst = "embedding_completo"
    where = build_where(CONFIG["ONLY_NULLS"], dst)
    total = count_rows(conn, table, where)
    if total == 0:
        print("[anexos] embedding_completo: nada por hacer.")
        return

    print(f"[anexos] {dst}: {total} filas a procesar (PDF o concat).")
    batches = math.ceil(total / CONFIG["DB_BATCH"])
    for b in range(batches):
        ids = fetch_ids(conn, table, idf, where, CONFIG["DB_BATCH"], b * CONFIG["DB_BATCH"])
        if not ids:
            break
        placeholders = ",".join(["%s"] * len(ids))
        cur = conn.cursor()
        cur.execute(
            f"SELECT `{idf}`, `ruta_archivo`, `nombre_anexo`, `texto_anexo` FROM `{table}` WHERE `{idf}` IN ({placeholders})",
            ids
        )
        rows = cur.fetchall()
        cur.close()

        flat_texts, map_rows = [], []
        for _id, ruta, nombre, texto in rows:
            text = ""
            if CONFIG["USE_PYMUPDF"] and ruta and str(ruta).strip():
                text = extract_pdf_text(str(ruta).strip(), CONFIG["PDF_MAX_PAGES"])
            if not text:
                parts = [nombre, texto]
                text = " | ".join([p for p in parts if p]) or ""
            chunks = chunk_text(text, CONFIG["MAX_CHARS"], CONFIG["CHUNK_CHARS"], CONFIG["CHUNK_OVERLAP"])
            if not chunks:
                continue
            for ch in chunks:
                flat_texts.append(preprocess_for_e5(ch))
                map_rows.append((_id, True))

        if not flat_texts:
            print(f"[anexos] {dst}: lote {b+1}/{batches} sin textos.")
            continue

        t0 = time.time()
        emb = encode_texts(model, flat_texts, normalize=CONFIG["NORMALIZE"], enc_batch=CONFIG["ENC_BATCH"])
        took = time.time() - t0

        by_id = {}
        for (rid, _), vec in zip(map_rows, emb):
            by_id.setdefault(rid, []).append(vec)
        pairs = [(rid, mean_pool(vs)) for rid, vs in by_id.items()]
        update_blob(conn, table, idf, dst, pairs)
        print(f"[OK anexos] {dst} lote {b+1}/{batches}: {len(pairs)} filas en {took:.2f}s.")


# =======================
# CLI
# =======================
def main():
    parser = argparse.ArgumentParser(description="Generador de embeddings (MySQL)")
    parser.add_argument("--tables", nargs="+", default=["documentos", "articulos", "modificaciones", "anexos"],
                        help="Qué tablas procesar: documentos articulos modificaciones anexos")
    parser.add_argument("--only-nulls", action="store_true", help="Procesa solo filas con embedding NULL (default)")
    parser.add_argument("--all", action="store_true", help="Procesa todas las filas (ignora ONLY_NULLS)")
    args = parser.parse_args()

    if args.all:
        CONFIG["ONLY_NULLS"] = False
    if args.only_nulls:
        CONFIG["ONLY_NULLS"] = True

    print(f"[INFO] Modelo: {CONFIG['MODEL_NAME']}")
    model = SentenceTransformer(CONFIG["MODEL_NAME"])

    conn = connect_db()
    try:
        tabs = set([t.lower() for t in args.tables])
        if "documentos" in tabs:
            process_documentos(model, conn, args)
        if "articulos" in tabs:
            process_articulos(model, conn, args)
        if "modificaciones" in tabs:
            process_modificaciones(model, conn, args)
        if "anexos" in tabs:
            process_anexos(model, conn, args)
    finally:
        conn.close()
        print("[DONE] Proceso completado.")

if __name__ == "__main__":
    main()
