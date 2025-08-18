import json
import os
import time
from typing import List, Tuple
import numpy as np
from mysql.connector.cursor import MySQLCursorDict
from .db import get_conn

TABLE = os.getenv("DB_TABLE", "tu_tabla")
PREFILTER_FULLTEXT = os.getenv("PREFILTER_FULLTEXT", "true").lower() == "true"
PREFILTER_LIMIT = int(os.getenv("PREFILTER_LIMIT", "500"))

def _cosine_topk(query_vec: np.ndarray, rows: List[dict], k: int) -> List[Tuple[dict, float]]:
    # rows: [{"id":.., "texto":.., "embedding": json_str}, ...]
    embs = []
    valid_rows = []
    for r in rows:
        try:
            e = np.array(json.loads(r["embedding"]), dtype=np.float32)
            if e.ndim != 1:
                continue
            embs.append(e)
            valid_rows.append(r)
        except Exception:
            continue

    if not embs:
        return []

    M = np.vstack(embs)  # (n, d)
    # Asumimos embeddings normalizados al indexar.
    sims = M @ query_vec  # producto punto == coseno
    top_idx = np.argsort(-sims)[:k]
    out = [(valid_rows[i], float(sims[i])) for i in top_idx]
    return out

def fetch_candidates(query: str) -> Tuple[List[dict], bool]:
    conn = get_conn()
    try:
        cur: MySQLCursorDict = conn.cursor(dictionary=True)
        if PREFILTER_FULLTEXT:
            sql = f"SELECT id, texto, embedding FROM {TABLE} WHERE MATCH(texto) AGAINST (%s IN NATURAL LANGUAGE MODE) LIMIT %s"
            cur.execute(sql, (query, PREFILTER_LIMIT))
            rows = cur.fetchall()
            return rows, True
        else:
            sql = f"SELECT id, texto, embedding FROM {TABLE}"
            cur.execute(sql)
            rows = cur.fetchall()
            return rows, False
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

def search_similar(query_vec: np.ndarray, query_text: str, k: int) -> Tuple[List[dict], float, int, bool]:
    t0 = time.time()
    rows, used_prefilter = fetch_candidates(query_text)
    top = _cosine_topk(query_vec, rows, k)
    took_ms = (time.time() - t0) * 1000.0
    results = [{
        "id": r["id"],
        "texto": r["texto"][:500],  # recorte simple para respuesta
        "score": score
    } for (r, score) in top]
    return results, took_ms, len(rows), used_prefilter
