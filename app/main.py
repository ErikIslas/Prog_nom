import os
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
from sentence_transformers import SentenceTransformer
import numpy as np
from dotenv import load_dotenv

from .search import search_similar
from .models import SearchResponse, SearchResult

load_dotenv()

MODEL_NAME = os.getenv("MODEL_NAME", "intfloat/multilingual-e5-base")
TOP_K_DEFAULT = int(os.getenv("TOP_K", "10"))

app = FastAPI(title="Semantic Search API (MySQL + ST)",
              version="0.1.0",
              description="FastAPI que busca por similitud de coseno usando embeddings almacenados en MySQL (JSON).")

# CORS (desarrollo)
allow_origins = os.getenv("CORS_ALLOW_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cargar modelo global (mismo que al indexar)
model = SentenceTransformer(MODEL_NAME)

@app.get("/health")
def health():
    return {"status": "ok", "model": MODEL_NAME}

@app.get("/search", response_model=SearchResponse)
def search(query: str = Query(..., min_length=1), k: int = Query(TOP_K_DEFAULT, ge=1, le=100)):
    # 1) Embedding normalizado para coseno
    qvec = model.encode(query, normalize_embeddings=True)
    if not isinstance(qvec, np.ndarray):
        qvec = np.array(qvec, dtype=np.float32)
    qvec = qvec.astype(np.float32)

    # 2) Buscar candidatos y rankear por coseno
    results, took_ms, total_examined, prefilter_used = search_similar(qvec, query, k)

    return SearchResponse(
        results=[SearchResult(**r) for r in results],
        took_ms=round(took_ms, 3),
        total_examined=total_examined,
        prefilter_used=prefilter_used,
    )
