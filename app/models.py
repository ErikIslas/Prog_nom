from pydantic import BaseModel
from typing import List, Optional

class SearchResult(BaseModel):
    id: int
    texto: str
    score: float

class SearchResponse(BaseModel):
    results: List[SearchResult]
    took_ms: float
    total_examined: int
    prefilter_used: bool
