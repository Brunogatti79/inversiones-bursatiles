from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class DocumentOut(BaseModel):
    id:          int
    file_name:   str
    file_type:   str
    market:      Optional[str]
    chunk_count: int
    indexed_at:  datetime
    modified_at: datetime
    size_bytes:  int

    class Config:
        from_attributes = True


class SearchRequest(BaseModel):
    query:     str
    market:    Optional[str] = None
    n_results: int = 8


class SearchResult(BaseModel):
    text:        str
    file_name:   str
    file_type:   str
    market:      Optional[str]
    score:       float
    chunk_index: int


class SearchResponse(BaseModel):
    results: list[SearchResult]
    total:   int


class AgentRequest(BaseModel):
    query:       str
    market:      Optional[str] = None
    output_type: str = "resumen"   # resumen | señales | riesgos | kpis | narrativa
    stream:      bool = False


class AgentResponse(BaseModel):
    answer:      str
    sources:     list[str]
    chunks_used: int
    output_type: str


class ReindexResponse(BaseModel):
    processed: int
    skipped:   int
    errors:    int
    message:   str


class StatusResponse(BaseModel):
    docs_folder:     str
    total_documents: int
    total_chunks:    int
    llm_provider:    str
    embedding_model: str
