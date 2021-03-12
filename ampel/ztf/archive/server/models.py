
from typing import List, Dict, Any
from pydantic import BaseModel

class AlertChunk(BaseModel):
    resume_token: str
    chunk_size: int
    chunks_remaining: int
    alerts: List[Dict[str, Any]]