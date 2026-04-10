# routers/query.py
from fastapi import APIRouter
from pydantic import BaseModel
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.agent import run

router = APIRouter()


class QueryRequest(BaseModel):
    question: str
    session_id: str = "default"
    skip_insight: bool = False


@router.post("/query")
async def nl_to_sql(req: QueryRequest):
    try:
        return run(req.question, req.session_id, req.skip_insight)
    except Exception as e:
        return {
            "insight": f"⚠️ Backend error: {str(e)}",
            "sql": "", "reasoning": "", "rows": [],
            "confidence": "LOW", "error": str(e)
        }
