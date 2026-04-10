from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Optional, Any
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from core.supabase_client import get_multi_table_data, get_column_sample
from core.viz_recommender import recommend
from core.sql_generator import recommend_visualization

router = APIRouter()

class ColumnSelection(BaseModel):
    table: str
    columns: List[str]

class VisualizeRequest(BaseModel):
    selections: List[ColumnSelection]
    limit: int = 500
    rows: Optional[List[Any]] = None  # pre-fetched rows from /api/query

class RecommendRequest(BaseModel):
    columns: List[dict]
    sample_data: Optional[List[dict]] = None

@router.post("/visualize")
async def visualize_data(req: VisualizeRequest):
    # If rows are passed directly, skip DB fetch
    if req.rows:
        rows = req.rows
        all_cols = []
        if rows:
            for k, v in rows[0].items():
                dtype = "numeric" if isinstance(v, (int, float)) else "date" if "date" in k.lower() or "time" in k.lower() else "text"
                all_cols.append({"name": k, "dtype": dtype})
        rec = recommend(all_cols, rows[:100])
        return {
            "rows": rows, "count": len(rows),
            "recommendation": rec, "columns": all_cols,
            "success": True, "error": None,
        }

    selections_dicts = [s.dict() for s in req.selections]
    result = get_multi_table_data(selections_dicts, req.limit)

    all_cols = []
    for sel in req.selections:
        for col in sel.columns:
            all_cols.append({"name": f"{sel.table}__{col}", "dtype": "unknown"})

    rec = recommend(all_cols, result.get("rows", [])[:100])

    return {
        "rows": result.get("rows", []),
        "count": result.get("count", 0),
        "recommendation": rec,
        "columns": all_cols,
        "success": result.get("success", False),
        "error": result.get("error"),
    }

@router.post("/recommend")
async def get_recommendation(req: RecommendRequest):
    rule_rec = recommend(req.columns, req.sample_data or [])
    llm_rec = None
    if req.sample_data:
        llm_rec = recommend_visualization(req.columns, req.sample_data)
    return {"rule_based": rule_rec, "llm_based": llm_rec, "best": rule_rec["best"]}
