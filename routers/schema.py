# routers/schema.py
from fastapi import APIRouter
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from core.supabase_client import get_schema, get_column_sample

router = APIRouter()


@router.get("/schema")
async def get_full_schema():
    """Returns all tables and columns — used by the visual dashboard column picker."""
    tables = get_schema()
    return {"tables": tables, "table_count": len(tables)}


@router.get("/schema/{table}/sample")
async def get_sample(table: str, columns: str = "", limit: int = 50):
    """Returns sample rows for preview in column picker."""
    cols = columns.split(",") if columns else []
    result = get_column_sample(table, cols, limit)
    return result
