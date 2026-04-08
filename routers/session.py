# routers/session.py
from fastapi import APIRouter
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from core.session_store import get, clear, list_sessions

router = APIRouter()


@router.get("/session/{session_id}")
async def get_session_history(session_id: str):
    s = get(session_id)
    return {
        "session_id": session_id,
        "history": s["history"],
        "turn_count": len(s["history"]),
    }


@router.delete("/session/{session_id}")
async def clear_session(session_id: str):
    clear(session_id)
    return {"cleared": session_id}


@router.get("/sessions")
async def list_all_sessions():
    return {"sessions": list_sessions()}
