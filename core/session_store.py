# core/session_store.py
from collections import defaultdict
from datetime import datetime

_sessions = defaultdict(lambda: {
    "history": [],
    "last_sql": None,
    "last_result": None,
    "last_context": {},   # structured: {metric, dimension, time_filter, filters}
    "created_at": datetime.now().isoformat(),
})


def get(session_id: str) -> dict:
    return _sessions[session_id]


def update(session_id: str, query: str, sql: str, result: list,
           reasoning: str = "", chunks_used: list = None,
           structured_context: dict = None):
    s = _sessions[session_id]
    s["history"].append({
        "ts": datetime.now().isoformat(),
        "query": query,
        "sql": sql,
        "reasoning": reasoning,
        "chunks_used": chunks_used or [],
        "row_count": len(result),
        "context": structured_context or {},
    })
    s["last_sql"] = sql
    s["last_result"] = result[:5]
    if structured_context:
        s["last_context"] = structured_context
    if len(s["history"]) > 20:
        s["history"] = s["history"][-20:]


def clear(session_id: str):
    _sessions[session_id] = {
        "history": [],
        "last_sql": None,
        "last_result": None,
        "last_context": {},
        "created_at": datetime.now().isoformat(),
    }


def list_sessions() -> list:
    return [
        {"id": k, "turns": len(v["history"]), "created_at": v["created_at"]}
        for k, v in _sessions.items()
    ]


def validate_sql(sql: str, chunks_used: list = None) -> dict:
    """Hard-rule validation layer after LLM generation."""
    errors = []
    warnings = []

    if not sql or not sql.strip():
        return {"valid": False, "errors": ["No SQL was generated"], "warnings": []}

    sql_lower = sql.lower()

    if "select *" in sql_lower:
        errors.append("SELECT * is not allowed — select explicit columns")

    if "refund" in sql_lower and "return" not in sql_lower:
        errors.append("Refunds must be joined through returns table")

    if "seller" in sql_lower and "order_item" not in sql_lower and "join sellers" in sql_lower:
        errors.append("Sellers must be joined via order_items, not directly from orders")

    if "warehouse" in sql_lower and "shipment" not in sql_lower:
        errors.append("Warehouses must be reached via shipments")

    if "settlement" in sql_lower and "order_item" not in sql_lower:
        errors.append("Settlements must be joined through order_items")

    if not any(kw in sql_lower for kw in ["join", "from"]):
        errors.append("Query must reference at least one table")

    # Warn about missing aliases
    for table, alias in [("orders", " o "), ("order_items", " oi "), ("products", " p ")]:
        if table in sql_lower and alias not in sql_lower:
            warnings.append(f"Missing alias for {table} table — use {alias.strip()}")

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
    }
